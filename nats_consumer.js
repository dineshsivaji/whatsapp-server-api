// nats_consumer.js
//
// NATS JetStream pull-consumers that live inside the WhatsApp server
// process. Loosely coupled: it does NOT import Baileys. The host server
// injects the send functions — sendText(to, text), sendMedia(to, buf,
// mimetype, filename) — and isReady(), so this module stays testable and
// easy to copy-adapt for future channels (Discord, SMS, …).
//
// Two subjects → two durable consumers (created by nats/setup-streams.sh):
//   notify.whatsapp        → consumer "whatsapp-bridge"  (text)
//       payload: JSON { "to": "<jid|group|"">", "text": "<message>" }
//   notify.whatsapp.media  → consumer "whatsapp-media"   (binary)
//       body:    raw file bytes
//       headers: To, X-Filename, X-Mimetype  (+ optional Nats-Msg-Id)
//
// Semantics per message (both consumers):
//   - bad / invalid payload                 → term()  (don't retry)
//   - !isReady() (sock not connected yet)   → nak(30s)
//   - send resolves                         → ack()
//   - send rejects                          → nak(backoff)
//
// Crash safety: JetStream holds the message until ack(); a process
// crash mid-delivery just means redelivery on next start.

const { connect } = require("nats");

const NATS_URL            = process.env.NATS_URL            || "nats://127.0.0.1:4222";
const NATS_STREAM         = process.env.NATS_STREAM         || "NOTIFY";
const NATS_CONSUMER       = process.env.NATS_CONSUMER       || "whatsapp-bridge";
const NATS_MEDIA_CONSUMER = process.env.NATS_MEDIA_CONSUMER || "whatsapp-media";

// Backoff used when WhatsApp is up but a send fails. Indexed by JetStream's
// deliveryCount (1-based). After the last entry we reuse it; the consumer's
// max-deliver caps total attempts.
const NAK_DELAYS_MS = [
    5_000,
    15_000,
    30_000,
    60_000,
    5  * 60_000,
    10 * 60_000,
    30 * 60_000,
];

const NOT_READY_NAK_MS = 30_000;

// consume() pre-fetch budget. Small because we process sequentially and a
// slow send shouldn't hold lots of messages in ack-pending state.
const MAX_MESSAGES = 10;
const EXPIRES_MS   = 30_000;

function nakDelay(deliveryCount) {
    const i = Math.min(Math.max(deliveryCount - 1, 0), NAK_DELAYS_MS.length - 1);
    return NAK_DELAYS_MS[i];
}

function msgIdOf(msg) {
    return (msg.headers && typeof msg.headers.get === "function" && msg.headers.get("Nats-Msg-Id"))
        || `seq-${msg.seq}`;
}

async function startNatsConsumer({ sendText, sendMedia, isReady, log = console }) {
    if (typeof sendText !== "function") {
        throw new Error("startNatsConsumer: sendText(to, text) is required");
    }
    if (typeof sendMedia !== "function") {
        throw new Error("startNatsConsumer: sendMedia(to, buf, mimetype, filename) is required");
    }
    if (typeof isReady !== "function") {
        throw new Error("startNatsConsumer: isReady() is required");
    }

    const nc = await connect({
        servers: NATS_URL,
        name: "whatsapp-api",
        reconnect: true,
        maxReconnectAttempts: -1,
        reconnectTimeWait: 2_000,
    });
    log.info(`[nats] connected to ${NATS_URL}`);

    const js = nc.jetstream();

    // js.consumers.get(...) binds to the durable consumers created by
    // nats/setup-streams.sh. consume() returns an async iterable that keeps
    // the pull loop alive across reconnects and pulls in the background.
    const textConsumer  = await js.consumers.get(NATS_STREAM, NATS_CONSUMER);
    const mediaConsumer = await js.consumers.get(NATS_STREAM, NATS_MEDIA_CONSUMER);
    log.info(`[nats] bound ${NATS_STREAM}/${NATS_CONSUMER} (text) + ${NATS_STREAM}/${NATS_MEDIA_CONSUMER} (media)`);

    // Run both loops concurrently; neither resolves under normal operation.
    await Promise.all([
        runLoop(textConsumer,  (msg) => handleText(msg,  { sendText,  isReady, log }), log),
        runLoop(mediaConsumer, (msg) => handleMedia(msg, { sendMedia, isReady, log }), log),
    ]);

    log.warn("[nats] consume loops ended");
}

async function runLoop(consumer, handler, log) {
    const messages = await consumer.consume({ max_messages: MAX_MESSAGES, expires: EXPIRES_MS });
    for await (const msg of messages) {
        try {
            await handler(msg);
        } catch (err) {
            // Defensive: handlers shouldn't throw, but if one does, leave the
            // message unacked so JetStream redelivers rather than killing the loop.
            log.error(`[nats] unhandled error in handler: ${err.message || err}`);
            try { msg.nak(30_000); } catch (_) { /* already settled */ }
        }
    }
}

async function handleText(msg, { sendText, isReady, log }) {
    const delivered = msg.info.deliveryCount;
    const msgId = msgIdOf(msg);

    let payload;
    try {
        const decoded = JSON.parse(msg.string());
        if (!decoded || typeof decoded !== "object"
            || typeof decoded.to !== "string"
            || typeof decoded.text !== "string") {
            throw new Error("payload missing required keys (to, text)");
        }
        payload = decoded;
    } catch (err) {
        log.error(`[nats] bad text payload msg_id=${msgId} delivered=${delivered}: ${err.message}`);
        msg.term();
        return;
    }

    if (!isReady()) {
        log.warn(`[nats] sock not ready (text) msg_id=${msgId} delivered=${delivered} — nak ${NOT_READY_NAK_MS}ms`);
        msg.nak(NOT_READY_NAK_MS);
        return;
    }

    try {
        await sendText(payload.to, payload.text);
        log.info(`[nats] text delivered msg_id=${msgId} delivered=${delivered}`);
        msg.ack();
    } catch (err) {
        const delay = nakDelay(delivered);
        log.warn(`[nats] sendText threw msg_id=${msgId} delivered=${delivered}: ${err.message || err} — nak ${delay}ms`);
        msg.nak(delay);
    }
}

async function handleMedia(msg, { sendMedia, isReady, log }) {
    const delivered = msg.info.deliveryCount;
    const msgId = msgIdOf(msg);

    const h = msg.headers;
    const to       = (h && h.get("To")) || "";
    const filename = (h && h.get("X-Filename")) || "attachment";
    const mimetype = (h && h.get("X-Mimetype")) || "application/octet-stream";
    const buf = Buffer.from(msg.data); // raw bytes, no base64

    if (buf.length === 0) {
        log.error(`[nats] empty media body msg_id=${msgId} delivered=${delivered} — term`);
        msg.term();
        return;
    }

    if (!isReady()) {
        log.warn(`[nats] sock not ready (media) msg_id=${msgId} delivered=${delivered} — nak ${NOT_READY_NAK_MS}ms`);
        msg.nak(NOT_READY_NAK_MS);
        return;
    }

    try {
        await sendMedia(to, buf, mimetype, filename);
        log.info(`[nats] media delivered msg_id=${msgId} file=${filename} bytes=${buf.length} delivered=${delivered}`);
        msg.ack();
    } catch (err) {
        const delay = nakDelay(delivered);
        log.warn(`[nats] sendMedia threw msg_id=${msgId} file=${filename} delivered=${delivered}: ${err.message || err} — nak ${delay}ms`);
        msg.nak(delay);
    }
}

module.exports = { startNatsConsumer };
