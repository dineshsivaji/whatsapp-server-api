// nats_consumer.js
//
// NATS JetStream pull-consumer that lives inside the WhatsApp server
// process. Loosely coupled: it does NOT import Baileys. The host server
// passes in two small functions — sendMessage(to, text) and isReady() —
// so this module is testable and easy to copy-adapt for future channels
// (Discord, SMS, …).
//
// Wire protocol on `notify.whatsapp`:
//   { "to": "<jid or group id>", "text": "<message>" }
//
// Semantics per message:
//   - bad JSON / missing keys              → term()  (don't retry)
//   - !isReady() (sock not connected yet)  → nak(30s)
//   - sendMessage resolves                 → ack()
//   - sendMessage rejects                  → nak(backoff)
//
// Crash safety: JetStream holds the message until ack(); a process
// crash mid-delivery just means redelivery on next start.

const { connect } = require("nats");

const NATS_URL      = process.env.NATS_URL      || "nats://127.0.0.1:4222";
const NATS_STREAM   = process.env.NATS_STREAM   || "NOTIFY";
const NATS_CONSUMER = process.env.NATS_CONSUMER || "whatsapp-bridge";

// Backoff used when WhatsApp is up but sendMessage fails. Indexed by
// JetStream's deliveryCount (1-based). After the last entry, we keep
// using the last value; the consumer's max-deliver caps total attempts.
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

// consume() pre-fetch budget. Small because we process sequentially and
// a slow send shouldn't hold lots of messages in ack-pending state.
const MAX_MESSAGES = 10;
const EXPIRES_MS   = 30_000;

function nakDelay(deliveryCount) {
    const i = Math.min(Math.max(deliveryCount - 1, 0), NAK_DELAYS_MS.length - 1);
    return NAK_DELAYS_MS[i];
}

async function startNatsConsumer({ sendMessage, isReady, log = console }) {
    if (typeof sendMessage !== "function") {
        throw new Error("startNatsConsumer: sendMessage(to, text) is required");
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

    // New nats.js consumer API. js.consumers.get(...) gives us a handle
    // to the durable consumer created by nats/setup-streams.sh. The
    // legacy js.pullSubscribe(...) path no longer exposes .fetch() in
    // current nats.js, so we use consume() here — it's the supported
    // modern idiom and handles reconnection / heartbeats automatically.
    const js = nc.jetstream();
    const consumer = await js.consumers.get(NATS_STREAM, NATS_CONSUMER);
    log.info(`[nats] bound to ${NATS_STREAM}/${NATS_CONSUMER}`);

    // consume() returns an async iterable that yields messages
    // continuously. It keeps the pull loop alive across server
    // reconnects and pulls in the background; we just iterate.
    const messages = await consumer.consume({
        max_messages: MAX_MESSAGES,
        expires: EXPIRES_MS,
    });

    for await (const msg of messages) {
        try {
            await handle(msg, { sendMessage, isReady, log });
        } catch (err) {
            // Defensive: nothing in handle() should throw, but if it
            // does, leave the message unacked so JetStream redelivers
            // after ack-wait (30s) rather than killing the consumer.
            log.error(`[nats] unhandled error in handle(): ${err.message || err}`);
            try { msg.nak(30_000); } catch (_) { /* already settled */ }
        }
    }

    log.warn("[nats] consume iterator ended");
}

async function handle(msg, { sendMessage, isReady, log }) {
    const delivered = msg.info.deliveryCount;
    const msgId =
        (msg.headers && typeof msg.headers.get === "function" && msg.headers.get("Nats-Msg-Id"))
        || `seq-${msg.seq}`;

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
        log.error(`[nats] bad payload msg_id=${msgId} delivered=${delivered}: ${err.message}`);
        msg.term();
        return;
    }

    if (!isReady()) {
        log.warn(`[nats] sock not ready msg_id=${msgId} delivered=${delivered} — nak ${NOT_READY_NAK_MS}ms`);
        msg.nak(NOT_READY_NAK_MS);
        return;
    }

    try {
        await sendMessage(payload.to, payload.text);
        log.info(`[nats] delivered msg_id=${msgId} delivered=${delivered}`);
        msg.ack();
    } catch (err) {
        const delay = nakDelay(delivered);
        log.warn(`[nats] sendMessage threw msg_id=${msgId} delivered=${delivered}: ${err.message || err} — nak ${delay}ms`);
        msg.nak(delay);
    }
}

module.exports = { startNatsConsumer };
