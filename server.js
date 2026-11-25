const {
    default: makeWASocket,
    useMultiFileAuthState,
    makeCacheableSignalKeyStore,
    DisconnectReason,
    fetchLatestWaWebVersion,
} = require("@whiskeysockets/baileys");
const P = require("pino");
const qrcode = require("qrcode-terminal");
const express = require("express");
const multer = require("multer");
const { startNatsConsumer } = require("./nats_consumer");

const PORT = process.env.PORT || 3001;
const GROUP_ID = process.env.GROUP_ID;

// Validate ENV
if (!GROUP_ID) {
    console.error("❌ GROUP_ID missing in environment");
    process.exit(1);
}

// Configure Multer to keep files strictly in system memory (RAM)
// This protects your home server mini PC's NVMe drive write limits
const storage = multer.memoryStorage();
const upload = multer(storage);

// =========================
// GLOBAL SOCKET
// =========================
let sock;

// Tracks live WhatsApp connection state for the /health endpoint.
let connectionState = {
    status: "connecting",        // "connecting" | "open" | "close"
    lastConnectedAt: null,
    lastDisconnectedAt: null,
    lastError: null,
    reconnectAttempts: 0,
};

// Set once the NATS subscriber is bound — prevents re-binding on Baileys reconnects.
let natsStarted = false;

// Helper function to resolve target WhatsApp address routing
function getTargetJid(to) {
    if (to) {
        let cleanTo = to.toString().replace(/[\s\-+]/g, "");
        if (!cleanTo.endsWith("@s.whatsapp.net") && !cleanTo.endsWith("@g.us")) {
            return `${cleanTo}@s.whatsapp.net`;
        }
        return cleanTo;
    }
    return GROUP_ID; // Fallback to default group configuration
}

// =========================
// HUMAN-LIKE SEND HELPERS
// =========================
// Baileys is configured with defaultQueryTimeoutMs: 0 (no timeout) for the
// main socket, so a presence query (sendPresenceUpdate) that never gets a
// response from WhatsApp would otherwise hang forever — and since sends
// happen one at a time off a queue, a single stuck presence call would
// silently block every message behind it, with no error logged. This
// wrapper guarantees the call resolves (or gives up) within
// PRESENCE_TIMEOUT_MS no matter what Baileys/WhatsApp does.
const PRESENCE_TIMEOUT_MS = 4000;

function withTimeout(promise, ms) {
    let timer;
    const timeout = new Promise((resolve) => {
        timer = setTimeout(resolve, ms); // resolve (not reject) — a timed-out
    });                                   // presence call is fine to ignore
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Simulates the "typing…" indicator before a text send, with a duration
// loosely based on message length so short and long messages don't take
// the same amount of time to "type". Jitter avoids an identical delay
// pattern on every send.
function typingDelayMs(text) {
    const words = text.trim().split(/\s+/).filter(Boolean).length || 1;
    const wpm = 35 + Math.random() * 25;              // vary "typing speed" 35-60 wpm per message
    const baseMs = (words / wpm) * 60_000;
    const jitter = 300 + Math.random() * 700;
    return Math.min(Math.max(baseMs + jitter, 800), 6000); // clamp to 0.8s - 6s
}

async function sendTextWithTyping(target, text) {
    try {
        await withTimeout(sock.sendPresenceUpdate("composing", target), PRESENCE_TIMEOUT_MS);
    } catch (_) { /* non-fatal, proceed to send regardless */ }

    await new Promise((r) => setTimeout(r, typingDelayMs(text)));

    await sock.sendMessage(target, { text });
}

// Media has no real WhatsApp-UI equivalent of "typing" for documents, so
// this just gives a short human-plausible pause (e.g. "picking a file")
// before the upload lands, rather than an instant send.
function mediaDelayMs() {
    const jitter = 800 + Math.random() * 1200;
    return jitter; // ~0.8s - 2s
}

async function sendMediaWithPause(target, buf, mimetype, filename) {
    try {
        await withTimeout(sock.sendPresenceUpdate("composing", target), PRESENCE_TIMEOUT_MS);
    } catch (_) { /* non-fatal */ }

    await new Promise((r) => setTimeout(r, mediaDelayMs()));

    await sock.sendMessage(target, {
        document: buf,
        mimetype,
        fileName: filename,
    });
}

// =========================
// EXPRESS SERVER (START ONCE)
// =========================
const app = express();
app.use(express.json());

app.get("/", (req, res) => {
    res.send("WhatsApp Bot Running ✅");
});

app.get("/health", (req, res) => {
    const healthy = connectionState.status === "open" && !!sock;
    res.status(healthy ? 200 : 503).json({
        status: healthy ? "healthy" : "unhealthy",
        whatsapp: connectionState.status,
        lastConnectedAt: connectionState.lastConnectedAt,
        lastDisconnectedAt: connectionState.lastDisconnectedAt,
        lastError: connectionState.lastError,
        reconnectAttempts: connectionState.reconnectAttempts,
        uptimeSeconds: Math.floor(process.uptime()),
    });
});

app.post("/send", async (req, res) => {
    const { message, to } = req.body;

    if (!message) {
        return res.status(400).json({ error: "message is required" });
    }

    if (!sock) {
        return res.status(500).json({ error: "WhatsApp not connected" });
    }

    // Determine target JID routing architecture
    let target;
    if (to) {
        // Normalize: remove spaces, dashes, or plus signs if any exist
        let cleanTo = to.toString().replace(/[\s\-+]/g, "");

        // If it's a phone number without any suffix, append the individual chat domain
        if (!cleanTo.endsWith("@s.whatsapp.net") && !cleanTo.endsWith("@g.us")) {
            target = `${cleanTo}@s.whatsapp.net`;
        } else {
            target = cleanTo;
        }
    } else {
        // Default fallback option straight to group configuration
        target = GROUP_ID;
    }

    try {
        await sendTextWithTyping(target, message);

        console.log("📤 Sent:", message);
        console.log("To:", target);

        res.json({ status: "sent", to: target });
    } catch (err) {
        console.error("Send error:", err);
        res.status(500).json({ error: "failed to send" });
    }
});

// ROUTE 2: New Attachment Endpoint (Processes Multi-part Streams)
app.post("/media", upload.single("file"), async (req, res) => {
    if (!sock) {
        return res.status(500).json({ error: "WhatsApp not connected" });
    }
    if (!req.file) {
        return res.status(400).json({ error: "No file payload detected in the request frame" });
    }

    const target = getTargetJid(req.body.to);

    try {
        console.log(`📥 Received document attachment internally: ${req.file.originalname}`);

        // Broadcast file media buffer smoothly using native Baileys protocol options
        await sendMediaWithPause(target, req.file.buffer, req.file.mimetype, req.file.originalname);

        console.log(`✅ Attachment [${req.file.originalname}] pushed successfully to ${target}`);
        res.json({ status: "media_sent", filename: req.file.originalname, to: target });
    } catch (err) {
        console.error("Failed to compile or relay media packet structure over WebSocket:", err);
        res.status(500).json({ error: "failed to route document attachment" });
    }
});

// Start server ONLY once
app.listen(PORT, () => {
    console.log(`🚀 API running on http://localhost:${PORT}`);
});

// =========================
// NATS INGRESS (separate from the HTTP routes)
// =========================
async function sendTextViaNats(to, text) {
    if (connectionState.status !== "open" || !sock) {
        throw new Error("WhatsApp not connected");
    }
    const target = getTargetJid(to);
    await sendTextWithTyping(target, text);
    console.log("📤 [nats] Sent:", text);
    console.log("To:", target);
}

async function sendMediaViaNats(to, buf, mimetype, filename) {
    if (connectionState.status !== "open" || !sock) {
        throw new Error("WhatsApp not connected");
    }
    const target = getTargetJid(to);
    await sendMediaWithPause(target, buf, mimetype, filename);
    console.log(`📤 [nats] Media sent: ${filename} (${buf.length} bytes)`);
    console.log("To:", target);
}

// =========================
// WHATSAPP BOT
// =========================
const logger = P({ level: "error" }); // Suppresses verbose packet telemetry logs

let isStarting = false;
let shuttingDown = false;
let reconnectTimer = null;
let saveCredsFn = null;

// Resolve the WA Web version once and reuse it. Re-fetching on every
// reconnect adds a network round trip to each retry, and when the fetch
// fails Baileys hands back a stale bundled version (seen in the logs as
// 2.3000.1043857760 during the DNS outage).
let cachedVersion = null;
async function resolveVersion() {
    if (cachedVersion) return cachedVersion;
    try {
        const latest = await fetchLatestWaWebVersion();
        if (latest?.version && latest.isLatest !== false && !latest.error) {
            cachedVersion = latest.version;
            console.log(`🌐 Resolved WhatsApp Web Version: ${cachedVersion.join(".")}`);
            return cachedVersion;
        }
    } catch (_) { /* fall through */ }
    const fallback = [2, 3000, 1017578434];
    console.log(`⚠️ Could not fetch remote version, using fallback ${fallback.join(".")} for this attempt`);
    return fallback;
}

// Exponential backoff: 3s, 6s, 12s ... capped at 5 min, with jitter. A flat
// 3s retry hammers WhatsApp during outages (DNS down, 503s) and rapid
// reconnect bursts are a known trigger for WhatsApp unlinking the device.
function backoffMs(attempt) {
    const base = Math.min(3000 * 2 ** Math.max(attempt - 1, 0), 5 * 60_000);
    return base + Math.floor(Math.random() * 1000);
}

function scheduleReconnect() {
    if (shuttingDown || reconnectTimer) return;
    connectionState.reconnectAttempts += 1;
    const delay = backoffMs(connectionState.reconnectAttempts);
    console.log(`🔁 Reconnecting in ${Math.round(delay / 1000)}s (attempt ${connectionState.reconnectAttempts})`);
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        startBot().catch((err) => {
            console.error("❌ startBot failed:", err);
            isStarting = false;
            scheduleReconnect();
        });
    }, delay);
}

// Tear down the previous socket fully before opening a new one. Without this
// the old socket's listeners (incl. creds.update) stay attached, and two
// sockets can end up writing the same auth/session files — that is what
// corrupts Signal sessions ("Bad MAC") and eventually gets the device logged out.
function disposeSocket(old) {
    if (!old) return;
    try { old.ev.removeAllListeners(); } catch (_) {}
    try { old.end(undefined); } catch (_) {}
}

async function startBot() {
    if (isStarting || shuttingDown) return;
    isStarting = true;

    // Persist session tokens securely in the configured auth folder space
    const { state, saveCreds } = await useMultiFileAuthState("auth_info_baileys");
    saveCredsFn = saveCreds;

    const version = await resolveVersion();

    disposeSocket(sock);
    sock = undefined;

    const thisSock = makeWASocket({
        version,
        auth: {
            creds: state.creds,
            // In-memory cache in front of the file key store: avoids racing
            // reads/writes of pre-key & session files, a common Bad MAC cause.
            keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        logger,
        mobile: false,
        browser: ["Ubuntu", "Chrome", "20.0.04"],
        defaultQueryTimeoutMs: 0,   // Prevent 408 Time-out issues during high data synchronization
        syncFullHistory: false,     // Skip heavy historical data downloads on startup
        markOnlineOnConnect: false, // Don't steal "online" from the phone (keeps phone notifications working)
    });
    sock = thisSock;

    thisSock.ev.on("creds.update", saveCreds);

    thisSock.ev.on("connection.update", (update) => {
        // Ignore late events from a socket we've already replaced.
        if (thisSock !== sock) return;

        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log("\n📱 Scan the QR Code below to link your server:");
            qrcode.generate(qr, { small: true });
        }

        if (connection) {
            connectionState.status = connection;
        }

        if (connection === "open") {
            console.log("✅ Success! Connected to WhatsApp Core Web Gateway Engine.");
            isStarting = false;
            connectionState.lastConnectedAt = new Date().toISOString();
            connectionState.lastError = null;
            connectionState.reconnectAttempts = 0;

            if (!natsStarted) {
                natsStarted = true;
                startNatsConsumer({
                    sendText: sendTextViaNats,
                    sendMedia: sendMediaViaNats,
                    isReady: () => connectionState.status === "open" && !!sock,
                }).catch((err) => {
                    console.error("❌ NATS consumer crashed:", err);
                    // Allow a later reconnect to retry binding NATS (e.g. NATS
                    // container wasn't up yet after a power cut).
                    natsStarted = false;
                });
            }
        }

        if (connection === "close") {
            isStarting = false;
            connectionState.lastDisconnectedAt = new Date().toISOString();
            connectionState.lastError = lastDisconnect?.error?.message || null;

            const err = lastDisconnect?.error;
            const statusCode = err?.output?.statusCode;
            const reasonName = Object.entries(DisconnectReason)
                .find(([, v]) => v === statusCode)?.[0] || "unknown";
            connectionState.lastError = `${reasonName} (${statusCode}): ${err?.message || ""}`;
            console.log("❌ Connection closed:", {
                statusCode,
                reasonName,
                message: err?.message,
                data: err?.data,                 // for stream conflicts, includes type e.g. device_removed / replaced
            });

            if (shuttingDown) return;

            if (statusCode === DisconnectReason.loggedOut) {
                console.error("🔒 Logged out by WhatsApp (401). Delete the auth folder and re-scan the QR. Exiting.");
                process.exit(0);
            }

            if (statusCode === DisconnectReason.connectionReplaced) {
                // 440: another client logged in with the SAME auth folder.
                // Reconnecting here makes the two instances kick each other
                // forever and gets the device unlinked. Stop instead.
                console.error("⚠️ Connection replaced (440): another instance is using this session. Stop the other one; not reconnecting.");
                return;
            }

            if (statusCode === DisconnectReason.badSession) {
                console.error("⚠️ Bad session (500) reported by WhatsApp — if this repeats, re-link the device.");
            }

            scheduleReconnect();
        }
    });
}

// =========================
// GRACEFUL SHUTDOWN
// =========================
// Flush credentials and close the socket (never logout()) so a power-cut
// shutdown doesn't leave half-written auth files behind.
async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`${signal} received, closing WhatsApp socket cleanly`);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    const force = setTimeout(() => process.exit(0), 8000);
    try {
        if (saveCredsFn) await saveCredsFn();
    } catch (err) {
        console.error("Failed to flush creds on shutdown:", err);
    }
    try { sock?.end(undefined); } catch (_) {}
    clearTimeout(force);
    process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// Start bot
startBot().catch((err) => {
    console.error("❌ Initial startBot failed:", err);
    isStarting = false;
    scheduleReconnect();
});
