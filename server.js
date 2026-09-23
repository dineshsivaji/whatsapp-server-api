const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestWaWebVersion } = require("@whiskeysockets/baileys");
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
        await sock.presenceSubscribe(target);
    } catch (_) {
        // Some chat types (e.g. groups you're not subscribed to yet) can
        // reject this — it's cosmetic, so ignore and carry on.
    }
    try {
        await sock.sendPresenceUpdate("composing", target);
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
        await sock.sendPresenceUpdate("composing", target);
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
let isStarting = false;

async function startBot() {
    if (isStarting) return;
    isStarting = true;

    // Persist session tokens securely in the configured auth folder space
    const { state, saveCreds } = await useMultiFileAuthState("auth_info_baileys");

    // Fetch or define a valid version array to pass the noise gate
    let version = [2, 3000, 1017578434];
    try {
        const latest = await fetchLatestWaWebVersion(); // Modern dynamic fetcher helper
        if (latest && latest.version) {
            version = latest.version;
            console.log(`🌐 Dynamically resolved latest WhatsApp Web Version: ${version.join('.')}`);
        }
    } catch (err) {
        console.log(`⚠️ Could not fetch remote version, falling back to static override: ${version.join('.')}`);
    }

    sock = makeWASocket({
        version: version,
        auth: state,
        logger: P({ level: "error" }), // Suppresses verbose packet telemetry logs
        mobile: false,
        browser: ["Ubuntu", "Chrome", "20.0.04"],
        defaultQueryTimeoutMs: 0,   // Prevent 408 Time-out issues during high data synchronization
        syncFullHistory: false,     // Skip heavy historical data downloads on startup
    });

    sock.ev.on("connection.update", (update) => {
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
                });
            }
        }

        if (connection === "close") {
            isStarting = false;
            connectionState.lastDisconnectedAt = new Date().toISOString();
            connectionState.lastError = lastDisconnect?.error?.message || null;

            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

            console.log("❌ Connection Closed. Reconnect Target Status:", shouldReconnect);

            if (shouldReconnect) {
                connectionState.reconnectAttempts += 1;
                setTimeout(startBot, 3000); // Safe delay step execution layout
            } else {
                console.error("🔒 Session logged out or permanently invalidated. Killing process to prevent rapid restart loops.");
                process.exit(0); // Exits cleanly so systemd / PM2 doesn't log cycle infinitely
            }
        }
    });

    sock.ev.on("creds.update", saveCreds);
}

// Start bot
startBot();
