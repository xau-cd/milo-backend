// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, SQLite, whatsapp-web.js (legacy), Evolution API (gateway WhatsApp),
//                BullMQ/Redis (file d'attente, optionnel), axios, Groq (Principal),
//                OpenRouter (Fallback), Nodemailer (fallback email), Gmail API (email OAuth)
// Format de réponse standardisé : { reply, images (optionnel), qrCode (optionnel), error }
// Version : 7.0.0 - Contexte par conversation_id, Function Calling email/WhatsApp réels,
//                    passerelle WhatsApp open-source, file d'attente résiliente
//
// ⚠️ NOTES DE MIGRATION IMPORTANTES :
// - Les routes historiques /api/whatsapp/connect et /api/whatsapp/send (whatsapp-web.js /
//   Puppeteer) sont CONSERVÉES pour compatibilité, mais sont désormais considérées "legacy".
// - Le nouveau chemin recommandé pour la prod est la passerelle REST (Evolution API ou
//   équivalent) pilotée via WHATSAPP_GATEWAY_URL / WHATSAPP_GATEWAY_API_KEY, avec les routes
//   /api/whatsapp/qr, /api/whatsapp/status et /api/whatsapp/webhook.
// - Les chemins REST de whatsappGateway suivent la convention Evolution API
//   (https://github.com/EvolutionAPI/evolution-api). Si tu utilises une autre passerelle,
//   adapte uniquement le bloc `whatsappGateway` ci-dessous.
// - Pour le Function Calling email, envoie le token OAuth Google dans le header
//   `Authorization: Bearer <access_token>` (ou `X-Google-Access-Token`) depuis le frontend.
//   Sans ce token, l'envoi retombe automatiquement sur le SMTP (Nodemailer) déjà configuré.
// - Pour la file d'attente WhatsApp résiliente, configure REDIS_URL et installe
//   les paquets `bullmq` et `ioredis` (npm install bullmq ioredis). Sans Redis, une file en
//   mémoire avec retry/backoff est utilisée automatiquement (non persistante entre redéploiements).

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const qrcode = require("qrcode");
const nodemailer = require("nodemailer");
const { Client, LocalAuth } = require("whatsapp-web.js");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

// ==================== CONFIGURATION GLOBALE ====================
const CONFIG = {
  PORT: process.env.PORT || 3000,
  ENV: process.env.NODE_ENV || "production",
  VERSION: "7.0.0",
  AGENT_NAME: "Luba",
  MAX_MESSAGE_LENGTH: 2000,
  MAX_HISTORY_LENGTH: 15,
  IMAGE_SEARCH_LIMIT: 6,
  WHATSAPP_QR_TIMEOUT: 30000,
  WHATSAPP_RETRY_DELAY: 3000,
  WHATSAPP_MAX_RETRIES: 2,
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions")
};

// ==================== VALIDATION DES VARIABLES D'ENVIRONNEMENT ====================
const requiredEnvVars = ["GROQ_API_KEY", "OPENROUTER_API_KEY"];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingEnvVars.length > 0) {
  console.error("=".repeat(60));
  console.error("❌ VARIABLES D'ENVIRONNEMENT OBLIGATOIRES MANQUANTES :");
  missingEnvVars.forEach((varName) => console.error(`   - ${varName}`));
  console.error("=".repeat(60));
  console.error("⚠️ Le service d'IA ne fonctionnera pas sans ces clés.");
}

if (!process.env.WHATSAPP_GATEWAY_URL) {
  console.warn("⚠️ WHATSAPP_GATEWAY_URL non configurée — la passerelle WhatsApp open-source (Evolution API) est désactivée.");
}
if (!process.env.WHATSAPP_WEBHOOK_SECRET) {
  console.warn("⚠️ WHATSAPP_WEBHOOK_SECRET non configuré — le webhook WhatsApp acceptera des requêtes NON authentifiées. À définir avant la mise en prod.");
}
if (!process.env.REDIS_URL) {
  console.warn("⚠️ REDIS_URL non configurée — file d'attente WhatsApp en mémoire (non persistante entre redémarrages).");
}

// ==================== CRÉATION DES DOSSIERS ====================
const dataDir = path.join(__dirname, "data");
const sessionsDir = path.join(__dirname, "sessions");

for (const dir of [dataDir, sessionsDir]) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Dossier créé: ${dir}`);
  }
}

// ==================== INITIALISATION SQLITE ====================
const db = new sqlite3.Database(CONFIG.DB_PATH, (err) => {
  if (err) {
    console.error("❌ Impossible d'ouvrir la base SQLite:", err.message);
    process.exit(1);
  }
});

db.run("PRAGMA journal_mode = WAL;");
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -32000;");
db.run("PRAGMA busy_timeout = 5000;");
db.run("PRAGMA temp_store = MEMORY;");
db.run("PRAGMA foreign_keys = ON;");

// NOTE SCHÉMA : la colonne `session_id` de `sessions` et `messages` est désormais
// utilisée comme `conversation_id` au niveau de l'API. Aucune migration de schéma
// n'est nécessaire : c'est la même colonne, juste une sémantique plus précise
// (une ligne = une conversation isolée, identifiée par conversation_id).
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    active_intent TEXT,
    intent_data TEXT
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    tool_calls TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at)`);

  db.run(`CREATE TABLE IF NOT EXISTS chat_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    message TEXT,
    response TEXT,
    system_action TEXT,
    llm_provider TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS whatsapp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    session_id TEXT UNIQUE,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

console.log("✅ Base de données SQLite initialisée");

// ==================== CONFIGURATION NODEMAILER (fallback SMTP) ====================
let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  emailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || "587", 10),
    secure: process.env.SMTP_PORT === "465",
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS
    },
    tls: { rejectUnauthorized: false },
    pool: true,
    maxConnections: 3,
    maxMessages: 50
  });

  emailTransporter.verify((err) => {
    if (err) console.error("⚠️ SMTP non joignable:", err.message);
    else console.log("✅ SMTP prêt (fallback email)");
  });
} else {
  console.warn("⚠️ SMTP non configuré — le fallback email sera désactivé (Gmail API OAuth reste possible).");
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// Nécessaire derrière le proxy inverse de Render pour que express-rate-limit
// et req.ip identifient correctement l'IP réelle du client.
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ==================== CORS ====================
const ALLOWED_ORIGINS = [
  "https://luba-ia.web.app",
  "https://luba-ia.firebaseapp.com",
  "https://milo-ead21.web.app", // legacy, conservé pour compatibilité
  "http://localhost:3000",
  "http://localhost:8080",
  "http://localhost:5173"
];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || ALLOWED_ORIGINS.includes(origin)) {
        callback(null, true);
      } else {
        console.warn(`⚠️ Origine CORS refusée: ${origin}`);
        callback(new Error("Origine non autorisée"));
      }
    },
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token"],
    credentials: true,
    maxAge: 86400
  })
);

app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));

// `verify` conserve le corps brut de la requête : indispensable pour valider la
// signature HMAC des webhooks WhatsApp (voir verifyWhatsappWebhookSignature).
app.use(
  express.json({
    limit: "10mb",
    verify: (req, res, buf) => {
      req.rawBody = buf;
    }
  })
);
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    return res.status(200).json({
      reply: "⚠️ Trop de requêtes. Veuillez réessayer dans 15 minutes.",
      error: true
    });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    return res.status(200).json({
      reply: "⚠️ Limite de requêtes atteinte.",
      error: true
    });
  }
});

// Limiteur dédié au webhook WhatsApp entrant (trafic potentiellement élevé, mais
// on ne veut pas bloquer la passerelle si elle retente un envoi).
const webhookLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: true, reply: "Trop de requêtes webhook." })
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const requestId = crypto.randomUUID();
  const start = Date.now();
  req.requestId = requestId;

  console.log(`\n📥 [${requestId}] ${req.method} ${req.url}`);
  if (req.body && Object.keys(req.body).length > 0) {
    console.log(`   Body: ${JSON.stringify(req.body).slice(0, 300)}`);
  }

  res.on("finish", () => {
    console.log(`📤 [${requestId}] ${res.statusCode} - ${Date.now() - start}ms`);
  });

  next();
});

// ==================== WRAPPER ASYNC POUR LES ROUTES ====================
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// ==================== HELPERS SQLITE PROMISIFIÉS ====================
function dbGet(query, params) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => (err ? reject(err) : resolve(row)));
  });
}

function dbAll(query, params) {
  return new Promise((resolve, reject) => {
    db.all(query, params, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}

function dbRun(query, params) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

// ==================== MIDDLEWARE D'AUTHENTIFICATION ====================
const authenticateUser = asyncHandler(async (req, res, next) => {
  const userId = req.body.userId || req.query.userId || req.headers["x-user-id"];

  if (!userId || typeof userId !== "string" || userId.trim().length === 0) {
    return res.status(200).json({
      reply: "⚠️ Authentification requise. Veuillez fournir un userId.",
      error: true
    });
  }

  req.userId = userId.trim();

  try {
    const user = await dbGet("SELECT * FROM users WHERE id = ?", [req.userId]);

    if (!user) {
      const safeDisplayName =
        typeof req.body.displayName === "string" ? req.body.displayName.slice(0, 120) : req.userId;
      await dbRun("INSERT INTO users (id, display_name) VALUES (?, ?)", [req.userId, safeDisplayName]);
    }

    next();
  } catch (err) {
    console.error("❌ Erreur authentification:", err.message);
    return res.status(200).json({ reply: "⚠️ Erreur interne.", error: true });
  }
});

// Extrait un token d'accès OAuth Google depuis l'en-tête Authorization (Bearer)
// ou depuis X-Google-Access-Token. Utilisé uniquement pour le Function Calling email.
function extractGoogleAccessToken(req) {
  const authHeader = req.headers.authorization || req.headers.Authorization;
  if (authHeader && authHeader.startsWith("Bearer ")) {
    return authHeader.slice(7).trim();
  }
  if (req.headers["x-google-access-token"]) {
    return String(req.headers["x-google-access-token"]).trim();
  }
  return null;
}

// ==================== SYSTEM PROMPT LUBA ====================
const LUBA_SYSTEM_PROMPT = {
  role: "system",
  content: `Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, une startup basée à Kinshasa, fondée en 2026.

IDENTITÉ (à respecter strictement) :
- Tu t'appelles Luba (ou Luba.ia). Tu ne t'appelles JAMAIS Milo, Milou, ou tout autre nom.
- Si on te demande qui t'a créée, tu réponds que tu es une IA développée par HIKLON Technology, startup à Kinshasa, fondée en 2026.
- Ton ton est chaleureux, intelligent et proactif.

RÈGLE STRICTE SUR LES IMAGES (OBLIGATOIRE, SANS EXCEPTION) :
- Dès que tu décris ou présentes une personnalité, un lieu, un objet, un concept scientifique ou un événement, tu DOIS obligatoirement utiliser l'outil search_images pour illustrer ta réponse.
- Dès que l'utilisateur te demande une recherche, une information, une actualité, ou pose une question de type "recherche"/"informe-toi"/"cherche" — que tu utilises search_web ou non — tu DOIS TOUJOURS ajouter au moins un appel à search_images en complément, pour illustrer le sujet de la recherche. N'attends JAMAIS que l'utilisateur te demande explicitement une photo : c'est systématique.
- Un toolCalls vide n'est autorisé que pour une réponse purement conversationnelle qui ne décrit ni sujet, ni lieu, ni recherche d'information (ex: salutation, remerciement, question sur toi-même).

FORMAT DE RÉPONSE OBLIGATOIRE :
Tu DOIS TOUJOURS répondre au format JSON strict :
{
  "replyText": "Ta réponse complète en Markdown",
  "toolCalls": [
    {
      "name": "search_images",
      "arguments": { "query": "sujet à illustrer" }
    }
  ]
}

Si aucun outil n'est nécessaire, utilise "toolCalls": [].

OUTILS DISPONIBLES :
- search_images : Rechercher des images (arguments: { query })
- search_web : Rechercher sur le web (arguments: { query })
- send_email : Envoyer un email réel (arguments: { recipient, subject, body })
- send_whatsapp_message : Envoyer un message WhatsApp réel (arguments: { phone_number, message })`
};

// ==================== CONFIGURATION LLM ====================
const LLM_PROVIDERS = {
  GROQ: {
    name: "groq",
    baseURL: "https://api.groq.com/openai/v1",
    model: process.env.GROQ_MODEL || "llama-3.3-70b-versatile",
    apiKey: process.env.GROQ_API_KEY,
    timeout: 30000,
    maxTokens: 1500,
    temperature: 0.7
  },
  OPENROUTER: {
    name: "openrouter",
    baseURL: "https://openrouter.ai/api/v1",
    model: process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat",
    apiKey: process.env.OPENROUTER_API_KEY,
    timeout: 45000,
    maxTokens: 1500,
    temperature: 0.7
  }
};

// ==================== GESTION DU CONTEXTE PAR CONVERSATION_ID ====================
// Chaque conversation (web, WhatsApp, etc.) est isolée par conversation_id. Le
// contexte chargé pour le LLM ne provient JAMAIS d'une autre conversation.

async function getSession(conversationId, userId) {
  const session = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);

  if (session) {
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
    return session;
  }

  await dbRun("INSERT INTO sessions (session_id, user_id) VALUES (?, ?)", [conversationId, userId]);
  return { session_id: conversationId, user_id: userId, active_intent: null, intent_data: null };
}

async function getHistory(conversationId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  const rows = await dbAll(
    "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
    [conversationId, limit]
  );
  return rows.reverse().map((row) => ({ role: row.role, content: row.content }));
}

async function saveMessage(conversationId, role, content) {
  await dbRun("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", [conversationId, role, content]);
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
}

async function setActiveIntent(conversationId, intentType, intentData = {}) {
  await dbRun("UPDATE sessions SET active_intent = ?, intent_data = ? WHERE session_id = ?", [
    intentType,
    JSON.stringify(intentData),
    conversationId
  ]);
}

async function getActiveIntent(conversationId) {
  const row = await dbGet("SELECT active_intent, intent_data FROM sessions WHERE session_id = ?", [conversationId]);
  if (!row || !row.active_intent) return null;

  try {
    return { type: row.active_intent, data: JSON.parse(row.intent_data || "{}") };
  } catch (e) {
    console.error("❌ intent_data corrompu, réinitialisation:", e.message);
    return null;
  }
}

async function clearActiveIntent(conversationId) {
  await dbRun("UPDATE sessions SET active_intent = NULL, intent_data = NULL WHERE session_id = ?", [conversationId]);
}

// Vérifie qu'une conversation appartient bien à l'utilisateur authentifié
// (empêche un utilisateur de lire/écrire le contexte d'une conversation d'un autre).
async function assertConversationOwnership(conversationId, userId) {
  const existing = await dbGet("SELECT user_id FROM sessions WHERE session_id = ?", [conversationId]);
  if (existing && existing.user_id && existing.user_id !== userId) {
    const err = new Error("Cette conversation n'appartient pas à cet utilisateur.");
    err.code = "CONVERSATION_OWNERSHIP";
    throw err;
  }
}

// ==================== OUTILS DE RECHERCHE ====================
async function searchWikimediaImages(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };

  try {
    console.log(`🖼️ Recherche d'images: "${query}"`);

    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;

    const response = await axios.get(url, { timeout: 15000 });
    const pages = response.data?.query?.pages;

    if (!pages) return { images: [] };

    const images = Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        thumbnail: page.imageinfo?.[0]?.thumburl || null,
        title: page.title || "Image",
        description:
          page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
        width: page.imageinfo?.[0]?.thumbwidth || null,
        height: page.imageinfo?.[0]?.thumbheight || null,
        pageUrl: page.imageinfo?.[0]?.descriptionurl || null
      }))
      .filter((img) => img.url);

    console.log(`   ${images.length} image(s) trouvée(s)`);
    return { images };
  } catch (error) {
    console.error("❌ Erreur recherche images:", error.message);
    return { images: [], error: error.message };
  }
}

async function searchWeb(query) {
  if (!query || typeof query !== "string") return { results: [] };

  try {
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1`;
    const response = await axios.get(ddgUrl, { timeout: 10000 });

    const results = [];
    if (response.data?.AbstractText) {
      results.push({
        title: response.data.Heading || "Résultat",
        snippet: response.data.AbstractText,
        url: response.data.AbstractURL || null
      });
    }

    return { results };
  } catch (error) {
    console.error("❌ Erreur recherche web:", error.message);
    return { results: [], error: error.message };
  }
}

// ==================== ENVOI D'EMAIL : GMAIL API (OAuth) + FALLBACK SMTP ====================

// Envoi réel via l'API Gmail, en utilisant le token OAuth Bearer transmis par le frontend.
async function sendEmailViaGmail(accessToken, recipient, subject, body) {
  const messageLines = [
    `To: ${recipient}`,
    `Subject: =?utf-8?B?${Buffer.from(subject || "(sans sujet)").toString("base64")}?=`,
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    "",
    body || ""
  ];
  const rawMessage = messageLines.join("\r\n");
  const encodedMessage = Buffer.from(rawMessage)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");

  const response = await axios.post(
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    { raw: encodedMessage },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      },
      timeout: 15000
    }
  );

  return { success: true, provider: "gmail", messageId: response.data?.id || null };
}

// Fallback SMTP (Nodemailer), utilisé si aucun token Google n'est fourni.
async function sendEmailViaSMTP(to, subject, body) {
  if (!emailTransporter) return { success: false, error: "SMTP non configuré" };

  try {
    const info = await emailTransporter.sendMail({
      from: process.env.EMAIL_FROM || `"Luba" <${process.env.SMTP_USER}>`,
      to,
      subject: subject || "(sans sujet)",
      html: `<div style="font-family: Arial; padding: 20px;">${String(body || "").replace(/\n/g, "<br>")}</div>`,
      text: body || ""
    });

    return { success: true, provider: "smtp", messageId: info.messageId };
  } catch (error) {
    console.error("❌ Erreur envoi email SMTP:", error.message);
    return { success: false, error: error.message };
  }
}

// Point d'entrée unique pour le Function Calling send_email : bascule automatiquement
// entre Gmail API (si un token OAuth est présent) et SMTP (sinon).
async function dispatchSendEmail({ googleAccessToken, recipient, subject, body }) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!recipient || !emailRegex.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  let result;
  if (googleAccessToken) {
    try {
      result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body);
    } catch (error) {
      const apiError = error.response?.data?.error?.message || error.message;
      console.error("❌ Erreur envoi email via Gmail API:", apiError);
      result = { success: false, error: `Gmail API: ${apiError}` };
    }
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  try {
    await dbRun("INSERT INTO email_logs (to_email, subject, status) VALUES (?, ?, ?)", [
      recipient,
      subject || null,
      result.success ? "sent" : "failed"
    ]);
  } catch (logErr) {
    console.error("⚠️ Impossible de journaliser l'email:", logErr.message);
  }

  return result;
}

// ==================== PASSERELLE WHATSAPP OPEN-SOURCE (Evolution API) ====================
// Client REST vers une instance Evolution API (ou compatible) auto-hébergée, ex. via Docker.
// ⚠️ Adapte les chemins ci-dessous si tu utilises une autre passerelle (Baileys-api, etc.).
const WHATSAPP_GATEWAY = {
  baseURL: (process.env.WHATSAPP_GATEWAY_URL || "").replace(/\/+$/, ""),
  apiKey: process.env.WHATSAPP_GATEWAY_API_KEY || "",
  instance: process.env.WHATSAPP_INSTANCE_NAME || "luba"
};

const whatsappGateway = {
  configured() {
    return Boolean(WHATSAPP_GATEWAY.baseURL && WHATSAPP_GATEWAY.apiKey);
  },

  async getQRCode() {
    if (!this.configured()) {
      throw new Error("Passerelle WhatsApp non configurée (WHATSAPP_GATEWAY_URL / WHATSAPP_GATEWAY_API_KEY manquants)");
    }
    const response = await axios.get(
      `${WHATSAPP_GATEWAY.baseURL}/instance/connect/${WHATSAPP_GATEWAY.instance}`,
      { headers: { apikey: WHATSAPP_GATEWAY.apiKey }, timeout: 15000 }
    );
    return response.data;
  },

  async getStatus() {
    if (!this.configured()) throw new Error("Passerelle WhatsApp non configurée");
    const response = await axios.get(
      `${WHATSAPP_GATEWAY.baseURL}/instance/connectionState/${WHATSAPP_GATEWAY.instance}`,
      { headers: { apikey: WHATSAPP_GATEWAY.apiKey }, timeout: 10000 }
    );
    return response.data;
  },

  async sendMessage(phoneNumber, message) {
    if (!this.configured()) throw new Error("Passerelle WhatsApp non configurée");

    const cleanNumber = String(phoneNumber).replace(/[^\d]/g, "");
    if (!cleanNumber) throw new Error("Numéro de téléphone invalide");

    const response = await axios.post(
      `${WHATSAPP_GATEWAY.baseURL}/message/sendText/${WHATSAPP_GATEWAY.instance}`,
      { number: cleanNumber, text: message },
      { headers: { apikey: WHATSAPP_GATEWAY.apiKey, "Content-Type": "application/json" }, timeout: 15000 }
    );
    return response.data;
  }
};

// ==================== FILE D'ATTENTE RÉSILIENTE (BullMQ+Redis, ou repli mémoire) ====================
let BullMQ = null;
let IORedis = null;
try {
  // eslint-disable-next-line global-require
  BullMQ = require("bullmq");
  // eslint-disable-next-line global-require
  IORedis = require("ioredis");
} catch (e) {
  BullMQ = null;
  IORedis = null;
}

const useRedisQueue = Boolean(process.env.REDIS_URL) && Boolean(BullMQ) && Boolean(IORedis);
let whatsappQueue = null;
let whatsappWorker = null;

// File de repli en mémoire, avec concurrence limitée et retry en backoff exponentiel.
// Utilisée automatiquement si Redis/BullMQ ne sont pas disponibles.
class InMemoryRetryQueue {
  constructor(processFn, { concurrency = 2, maxAttempts = 5, baseDelayMs = 2000 } = {}) {
    this.processFn = processFn;
    this.concurrency = concurrency;
    this.maxAttempts = maxAttempts;
    this.baseDelayMs = baseDelayMs;
    this.pending = [];
    this.active = 0;
  }

  push(data) {
    this.pending.push({ data, attempts: 0 });
    this._drain();
  }

  _drain() {
    while (this.active < this.concurrency && this.pending.length > 0) {
      const job = this.pending.shift();
      this.active++;
      this._process(job).finally(() => {
        this.active--;
        this._drain();
      });
    }
  }

  async _process(job) {
    try {
      await this.processFn(job.data);
    } catch (err) {
      job.attempts++;
      console.error(`⚠️ Échec job WhatsApp (tentative ${job.attempts}/${this.maxAttempts}):`, err.message);
      if (job.attempts < this.maxAttempts) {
        const delay = this.baseDelayMs * Math.pow(2, job.attempts - 1);
        setTimeout(() => {
          this.pending.push(job);
          this._drain();
        }, delay);
      } else {
        console.error(`❌ Abandon définitif du job WhatsApp après ${this.maxAttempts} tentatives:`, JSON.stringify(job.data));
      }
    }
  }
}

let inMemoryWhatsappQueue = null;

if (useRedisQueue) {
  const connection = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });

  whatsappQueue = new BullMQ.Queue("whatsapp-outbound", { connection });
  whatsappWorker = new BullMQ.Worker(
    "whatsapp-outbound",
    async (job) => {
      const { phoneNumber, message } = job.data;
      await whatsappGateway.sendMessage(phoneNumber, message);
    },
    {
      connection,
      concurrency: 3,
      limiter: { max: 10, duration: 1000 } // limite de débit : 10 messages/seconde max
    }
  );

  whatsappWorker.on("failed", (job, err) => {
    console.error(`❌ Job WhatsApp BullMQ échoué (${job?.id}):`, err.message);
  });
  whatsappWorker.on("completed", (job) => {
    console.log(`✅ Job WhatsApp BullMQ envoyé (${job.id})`);
  });

  console.log("✅ File d'attente WhatsApp : BullMQ + Redis (persistante, avec retry)");
} else {
  inMemoryWhatsappQueue = new InMemoryRetryQueue(
    async ({ phoneNumber, message }) => {
      await whatsappGateway.sendMessage(phoneNumber, message);
    },
    { concurrency: 2, maxAttempts: 5, baseDelayMs: 2000 }
  );
  console.log("⚠️ File d'attente WhatsApp : en mémoire (repli, non persistante — configure REDIS_URL + bullmq/ioredis pour la version production complète)");
}

// Point d'entrée unique pour empiler un envoi WhatsApp sortant, quelle que soit la file utilisée.
async function enqueueWhatsAppSend(phoneNumber, message) {
  if (useRedisQueue) {
    await whatsappQueue.add(
      "send",
      { phoneNumber, message },
      { attempts: 5, backoff: { type: "exponential", delay: 2000 }, removeOnComplete: 100, removeOnFail: 500 }
    );
  } else {
    inMemoryWhatsappQueue.push({ phoneNumber, message });
  }
}

// Envoi WhatsApp "intelligent" : utilise la nouvelle passerelle (avec file d'attente)
// si elle est configurée, sinon retombe sur l'ancien whatsapp-web.js (legacy, direct, sans retry).
async function sendWhatsAppSmart(userId, phoneNumber, message) {
  if (whatsappGateway.configured()) {
    await enqueueWhatsAppSend(phoneNumber, message);
    return { success: true, queued: true, provider: "gateway" };
  }
  const result = await whatsappManager.sendMessage(userId, phoneNumber, message);
  return { ...result, provider: "legacy-whatsapp-web.js" };
}

// Validation de la signature du webhook WhatsApp entrant.
// Accepte soit une clé partagée dans le header `apikey`, soit une signature HMAC-SHA256
// (header `x-webhook-signature` ou `x-hub-signature-256`) calculée sur le corps brut.
function verifyWhatsappWebhookSignature(req) {
  const secret = process.env.WHATSAPP_WEBHOOK_SECRET;
  if (!secret) return true; // pas de secret configuré = validation désactivée (à éviter en prod)

  const providedApiKey = req.headers["apikey"];
  if (providedApiKey && providedApiKey === secret) return true;

  const signatureHeader = req.headers["x-webhook-signature"] || req.headers["x-hub-signature-256"];
  if (signatureHeader && req.rawBody) {
    const expected = crypto.createHmac("sha256", secret).update(req.rawBody).digest("hex");
    const provided = String(signatureHeader).replace(/^sha256=/, "");
    try {
      return crypto.timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(provided, "hex"));
    } catch (e) {
      return false;
    }
  }

  return false;
}

// ==================== GESTION WHATSAPP LEGACY (whatsapp-web.js / Puppeteer) ====================
// Conservée pour compatibilité ascendante avec /api/whatsapp/connect et /api/whatsapp/send.
class WhatsAppManager {
  constructor() {
    this.clients = new Map();
  }

  async initClient(userId, retryCount = 0) {
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.ready) return { connected: true, qrCode: null };
    }

    const client = new Client({
      authStrategy: new LocalAuth({ clientId: userId, dataPath: CONFIG.SESSIONS_PATH }),
      puppeteer: {
        headless: true,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-accelerated-2d-canvas",
          "--no-first-run",
          "--no-zygote",
          "--single-process",
          "--disable-gpu"
        ]
      }
    });

    const sessionData = { client, qrCode: null, ready: false };
    this.clients.set(userId, sessionData);

    client.on("qr", async (qr) => {
      try {
        const qrDataUrl = await qrcode.toDataURL(qr, {
          width: 600,
          margin: 2,
          color: { dark: "#000000", light: "#FFFFFF" }
        });
        sessionData.qrCode = qrDataUrl;
        console.log(`📱 QR Code (legacy) généré pour ${userId}`);
      } catch (error) {
        console.error("❌ Erreur génération QR (legacy):", error.message);
      }
    });

    client.on("ready", () => {
      sessionData.ready = true;
      sessionData.qrCode = null;
      db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
      console.log(`✅ WhatsApp (legacy) connecté pour ${userId}`);
    });

    client.on("auth_failure", (msg) => {
      console.error(`❌ Échec d'authentification WhatsApp (legacy) pour ${userId}:`, msg);
      sessionData.ready = false;
    });

    client.on("disconnected", (reason) => {
      sessionData.ready = false;
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
      console.warn(`⚠️ WhatsApp (legacy) déconnecté pour ${userId}: ${reason}`);
      this.clients.delete(userId);
    });

    try {
      await client.initialize();
      return { connected: false, qrCode: null };
    } catch (error) {
      if (retryCount < CONFIG.WHATSAPP_MAX_RETRIES) {
        this.clients.delete(userId);
        await new Promise((resolve) => setTimeout(resolve, CONFIG.WHATSAPP_RETRY_DELAY));
        return this.initClient(userId, retryCount + 1);
      }
      this.clients.delete(userId);
      throw error;
    }
  }

  async sendMessage(userId, to, message) {
    const session = this.clients.get(userId);
    if (!session || !session.ready) {
      const error = new Error("WhatsApp (legacy) non connecté");
      error.code = "WHATSAPP_NOT_CONNECTED";
      throw error;
    }

    let formattedTo = String(to).replace(/[^\d]/g, "");
    if (!formattedTo) {
      const error = new Error("Numéro de destinataire invalide");
      error.code = "INVALID_RECIPIENT";
      throw error;
    }
    if (!formattedTo.startsWith("55")) formattedTo = "55" + formattedTo;
    formattedTo += "@c.us";

    const chat = await session.client.getChatById(formattedTo);
    await session.client.sendMessage(chat.id._serialized, message);

    return { success: true, to };
  }

  getQRCode(userId) {
    return this.clients.get(userId)?.qrCode || null;
  }

  async destroyAll() {
    for (const [userId, session] of this.clients) {
      if (session.client) {
        try {
          await session.client.destroy();
        } catch (e) {
          console.error(`⚠️ Erreur fermeture client WhatsApp legacy (${userId}):`, e.message);
        }
      }
    }
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== APPEL LLM ====================
async function callLLM(messages) {
  const provider = LLM_PROVIDERS.GROQ;

  if (!provider.apiKey && !LLM_PROVIDERS.OPENROUTER.apiKey) {
    throw new Error("Aucune clé API LLM configurée (GROQ_API_KEY / OPENROUTER_API_KEY)");
  }

  if (provider.apiKey) {
    try {
      const response = await axios.post(
        `${provider.baseURL}/chat/completions`,
        {
          model: provider.model,
          messages: [LUBA_SYSTEM_PROMPT, ...messages],
          temperature: provider.temperature,
          max_tokens: provider.maxTokens,
          response_format: { type: "json_object" }
        },
        {
          headers: {
            Authorization: `Bearer ${provider.apiKey}`,
            "Content-Type": "application/json"
          },
          timeout: provider.timeout
        }
      );

      const content = response.data?.choices?.[0]?.message?.content;
      if (!content) throw new Error("Réponse Groq vide");

      return JSON.parse(content);
    } catch (error) {
      console.error("❌ Erreur Groq:", error.message);
    }
  }

  const fallback = LLM_PROVIDERS.OPENROUTER;
  if (!fallback.apiKey) throw new Error("OPENROUTER_API_KEY non configurée pour le fallback");

  const response = await axios.post(
    `${fallback.baseURL}/chat/completions`,
    {
      model: fallback.model,
      messages: [LUBA_SYSTEM_PROMPT, ...messages],
      temperature: fallback.temperature,
      max_tokens: fallback.maxTokens,
      response_format: { type: "json_object" }
    },
    {
      headers: {
        Authorization: `Bearer ${fallback.apiKey}`,
        "Content-Type": "application/json",
        "HTTP-Referer": "https://luba-ia.web.app",
        "X-Title": "Luba.ia Assistant"
      },
      timeout: fallback.timeout
    }
  );

  const content = response.data?.choices?.[0]?.message?.content;
  if (!content) throw new Error("Réponse OpenRouter vide");

  return JSON.parse(content);
}

// ==================== BOUCLE REACT ====================
// context : { conversationId, userId, message, googleAccessToken, channel }
async function handleChat({ conversationId, userId, message, googleAccessToken = null, channel = "web" }) {
  await getSession(conversationId, userId);

  const activeIntent = await getActiveIntent(conversationId);
  if (activeIntent) {
    return await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken });
  }

  const history = await getHistory(conversationId);
  await saveMessage(conversationId, "user", message);

  const messages = [...history, { role: "user", content: message }];

  let keepRunning = true;
  let maxLoops = 5;
  let finalResponse = null;
  let imageUrls = [];

  while (keepRunning && maxLoops > 0) {
    maxLoops--;

    let llmResponse;
    try {
      llmResponse = await callLLM(messages);
    } catch (error) {
      console.error("❌ Erreur LLM (Groq + OpenRouter):", error.message);
      finalResponse = "⚠️ Le service d'IA est momentanément indisponible. Veuillez réessayer dans un instant.";
      break;
    }

    if (llmResponse.toolCalls && llmResponse.toolCalls.length > 0) {
      for (const toolCall of llmResponse.toolCalls) {
        let toolResult;

        try {
          switch (toolCall.name) {
            case "search_images":
              toolResult = await searchWikimediaImages(toolCall.arguments?.query);
              if (toolResult.images) {
                imageUrls = imageUrls.concat(toolResult.images.map((img) => img.url));
              }
              break;

            case "search_web":
              toolResult = await searchWeb(toolCall.arguments?.query);
              break;

            case "send_email":
              toolResult = await dispatchSendEmail({
                googleAccessToken,
                recipient: toolCall.arguments?.recipient || toolCall.arguments?.to,
                subject: toolCall.arguments?.subject,
                body: toolCall.arguments?.body
              });
              break;

            // "send_whatsapp" conservé en alias pour compatibilité avec d'anciens prompts/clients.
            case "send_whatsapp_message":
            case "send_whatsapp":
              toolResult = await sendWhatsAppSmart(
                userId,
                toolCall.arguments?.phone_number || toolCall.arguments?.to,
                toolCall.arguments?.message
              );
              break;

            default:
              toolResult = { error: "Outil inconnu" };
          }
        } catch (toolError) {
          console.error(`❌ Erreur outil ${toolCall.name}:`, toolError.message);
          toolResult = { success: false, error: toolError.message };
        }

        messages.push({
          role: "assistant",
          content: `Résultat de l'outil ${toolCall.name}: ${JSON.stringify(toolResult)}`
        });
      }

      messages.push({
        role: "user",
        content: "Formule maintenant ta réponse finale complète avec les résultats des outils."
      });

      keepRunning = true;
    } else {
      finalResponse = llmResponse.replyText || "Je n'ai pas pu générer une réponse.";
      keepRunning = false;
    }
  }

  if (!finalResponse) {
    finalResponse = "Je rencontre des difficultés techniques. Veuillez réessayer.";
  }

  if (imageUrls.length > 0) {
    const imageMarkdown = imageUrls.map((url, index) => `![Image ${index + 1}](${url})`).join("\n\n");
    finalResponse += `\n\n---\n\n📷 **Illustrations :**\n\n${imageMarkdown}`;
  }

  await saveMessage(conversationId, "assistant", finalResponse);

  return {
    reply: finalResponse,
    images: imageUrls,
    error: false
  };
}

// ==================== GESTION DES INTENTIONS GUIDÉES (multi-tour) ====================
async function handleActiveIntent(conversationId, activeIntent, userMessage, context = {}) {
  const { userId, googleAccessToken } = context;

  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data;

      if (data.step === "NEED_NUMBER") {
        const phoneRegex = /^(\+?\d{1,3}[-.\s]?)?\d{9,15}$/;
        if (phoneRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "WHATSAPP", {
            step: "NEED_MESSAGE",
            recipient: userMessage.trim()
          });
          await saveMessage(conversationId, "user", userMessage);
          await saveMessage(conversationId, "assistant", `✅ Numéro enregistré. Quel message voulez-vous envoyer ?`);

          return {
            reply: `✅ Numéro enregistré. Quel message voulez-vous envoyer à ${userMessage.trim()} ?`,
            error: false
          };
        }

        return {
          reply: "⚠️ Numéro invalide. Veuillez fournir un numéro valide (ex: +33612345678).",
          error: true
        };
      }

      if (data.step === "NEED_MESSAGE") {
        try {
          const result = await sendWhatsAppSmart(userId, data.recipient, userMessage);
          await clearActiveIntent(conversationId);
          await saveMessage(conversationId, "user", userMessage);
          await saveMessage(
            conversationId,
            "assistant",
            result.queued ? `✅ Message mis en file d'envoi vers ${data.recipient}` : `✅ Message envoyé à ${data.recipient}`
          );

          return {
            reply: result.queued
              ? `✅ Message WhatsApp mis en file d'envoi vers ${data.recipient} !`
              : `✅ Message WhatsApp envoyé avec succès à ${data.recipient} !`,
            error: false
          };
        } catch (error) {
          return {
            reply: `⚠️ Erreur d'envoi: ${error.message}`,
            error: true
          };
        }
      }
      break;
    }

    case "EMAIL": {
      const data = activeIntent.data;

      if (data.step === "NEED_RECIPIENT") {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (emailRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "EMAIL", {
            step: "NEED_SUBJECT",
            recipient: userMessage.trim()
          });
          return {
            reply: "✅ Destinataire enregistré. Quel est le sujet de l'email ?",
            error: false
          };
        }
        return {
          reply: "⚠️ Adresse email invalide.",
          error: true
        };
      }

      if (data.step === "NEED_SUBJECT") {
        await setActiveIntent(conversationId, "EMAIL", {
          step: "NEED_BODY",
          recipient: data.recipient,
          subject: userMessage
        });
        return {
          reply: "✅ Sujet enregistré. Quel est le contenu de l'email ?",
          error: false
        };
      }

      if (data.step === "NEED_BODY") {
        const result = await dispatchSendEmail({
          googleAccessToken,
          recipient: data.recipient,
          subject: data.subject,
          body: userMessage
        });
        await clearActiveIntent(conversationId);

        if (result.success) {
          return {
            reply: `✅ Email envoyé à ${data.recipient} (via ${result.provider}) !`,
            error: false
          };
        }
        return {
          reply: `⚠️ Erreur: ${result.error}`,
          error: true
        };
      }
      break;
    }
  }

  await clearActiveIntent(conversationId);
  return { reply: "Je ne comprends plus l'action. Recommençons.", error: true };
}

// ==================== ROUTES ====================

app.get("/", (req, res) => {
  res.json({ reply: `✅ Serveur ${CONFIG.AGENT_NAME} opérationnel`, error: false, version: CONFIG.VERSION });
});

app.get(
  "/api/health",
  asyncHandler(async (req, res) => {
    let dbOk = true;
    try {
      await dbGet("SELECT 1", []);
    } catch (e) {
      dbOk = false;
    }

    res.json({
      reply: `✅ Serveur ${CONFIG.AGENT_NAME} en bonne santé`,
      error: !dbOk,
      data: {
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
        database: dbOk ? "ok" : "erreur",
        whatsapp: {
          gatewayConfigured: whatsappGateway.configured(),
          legacySessionsActives: whatsappManager.clients.size,
          queue: useRedisQueue ? "bullmq+redis" : "memoire (repli)"
        },
        llmProviders: {
          groq: Boolean(LLM_PROVIDERS.GROQ.apiKey),
          openrouter: Boolean(LLM_PROVIDERS.OPENROUTER.apiKey)
        },
        email: {
          smtp: Boolean(emailTransporter),
          gmailOAuth: "à la demande (token transmis par requête)"
        }
      }
    });
  })
);

// ==================== ROUTE CHAT PRINCIPALE (isolée par conversation_id) ====================
app.post(
  "/api/chat",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(200).json({
        reply: "⚠️ Le paramètre 'message' est obligatoire.",
        error: true
      });
    }

    if (message.length > CONFIG.MAX_MESSAGE_LENGTH) {
      return res.status(200).json({
        reply: `⚠️ Message trop long (max ${CONFIG.MAX_MESSAGE_LENGTH} caractères).`,
        error: true
      });
    }

    // conversation_id requis : si absent, on en génère un nouveau et on le renvoie au
    // frontend, qui doit le persister (localStorage, etc.) pour les tours suivants.
    if (!conversationId || typeof conversationId !== "string") {
      conversationId = `conv_${crypto.randomUUID()}`;
      isNewConversation = true;
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }

    const googleAccessToken = extractGoogleAccessToken(req);

    try {
      const result = await handleChat({
        conversationId,
        userId: req.userId,
        message: message.trim(),
        googleAccessToken,
        channel: "web"
      });
      return res.status(200).json({ ...result, conversationId, isNewConversation });
    } catch (error) {
      console.error("❌ Erreur /api/chat:", error.message);
      return res.status(200).json({
        reply: "⚠️ Une erreur est survenue. Veuillez réessayer.",
        error: true,
        conversationId
      });
    }
  })
);

// ==================== ROUTES WHATSAPP — NOUVELLE PASSERELLE (Evolution API) ====================

// Pairing : génère/retourne le QR code de connexion de l'instance WhatsApp.
// Protégée par ADMIN_API_KEY si définie (opération d'infrastructure, pas liée à un chat précis).
app.get(
  "/api/whatsapp/qr",
  strictLimiter,
  asyncHandler(async (req, res) => {
    if (process.env.ADMIN_API_KEY && req.headers["x-admin-key"] !== process.env.ADMIN_API_KEY) {
      return res.status(401).json({ error: true, reply: "⚠️ Non autorisé." });
    }

    try {
      const data = await whatsappGateway.getQRCode();
      return res.status(200).json({ reply: "📱 QR Code généré.", error: false, data });
    } catch (error) {
      console.error("❌ Erreur QR WhatsApp (gateway):", error.message);
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
  })
);

// Statut de connexion de l'instance WhatsApp (connecté / déconnecté / en attente).
app.get(
  "/api/whatsapp/status",
  strictLimiter,
  asyncHandler(async (req, res) => {
    try {
      const data = await whatsappGateway.getStatus();
      return res.status(200).json({ reply: "Statut récupéré.", error: false, data });
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
  })
);

// Webhook de réception des messages WhatsApp entrants depuis la passerelle.
// Le numéro de téléphone de l'expéditeur sert de conversation_id pour ce canal.
app.post(
  "/api/whatsapp/webhook",
  webhookLimiter,
  asyncHandler(async (req, res) => {
    if (!verifyWhatsappWebhookSignature(req)) {
      console.warn("⚠️ Webhook WhatsApp: signature invalide ou manquante");
      return res.status(401).json({ error: true, reply: "Signature invalide." });
    }

    // Réponse immédiate : la plupart des passerelles WhatsApp attendent un 200 rapide
    // et retentent l'envoi du webhook si la réponse tarde trop.
    res.status(200).json({ received: true });

    try {
      const payload = req.body;
      // Convention Evolution API : payload.data.key.remoteJid / payload.data.message.conversation
      const remoteJid = payload?.data?.key?.remoteJid || payload?.data?.from;
      const fromMe = payload?.data?.key?.fromMe;
      const text =
        payload?.data?.message?.conversation ||
        payload?.data?.message?.extendedTextMessage?.text ||
        payload?.data?.body;

      if (!remoteJid || fromMe || !text) return; // ignore les messages sortants ou événements sans texte

      const phoneNumber = String(remoteJid).replace(/@.*$/, "");
      const conversationId = `whatsapp_${phoneNumber}`;

      const result = await handleChat({
        conversationId,
        userId: conversationId, // sur ce canal, le numéro de téléphone fait office d'identité
        message: String(text).slice(0, CONFIG.MAX_MESSAGE_LENGTH),
        channel: "whatsapp"
      });

      if (result?.reply) {
        // On retire le markdown d'images avant l'envoi WhatsApp (pas de rendu markdown côté WhatsApp).
        const plainReply = result.reply.replace(/!\[.*?\]\(.*?\)/g, "").trim();
        await enqueueWhatsAppSend(phoneNumber, plainReply || "🙂");
      }
    } catch (error) {
      console.error("❌ Erreur traitement webhook WhatsApp:", error.message);
    }
  })
);

// ==================== ROUTES WHATSAPP — LEGACY (whatsapp-web.js) ====================
// Conservées telles quelles pour compatibilité ascendante avec le frontend existant.

app.post(
  "/api/whatsapp/connect",
  strictLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    try {
      const result = await whatsappManager.initClient(req.userId);

      if (result.connected) {
        return res.status(200).json({
          reply: "✅ WhatsApp est déjà connecté.",
          error: false
        });
      }

      let qrCode = null;
      const startTime = Date.now();

      while (!qrCode && Date.now() - startTime < CONFIG.WHATSAPP_QR_TIMEOUT) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        qrCode = whatsappManager.getQRCode(req.userId);
      }

      if (qrCode) {
        return res.status(200).json({
          reply: "📱 Scannez ce QR Code avec WhatsApp :",
          qrCode: qrCode,
          error: false
        });
      }

      return res.status(200).json({
        reply: "⚠️ Délai dépassé. Veuillez réessayer.",
        error: true
      });
    } catch (error) {
      console.error("❌ Erreur WhatsApp connect (legacy):", error.message);
      return res.status(200).json({
        reply: "⚠️ Erreur lors de la connexion WhatsApp.",
        error: true
      });
    }
  })
);

app.post(
  "/api/whatsapp/send",
  strictLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    if (!req.body.to || !req.body.message) {
      return res.status(200).json({
        reply: "⚠️ Les paramètres 'to' et 'message' sont obligatoires.",
        error: true
      });
    }

    try {
      const result = await whatsappManager.sendMessage(req.userId, req.body.to, req.body.message);

      return res.status(200).json({
        reply: `✅ Message envoyé à ${req.body.to}`,
        error: false,
        data: result
      });
    } catch (error) {
      return res.status(200).json({
        reply: `⚠️ Erreur: ${error.message}`,
        error: true
      });
    }
  })
);

// ==================== ROUTE INITIALISATION D'INTENTION (multi-tour, requiert conversationId) ====================
app.post(
  "/api/intent/init",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const { intentType, conversationId, conversation_id: conversationIdSnake } = req.body;
    const convId = conversationId || conversationIdSnake;

    if (!convId || typeof convId !== "string") {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'conversationId' est obligatoire.", error: true });
    }

    try {
      await assertConversationOwnership(convId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }

    await getSession(convId, req.userId);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(convId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.status(200).json({
        reply: "📱 Envoi WhatsApp initié. Quel est le numéro du destinataire ?",
        error: false
      });
    }

    if (intentType === "EMAIL") {
      await setActiveIntent(convId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.status(200).json({
        reply: "📧 Envoi d'email initié. Quelle est l'adresse du destinataire ?",
        error: false
      });
    }

    return res.status(200).json({
      reply: "⚠️ Type d'intention inconnu.",
      error: true
    });
  })
);

// ==================== ROUTE EFFACER MÉMOIRE (par conversation_id) ====================
app.post(
  "/api/memory/clear",
  authenticateUser,
  asyncHandler(async (req, res) => {
    const conversationId = req.body.conversationId || req.body.conversation_id;

    if (!conversationId || typeof conversationId !== "string") {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'conversationId' est obligatoire.", error: true });
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);
    await clearActiveIntent(conversationId);

    return res.status(200).json({
      reply: "✅ Mémoire de cette conversation effacée.",
      error: false
    });
  })
);

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ 404 — Route non trouvée: ${req.method} ${req.originalUrl}`);
  res.status(404).json({
    reply: `⚠️ Route non trouvée: ${req.method} ${req.originalUrl}`,
    error: true,
    hint: "Vérifie que l'URL appelée par le frontend correspond exactement à une route existante (voir liste ci-dessous).",
    availableRoutes: [
      "GET /",
      "GET /api/health",
      "POST /api/chat",
      "GET /api/whatsapp/qr",
      "GET /api/whatsapp/status",
      "POST /api/whatsapp/webhook",
      "POST /api/whatsapp/connect (legacy)",
      "POST /api/whatsapp/send (legacy)",
      "POST /api/intent/init",
      "POST /api/memory/clear"
    ]
  });
});

// ==================== GESTIONNAIRE D'ERREURS GLOBAL ====================
app.use((err, req, res, next) => {
  if (err?.type === "entity.parse.failed") {
    return res.status(200).json({ reply: "⚠️ Corps de requête JSON invalide.", error: true });
  }

  if (err?.message === "Origine non autorisée") {
    return res.status(200).json({ reply: "⚠️ Origine non autorisée.", error: true });
  }

  console.error(`❌ [${req.requestId || "unknown"}] Erreur non gérée:`, err?.message || err);
  return res.status(200).json({
    reply: "⚠️ Une erreur interne est survenue. Veuillez réessayer.",
    error: true
  });
});

// ==================== DÉMARRAGE ====================
const PORT = CONFIG.PORT;

const server = app.listen(PORT, () => {
  console.log("\n" + "=".repeat(60));
  console.log(`🚀 SERVEUR ${CONFIG.AGENT_NAME.toUpperCase()} DÉMARRÉ (Production Ready)`);
  console.log("=".repeat(60));
  console.log(`🔢 Version : ${CONFIG.VERSION}`);
  console.log(`🔌 Port : ${PORT}`);
  console.log(`🧠 Mémoire : SQLite, isolée par conversation_id`);
  console.log(`🔄 Boucle ReAct : Activée`);
  console.log(`📧 Email : Gmail API (OAuth) + fallback SMTP (${emailTransporter ? "configuré" : "non configuré"})`);
  console.log(`📱 WhatsApp gateway (Evolution API) : ${whatsappGateway.configured() ? "configurée" : "NON configurée"}`);
  console.log(`📱 WhatsApp legacy (whatsapp-web.js) : conservée pour compatibilité`);
  console.log(`📬 File d'attente WhatsApp : ${useRedisQueue ? "BullMQ + Redis" : "en mémoire (repli)"}`);
  console.log(`🌐 Origines CORS autorisées : ${ALLOWED_ORIGINS.join(", ")}`);
  console.log("=".repeat(60) + "\n");
});

server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`\n🛑 Arrêt gracieux (${signal})...`);

  const forceExitTimer = setTimeout(() => {
    console.error("⏱️ Arrêt forcé après délai dépassé");
    process.exit(1);
  }, 10000);

  try {
    await whatsappManager.destroyAll();
    if (whatsappWorker) await whatsappWorker.close();
    if (whatsappQueue) await whatsappQueue.close();
    if (emailTransporter) emailTransporter.close();

    await new Promise((resolve) => {
      db.close((err) => {
        if (err) console.error("⚠️ Erreur fermeture DB:", err.message);
        resolve();
      });
    });

    server.close(() => {
      clearTimeout(forceExitTimer);
      console.log("✅ Arrêt terminé");
      process.exit(0);
    });
  } catch (err) {
    console.error("❌ Erreur pendant l'arrêt gracieux:", err.message);
    clearTimeout(forceExitTimer);
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (error) => {
  console.error("❌ Erreur non capturée:", error.message);
});
process.on("unhandledRejection", (reason) => {
  console.error("❌ Promesse rejetée non gérée:", reason);
});

module.exports = app;
