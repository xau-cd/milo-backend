// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, SQLite, whatsapp-web.js, qrcode, axios, Groq (Principal), OpenRouter (Fallback), Nodemailer
// Format de réponse standardisé : { reply, images (optionnel), qrCode (optionnel), error }
// Version : 6.1.0 - Production Ready avec Mémoire, ReAct Loop et durcissement (Luba.ia)

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
  VERSION: "6.1.0",
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

// ==================== CONFIGURATION NODEMAILER ====================
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
    else console.log("✅ SMTP prêt à envoyer des emails");
  });
} else {
  console.warn("⚠️ SMTP non configuré — l'envoi d'email sera désactivé.");
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
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id"],
    credentials: true,
    maxAge: 86400
  })
);

app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));
app.use(express.json({ limit: "10mb" }));
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
// Garantit qu'aucune promesse rejetée dans une route ne peut faire planter
// le process ni rester sans réponse envoyée au client.
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// ==================== MIDDLEWARE D'AUTHENTIFICATION ====================
function dbGet(query, params) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => (err ? reject(err) : resolve(row)));
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

// ==================== SYSTEM PROMPT LUBA ====================
const LUBA_SYSTEM_PROMPT = {
  role: "system",
  content: `Tu es LUBA (Luba.ia), une assistante IA intelligente, chaleureuse et proactive développée par HIKLON Technology.

RÈGLE STRICTE SUR LES IMAGES : Dès que tu décris ou présentes une personnalité, un lieu, un objet, un concept scientifique ou un événement, tu DOIS obligatoirement et automatiquement utiliser ton outil de recherche d'images pour illustrer ta réponse. N'attends JAMAIS que l'utilisateur te demande explicitement une photo.

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
- send_email : Envoyer un email (arguments: { to, subject, body })
- send_whatsapp : Envoyer un WhatsApp (arguments: { to, message })`
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

// ==================== FONCTIONS DE MÉMOIRE ====================
async function getSession(sessionId, userId) {
  const session = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [sessionId]);

  if (session) {
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [sessionId]);
    return session;
  }

  await dbRun("INSERT INTO sessions (session_id, user_id) VALUES (?, ?)", [sessionId, userId]);
  return { session_id: sessionId, user_id: userId, active_intent: null, intent_data: null };
}

async function getHistory(sessionId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  return new Promise((resolve, reject) => {
    db.all(
      "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
      [sessionId, limit],
      (err, rows) => {
        if (err) return reject(err);
        resolve(rows.reverse().map((row) => ({ role: row.role, content: row.content })));
      }
    );
  });
}

async function saveMessage(sessionId, role, content) {
  await dbRun("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", [sessionId, role, content]);
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [sessionId]);
}

async function setActiveIntent(sessionId, intentType, intentData = {}) {
  await dbRun("UPDATE sessions SET active_intent = ?, intent_data = ? WHERE session_id = ?", [
    intentType,
    JSON.stringify(intentData),
    sessionId
  ]);
}

async function getActiveIntent(sessionId) {
  const row = await dbGet("SELECT active_intent, intent_data FROM sessions WHERE session_id = ?", [sessionId]);
  if (!row || !row.active_intent) return null;

  try {
    return { type: row.active_intent, data: JSON.parse(row.intent_data || "{}") };
  } catch (e) {
    console.error("❌ intent_data corrompu, réinitialisation:", e.message);
    return null;
  }
}

async function clearActiveIntent(sessionId) {
  await dbRun("UPDATE sessions SET active_intent = NULL, intent_data = NULL WHERE session_id = ?", [sessionId]);
}

// ==================== OUTILS ====================
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

async function sendEmail(to, subject, body) {
  if (!emailTransporter) return { success: false, error: "SMTP non configuré" };

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!to || !emailRegex.test(String(to).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  try {
    const info = await emailTransporter.sendMail({
      from: process.env.EMAIL_FROM || `"Luba" <${process.env.SMTP_USER}>`,
      to,
      subject: subject || "(sans sujet)",
      html: `<div style="font-family: Arial; padding: 20px;">${String(body || "").replace(/\n/g, "<br>")}</div>`,
      text: body || ""
    });

    return { success: true, messageId: info.messageId };
  } catch (error) {
    console.error("❌ Erreur envoi email:", error.message);
    return { success: false, error: error.message };
  }
}

// ==================== GESTION WHATSAPP ====================
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
        console.log(`📱 QR Code généré pour ${userId}`);
      } catch (error) {
        console.error("❌ Erreur génération QR:", error.message);
      }
    });

    client.on("ready", () => {
      sessionData.ready = true;
      sessionData.qrCode = null;
      db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
      console.log(`✅ WhatsApp connecté pour ${userId}`);
    });

    client.on("auth_failure", (msg) => {
      console.error(`❌ Échec d'authentification WhatsApp pour ${userId}:`, msg);
      sessionData.ready = false;
    });

    client.on("disconnected", (reason) => {
      sessionData.ready = false;
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
      console.warn(`⚠️ WhatsApp déconnecté pour ${userId}: ${reason}`);
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
      const error = new Error("WhatsApp non connecté");
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
          console.error(`⚠️ Erreur fermeture client WhatsApp (${userId}):`, e.message);
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

  // Fallback OpenRouter
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
async function handleChat(userId, userMessage) {
  const sessionId = `user_${userId}`;
  await getSession(sessionId, userId);

  // Vérifier l'intention active
  const activeIntent = await getActiveIntent(sessionId);
  if (activeIntent) {
    return await handleActiveIntent(sessionId, activeIntent, userMessage);
  }

  // Récupérer l'historique
  const history = await getHistory(sessionId);

  // Sauvegarder le message utilisateur
  await saveMessage(sessionId, "user", userMessage);

  const messages = [...history, { role: "user", content: userMessage }];

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
                imageUrls = toolResult.images.map((img) => img.url);
              }
              break;
            case "search_web":
              toolResult = await searchWeb(toolCall.arguments?.query);
              break;
            case "send_email":
              toolResult = await sendEmail(
                toolCall.arguments?.to,
                toolCall.arguments?.subject,
                toolCall.arguments?.body
              );
              break;
            case "send_whatsapp":
              toolResult = await whatsappManager.sendMessage(
                userId,
                toolCall.arguments?.to,
                toolCall.arguments?.message
              );
              break;
            default:
              toolResult = { error: "Outil inconnu" };
          }
        } catch (toolError) {
          console.error(`❌ Erreur outil ${toolCall.name}:`, toolError.message);
          toolResult = { error: toolError.message };
        }

        messages.push({
          role: "assistant",
          content: `Résultat de l'outil ${toolCall.name}: ${JSON.stringify(toolResult)}`
        });
      }

      // Demander au LLM de formuler la réponse finale avec les résultats
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

  // Ajouter les images en Markdown
  if (imageUrls.length > 0) {
    const imageMarkdown = imageUrls.map((url, index) => `![Image ${index + 1}](${url})`).join("\n\n");
    finalResponse += `\n\n---\n\n📷 **Illustrations :**\n\n${imageMarkdown}`;
  }

  // Sauvegarder la réponse
  await saveMessage(sessionId, "assistant", finalResponse);

  return {
    reply: finalResponse,
    images: imageUrls,
    error: false
  };
}

// ==================== GESTION DES INTENTIONS ====================
async function handleActiveIntent(sessionId, activeIntent, userMessage) {
  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data;

      if (data.step === "NEED_NUMBER") {
        const phoneRegex = /^(\+?\d{1,3}[-.\s]?)?\d{9,15}$/;
        if (phoneRegex.test(userMessage.trim())) {
          await setActiveIntent(sessionId, "WHATSAPP", {
            step: "NEED_MESSAGE",
            recipient: userMessage.trim()
          });
          await saveMessage(sessionId, "user", userMessage);
          await saveMessage(sessionId, "assistant", `✅ Numéro enregistré. Quel message voulez-vous envoyer ?`);

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
          await whatsappManager.sendMessage(sessionId.replace("user_", ""), data.recipient, userMessage);
          await clearActiveIntent(sessionId);
          await saveMessage(sessionId, "user", userMessage);
          await saveMessage(sessionId, "assistant", `✅ Message envoyé à ${data.recipient}`);

          return {
            reply: `✅ Message WhatsApp envoyé avec succès à ${data.recipient} !`,
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
          await setActiveIntent(sessionId, "EMAIL", {
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
        await setActiveIntent(sessionId, "EMAIL", {
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
        const result = await sendEmail(data.recipient, data.subject, userMessage);
        await clearActiveIntent(sessionId);

        if (result.success) {
          return {
            reply: `✅ Email envoyé à ${data.recipient} !`,
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

  await clearActiveIntent(sessionId);
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
        whatsappSessionsActives: whatsappManager.clients.size,
        llmProviders: {
          groq: Boolean(LLM_PROVIDERS.GROQ.apiKey),
          openrouter: Boolean(LLM_PROVIDERS.OPENROUTER.apiKey)
        },
        smtp: Boolean(emailTransporter)
      }
    });
  })
);

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post(
  "/api/chat",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const message = req.body.message;

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

    try {
      const result = await handleChat(req.userId, message.trim());
      return res.status(200).json(result);
    } catch (error) {
      console.error("❌ Erreur /api/chat:", error.message);
      return res.status(200).json({
        reply: "⚠️ Une erreur est survenue. Veuillez réessayer.",
        error: true
      });
    }
  })
);

// ==================== ROUTE CONNEXION WHATSAPP ====================
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

      // Attendre le QR code
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
      console.error("❌ Erreur WhatsApp connect:", error.message);
      return res.status(200).json({
        reply: "⚠️ Erreur lors de la connexion WhatsApp.",
        error: true
      });
    }
  })
);

// ==================== ROUTE ENVOI WHATSAPP ====================
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

// ==================== ROUTE INITIALISATION D'INTENTION ====================
app.post(
  "/api/intent/init",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const { intentType } = req.body;
    const sessionId = `user_${req.userId}`;

    if (intentType === "WHATSAPP") {
      await setActiveIntent(sessionId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.status(200).json({
        reply: "📱 Envoi WhatsApp initié. Quel est le numéro du destinataire ?",
        error: false
      });
    }

    if (intentType === "EMAIL") {
      await setActiveIntent(sessionId, "EMAIL", { step: "NEED_RECIPIENT" });
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

// ==================== ROUTE EFFACER MÉMOIRE ====================
app.post(
  "/api/memory/clear",
  authenticateUser,
  asyncHandler(async (req, res) => {
    const sessionId = `user_${req.userId}`;

    await dbRun("DELETE FROM messages WHERE session_id = ?", [sessionId]);
    await clearActiveIntent(sessionId);

    return res.status(200).json({
      reply: "✅ Mémoire effacée.",
      error: false
    });
  })
);

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  res.status(404).json({
    reply: "⚠️ Route non trouvée.",
    error: true
  });
});

// ==================== GESTIONNAIRE D'ERREURS GLOBAL ====================
// Filet de sécurité final : capture les erreurs de parsing JSON (body-parser),
// les erreurs CORS, et toute exception non gérée remontée par asyncHandler,
// pour ne jamais laisser une requête sans réponse ni faire planter le process.
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
  console.log(`🧠 Mémoire : SQLite (persistante)`);
  console.log(`🔄 Boucle ReAct : Activée`);
  console.log(`🎯 Machine à états : Activée`);
  console.log(`🖼️ Images Wikimedia : Activé`);
  console.log(`📱 WhatsApp QR Code : Activé`);
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
