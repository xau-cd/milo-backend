// ==================== INDEX.JS - CERVEAU MILO (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, SQLite, whatsapp-web.js, qrcode, axios, Groq (Principal), OpenRouter (Fallback), Nodemailer
// Format de réponse strict : { reply: "texte" }
// Optimisé pour Render 512 Mo

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const cheerio = require("cheerio");
const qrcode = require("qrcode");
const nodemailer = require("nodemailer");
const { Client, LocalAuth } = require("whatsapp-web.js");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

// ==================== CONFIGURATION ET VALIDATION DES VARIABLES D'ENVIRONNEMENT ====================
const requiredEnvVars = ["GROQ_API_KEY", "OPENROUTER_API_KEY"];

const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.warn(`⚠️ Variables d'environnement obligatoires manquantes: ${missingEnvVars.join(", ")}`);
}

// ==================== INITIALISATION SQLITE AVEC MODE WAL ====================
const dbDir = path.join(__dirname, "data");
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

const db = new sqlite3.Database(path.join(dbDir, "milo.db"));

// Activer le mode WAL
db.run("PRAGMA journal_mode = WAL;", (err) => {
  if (err) {
    console.error("❌ Erreur activation WAL:", err.message);
  } else {
    console.log("✅ Mode WAL activé");
  }
});

// Optimisations SQLite
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -32000;");
db.run("PRAGMA busy_timeout = 5000;");
db.run("PRAGMA temp_store = MEMORY;");

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

  db.run(`CREATE TABLE IF NOT EXISTS chat_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    message TEXT,
    response TEXT,
    system_action TEXT,
    llm_provider TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS tool_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    action TEXT,
    result_type TEXT,
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

  db.run(`CREATE TABLE IF NOT EXISTS llm_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT,
    model TEXT,
    response_time INTEGER,
    status TEXT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

// ==================== CONFIGURATION NODEMAILER ====================
let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  emailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || "587"),
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
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();

const corsOptions = {
  origin: function (origin, callback) {
    const allowedOrigins = [
      "https://milo-ead21.web.app",
      "http://localhost:3000",
      "http://localhost:8080",
      "http://localhost:5173"
    ];
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Origine non autorisée par CORS"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id"],
  credentials: true,
  maxAge: 86400
};

app.use(cors(corsOptions));
app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  handler: (req, res) => {
    return res.status(429).json({ reply: "Trop de requêtes. Réessayez dans 15 minutes." });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 30,
  handler: (req, res) => {
    return res.status(429).json({ reply: "Limite de requêtes atteinte." });
  }
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();
  req.requestId = requestId;
  
  console.log(`📥 [${requestId}] ${req.method} ${req.url}`);
  
  res.on("finish", () => {
    const duration = Date.now() - start;
    console.log(`📤 [${requestId}] ${req.method} ${req.url} - ${res.statusCode} - ${duration}ms`);
  });
  
  next();
});

// ==================== MIDDLEWARE D'AUTHENTIFICATION ====================
const authenticateUser = (req, res, next) => {
  try {
    const userId = req.body.userId || req.query.userId || req.headers["x-user-id"];
    
    if (!userId) {
      return res.status(401).json({ reply: "Authentification requise. userId manquant." });
    }
    
    db.get("SELECT * FROM users WHERE id = ?", [userId], (err, user) => {
      if (err) {
        return res.status(500).json({ reply: "Erreur interne du serveur." });
      }
      
      if (!user) {
        db.run(
          "INSERT INTO users (id, display_name) VALUES (?, ?)",
          [userId, req.body.displayName || userId],
          (insertErr) => {
            if (insertErr) {
              return res.status(500).json({ reply: "Erreur lors de la création de l'utilisateur." });
            }
            req.user = { id: userId };
            next();
          }
        );
      } else {
        req.user = user;
        next();
      }
    });
  } catch (error) {
    return res.status(500).json({ reply: "Erreur d'authentification." });
  }
};

// ==================== PROMPT SYSTÈME AVEC PERSONNALITÉ PROACTIVE ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, un assistant IA intelligent et proactif développé par HIKLON Technology.

RÈGLE ABSOLUE DE PERSONNALITÉ :
Tu es chaleureux, précis et toujours prêt à aider. Tu réponds de manière complète et détaillée.

RÈGLE ABSOLUE D'ILLUSTRATION AUTOMATIQUE :
Chaque fois que tu fournis des informations sur une personne célèbre, un lieu, un animal, un objet, ou un concept visuel, tu DOIS obligatoirement utiliser ton outil de recherche d'images en arrière-plan pour illustrer ta réponse.
N'attends jamais que l'utilisateur te demande une photo.
Sois naturel, donne les informations demandées et insère l'image de manière fluide dans ta réponse.

RÈGLE DE CONFIDENTIALITÉ :
Tu ne dois jamais expliquer ton fonctionnement interne ni citer les technologies que tu utilises.
Tu présentes toutes tes fonctions comme tes capacités natives HIKLON.

FORMAT DE RÉPONSE OBLIGATOIRE :
Tu DOIS TOUJOURS répondre au format JSON strict :
{
  "replyText": "Ta réponse complète avec informations",
  "systemAction": "ACTION_TYPE",
  "payload": {}
}

ACTIONS DISPONIBLES :
- "NONE" : Réponse sans action
- "SEARCH_IMAGES" : Rechercher des images pour illustrer (payload: { "query": "sujet à illustrer" })
- "SEND_EMAIL" : Envoyer un email (payload: { "to", "subject", "body" })
- "SEND_WHATSAPP" : Envoyer un WhatsApp (payload: { "to", "message" })
- "SEARCH_WEB" : Rechercher sur le web (payload: { "query" })

EXEMPLE D'ILLUSTRATION AUTOMATIQUE :
Utilisateur: "Parle-moi de Cristiano Ronaldo"
Réponse: {
  "replyText": "Cristiano Ronaldo est un footballeur portugais...",
  "systemAction": "SEARCH_IMAGES",
  "payload": { "query": "Cristiano Ronaldo portrait" }
}

EXEMPLE POUR UN CONCEPT :
Utilisateur: "C'est quoi un virus ?"
Réponse: {
  "replyText": "Un virus est un agent infectieux...",
  "systemAction": "SEARCH_IMAGES",
  "payload": { "query": "virus microscope" }
}
`;

// ==================== ARCHITECTURE DUAL-LLM ====================
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

// ==================== FONCTION POUR APPELER GROQ ====================
async function callGroq(userMessage, history = []) {
  const provider = LLM_PROVIDERS.GROQ;
  if (!provider.apiKey) throw new Error("GROQ_API_KEY non configurée");
  
  const messages = [
    { role: "system", content: MILO_SYSTEM_PROMPT },
    ...history.slice(-10).map((m) => ({ 
      role: m.role || "user", 
      content: typeof m.content === "string" ? m.content : m.message || ""
    })),
    { role: "user", content: userMessage }
  ];
  
  const startTime = Date.now();
  
  try {
    const response = await axios.post(
      `${provider.baseURL}/chat/completions`,
      {
        model: provider.model,
        messages,
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
    
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, Date.now() - startTime]
    );
    
    return { content, provider: provider.name, model: provider.model };
  } catch (error) {
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, Date.now() - startTime, error.message]
    );
    throw error;
  }
}

// ==================== FONCTION POUR APPELER OPENROUTER (FALLBACK) ====================
async function callOpenRouter(userMessage, history = []) {
  const provider = LLM_PROVIDERS.OPENROUTER;
  if (!provider.apiKey) throw new Error("OPENROUTER_API_KEY non configurée");
  
  const messages = [
    { role: "system", content: MILO_SYSTEM_PROMPT },
    ...history.slice(-10).map((m) => ({ 
      role: m.role || "user", 
      content: typeof m.content === "string" ? m.content : m.message || ""
    })),
    { role: "user", content: userMessage }
  ];
  
  const startTime = Date.now();
  
  try {
    const response = await axios.post(
      `${provider.baseURL}/chat/completions`,
      {
        model: provider.model,
        messages,
        temperature: provider.temperature,
        max_tokens: provider.maxTokens,
        response_format: { type: "json_object" }
      },
      {
        headers: {
          Authorization: `Bearer ${provider.apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://milo-ead21.web.app",
          "X-Title": "MILO Assistant"
        },
        timeout: provider.timeout
      }
    );
    
    const content = response.data?.choices?.[0]?.message?.content;
    if (!content) throw new Error("Réponse OpenRouter vide");
    
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, Date.now() - startTime]
    );
    
    return { content, provider: provider.name, model: provider.model };
  } catch (error) {
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, Date.now() - startTime, error.message]
    );
    throw error;
  }
}

// ==================== FONCTION PRINCIPALE AVEC FALLBACK ====================
async function callLLMWithFallback(userMessage, history = [], userId = null) {
  try {
    const groqResult = await callGroq(userMessage, history);
    return parseLLMResponse(groqResult.content, groqResult.provider, groqResult.model);
  } catch (groqError) {
    console.warn(`⚠️ Groq échoué: ${groqError.message}. Basculement vers OpenRouter...`);
    
    try {
      const openRouterResult = await callOpenRouter(userMessage, history);
      return parseLLMResponse(openRouterResult.content, openRouterResult.provider, openRouterResult.model);
    } catch (openRouterError) {
      return {
        replyText: "Je rencontre des difficultés techniques. Veuillez réessayer.",
        systemAction: "NONE",
        payload: {},
        error: true
      };
    }
  }
}

// ==================== PARSER LA RÉPONSE JSON ====================
function parseLLMResponse(content, provider, model) {
  try {
    let cleanContent = content.trim();
    if (cleanContent.startsWith("```json")) {
      cleanContent = cleanContent.replace(/```json\n?/g, "").replace(/```\n?/g, "");
    } else if (cleanContent.startsWith("```")) {
      cleanContent = cleanContent.replace(/```\n?/g, "");
    }
    
    const parsed = JSON.parse(cleanContent);
    
    if (!parsed.replyText || typeof parsed.replyText !== "string") {
      throw new Error("Champ 'replyText' manquant");
    }
    
    return {
      replyText: parsed.replyText,
      systemAction: parsed.systemAction || "NONE",
      payload: parsed.payload || {},
      provider,
      model
    };
  } catch (error) {
    console.error("❌ Erreur parsing JSON:", error.message);
    return {
      replyText: content.replace(/```json\n?|\n?```/g, "").trim(),
      systemAction: "NONE",
      payload: {},
      provider,
      model
    };
  }
}

// ==================== RECHERCHE D'IMAGES ====================
async function searchWikimediaImages(query, limit = 6) {
  try {
    console.log(`🖼️ Recherche d'images: "${query}"`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    
    const response = await axios.get(url, { timeout: 15000 });
    const pages = response.data?.query?.pages;
    
    if (!pages) return [];
    
    return Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        title: page.title || "Image",
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null
      }))
      .filter((img) => img.url);
  } catch (error) {
    console.error("❌ Erreur recherche images:", error.message);
    return [];
  }
}

// ==================== RECHERCHE WEB ====================
async function searchWebAdvanced(query) {
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
    
    return { query, results, totalResults: results.length };
  } catch (error) {
    return { query, results: [], totalResults: 0, error: error.message };
  }
}

// ==================== GESTION EMAIL ====================
async function sendEmail(to, subject, body, userId = null) {
  if (!emailTransporter) throw new Error("Serveur SMTP non configuré");
  
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(to)) throw new Error(`Format d'email invalide: ${to}`);
  
  const mailOptions = {
    from: process.env.EMAIL_FROM || `"MILO" <${process.env.SMTP_USER}>`,
    to,
    subject,
    html: `<div style="font-family: Arial; padding: 20px;">${body.replace(/\n/g, "<br>")}</div>`,
    text: body
  };
  
  const info = await emailTransporter.sendMail(mailOptions);
  
  if (userId) {
    db.run(
      "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, 'sent')",
      [userId, to, subject]
    );
  }
  
  return { success: true, messageId: info.messageId };
}

// ==================== GESTION WHATSAPP ====================
class WhatsAppManager {
  constructor() {
    this.clients = new Map();
  }

  async initClient(userId) {
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.ready) return { connected: true };
    }

    const client = new Client({
      authStrategy: new LocalAuth({ clientId: userId, dataPath: path.join(__dirname, "sessions") }),
      puppeteer: {
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerate',
          '--no-first-run',
          '--no-zygote',
          '--single-process',
          '--disable-gpu'
        ]
      }
    });

    const sessionData = { client, qrCode: null, ready: false };
    this.clients.set(userId, sessionData);

    client.on("qr", async (qr) => {
      sessionData.qrCode = await qrcode.toDataURL(qr, { width: 600 });
    });

    client.on("ready", () => {
      sessionData.ready = true;
      sessionData.qrCode = null;
      db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
    });

    client.on("disconnected", () => {
      sessionData.ready = false;
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
    });

    try {
      await client.initialize();
      return { connected: false };
    } catch (error) {
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
    
    let formattedTo = to.replace(/[^\d]/g, "");
    if (!formattedTo.startsWith("55")) formattedTo = "55" + formattedTo;
    formattedTo += "@c.us";
    
    const chat = await session.client.getChatById(formattedTo);
    await session.client.sendMessage(chat.id._serialized, message);
    
    return { success: true, to };
  }

  getQRCode(userId) {
    return this.clients.get(userId)?.qrCode || null;
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== ROUTES ====================

app.get("/", (req, res) => {
  res.json({ status: "ok", message: "Serveur MILO opérationnel", version: "3.0.0" });
});

app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    services: {
      groq: process.env.GROQ_API_KEY ? "configured" : "not_configured",
      openrouter: process.env.OPENROUTER_API_KEY ? "configured" : "not_configured",
      email: emailTransporter ? "configured" : "not_configured"
    },
    memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB"
  });
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post("/api/chat", apiLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 500));
    
    const { message, history } = req.body;
    const userId = req.user.id;
    
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ error: "message manquant dans la requête", reply: "Veuillez fournir un message." });
    }
    
    console.log(`💬 Chat de ${userId}: "${message.slice(0, 80)}"`);
    
    // Appel au LLM avec le system prompt proactif
    const aiResult = await callLLMWithFallback(message, history || [], userId);
    
    let finalReplyText = aiResult.replyText;
    let imageUrls = [];
    
    // Exécuter l'action automatiquement
    if (aiResult.systemAction === "SEARCH_IMAGES" && aiResult.payload?.query) {
      console.log(`🖼️ Illustration automatique: "${aiResult.payload.query}"`);
      imageUrls = await searchWikimediaImages(aiResult.payload.query, 4);
      
      if (imageUrls.length > 0) {
        // Ajouter les URLs d'images à la réponse
        const imageLinks = imageUrls.map(img => img.url).join("\n");
        finalReplyText += `\n\n📷 Illustrations :\n${imageLinks}`;
      }
    }
    
    // Logger
    db.run(
      "INSERT INTO chat_logs (user_id, message, response, system_action, llm_provider) VALUES (?, ?, ?, ?, ?)",
      [userId, message, finalReplyText, aiResult.systemAction || "NONE", aiResult.provider || "unknown"]
    );
    
    // Réponse avec images si disponibles
    const responseObj = { reply: finalReplyText };
    if (imageUrls.length > 0) {
      responseObj.images = imageUrls;
    }
    
    return res.status(200).json(responseObj);
    
  } catch (error) {
    console.error("❌ Erreur /api/chat:", error.message);
    return res.status(500).json({ 
      reply: "Une erreur est survenue. Veuillez réessayer.",
      error: error.message
    });
  }
});

// ==================== ROUTE WHATSAPP CHAT ====================
app.post("/api/whatsapp/chat", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 500));
    
    const { userId, message } = req.body;
    
    // Validation explicite
    if (!userId) {
      return res.status(400).json({ error: "userId manquant dans la requête" });
    }
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ error: "message manquant ou invalide dans la requête" });
    }
    
    console.log(`💬 WhatsApp Chat: ${userId} → "${message.slice(0, 80)}"`);
    
    // Traiter avec le LLM
    const aiResult = await callLLMWithFallback(message, [], userId);
    
    return res.status(200).json({ 
      reply: aiResult.replyText,
      provider: aiResult.provider
    });
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/chat:", error.message);
    return res.status(500).json({ 
      reply: "Erreur lors du traitement WhatsApp.",
      error: error.message
    });
  }
});

// ==================== ROUTE EMAIL CHAT ====================
app.post("/api/email/chat", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 500));
    
    const { userId, to, subject, body } = req.body;
    
    // Validation explicite
    if (!userId) {
      return res.status(400).json({ error: "userId manquant dans la requête" });
    }
    if (!to) {
      return res.status(400).json({ error: "destinataire (to) manquant dans la requête" });
    }
    if (!subject) {
      return res.status(400).json({ error: "sujet (subject) manquant dans la requête" });
    }
    if (!body) {
      return res.status(400).json({ error: "contenu (body) manquant dans la requête" });
    }
    
    console.log(`📧 Email Chat: ${userId} → ${to}`);
    
    const emailResult = await sendEmail(to, subject, body, userId);
    
    return res.status(200).json({ 
      reply: `✅ Email envoyé avec succès à ${to}`,
      messageId: emailResult.messageId
    });
    
  } catch (error) {
    console.error("❌ Erreur /api/email/chat:", error.message);
    return res.status(500).json({ 
      reply: "Erreur lors de l'envoi de l'email.",
      error: error.message
    });
  }
});

// ==================== ROUTE ENVOI WHATSAPP ====================
app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 500));
    
    const { userId, to, message } = req.body;
    
    if (!userId) {
      return res.status(400).json({ error: "userId manquant dans la requête" });
    }
    if (!to) {
      return res.status(400).json({ error: "destinataire (to) manquant dans la requête" });
    }
    if (!message) {
      return res.status(400).json({ error: "message manquant dans la requête" });
    }
    
    const result = await whatsappManager.sendMessage(userId, to, message);
    
    return res.status(200).json({ 
      reply: `✅ Message WhatsApp envoyé à ${to}`,
      ...result
    });
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/send:", error.message);
    
    if (error.code === "WHATSAPP_NOT_CONNECTED") {
      return res.status(400).json({ 
        error: "WhatsApp non connecté",
        reply: "Veuillez d'abord scanner le QR Code pour connecter WhatsApp."
      });
    }
    
    return res.status(500).json({ 
      reply: "Erreur lors de l'envoi WhatsApp.",
      error: error.message
    });
  }
});

// ==================== ROUTE QR CODE WHATSAPP ====================
app.post("/api/whatsapp/qr", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 200));
    
    const userId = req.body.userId || req.user.id;
    
    if (!userId) {
      return res.status(400).json({ error: "userId manquant dans la requête" });
    }
    
    await whatsappManager.initClient(userId);
    
    let qr = null;
    const startTime = Date.now();
    while (!qr && Date.now() - startTime < 30000) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      qr = whatsappManager.getQRCode(userId);
    }
    
    if (qr) {
      return res.status(200).json({ 
        reply: "QR Code généré. Scannez pour connecter WhatsApp.",
        qrBase64: qr
      });
    } else {
      return res.status(408).json({ 
        reply: "Délai dépassé pour la génération du QR Code."
      });
    }
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/qr:", error.message);
    return res.status(500).json({ 
      reply: "Erreur lors de la génération du QR Code.",
      error: error.message
    });
  }
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  res.status(404).json({ 
    error: "Route non trouvée",
    reply: "Cette route n'existe pas."
  });
});

// ==================== GESTION DES ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express:", err.message);
  
  if (err instanceof SyntaxError && err.status === 400 && "body" in err) {
    return res.status(400).json({ error: "JSON invalide dans la requête" });
  }
  
  res.status(500).json({ 
    error: "Erreur interne du serveur",
    reply: "Une erreur est survenue."
  });
});

// ==================== DÉMARRAGE ====================
const PORT = process.env.PORT || 3000;

const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`🤖 LLM Principal : Groq (${LLM_PROVIDERS.GROQ.model})`);
  console.log(`🔄 LLM Fallback : OpenRouter (${LLM_PROVIDERS.OPENROUTER.model})`);
  console.log(`🖼️ Illustration automatique : ACTIVÉE`);
  console.log("========================================");
});

server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Arrêt gracieux (${signal})...`);
  
  for (const [userId, session] of whatsappManager.clients) {
    if (session.client) {
      try { await session.client.destroy(); } catch (e) {}
    }
  }
  
  if (emailTransporter) emailTransporter.close();
  
  db.close((err) => {
    server.close(() => {
      console.log("✅ Arrêt terminé");
      process.exit(0);
    });
    
    setTimeout(() => process.exit(1), 10000);
  });
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("uncaughtException", (error) => {
  console.error("❌ Erreur non capturée:", error.message);
});

process.on("unhandledRejection", (reason) => {
  console.error("❌ Promesse rejetée:", reason);
});

module.exports = app;
