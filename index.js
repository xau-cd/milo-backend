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
const requiredEnvVars = [
  "GROQ_API_KEY",
  "OPENROUTER_API_KEY"
];

const optionalEnvVars = [
  "SMTP_HOST",
  "SMTP_PORT",
  "SMTP_USER",
  "SMTP_PASS",
  "EMAIL_FROM"
];

const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.warn(`⚠️ Variables d'environnement obligatoires manquantes: ${missingEnvVars.join(", ")}`);
  console.warn("⚠️ Le service d'IA ne fonctionnera pas sans ces clés.");
}

const missingOptionalVars = optionalEnvVars.filter(varName => !process.env[varName]);
if (missingOptionalVars.length > 0) {
  console.warn(`⚠️ Variables optionnelles manquantes: ${missingOptionalVars.join(", ")}`);
  console.warn("⚠️ Certaines fonctionnalités (email) seront désactivées.");
}

// ==================== INITIALISATION SQLITE AVEC MODE WAL ====================
const dbDir = path.join(__dirname, "data");
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
  console.log(`📁 Dossier de données créé: ${dbDir}`);
}

const db = new sqlite3.Database(path.join(dbDir, "milo.db"));

// Activer le mode WAL pour de meilleures performances et stabilité
db.run("PRAGMA journal_mode = WAL;", (err) => {
  if (err) {
    console.error("❌ Erreur activation WAL:", err.message);
  } else {
    console.log("✅ Mode WAL activé pour SQLite");
  }
});

// Optimisations supplémentaires pour SQLite
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -32000;"); // 32MB de cache pour économiser la RAM
db.run("PRAGMA busy_timeout = 5000;"); // Attendre 5 secondes si la DB est occupée
db.run("PRAGMA temp_store = MEMORY;"); // Stockage temporaire en mémoire

db.serialize(() => {
  // Table des utilisateurs
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    rendell_credentials TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des logs de chat
  db.run(`CREATE TABLE IF NOT EXISTS chat_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    message TEXT,
    response TEXT,
    system_action TEXT,
    llm_provider TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des logs d'outils
  db.run(`CREATE TABLE IF NOT EXISTS tool_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    action TEXT,
    result_type TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des sessions WhatsApp
  db.run(`CREATE TABLE IF NOT EXISTS whatsapp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    session_id TEXT UNIQUE,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des emails envoyés
  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des métriques LLM
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
    tls: {
      rejectUnauthorized: false
    },
    pool: true,
    maxConnections: 3,
    maxMessages: 50
  });

  // Vérification de la connexion email au démarrage (non bloquante)
  emailTransporter.verify((error, success) => {
    if (error) {
      console.error("❌ Erreur de connexion SMTP:", error.message);
    } else {
      console.log("✅ Serveur SMTP connecté et prêt à envoyer des emails");
    }
  });
} else {
  console.log("ℹ️ Configuration SMTP incomplète. L'envoi d'emails est désactivé.");
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// Configuration CORS
const corsOptions = {
  origin: function (origin, callback) {
    const allowedOrigins = [
      "https://milo-ead21.web.app",
      "http://localhost:3000",
      "http://localhost:8080",
      "http://localhost:5173",
      "http://localhost:5000"
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
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: false // Désactivé pour éviter les problèmes avec les images
  })
);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limite de 100 requêtes par fenêtre
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit dépassé pour ${req.ip}`);
    return res.status(429).json({ 
      reply: "Trop de requêtes. Veuillez réessayer dans 15 minutes."
    });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 heure
  max: 30, // limite de 30 requêtes par heure pour les actions sensibles
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit strict dépassé pour ${req.ip}`);
    return res.status(429).json({ 
      reply: "Limite de requêtes atteinte pour cette action."
    });
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
      console.warn(`⚠️ Tentative d'accès sans authentification`);
      return res.status(401).json({
        reply: "Authentification requise. Veuillez fournir un userId."
      });
    }
    
    // Vérifier que l'utilisateur existe dans la base de données
    db.get("SELECT * FROM users WHERE id = ?", [userId], (err, user) => {
      if (err) {
        console.error("❌ Erreur DB lors de l'authentification:", err.message);
        return res.status(500).json({
          reply: "Erreur interne du serveur."
        });
      }
      
      if (!user) {
        // Créer l'utilisateur s'il n'existe pas
        db.run(
          "INSERT INTO users (id, display_name, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
          [userId, req.body.displayName || userId],
          (insertErr) => {
            if (insertErr) {
              console.error("❌ Erreur création utilisateur:", insertErr.message);
              return res.status(500).json({
                reply: "Erreur lors de la création de l'utilisateur."
              });
            }
            req.user = { id: userId, displayName: req.body.displayName || userId };
            next();
          }
        );
      } else {
        req.user = user;
        next();
      }
    });
  } catch (error) {
    console.error("❌ Erreur d'authentification:", error.message);
    return res.status(500).json({
      reply: "Erreur d'authentification."
    });
  }
};

// ==================== PROMPT SYSTÈME CONFIDENTIEL ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, une Intelligence Artificielle propriétaire avancée développée par HIKLON Technology.
Tu réponds avec précision, sans paresse et de manière complète.

RÈGLE ABSOLUE DE CONFIDENTIALITÉ :
Tu ne dois sous aucun prétexte expliquer ton fonctionnement interne, ni citer les technologies, API, bibliothèques ou services tiers que tu utilises.
Tu présentes toutes tes fonctions comme tes propres capacités natives HIKLON.
Si un utilisateur te demande comment tu fonctionnes, réponds simplement que tu es propulsé par les technologies propriétaires d'IA de HIKLON Technologies.

FORMAT DE RÉPONSE OBLIGATOIRE :
Tu DOIS TOUJOURS répondre au format JSON strict suivant :
{
  "replyText": "Ta réponse visible à l'utilisateur",
  "systemAction": "ACTION_TYPE",
  "payload": {}
}

ACTIONS DISPONIBLES :
- "NONE" : Réponse normale
- "SEND_EMAIL" : Envoyer un email
- "SEND_WHATSAPP" : Envoyer un message WhatsApp
- "SEARCH_WEB" : Rechercher sur le web
- "SEARCH_IMAGES" : Rechercher des images
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
  console.log("🚀 Appel Groq (LLM Principal)");
  
  const provider = LLM_PROVIDERS.GROQ;
  if (!provider.apiKey) {
    throw new Error("GROQ_API_KEY non configurée");
  }
  
  const messages = [
    { role: "system", content: MILO_SYSTEM_PROMPT },
    ...history.slice(-10).map((m) => ({ 
      role: m.role || "user", 
      content: typeof m.content === "string" ? m.content : m.message || JSON.stringify(m.content || "")
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
    if (!content) {
      throw new Error("Réponse Groq vide");
    }
    
    const responseTime = Date.now() - startTime;
    console.log(`✅ Groq répondu en ${responseTime}ms`);
    
    // Logger la métrique
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, responseTime],
      (err) => { if (err) console.error("❌ Erreur log métrique:", err.message); }
    );
    
    return {
      content,
      provider: provider.name,
      model: provider.model,
      responseTime
    };
  } catch (error) {
    const responseTime = Date.now() - startTime;
    console.error(`❌ Erreur Groq (${responseTime}ms):`, error.message);
    
    // Logger l'échec
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, responseTime, error.message],
      (err) => { if (err) console.error("❌ Erreur log métrique:", err.message); }
    );
    
    throw error;
  }
}

// ==================== FONCTION POUR APPELER OPENROUTER (FALLBACK) ====================
async function callOpenRouter(userMessage, history = []) {
  console.log("🔄 Fallback vers OpenRouter");
  
  const provider = LLM_PROVIDERS.OPENROUTER;
  if (!provider.apiKey) {
    throw new Error("OPENROUTER_API_KEY non configurée");
  }
  
  const messages = [
    { role: "system", content: MILO_SYSTEM_PROMPT },
    ...history.slice(-10).map((m) => ({ 
      role: m.role || "user", 
      content: typeof m.content === "string" ? m.content : m.message || JSON.stringify(m.content || "")
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
    if (!content) {
      throw new Error("Réponse OpenRouter vide");
    }
    
    const responseTime = Date.now() - startTime;
    console.log(`✅ OpenRouter répondu en ${responseTime}ms`);
    
    // Logger la métrique
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, responseTime],
      (err) => { if (err) console.error("❌ Erreur log métrique:", err.message); }
    );
    
    return {
      content,
      provider: provider.name,
      model: provider.model,
      responseTime
    };
  } catch (error) {
    const responseTime = Date.now() - startTime;
    console.error(`❌ Erreur OpenRouter (${responseTime}ms):`, error.message);
    
    // Logger l'échec
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, responseTime, error.message],
      (err) => { if (err) console.error("❌ Erreur log métrique:", err.message); }
    );
    
    throw error;
  }
}

// ==================== FONCTION PRINCIPALE AVEC FALLBACK AUTOMATIQUE ====================
async function callLLMWithFallback(userMessage, history = [], userId = null) {
  console.log(`🤖 Traitement LLM pour ${userId || "anonymous"}`);
  
  // Essayer Groq en premier
  try {
    const groqResult = await callGroq(userMessage, history);
    return parseLLMResponse(groqResult.content, groqResult.provider, groqResult.model);
  } catch (groqError) {
    console.warn(`⚠️ Groq a échoué: ${groqError.message}`);
    console.warn(`🔄 Basculement automatique vers OpenRouter...`);
    
    // Fallback vers OpenRouter
    try {
      const openRouterResult = await callOpenRouter(userMessage, history);
      return parseLLMResponse(openRouterResult.content, openRouterResult.provider, openRouterResult.model);
    } catch (openRouterError) {
      console.error(`❌ OpenRouter a également échoué: ${openRouterError.message}`);
      
      // Les deux fournisseurs ont échoué
      return {
        replyText: "Je rencontre des difficultés techniques avec mes services d'IA. Veuillez réessayer dans un instant.",
        systemAction: "NONE",
        payload: {},
        error: true,
        providersFailed: true
      };
    }
  }
}

// ==================== FONCTION POUR PARSER LA RÉPONSE JSON ====================
function parseLLMResponse(content, provider, model) {
  try {
    // Nettoyer le contenu si nécessaire
    let cleanContent = content.trim();
    if (cleanContent.startsWith("```json")) {
      cleanContent = cleanContent.replace(/```json\n?/g, "").replace(/```\n?/g, "");
    } else if (cleanContent.startsWith("```")) {
      cleanContent = cleanContent.replace(/```\n?/g, "");
    }
    
    // Parser le JSON
    const parsedResponse = JSON.parse(cleanContent);
    
    // Valider les champs requis
    if (!parsedResponse.replyText || typeof parsedResponse.replyText !== "string") {
      throw new Error("Champ 'replyText' manquant ou invalide");
    }
    
    const systemAction = parsedResponse.systemAction || "NONE";
    const payload = parsedResponse.payload || {};
    
    console.log(`✅ Réponse parsée - Action: ${systemAction} (via ${provider}/${model})`);
    
    return {
      replyText: parsedResponse.replyText,
      systemAction,
      payload,
      error: false,
      provider,
      model
    };
  } catch (parseError) {
    console.error("❌ Erreur parsing JSON:", parseError.message);
    console.error("   Contenu brut:", content.slice(0, 500));
    
    // Fallback: retourner le contenu brut comme texte
    return {
      replyText: content.replace(/```json\n?|\n?```/g, "").trim(),
      systemAction: "NONE",
      payload: {},
      error: false,
      provider,
      model
    };
  }
}

// ==================== FONCTION D'EXTRACTION DU TEXTE ====================
function extractTextFromModel(response) {
  // Cas 1: Si la réponse est déjà une chaîne de caractères
  if (typeof response === "string") {
    return response;
  }
  
  // Cas 2: Si c'est notre format standard
  if (response && typeof response === "object" && response.replyText) {
    return response.replyText;
  }
  
  // Cas 3: Si c'est un objet OpenAI/Groq/OpenRouter standard
  if (response?.choices?.[0]?.message?.content) {
    return response.choices[0].message.content;
  }
  
  // Cas 4: Si c'est un objet avec une propriété content
  if (response?.content) {
    return response.content;
  }
  
  // Cas 5: Si c'est un objet avec une propriété text
  if (response?.text) {
    return response.text;
  }
  
  // Cas 6: Si c'est un objet avec une propriété message
  if (response?.message && typeof response.message === "string") {
    return response.message;
  }
  
  // Cas final: réponse vide ou invalide
  return "Je n'ai pas pu générer une réponse. Veuillez réessayer.";
}

// ==================== FONCTIONS DE RECHERCHE ====================
async function searchWebAdvanced(query) {
  try {
    console.log(`🔍 Recherche web: "${query}"`);
    
    // Essayer DuckDuckGo Instant Answer
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const ddgResponse = await axios.get(ddgUrl, { timeout: 10000 });
    
    const results = [];
    
    if (ddgResponse.data?.AbstractText) {
      results.push({
        title: ddgResponse.data.Heading || "Résultat",
        snippet: ddgResponse.data.AbstractText,
        url: ddgResponse.data.AbstractURL || null,
        source: "DuckDuckGo"
      });
    }
    
    return {
      query,
      results: results.slice(0, 5),
      totalResults: results.length,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("❌ Erreur recherche web:", error.message);
    return {
      query,
      results: [],
      totalResults: 0,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

async function searchWikimediaImages(query, limit = 10) {
  try {
    console.log(`🖼️ Recherche d'images: "${query}"`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    
    const response = await axios.get(url, { timeout: 15000 });
    const pages = response.data?.query?.pages;
    
    if (!pages) {
      return [];
    }
    
    const images = Object.values(pages)
      .map((page) => {
        const imageInfo = page.imageinfo?.[0];
        
        return {
          url: imageInfo?.thumburl || imageInfo?.url || null,
          title: page.title || "Sans titre",
          description: imageInfo?.extmetadata?.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
          width: imageInfo?.thumbwidth || null,
          height: imageInfo?.thumbheight || null
        };
      })
      .filter((img) => img.url);
    
    return images;
  } catch (error) {
    console.error("❌ Erreur Wikimedia:", error.message);
    return [];
  }
}

// ==================== GESTION EMAIL ====================
async function sendEmail(to, subject, body, userId = null) {
  try {
    if (!emailTransporter) {
      throw new Error("Serveur SMTP non configuré");
    }
    
    // Validation
    if (!to || !subject || !body) {
      throw new Error("Paramètres email incomplets");
    }
    
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      throw new Error(`Format d'email invalide: ${to}`);
    }
    
    const mailOptions = {
      from: process.env.EMAIL_FROM || `"MILO Assistant" <${process.env.SMTP_USER}>`,
      to,
      subject,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
          <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px 10px 0 0;">
            <h1 style="color: white; margin: 0;">📧 Message de MILO</h1>
          </div>
          <div style="background: #f8f9fa; padding: 20px; border-radius: 0 0 10px 10px;">
            <p style="color: #333; font-size: 14px; line-height: 1.6;">${body.replace(/\n/g, "<br>")}</p>
          </div>
          <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px;">
            <p>Envoyé par MILO - Assistant IA de HIKLON Technologies</p>
          </div>
        </div>
      `,
      text: body
    };
    
    const info = await emailTransporter.sendMail(mailOptions);
    console.log(`✅ Email envoyé à ${to}`);
    
    // Logger
    if (userId) {
      db.run(
        "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, 'sent')",
        [userId, to, subject],
        (err) => { if (err) console.error("❌ Erreur log email:", err.message); }
      );
    }
    
    return { success: true, messageId: info.messageId };
  } catch (error) {
    console.error("❌ Erreur envoi email:", error.message);
    throw error;
  }
}

// ==================== GESTION WHATSAPP ====================
class WhatsAppManager {
  constructor() {
    this.clients = new Map();
    this.maxRetries = 2;
    this.retryDelay = 3000;
  }

  async initClient(userId, retryCount = 0) {
    console.log(`📱 Init WhatsApp pour ${userId}`);
    
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.status === "ready" && existing.client.info) {
        return { connected: true, status: "ready" };
      }
      if (existing.status === "initializing") {
        return { connected: false, status: "initializing" };
      }
    }

    const client = new Client({
      authStrategy: new LocalAuth({ 
        clientId: userId, 
        dataPath: path.join(__dirname, "sessions") 
      }),
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

    const sessionData = { 
      client, 
      qrCode: null, 
      status: "initializing",
      ready: false
    };
    
    this.clients.set(userId, sessionData);

    client.on("qr", async (qr) => {
      try {
        sessionData.qrCode = await qrcode.toDataURL(qr, {
          width: 600,
          margin: 2
        });
        sessionData.status = "waiting_scan";
      } catch (error) {
        console.error("❌ Erreur génération QR:", error.message);
      }
    });

    client.on("ready", () => {
      console.log(`✅ WhatsApp connecté pour ${userId}`);
      sessionData.status = "ready";
      sessionData.ready = true;
      sessionData.qrCode = null;
      
      db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
    });

    client.on("disconnected", (reason) => {
      console.log(`🔌 WhatsApp déconnecté: ${reason}`);
      sessionData.status = "disconnected";
      sessionData.ready = false;
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
    });

    try {
      await client.initialize();
      return { connected: false, status: "initializing" };
    } catch (error) {
      console.error(`❌ Erreur init WhatsApp:`, error.message);
      
      if (retryCount < this.maxRetries) {
        this.clients.delete(userId);
        await new Promise(resolve => setTimeout(resolve, this.retryDelay));
        return this.initClient(userId, retryCount + 1);
      }
      
      this.clients.delete(userId);
      throw error;
    }
  }

  async sendMessage(userId, to, message) {
    const session = this.clients.get(userId);
    if (!session || !session.client || !session.ready) {
      const error = new Error(`WhatsApp non connecté`);
      error.code = "WHATSAPP_NOT_CONNECTED";
      throw error;
    }
    
    try {
      let formattedTo = to.replace(/[^\d]/g, "");
      if (!formattedTo.startsWith("55")) {
        formattedTo = "55" + formattedTo;
      }
      formattedTo = formattedTo + "@c.us";
      
      const chat = await session.client.getChatById(formattedTo);
      await session.client.sendMessage(chat.id._serialized, message);
      
      return { success: true, to };
    } catch (error) {
      console.error(`❌ Erreur envoi WhatsApp:`, error.message);
      throw error;
    }
  }

  getQRCode(userId) {
    const session = this.clients.get(userId);
    return session ? session.qrCode : null;
  }

  getStatus(userId) {
    const session = this.clients.get(userId);
    if (!session) return { connected: false, status: "not_initialized" };
    
    return {
      connected: session.ready,
      status: session.status,
      user: session.ready && session.client.info ? session.client.info.pushname : null
    };
  }

  async logout(userId) {
    const session = this.clients.get(userId);
    if (session && session.client) {
      try {
        await session.client.logout();
        await session.client.destroy();
      } catch (error) {
        console.error("❌ Erreur logout:", error.message);
      }
      this.clients.delete(userId);
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
      return { success: true };
    }
    return { success: false, error: "Session non trouvée" };
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== ROUTES ====================

// Route racine
app.get("/", (req, res) => {
  res.json({ 
    status: "ok", 
    message: "Serveur MILO opérationnel",
    version: "2.0.0",
    timestamp: new Date().toISOString()
  });
});

// Healthcheck
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    services: {
      database: "connected",
      email: emailTransporter ? "configured" : "not_configured",
      groq: process.env.GROQ_API_KEY ? "configured" : "not_configured",
      openrouter: process.env.OPENROUTER_API_KEY ? "configured" : "not_configured"
    },
    uptime: process.uptime(),
    memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB"
  });
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post("/api/chat", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { message, history } = req.body;
    const userId = req.user.id;
    
    // Validation
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ reply: "Le champ 'message' est requis." });
    }
    
    if (message.length > 2000) {
      return res.status(400).json({ reply: "Message trop long (maximum 2000 caractères)." });
    }
    
    console.log(`💬 Chat de ${userId}: "${message.slice(0, 80)}${message.length > 80 ? "..." : ""}"`);
    
    // Appel au LLM
    const aiResult = await callLLMWithFallback(message, history || [], userId);
    
    // Extraire le texte final
    let finalReplyText = aiResult.replyText || extractTextFromModel(aiResult);
    
    // Exécuter l'action si nécessaire
    if (aiResult.systemAction && aiResult.systemAction !== "NONE" && !aiResult.error) {
      try {
        const actionResult = await executeAction(aiResult.systemAction, aiResult.payload, userId);
        if (actionResult.replyText) {
          finalReplyText = actionResult.replyText;
        }
      } catch (actionError) {
        console.error(`❌ Erreur action ${aiResult.systemAction}:`, actionError.message);
        finalReplyText = `Je n'ai pas pu exécuter l'action: ${actionError.message}`;
      }
    }
    
    // Logger
    db.run(
      "INSERT INTO chat_logs (user_id, message, response, system_action, llm_provider) VALUES (?, ?, ?, ?, ?)",
      [userId, message, finalReplyText, aiResult.systemAction || "NONE", aiResult.provider || "unknown"],
      (err) => { if (err) console.error("❌ Erreur log chat:", err.message); }
    );
    
    // RÉPONSE AU FORMAT SIMPLE
    console.log(`✅ Réponse: "${finalReplyText.slice(0, 80)}${finalReplyText.length > 80 ? "..." : ""}"`);
    return res.status(200).json({ reply: finalReplyText });
    
  } catch (error) {
    console.error("❌ Erreur /api/chat:", error.message);
    console.error("   Stack:", error.stack);
    
    return res.status(500).json({ 
      reply: "Une erreur est survenue lors du traitement de votre message. Veuillez réessayer." 
    });
  }
});

// ==================== FONCTION D'EXÉCUTION DES ACTIONS ====================
async function executeAction(action, payload, userId) {
  console.log(`⚡ Action: ${action}`);
  
  switch (action) {
    case "SEARCH_WEB": {
      const query = payload.query;
      if (!query) throw new Error("Paramètre 'query' requis");
      
      const results = await searchWebAdvanced(query);
      const textResults = results.results.map(r => 
        `${r.title}: ${r.snippet}${r.url ? ` (${r.url})` : ""}`
      ).join("\n\n");
      
      return {
        replyText: `Résultats pour "${query}":\n\n${textResults || "Aucun résultat trouvé."}`
      };
    }
    
    case "SEARCH_IMAGES": {
      const query = payload.query;
      if (!query) throw new Error("Paramètre 'query' requis");
      
      const images = await searchWikimediaImages(query, payload.limit || 5);
      if (images.length === 0) {
        return { replyText: `Aucune image trouvée pour "${query}".` };
      }
      
      const imageUrls = images.map(img => img.url).join("\n");
      return {
        replyText: `Voici ${images.length} image(s) pour "${query}":\n\n${imageUrls}`
      };
    }
    
    case "SEND_EMAIL": {
      const { to, subject, body } = payload;
      if (!to || !subject || !body) throw new Error("Paramètres email incomplets");
      
      await sendEmail(to, subject, body, userId);
      return { replyText: `✅ Email envoyé avec succès à ${to}` };
    }
    
    case "SEND_WHATSAPP": {
      const { to, message } = payload;
      if (!to || !message) throw new Error("Paramètres WhatsApp incomplets");
      
      await whatsappManager.sendMessage(userId, to, message);
      return { replyText: `✅ Message WhatsApp envoyé à ${to}` };
    }
    
    default:
      throw new Error(`Action inconnue: ${action}`);
  }
}

// ==================== ROUTES OUTILS ====================
app.post("/api/tools", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const userId = req.user.id;
    
    if (!action) {
      return res.status(400).json({ reply: "Action requise." });
    }
    
    console.log(`🛠️ Outil: ${action}`);
    
    let result;
    
    switch (action) {
      case "search_web": {
        if (!data.query) return res.status(400).json({ reply: "Paramètre 'query' requis." });
        const webResults = await searchWebAdvanced(data.query);
        const textResults = webResults.results.map(r => `${r.title}: ${r.snippet}`).join("\n\n");
        result = { reply: textResults || "Aucun résultat trouvé." };
        break;
      }
      
      case "send_email": {
        const { to, subject, body } = data;
        if (!to || !subject || !body) return res.status(400).json({ reply: "Paramètres incomplets." });
        
        try {
          await sendEmail(to, subject, body, userId);
          result = { reply: `✅ Email envoyé à ${to}` };
        } catch (error) {
          result = { reply: `❌ Erreur: ${error.message}` };
        }
        break;
      }
      
      case "send_whatsapp": {
        const { to, message } = data;
        if (!to || !message) return res.status(400).json({ reply: "Paramètres incomplets." });
        
        try {
          await whatsappManager.sendMessage(userId, to, message);
          result = { reply: `✅ Message envoyé à ${to}` };
        } catch (error) {
          result = { reply: `❌ Erreur: ${error.message}` };
        }
        break;
      }
      
      case "whatsapp_qr": {
        await whatsappManager.initClient(userId);
        
        let qr = null;
        const startTime = Date.now();
        while (!qr && Date.now() - startTime < 30000) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          qr = whatsappManager.getQRCode(userId);
        }
        
        if (qr) {
          result = { reply: "QR Code généré. Scannez-le pour connecter WhatsApp.", qrBase64: qr };
        } else {
          result = { reply: "Délai dépassé. Veuillez réessayer." };
        }
        break;
      }
      
      default:
        return res.status(400).json({ reply: `Action inconnue: ${action}` });
    }
    
    return res.json(result);
    
  } catch (error) {
    console.error("❌ Erreur /api/tools:", error.message);
    return res.status(500).json({ reply: "Erreur lors de l'exécution de l'outil." });
  }
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  res.status(404).json({ 
    reply: "Route non trouvée.",
    path: req.url
  });
});

// ==================== GESTION DES ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express:", err.message);
  
  if (err instanceof SyntaxError && err.status === 400 && "body" in err) {
    return res.status(400).json({ reply: "JSON invalide dans la requête." });
  }
  
  res.status(500).json({ reply: "Erreur interne du serveur." });
});

// ==================== DÉMARRAGE ====================
const PORT = process.env.PORT || 3000;

const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`💾 Base de données : SQLite (WAL mode)`);
  console.log(`🤖 LLM Principal : Groq (${LLM_PROVIDERS.GROQ.model})`);
  console.log(`🔄 LLM Fallback : OpenRouter (${LLM_PROVIDERS.OPENROUTER.model})`);
  console.log(`📧 Email : ${emailTransporter ? "configuré" : "désactivé"}`);
  console.log(`💬 WhatsApp : disponible`);
  console.log("========================================");
});

// Timeouts
server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Arrêt gracieux (${signal})...`);
  
  for (const [userId, session] of whatsappManager.clients) {
    if (session.client) {
      try {
        await session.client.destroy();
      } catch (error) {
        console.error("❌ Erreur fermeture session:", error.message);
      }
    }
  }
  
  if (emailTransporter) {
    emailTransporter.close();
  }
  
  db.close((err) => {
    if (err) console.error("❌ Erreur fermeture DB:", err.message);
    
    server.close(() => {
      console.log("✅ Arrêt terminé");
      process.exit(0);
    });
    
    setTimeout(() => {
      console.error("⚠️ Arrêt forcé");
      process.exit(1);
    }, 10000);
  });
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("uncaughtException", (error) => {
  console.error("❌ Erreur non capturée:", error);
});

process.on("unhandledRejection", (reason) => {
  console.error("❌ Promesse rejetée:", reason);
});

module.exports = app;
