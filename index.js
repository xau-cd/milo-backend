// ==================== INDEX.JS - CERVEAU MILO (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, SQLite, whatsapp-web.js, qrcode, axios, Groq (Principal), OpenRouter (Fallback), Nodemailer
// Format de réponse standardisé : { reply, qrCode (optionnel), images (optionnel), error }
// Version : 4.0.0 - Production Ready

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

// ==================== CONFIGURATION GLOBALE ====================
const CONFIG = {
  PORT: process.env.PORT || 3000,
  ENV: process.env.NODE_ENV || "development",
  VERSION: "4.0.0",
  MAX_MESSAGE_LENGTH: 2000,
  MAX_HISTORY_LENGTH: 10,
  IMAGE_SEARCH_LIMIT: 6,
  WHATSAPP_QR_TIMEOUT: 30000,
  WHATSAPP_RETRY_DELAY: 3000,
  WHATSAPP_MAX_RETRIES: 2,
  DB_PATH: path.join(__dirname, "data", "milo.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions")
};

// ==================== VALIDATION DES VARIABLES D'ENVIRONNEMENT ====================
const requiredEnvVars = ["GROQ_API_KEY", "OPENROUTER_API_KEY"];
const optionalEnvVars = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "EMAIL_FROM", "GROQ_MODEL", "OPENROUTER_MODEL"];

const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.error("=".repeat(60));
  console.error("❌ VARIABLES D'ENVIRONNEMENT OBLIGATOIRES MANQUANTES :");
  missingEnvVars.forEach(varName => {
    console.error(`   - ${varName}`);
  });
  console.error("=".repeat(60));
  console.error("⚠️ Le service d'IA ne fonctionnera pas sans ces clés.");
  console.error("⚠️ Ajoutez-les dans votre fichier .env ou dans les variables d'environnement de Render.");
  console.error("=".repeat(60));
}

const missingOptionalVars = optionalEnvVars.filter(varName => !process.env[varName]);
if (missingOptionalVars.length > 0) {
  console.warn("=".repeat(60));
  console.warn("⚠️ VARIABLES OPTIONNELLES MANQUANTES :");
  missingOptionalVars.forEach(varName => {
    console.warn(`   - ${varName}`);
  });
  console.warn("=".repeat(60));
  console.warn("ℹ️ Certaines fonctionnalités seront désactivées.");
  console.warn("=".repeat(60));
}

// ==================== CRÉATION DES DOSSIERS NÉCESSAIRES ====================
const dataDir = path.join(__dirname, "data");
const sessionsDir = path.join(__dirname, "sessions");

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
  console.log(`📁 Dossier de données créé: ${dataDir}`);
}

if (!fs.existsSync(sessionsDir)) {
  fs.mkdirSync(sessionsDir, { recursive: true });
  console.log(`📁 Dossier de sessions WhatsApp créé: ${sessionsDir}`);
}

// ==================== INITIALISATION SQLITE ====================
const db = new sqlite3.Database(CONFIG.DB_PATH);

// Activation du mode WAL pour de meilleures performances
db.run("PRAGMA journal_mode = WAL;", (err) => {
  if (err) {
    console.error("❌ Erreur activation WAL:", err.message);
  } else {
    console.log("✅ Mode WAL activé pour SQLite");
  }
});

// Optimisations SQLite
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -32000;"); // 32MB de cache
db.run("PRAGMA busy_timeout = 5000;"); // 5 secondes de timeout
db.run("PRAGMA temp_store = MEMORY;"); // Stockage temporaire en mémoire
db.run("PRAGMA foreign_keys = ON;"); // Activation des clés étrangères

// Création des tables
db.serialize(() => {
  // Table des utilisateurs
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE,
      display_name TEXT,
      whatsapp_connected INTEGER DEFAULT 0,
      whatsapp_session_id TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Table des logs de chat
  db.run(`
    CREATE TABLE IF NOT EXISTS chat_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      message TEXT,
      response TEXT,
      system_action TEXT,
      llm_provider TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Table des logs d'outils
  db.run(`
    CREATE TABLE IF NOT EXISTS tool_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      action TEXT,
      result_type TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Table des sessions WhatsApp
  db.run(`
    CREATE TABLE IF NOT EXISTS whatsapp_sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      session_id TEXT UNIQUE,
      status TEXT DEFAULT 'pending',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Table des emails envoyés
  db.run(`
    CREATE TABLE IF NOT EXISTS email_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      to_email TEXT,
      subject TEXT,
      status TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Table des métriques LLM
  db.run(`
    CREATE TABLE IF NOT EXISTS llm_metrics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider TEXT,
      model TEXT,
      response_time INTEGER,
      status TEXT,
      error_message TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  console.log("✅ Tables SQLite créées avec succès");
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
    maxMessages: 50,
    connectionTimeout: 10000,
    greetingTimeout: 10000,
    socketTimeout: 15000
  });

  emailTransporter.verify((error, success) => {
    if (error) {
      console.error("❌ Erreur de connexion SMTP:", error.message);
      console.warn("⚠️ L'envoi d'emails sera désactivé.");
      emailTransporter = null;
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
      "https://milo-hiklon.web.app",
      "http://localhost:3000",
      "http://localhost:8080",
      "http://localhost:5173",
      "http://localhost:5000",
      "http://127.0.0.1:3000",
      "http://127.0.0.1:8080"
    ];
    
    if (!origin || allowedOrigins.includes(origin) || CONFIG.ENV === "development") {
      callback(null, true);
    } else {
      console.warn(`⚠️ Origine non autorisée par CORS: ${origin}`);
      callback(new Error("Origine non autorisée par CORS"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id"],
  credentials: true,
  maxAge: 86400,
  preflightContinue: false,
  optionsSuccessStatus: 204
};

app.use(cors(corsOptions));

// Configuration Helmet pour la sécurité
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: false,
    hsts: {
      maxAge: 31536000,
      includeSubDomains: true,
      preload: true
    }
  })
);

// Parsing du corps des requêtes
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // 100 requêtes par fenêtre
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    reply: "⚠️ Trop de requêtes. Veuillez réessayer dans 15 minutes.",
    error: true,
    retryAfter: 900
  },
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit dépassé pour ${req.ip} sur ${req.url}`);
    return res.status(429).json({
      reply: "⚠️ Trop de requêtes. Veuillez réessayer dans 15 minutes.",
      error: true
    });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 heure
  max: 30, // 30 requêtes par heure
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit strict dépassé pour ${req.ip} sur ${req.url}`);
    return res.status(429).json({
      reply: "⚠️ Limite de requêtes atteinte pour cette action. Veuillez réessayer dans une heure.",
      error: true
    });
  }
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();
  req.requestId = requestId;
  
  const timestamp = new Date().toISOString();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`📥 [${timestamp}] Requête reçue`);
  console.log(`   ID: ${requestId}`);
  console.log(`   Méthode: ${req.method}`);
  console.log(`   URL: ${req.url}`);
  console.log(`   IP: ${req.ip}`);
  console.log(`   User-Agent: ${req.headers["user-agent"]?.slice(0, 100) || "inconnu"}`);
  
  if (req.body && Object.keys(req.body).length > 0) {
    console.log(`   Body: ${JSON.stringify(req.body).slice(0, 500)}`);
  }
  
  res.on("finish", () => {
    const duration = Date.now() - start;
    console.log(`📤 [${timestamp}] Réponse envoyée`);
    console.log(`   ID: ${requestId}`);
    console.log(`   Status: ${res.statusCode}`);
    console.log(`   Durée: ${duration}ms`);
    console.log(`${"=".repeat(60)}\n`);
  });
  
  next();
});

// ==================== MIDDLEWARE D'AUTHENTIFICATION ====================
const authenticateUser = (req, res, next) => {
  try {
    const userId = req.body.userId || req.query.userId || req.headers["x-user-id"];
    
    if (!userId) {
      console.warn(`⚠️ Tentative d'accès sans authentification`);
      return res.status(200).json({
        reply: "⚠️ Authentification requise. Veuillez fournir un userId.",
        error: true
      });
    }
    
    // Vérifier que l'utilisateur existe dans la base de données
    db.get("SELECT * FROM users WHERE id = ?", [userId], (err, user) => {
      if (err) {
        console.error("❌ Erreur DB lors de l'authentification:", err.message);
        return res.status(200).json({
          reply: "⚠️ Erreur interne du serveur lors de l'authentification.",
          error: true
        });
      }
      
      if (!user) {
        // Créer l'utilisateur s'il n'existe pas
        const displayName = req.body.displayName || userId;
        db.run(
          "INSERT INTO users (id, display_name, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
          [userId, displayName],
          (insertErr) => {
            if (insertErr) {
              console.error("❌ Erreur création utilisateur:", insertErr.message);
              return res.status(200).json({
                reply: "⚠️ Erreur lors de la création de l'utilisateur.",
                error: true
              });
            }
            console.log(`✅ Nouvel utilisateur créé: ${userId}`);
            req.user = { id: userId, displayName };
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
    return res.status(200).json({
      reply: "⚠️ Erreur d'authentification.",
      error: true
    });
  }
};

// ==================== SYSTEM PROMPT MILO ====================
const MILO_SYSTEM_PROMPT = {
  role: "system",
  content: `Tu es MILO, un assistant IA intelligent, chaleureux et proactif développé par HIKLON Technology.

RÈGLE STRICTE SUR LES IMAGES : Dès que tu décris ou présentes une personnalité, un lieu, un objet, un concept scientifique ou un événement, tu DOIS obligatoirement et automatiquement utiliser ton outil de recherche d'images pour illustrer ta réponse. N'attends JAMAIS que l'utilisateur te demande explicitement une photo ou une image.

RÈGLE DE CONFIDENTIALITÉ :
Tu ne dois jamais expliquer ton fonctionnement interne ni citer les technologies que tu utilises.
Tu présentes toutes tes fonctions comme tes capacités natives HIKLON.

FORMAT DE RÉPONSE OBLIGATOIRE :
Tu DOIS TOUJOURS répondre au format JSON strict :
{
  "replyText": "Ta réponse complète en Markdown",
  "systemAction": "ACTION_TYPE",
  "payload": {}
}

ACTIONS DISPONIBLES :
- "NONE" : Réponse sans action particulière
- "SEARCH_IMAGES" : Rechercher des images pour illustrer (payload: { "query": "sujet à illustrer" })
- "SEND_EMAIL" : Envoyer un email (payload: { "to", "subject", "body" })
- "SEND_WHATSAPP" : Envoyer un WhatsApp (payload: { "to", "message" })
- "SEARCH_WEB" : Rechercher sur le web (payload: { "query" })

EXEMPLES D'ILLUSTRATION AUTOMATIQUE :
1. Utilisateur: "Parle-moi de Cristiano Ronaldo"
   Réponse: {
     "replyText": "Cristiano Ronaldo est un footballeur portugais légendaire...",
     "systemAction": "SEARCH_IMAGES",
     "payload": { "query": "Cristiano Ronaldo portrait" }
   }

2. Utilisateur: "C'est quoi un virus ?"
   Réponse: {
     "replyText": "Un virus est un agent infectieux microscopique...",
     "systemAction": "SEARCH_IMAGES",
     "payload": { "query": "virus microscope" }
   }

3. Utilisateur: "Montre-moi la Tour Eiffel"
   Réponse: {
     "replyText": "La Tour Eiffel est un monument emblématique de Paris...",
     "systemAction": "SEARCH_IMAGES",
     "payload": { "query": "Tour Eiffel Paris" }
   }

RÈGLES SUPPLÉMENTAIRES :
- Sois toujours précis et factuel dans tes réponses.
- Utilise le Markdown pour formater tes réponses (gras, italique, listes, titres).
- Si tu ne sais pas quelque chose, dis-le honnêtement.
- Reste toujours courtois et professionnel.
- Adapte ton langage au niveau de l'utilisateur.`
};

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
  
  if (!provider.apiKey) {
    throw new Error("GROQ_API_KEY non configurée");
  }
  
  const messages = [
    MILO_SYSTEM_PROMPT,
    ...history.slice(-CONFIG.MAX_HISTORY_LENGTH).map((m) => ({
      role: m.role || "user",
      content: typeof m.content === "string" ? m.content : m.message || ""
    })),
    { role: "user", content: userMessage }
  ];
  
  const startTime = Date.now();
  
  try {
    console.log(`🚀 Appel Groq (${provider.model})...`);
    
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
    console.log(`✅ Groq a répondu en ${responseTime}ms`);
    
    // Logger la métrique
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, responseTime],
      (err) => {
        if (err) console.error("❌ Erreur log métrique:", err.message);
      }
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
    
    if (error.response) {
      console.error(`   Status: ${error.response.status}`);
      console.error(`   Data: ${JSON.stringify(error.response.data).slice(0, 300)}`);
    }
    
    // Logger l'échec
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, responseTime, error.message],
      (err) => {
        if (err) console.error("❌ Erreur log métrique:", err.message);
      }
    );
    
    throw error;
  }
}

// ==================== FONCTION POUR APPELER OPENROUTER ====================
async function callOpenRouter(userMessage, history = []) {
  const provider = LLM_PROVIDERS.OPENROUTER;
  
  if (!provider.apiKey) {
    throw new Error("OPENROUTER_API_KEY non configurée");
  }
  
  const messages = [
    MILO_SYSTEM_PROMPT,
    ...history.slice(-CONFIG.MAX_HISTORY_LENGTH).map((m) => ({
      role: m.role || "user",
      content: typeof m.content === "string" ? m.content : m.message || ""
    })),
    { role: "user", content: userMessage }
  ];
  
  const startTime = Date.now();
  
  try {
    console.log(`🔄 Appel OpenRouter (${provider.model})...`);
    
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
    console.log(`✅ OpenRouter a répondu en ${responseTime}ms`);
    
    // Logger la métrique
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status) VALUES (?, ?, ?, 'success')",
      [provider.name, provider.model, responseTime],
      (err) => {
        if (err) console.error("❌ Erreur log métrique:", err.message);
      }
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
    
    if (error.response) {
      console.error(`   Status: ${error.response.status}`);
      console.error(`   Data: ${JSON.stringify(error.response.data).slice(0, 300)}`);
    }
    
    // Logger l'échec
    db.run(
      "INSERT INTO llm_metrics (provider, model, response_time, status, error_message) VALUES (?, ?, ?, 'failed', ?)",
      [provider.name, provider.model, responseTime, error.message],
      (err) => {
        if (err) console.error("❌ Erreur log métrique:", err.message);
      }
    );
    
    throw error;
  }
}

// ==================== FONCTION PRINCIPALE AVEC FALLBACK ====================
async function callLLMWithFallback(userMessage, history = []) {
  console.log(`🤖 Traitement LLM pour le message: "${userMessage.slice(0, 80)}..."`);
  
  // Essayer Groq en premier
  try {
    const groqResult = await callGroq(userMessage, history);
    return groqResult;
  } catch (groqError) {
    console.warn(`⚠️ Groq a échoué: ${groqError.message}`);
    console.warn(`🔄 Basculement automatique vers OpenRouter...`);
    
    // Fallback vers OpenRouter
    try {
      const openRouterResult = await callOpenRouter(userMessage, history);
      console.log(`✅ Fallback OpenRouter réussi`);
      return openRouterResult;
    } catch (openRouterError) {
      console.error(`❌ OpenRouter a également échoué: ${openRouterError.message}`);
      throw new Error("Tous les fournisseurs LLM ont échoué");
    }
  }
}

// ==================== PARSER LA RÉPONSE JSON ====================
function parseLLMResponse(content, provider) {
  try {
    let cleanContent = content.trim();
    
    // Nettoyer les backticks si présents
    if (cleanContent.startsWith("```json")) {
      cleanContent = cleanContent.replace(/```json\n?/g, "").replace(/```\n?/g, "");
    } else if (cleanContent.startsWith("```")) {
      cleanContent = cleanContent.replace(/```\n?/g, "");
    }
    
    // Parser le JSON
    const parsed = JSON.parse(cleanContent);
    
    return {
      replyText: parsed.replyText || "Je n'ai pas pu générer une réponse.",
      systemAction: parsed.systemAction || "NONE",
      payload: parsed.payload || {},
      provider
    };
  } catch (error) {
    console.error("❌ Erreur parsing JSON:", error.message);
    console.error("   Contenu brut:", content.slice(0, 300));
    
    // Fallback : retourner le contenu brut comme texte
    return {
      replyText: content.replace(/```json\n?|\n?```/g, "").trim(),
      systemAction: "NONE",
      payload: {},
      provider
    };
  }
}

// ==================== RECHERCHE D'IMAGES ====================
async function searchWikimediaImages(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  try {
    console.log(`🖼️ Recherche d'images Wikimedia: "${query}" (limite: ${limit})`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        "User-Agent": "MILO/4.0.0 (HIKLON Technology)"
      }
    });
    
    const pages = response.data?.query?.pages;
    
    if (!pages) {
      console.log("   Aucune image trouvée");
      return [];
    }
    
    const images = Object.values(pages)
      .map((page) => {
        const imageInfo = page.imageinfo?.[0];
        const extmetadata = imageInfo?.extmetadata || {};
        
        return {
          url: imageInfo?.thumburl || imageInfo?.url || null,
          thumbnail: imageInfo?.thumburl || null,
          title: page.title || "Image",
          description: extmetadata.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
          width: imageInfo?.thumbwidth || null,
          height: imageInfo?.thumbheight || null,
          pageUrl: imageInfo?.descriptionurl || null
        };
      })
      .filter((img) => img.url);
    
    console.log(`   ${images.length} image(s) trouvée(s)`);
    return images;
  } catch (error) {
    console.error("❌ Erreur recherche images:", error.message);
    return [];
  }
}

// ==================== RECHERCHE WEB ====================
async function searchWebAdvanced(query) {
  try {
    console.log(`🔍 Recherche web: "${query}"`);
    
    // Essayer DuckDuckGo
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const ddgResponse = await axios.get(ddgUrl, {
      timeout: 10000,
      headers: {
        "User-Agent": "MILO/4.0.0 (HIKLON Technology)"
      }
    });
    
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
    
    console.log(`📧 Envoi email à ${to}...`);
    
    const mailOptions = {
      from: process.env.EMAIL_FROM || `"MILO Assistant" <${process.env.SMTP_USER}>`,
      to,
      subject,
      html: `
        <!DOCTYPE html>
        <html lang="fr">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>${subject}</title>
        </head>
        <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: #f5f5f5;">
          <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0; text-align: center;">
              <h1 style="color: white; margin: 0; font-size: 24px;">📧 Message de MILO</h1>
              <p style="color: #e0e0e0; margin: 10px 0 0; font-size: 14px;">Assistant IA de HIKLON Technologies</p>
            </div>
            <div style="background: white; padding: 30px; border-radius: 0 0 10px 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
              <h2 style="color: #333; margin: 0 0 20px; font-size: 20px;">${subject}</h2>
              <div style="color: #555; font-size: 14px; line-height: 1.6;">
                ${body.replace(/\n/g, "<br>")}
              </div>
            </div>
            <div style="margin-top: 20px; text-align: center; color: #999; font-size: 12px;">
              <p>Cet email a été envoyé par MILO, votre assistant IA personnel.</p>
              <p>© ${new Date().getFullYear()} HIKLON Technologies. Tous droits réservés.</p>
            </div>
          </div>
        </body>
        </html>
      `,
      text: body
    };
    
    const info = await emailTransporter.sendMail(mailOptions);
    console.log(`✅ Email envoyé à ${to} (Message ID: ${info.messageId})`);
    
    // Logger l'envoi
    if (userId) {
      db.run(
        "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, 'sent')",
        [userId, to, subject],
        (err) => {
          if (err) console.error("❌ Erreur log email:", err.message);
        }
      );
    }
    
    return {
      success: true,
      messageId: info.messageId,
      to,
      subject,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("❌ Erreur envoi email:", error.message);
    
    // Logger l'échec
    if (userId) {
      db.run(
        "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, 'failed')",
        [userId, to || "inconnu", subject || "sans sujet"],
        (err) => {
          if (err) console.error("❌ Erreur log email:", err.message);
        }
      );
    }
    
    throw error;
  }
}

// ==================== GESTION WHATSAPP ====================
class WhatsAppManager {
  constructor() {
    this.clients = new Map();
    this.maxRetries = CONFIG.WHATSAPP_MAX_RETRIES;
    this.retryDelay = CONFIG.WHATSAPP_RETRY_DELAY;
  }

  async initClient(userId, retryCount = 0) {
    console.log(`📱 Initialisation WhatsApp pour ${userId} (tentative ${retryCount + 1}/${this.maxRetries + 1})`);
    
    // Vérifier si le client existe déjà
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.status === "ready" && existing.client.info) {
        console.log(`   WhatsApp déjà connecté pour ${userId}`);
        return {
          connected: true,
          status: "ready",
          qrCode: null
        };
      }
      if (existing.status === "initializing") {
        console.log(`   Initialisation déjà en cours pour ${userId}`);
        return {
          connected: false,
          status: "initializing",
          qrCode: null
        };
      }
    }

    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: userId,
        dataPath: CONFIG.SESSIONS_PATH
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
          '--disable-gpu',
          '--disable-software-rasterizer',
          '--disable-extensions'
        ]
      }
    });

    const sessionData = {
      client,
      qrCode: null,
      status: "initializing",
      ready: false,
      initAttempts: 0
    };

    this.clients.set(userId, sessionData);

    // Event: QR Code généré
    client.on("qr", async (qr) => {
      try {
        console.log(`📱 QR Code généré pour ${userId}`);
        const qrDataUrl = await qrcode.toDataURL(qr, {
          width: 600,
          margin: 2,
          color: {
            dark: "#000000",
            light: "#FFFFFF"
          }
        });
        sessionData.qrCode = qrDataUrl;
        sessionData.status = "waiting_scan";
        
        // Mettre à jour la base de données
        db.run(
          "INSERT OR REPLACE INTO whatsapp_sessions (user_id, session_id, status, updated_at) VALUES (?, ?, 'waiting_scan', CURRENT_TIMESTAMP)",
          [userId, userId],
          (err) => {
            if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
          }
        );
      } catch (error) {
        console.error("❌ Erreur génération QR:", error.message);
      }
    });

    // Event: WhatsApp prêt
    client.on("ready", () => {
      console.log(`✅ WhatsApp connecté pour ${userId}`);
      sessionData.status = "ready";
      sessionData.ready = true;
      sessionData.qrCode = null;
      
      db.run(
        "UPDATE users SET whatsapp_connected = 1, whatsapp_session_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [userId, userId],
        (err) => {
          if (err) console.error("❌ Erreur DB mise à jour WhatsApp:", err.message);
        }
      );
      
      db.run(
        "INSERT OR REPLACE INTO whatsapp_sessions (user_id, session_id, status, updated_at) VALUES (?, ?, 'connected', CURRENT_TIMESTAMP)",
        [userId, userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
    });

    // Event: Authentifié
    client.on("authenticated", () => {
      console.log(`🔐 WhatsApp authentifié pour ${userId}`);
      sessionData.status = "authenticated";
    });

    // Event: Échec d'authentification
    client.on("auth_failure", (msg) => {
      console.error(`❌ Échec auth WhatsApp ${userId}:`, msg);
      sessionData.qrCode = null;
      sessionData.status = "failed";
      sessionData.ready = false;
    });

    // Event: Déconnexion
    client.on("disconnected", (reason) => {
      console.log(`🔌 WhatsApp déconnecté pour ${userId}: ${reason}`);
      sessionData.status = "disconnected";
      sessionData.ready = false;
      sessionData.qrCode = null;
      
      db.run(
        "UPDATE users SET whatsapp_connected = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB déconnexion WhatsApp:", err.message);
        }
      );
      
      db.run(
        "UPDATE whatsapp_sessions SET status = 'disconnected', updated_at = CURRENT_TIMESTAMP WHERE session_id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
      
      // Nettoyer après un délai
      setTimeout(() => {
        if (this.clients.has(userId) && this.clients.get(userId).status === "disconnected") {
          this.clients.delete(userId);
          console.log(`🗑️ Session WhatsApp supprimée pour ${userId}`);
        }
      }, 60000);
    });

    // Initialiser le client
    try {
      await client.initialize();
      console.log(`✅ Initialisation lancée pour ${userId}`);
      return {
        connected: false,
        status: "initializing",
        qrCode: null
      };
    } catch (error) {
      console.error(`❌ Erreur init WhatsApp pour ${userId}:`, error.message);
      
      if (retryCount < this.maxRetries) {
        console.log(`🔄 Retry ${retryCount + 1}/${this.maxRetries} dans ${this.retryDelay}ms`);
        this.clients.delete(userId);
        await new Promise(resolve => setTimeout(resolve, this.retryDelay));
        return this.initClient(userId, retryCount + 1);
      }
      
      this.clients.delete(userId);
      throw error;
    }
  }

  async sendMessage(userId, to, message) {
    console.log(`📤 Envoi WhatsApp de ${userId} à ${to}`);
    
    const session = this.clients.get(userId);
    if (!session || !session.client || !session.ready) {
      const error = new Error(`WhatsApp non connecté pour ${userId}`);
      error.code = "WHATSAPP_NOT_CONNECTED";
      throw error;
    }
    
    try {
      // Formater le numéro de téléphone
      let formattedTo = to.replace(/[^\d]/g, "");
      if (!formattedTo.startsWith("55")) {
        formattedTo = "55" + formattedTo;
      }
      formattedTo = formattedTo + "@c.us";
      
      console.log(`   Numéro formaté: ${formattedTo}`);
      
      // Vérifier que le chat existe
      const chat = await session.client.getChatById(formattedTo);
      
      // Envoyer le message
      const result = await session.client.sendMessage(chat.id._serialized, message);
      
      console.log(`✅ Message WhatsApp envoyé à ${to}`);
      
      // Logger l'envoi
      db.run(
        "INSERT INTO tool_logs (user_id, action, result_type, created_at) VALUES (?, ?, 'whatsapp_sent', CURRENT_TIMESTAMP)",
        [userId, `send_whatsapp_${to}`],
        (err) => {
          if (err) console.error("❌ Erreur logging WhatsApp:", err.message);
        }
      );
      
      return {
        success: true,
        to,
        messageId: result.id?._serialized || null,
        timestamp: new Date().toISOString()
      };
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
    if (!session) {
      return {
        connected: false,
        status: "not_initialized",
        qrAvailable: false
      };
    }
    
    return {
      connected: session.ready,
      status: session.status,
      qrAvailable: !!session.qrCode,
      user: session.ready && session.client.info ? session.client.info.pushname || null : null
    };
  }

  async logout(userId) {
    console.log(`🔌 Déconnexion WhatsApp pour ${userId}`);
    
    const session = this.clients.get(userId);
    if (session && session.client) {
      try {
        await session.client.logout();
        await session.client.destroy();
        console.log(`✅ WhatsApp déconnecté pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur déconnexion WhatsApp:`, error.message);
      }
      
      this.clients.delete(userId);
      
      db.run(
        "UPDATE users SET whatsapp_connected = 0, whatsapp_session_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB logout WhatsApp:", err.message);
        }
      );
      
      db.run(
        "UPDATE whatsapp_sessions SET status = 'logged_out', updated_at = CURRENT_TIMESTAMP WHERE session_id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
      
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
    reply: "✅ Serveur MILO opérationnel",
    error: false,
    data: {
      version: CONFIG.VERSION,
      timestamp: new Date().toISOString()
    }
  });
});

// Healthcheck
app.get("/api/health", (req, res) => {
  res.json({
    reply: "✅ Serveur MILO en bonne santé",
    error: false,
    data: {
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      version: CONFIG.VERSION,
      services: {
        database: "connected",
        groq: process.env.GROQ_API_KEY ? "configured" : "not_configured",
        openrouter: process.env.OPENROUTER_API_KEY ? "configured" : "not_configured",
        email: emailTransporter ? "configured" : "not_configured",
        whatsapp: "available"
      },
      memory: {
        usage: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + "MB",
        total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024) + "MB",
        rss: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB"
      }
    }
  });
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post("/api/chat", apiLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 500));
    
    // Validation explicite
    if (!req.body || !req.body.message) {
      return res.status(200).json({
        reply: "⚠️ Le paramètre 'message' est obligatoire.",
        error: true
      });
    }
    
    const { message, history } = req.body;
    const userId = req.user.id;
    
    if (typeof message !== "string" || message.trim().length === 0) {
      return res.status(200).json({
        reply: "⚠️ Le message ne peut pas être vide.",
        error: true
      });
    }
    
    if (message.length > CONFIG.MAX_MESSAGE_LENGTH) {
      return res.status(200).json({
        reply: `⚠️ Message trop long (maximum ${CONFIG.MAX_MESSAGE_LENGTH} caractères).`,
        error: true
      });
    }
    
    console.log(`💬 Chat de ${userId}: "${message.slice(0, 80)}${message.length > 80 ? "..." : ""}"`);
    
    // Appel au LLM avec fallback
    const llmResult = await callLLMWithFallback(message, history || []);
    const aiResult = parseLLMResponse(llmResult.content, llmResult.provider);
    
    let finalReply = aiResult.replyText;
    let imageUrls = [];
    
    // Exécuter l'action automatiquement
    if (aiResult.systemAction === "SEARCH_IMAGES" && aiResult.payload?.query) {
      try {
        console.log(`🖼️ Illustration automatique: "${aiResult.payload.query}"`);
        const images = await searchWikimediaImages(aiResult.payload.query, 4);
        
        if (images.length > 0) {
          imageUrls = images.map(img => img.url);
          
          // Ajouter les images en Markdown à la réponse
          const imageMarkdown = images.map((img, index) => {
            return `![Image ${index + 1}](${img.url})`;
          }).join("\n\n");
          
          finalReply += `\n\n---\n\n📷 **Illustrations :**\n\n${imageMarkdown}`;
        }
      } catch (imageError) {
        console.error("❌ Erreur illustration:", imageError.message);
        // Ne pas bloquer la réponse si les images échouent
      }
    }
    
    // Logger le chat
    db.run(
      "INSERT INTO chat_logs (user_id, message, response, system_action, llm_provider) VALUES (?, ?, ?, ?, ?)",
      [userId, message, finalReply, aiResult.systemAction || "NONE", aiResult.provider || "unknown"],
      (err) => {
        if (err) console.error("❌ Erreur log chat:", err.message);
      }
    );
    
    // Construire la réponse
    const responseObj = {
      reply: finalReply,
      error: false
    };
    
    // Ajouter les images si disponibles
    if (imageUrls.length > 0) {
      responseObj.images = imageUrls;
    }
    
    console.log(`✅ Réponse envoyée: "${finalReply.slice(0, 80)}${finalReply.length > 80 ? "..." : ""}"`);
    return res.status(200).json(responseObj);
    
  } catch (error) {
    console.error("❌ Erreur /api/chat:", error.message);
    console.error("   Stack:", error.stack);
    
    return res.status(200).json({
      reply: "⚠️ Une erreur est survenue lors du traitement de votre message. Veuillez réessayer.",
      error: true
    });
  }
});

// ==================== ROUTE CONNEXION WHATSAPP ====================
app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 300));
    
    const userId = req.user.id;
    
    // Initialiser le client WhatsApp
    const initResult = await whatsappManager.initClient(userId);
    
    if (initResult.connected) {
      return res.status(200).json({
        reply: "✅ WhatsApp est déjà connecté.",
        error: false
      });
    }
    
    // Attendre le QR code de manière asynchrone
    let qrCode = null;
    const startTime = Date.now();
    
    while (!qrCode && Date.now() - startTime < CONFIG.WHATSAPP_QR_TIMEOUT) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      qrCode = whatsappManager.getQRCode(userId);
    }
    
    if (qrCode) {
      console.log(`✅ QR Code prêt pour ${userId}`);
      return res.status(200).json({
        reply: "📱 Scannez ce QR Code avec WhatsApp pour vous connecter :",
        qrCode: qrCode,
        error: false
      });
    } else {
      return res.status(200).json({
        reply: "⚠️ Délai dépassé pour la génération du QR Code. Veuillez réessayer.",
        error: true
      });
    }
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/connect:", error.message);
    console.error("   Stack:", error.stack);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de la connexion WhatsApp. Veuillez réessayer.",
      error: true
    });
  }
});

// ==================== ROUTE ENVOI WHATSAPP ====================
app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 300));
    
    if (!req.body || !req.body.to || !req.body.message) {
      return res.status(200).json({
        reply: "⚠️ Les paramètres 'to' et 'message' sont obligatoires.",
        error: true
      });
    }
    
    const { to, message } = req.body;
    const userId = req.user.id;
    
    try {
      const result = await whatsappManager.sendMessage(userId, to, message);
      return res.status(200).json({
        reply: `✅ Message WhatsApp envoyé avec succès à ${to}`,
        error: false,
        data: result
      });
    } catch (whatsappError) {
      if (whatsappError.code === "WHATSAPP_NOT_CONNECTED") {
        return res.status(200).json({
          reply: "⚠️ WhatsApp n'est pas connecté. Utilisez /api/whatsapp/connect pour générer un QR Code.",
          error: true
        });
      }
      throw whatsappError;
    }
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/send:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de l'envoi du message WhatsApp.",
      error: true
    });
  }
});

// ==================== ROUTE CHAT WHATSAPP ====================
app.post("/api/whatsapp/chat", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 300));
    
    if (!req.body || !req.body.message) {
      return res.status(200).json({
        reply: "⚠️ Le paramètre 'message' est obligatoire.",
        error: true
      });
    }
    
    const { message } = req.body;
    
    // Traiter avec le LLM
    const llmResult = await callLLMWithFallback(message, []);
    const aiResult = parseLLMResponse(llmResult.content, llmResult.provider);
    
    return res.status(200).json({
      reply: aiResult.replyText,
      error: false
    });
    
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/chat:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors du traitement du message WhatsApp.",
      error: true
    });
  }
});

// ==================== ROUTE EMAIL CHAT ====================
app.post("/api/email/chat", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 300));
    
    if (!req.body || !req.body.to || !req.body.subject || !req.body.body) {
      return res.status(200).json({
        reply: "⚠️ Les paramètres 'to', 'subject' et 'body' sont obligatoires.",
        error: true
      });
    }
    
    const { to, subject, body } = req.body;
    const userId = req.user.id;
    
    try {
      const emailResult = await sendEmail(to, subject, body, userId);
      return res.status(200).json({
        reply: `✅ Email envoyé avec succès à ${to}`,
        error: false,
        data: emailResult
      });
    } catch (emailError) {
      return res.status(200).json({
        reply: `⚠️ Erreur lors de l'envoi de l'email: ${emailError.message}`,
        error: true
      });
    }
    
  } catch (error) {
    console.error("❌ Erreur /api/email/chat:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de l'envoi de l'email.",
      error: true
    });
  }
});

// ==================== ROUTE ENVOI EMAIL ====================
app.post("/api/email/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    console.log("Payload reçu :", JSON.stringify(req.body).slice(0, 300));
    
    if (!req.body || !req.body.to || !req.body.subject || !req.body.body) {
      return res.status(200).json({
        reply: "⚠️ Les paramètres 'to', 'subject' et 'body' sont obligatoires.",
        error: true
      });
    }
    
    const { to, subject, body } = req.body;
    const userId = req.user.id;
    
    try {
      const emailResult = await sendEmail(to, subject, body, userId);
      return res.status(200).json({
        reply: `✅ Email envoyé avec succès à ${to}`,
        error: false,
        data: emailResult
      });
    } catch (emailError) {
      return res.status(200).json({
        reply: `⚠️ Erreur lors de l'envoi: ${emailError.message}`,
        error: true
      });
    }
    
  } catch (error) {
    console.error("❌ Erreur /api/email/send:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de l'envoi de l'email.",
      error: true
    });
  }
});

// ==================== ROUTE STATUS WHATSAPP ====================
app.get("/api/whatsapp/status", authenticateUser, async (req, res) => {
  try {
    const userId = req.user.id;
    const status = whatsappManager.getStatus(userId);
    
    return res.status(200).json({
      reply: status.connected ? "✅ WhatsApp est connecté." : "⚠️ WhatsApp n'est pas connecté.",
      error: false,
      data: status
    });
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/status:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de la vérification du statut WhatsApp.",
      error: true
    });
  }
});

// ==================== ROUTE DÉCONNEXION WHATSAPP ====================
app.post("/api/whatsapp/logout", authenticateUser, async (req, res) => {
  try {
    const userId = req.user.id;
    const result = await whatsappManager.logout(userId);
    
    return res.status(200).json({
      reply: result.success ? "✅ WhatsApp déconnecté." : "⚠️ Erreur lors de la déconnexion.",
      error: !result.success,
      data: result
    });
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/logout:", error.message);
    
    return res.status(200).json({
      reply: "⚠️ Erreur lors de la déconnexion WhatsApp.",
      error: true
    });
  }
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée: ${req.method} ${req.url}`);
  
  res.status(404).json({
    reply: "⚠️ Route non trouvée.",
    error: true,
    data: {
      path: req.url,
      method: req.method,
      timestamp: new Date().toISOString()
    }
  });
});

// ==================== GESTION DES ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express:", err.message);
  console.error("   Stack:", err.stack);
  
  if (err instanceof SyntaxError && err.status === 400 && "body" in err) {
    return res.status(400).json({
      reply: "⚠️ JSON invalide dans la requête.",
      error: true
    });
  }
  
  res.status(500).json({
    reply: "⚠️ Erreur interne du serveur.",
    error: true
  });
});

// ==================== DÉMARRAGE DU SERVEUR ====================
const PORT = CONFIG.PORT;

const server = app.listen(PORT, () => {
  console.log("\n" + "=".repeat(60));
  console.log("🚀 SERVEUR MILO DÉMARRÉ");
  console.log("=".repeat(60));
  console.log(`📅 Date de démarrage : ${new Date().toISOString()}`);
  console.log(`🔢 Version : ${CONFIG.VERSION}`);
  console.log(`🌍 Environnement : ${CONFIG.ENV}`);
  console.log(`🔌 Port : ${PORT}`);
  console.log("-".repeat(60));
  console.log(`🤖 LLM Principal : Groq (${LLM_PROVIDERS.GROQ.model})`);
  console.log(`🔄 LLM Fallback : OpenRouter (${LLM_PROVIDERS.OPENROUTER.model})`);
  console.log(`📧 Email SMTP : ${emailTransporter ? "configuré" : "désactivé"}`);
  console.log(`💬 WhatsApp : disponible`);
  console.log(`🖼️ Illustration automatique : activée`);
  console.log(`💾 Base de données : SQLite (WAL mode)`);
  console.log("=".repeat(60) + "\n");
});

// Configuration des timeouts
server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`\n🛑 Signal ${signal} reçu. Arrêt gracieux en cours...`);
  
  // Fermer toutes les sessions WhatsApp
  for (const [userId, session] of whatsappManager.clients) {
    if (session.client) {
      try {
        await session.client.destroy();
        console.log(`👋 Session WhatsApp fermée pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur fermeture session ${userId}:`, error.message);
      }
    }
  }
  
  // Fermer la connexion email
  if (emailTransporter) {
    emailTransporter.close();
    console.log("📧 Connexion SMTP fermée");
  }
  
  // Fermer la base de données
  db.close((err) => {
    if (err) {
      console.error("❌ Erreur fermeture DB:", err.message);
    } else {
      console.log("💾 Base de données SQLite fermée proprement");
    }
    
    // Fermer le serveur HTTP
    server.close(() => {
      console.log("✅ Arrêt gracieux terminé");
      process.exit(0);
    });
    
    // Force exit après 10 secondes si le serveur ne se ferme pas
    setTimeout(() => {
      console.error("⚠️ Arrêt forcé après timeout");
      process.exit(1);
    }, 10000);
  });
}

// Gestion des signaux de terminaison
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGHUP", () => gracefulShutdown("SIGHUP"));

// Gestion des erreurs non capturées
process.on("uncaughtException", (error) => {
  console.error("❌ Erreur non capturée:", error.message);
  console.error("   Stack:", error.stack);
  // Ne pas quitter le processus pour les erreurs mineures
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Promesse rejetée non gérée:", reason);
  // Ne pas quitter le processus pour les rejets mineurs
});

// ==================== EXPORT ====================
module.exports = app;
