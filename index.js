// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 9.2.0
// Technologies : Express, SQLite, Supabase, @whiskeysockets/baileys, axios,
//                Groq + OpenRouter (routage multi-tier v100/v250),
//                Firebase Admin (auth), sources ouvertes sans clé API,
//                BullMQ/Redis (file d'attente), Gmail API/Resend/SMTP (email),
//                Multer (uploads), Vision Models (Qwen-VL, Llama-Vision)
//
// ⚠️ CHANGEMENTS CRITIQUES DE LA VERSION 9.2.0 :
// 1) RÉSILIENCE : Retry avec backoff exponentiel (max 3 tentatives) sur les erreurs
//    timeout/réseau. "Service IA non disponible" uniquement si TOUS les fallbacks échouent.
// 2) PAYLOAD ÉTENDU : Limite de message portée à 15000 caractères pour l'analyse de code.
// 3) SYNCHRONISATION MULTI-APPAREILS : Supabase comme source de vérité avec firebase_uid
//    comme clé étrangère. Historique accessible depuis n'importe quel appareil.
// 4) CONTEXT MANAGER DYNAMIQUE : Injection de System Prompt adapté au domaine (math,
//    cybersécurité, développement, etc.) avant chaque appel LLM.
// 5) FORMATAGE STRICT : Directive absolue pour blocs Markdown (```) et LaTeX ($ ou $$).
// 6) PIPELINE MULTIMODAL : Multer pour uploads, conversion Base64, routage automatique
//    vers modèles Vision (Qwen-VL, Llama-Vision) quand une image est présente.

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const qrcode = require("qrcode");
const nodemailer = require("nodemailer");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const pino = require("pino");
const multer = require("multer");
const { createClient } = require("@supabase/supabase-js");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} = require("@whiskeysockets/baileys");

// ==================== CONFIGURATION GLOBALE ====================
const CONFIG = {
  PORT: process.env.PORT || 3000,
  ENV: process.env.NODE_ENV || "production",
  VERSION: "9.2.0",
  AGENT_NAME: "Luba",
  MAX_MESSAGE_LENGTH: 15000, // Étendu pour l'analyse de code
  MAX_HISTORY_LENGTH: 20,
  IMAGE_SEARCH_LIMIT: 6,
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/1.0 (contact@luba.ia)",
  WHATSAPP_QR_TIMEOUT: 30000,
  WHATSAPP_RETRY_DELAY: 3000,
  V250_STEP_TIMEOUT: 55000,
  V250_ROUTE_TIMEOUT: 180000,
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  MAX_CONTEXT_TOKENS: 6000,
  MAX_RETRY_ATTEMPTS: 3,
  RETRY_BASE_DELAY_MS: 1000,
  RETRY_MAX_DELAY_MS: 8000,
  MAX_IMAGE_SIZE_MB: 10,
  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],
  VISION_MODEL_GROQ: "openai/gpt-4o-mini",
  VISION_MODEL_OPENROUTER: "qwen/qwen-2.5-vl-72b-instruct:free"
};

// ==================== VALIDATION DES VARIABLES D'ENVIRONNEMENT ====================
const requiredEnvVars = ["GROQ_API_KEY", "OPENROUTER_API_KEY"];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingEnvVars.length > 0) {
  console.error("=".repeat(60));
  console.error("❌ VARIABLES D'ENVIRONNEMENT OBLIGATOIRES MANQUANTES :");
  missingEnvVars.forEach((varName) => console.error(`   - ${varName}`));
  console.error("=".repeat(60));
}

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.warn("⚠️ SUPABASE : URL ou clé non configurée. Persistance Supabase désactivée.");
}

if (!process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  console.warn("⚠️ SÉCURITÉ : FIREBASE_SERVICE_ACCOUNT_JSON non configurée.");
}

// ==================== INITIALISATION SUPABASE ====================
let supabase = null;
if (process.env.SUPABASE_URL && process.env.SUPABASE_KEY) {
  supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false },
    db: { schema: "public" }
  });
  console.log("✅ Supabase initialisé — persistance multi-appareils active");
}

// ==================== CRÉATION DES DOSSIERS ====================
const dataDir = path.join(__dirname, "data");
const sessionsDir = path.join(__dirname, "sessions");
const uploadsDir = path.join(__dirname, "uploads");
for (const dir of [dataDir, sessionsDir, uploadsDir]) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Dossier créé: ${dir}`);
  }
}

// ==================== INITIALISATION SQLITE (fallback local) ====================
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
    firebase_uid TEXT UNIQUE,
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    last_seen_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`ALTER TABLE users ADD COLUMN firebase_uid TEXT`, (err) => {
    if (err && !/duplicate column/i.test(err.message)) {
      console.error("⚠️ Erreur migration firebase_uid:", err.message);
    }
  });

  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    firebase_uid TEXT,
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
  db.run(`CREATE INDEX IF NOT EXISTS idx_sessions_firebase_uid ON sessions(firebase_uid)`);

  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

console.log("✅ Base de données SQLite initialisée (fallback local)");

// ==================== FIREBASE ADMIN ====================
let firebaseAdmin = null;
try {
  firebaseAdmin = require("firebase-admin");
} catch (e) {
  firebaseAdmin = null;
}

let firebaseApp = null;
function parseFirebaseServiceAccount(raw) {
  try {
    return JSON.parse(raw);
  } catch (e) {
    try {
      return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
    } catch (e2) {
      throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide");
    }
  }
}

if (firebaseAdmin && process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  try {
    const serviceAccount = parseFirebaseServiceAccount(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    firebaseApp = firebaseAdmin.initializeApp({ credential: firebaseAdmin.credential.cert(serviceAccount) });
    console.log("✅ Firebase Admin initialisé");
  } catch (e) {
    console.error("❌ Erreur initialisation Firebase Admin:", e.message);
  }
}

// ==================== CONFIGURATION MULTER (Tâche 6) ====================
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  limits: {
    fileSize: CONFIG.MAX_IMAGE_SIZE_MB * 1024 * 1024,
    files: 3
  },
  fileFilter: (req, file, cb) => {
    if (CONFIG.ALLOWED_IMAGE_TYPES.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error(`Type de fichier non supporté. Types autorisés: ${CONFIG.ALLOWED_IMAGE_TYPES.join(", ")}`));
    }
  }
});

// ==================== UTILITAIRE DE CONVERSION BASE64 (Tâche 6) ====================
function convertImageToBase64(buffer, mimetype) {
  return {
    dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`,
    base64: buffer.toString("base64"),
    mimetype,
    size: buffer.length
  };
}

// ==================== CONFIGURATION NODEMAILER ====================
let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  emailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || "587", 10),
    secure: process.env.SMTP_PORT === "465",
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
    tls: { rejectUnauthorized: false },
    pool: true,
    maxConnections: 3,
    maxMessages: 50
  });
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

const ALLOWED_ORIGINS = [
  "https://luba-ia.web.app",
  "https://luba-ia.firebaseapp.com",
  "https://milo-ead21.web.app",
  "http://localhost:3000",
  "http://localhost:8080",
  "http://localhost:5173"
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) callback(null, true);
    else callback(new Error("Origine non autorisée"));
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token"],
  credentials: true,
  maxAge: 86400
}));

app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));

// Tâche 2 : Payload étendu pour l'analyse de code
app.use(express.json({
  limit: "20mb",
  verify: (req, res, buf) => {
    req.rawBody = buf;
  }
}));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

// ==================== RATE LIMITERS ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(200).json({ reply: "⚠️ Trop de requêtes. Réessaie dans 15 minutes.", error: true })
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(200).json({ reply: "⚠️ Limite de requêtes atteinte.", error: true })
});

// ==================== HELPERS SQLITE ====================
function dbGet(query, params) {
  return new Promise((resolve, reject) => db.get(query, params, (err, row) => (err ? reject(err) : resolve(row))));
}
function dbAll(query, params) {
  return new Promise((resolve, reject) => db.all(query, params, (err, rows) => (err ? reject(err) : resolve(rows))));
}
function dbRun(query, params) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

// ==================== AUTHENTIFICATION FIREBASE + SUPABASE ====================
const authenticateUser = asyncHandler(async (req, res, next) => {
  const providedUserId = req.body.userId || req.query.userId || req.headers["x-user-id"];
  const authHeader = req.headers.authorization || req.headers.Authorization;
  const bearerToken = authHeader && authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;

  let verifiedUserId = null;
  let verifiedEmail = null;
  let verifiedName = null;
  let firebaseUid = null;

  if (firebaseApp && bearerToken) {
    try {
      const decoded = await firebaseAdmin.auth(firebaseApp).verifyIdToken(bearerToken);
      verifiedUserId = decoded.uid;
      firebaseUid = decoded.uid;
      verifiedEmail = decoded.email || null;
      verifiedName = decoded.name || null;
    } catch (error) {
      console.warn("⚠️ Token Firebase invalide/expiré:", error.message);
      return res.status(200).json({ reply: "⚠️ Session invalide ou expirée. Reconnecte-toi.", error: true, code: "INVALID_TOKEN" });
    }
  }

  const userId = verifiedUserId || (typeof providedUserId === "string" ? providedUserId.trim() : null);

  if (!userId) {
    return res.status(200).json({ reply: "⚠️ Authentification requise.", error: true });
  }

  req.userId = userId;
  req.firebaseUid = firebaseUid || userId;
  req.verifiedIdentity = Boolean(verifiedUserId);

  try {
    // Synchronisation avec Supabase
    if (supabase) {
      await syncUserWithSupabase(req.firebaseUid, verifiedEmail, verifiedName);
    }

    // Fallback SQLite
    const user = await dbGet("SELECT * FROM users WHERE id = ?", [userId]);
    if (!user) {
      await dbRun("INSERT INTO users (id, firebase_uid, email, display_name, last_seen_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)", [
        userId, req.firebaseUid, verifiedEmail, verifiedName || userId
      ]);
    } else {
      await dbRun(
        "UPDATE users SET last_seen_at = CURRENT_TIMESTAMP, firebase_uid = COALESCE(?, firebase_uid), email = COALESCE(?, email), display_name = COALESCE(?, display_name) WHERE id = ?",
        [req.firebaseUid, verifiedEmail, verifiedName, userId]
      );
    }
    next();
  } catch (err) {
    console.error("❌ Erreur authentification:", err.message);
    return res.status(200).json({ reply: "⚠️ Erreur interne.", error: true });
  }
});

// ==================== SYNCHRONISATION SUPABASE ====================
async function syncUserWithSupabase(firebaseUid, email, displayName) {
  if (!supabase || !firebaseUid) return;
  
  try {
    const { data: existingUser, error: fetchError } = await supabase
      .from("users")
      .select("firebase_uid")
      .eq("firebase_uid", firebaseUid)
      .single();

    if (fetchError && fetchError.code !== "PGRST116") {
      console.error("❌ Erreur Supabase fetch user:", fetchError.message);
      return;
    }

    if (!existingUser) {
      const { error: insertError } = await supabase
        .from("users")
        .insert({
          firebase_uid: firebaseUid,
          email: email,
          display_name: displayName,
          last_seen_at: new Date().toISOString()
        });
      
      if (insertError) console.error("❌ Erreur Supabase insert user:", insertError.message);
    } else {
      const { error: updateError } = await supabase
        .from("users")
        .update({ last_seen_at: new Date().toISOString() })
        .eq("firebase_uid", firebaseUid);
      
      if (updateError) console.error("❌ Erreur Supabase update user:", updateError.message);
    }
  } catch (error) {
    console.error("❌ Erreur sync Supabase:", error.message);
  }
}

async function syncSessionWithSupabase(sessionId, firebaseUid, userId) {
  if (!supabase || !firebaseUid) return;
  
  try {
    const { data: existingSession, error: fetchError } = await supabase
      .from("sessions")
      .select("session_id")
      .eq("session_id", sessionId)
      .single();

    if (fetchError && fetchError.code !== "PGRST116") {
      console.error("❌ Erreur Supabase fetch session:", fetchError.message);
      return;
    }

    if (!existingSession) {
      const { error: insertError } = await supabase
        .from("sessions")
        .insert({
          session_id: sessionId,
          firebase_uid: firebaseUid,
          user_id: userId,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        });
      
      if (insertError) console.error("❌ Erreur Supabase insert session:", insertError.message);
    } else {
      const { error: updateError } = await supabase
        .from("sessions")
        .update({ updated_at: new Date().toISOString() })
        .eq("session_id", sessionId);
      
      if (updateError) console.error("❌ Erreur Supabase update session:", updateError.message);
    }
  } catch (error) {
    console.error("❌ Erreur sync session Supabase:", error.message);
  }
}

async function syncMessageWithSupabase(sessionId, role, content, firebaseUid) {
  if (!supabase || !firebaseUid) return;
  
  try {
    const { error: insertError } = await supabase
      .from("messages")
      .insert({
        session_id: sessionId,
        firebase_uid: firebaseUid,
        role: role,
        content: content,
        created_at: new Date().toISOString()
      });
    
    if (insertError) console.error("❌ Erreur Supabase insert message:", insertError.message);
  } catch (error) {
    console.error("❌ Erreur sync message Supabase:", error.message);
  }
}

// ==================== CONTEXT MANAGER DYNAMIQUE (Tâche 4) ====================
class DynamicContextManager {
  constructor() {
    this.domainPatterns = [
      {
        domain: "mathematics",
        keywords: ["math", "calcul", "équation", "equation", "algèbre", "algebra", "géométrie", "geometry", 
                   "calculus", "intégrale", "integrale", "dérivée", "derivative", "théorème", "theorem", 
                   "nombre", "number", "fonction", "function", "limite", "limit", "matrice", "matrix"],
        systemPrompt: "Tu es un expert en mathématiques. Utilise LaTeX ($ pour inline, $$ pour display) pour toutes les formules. Détaille chaque étape du raisonnement. Sois rigoureux et pédagogique."
      },
      {
        domain: "cybersecurity",
        keywords: ["sécurité", "security", "cyber", "hack", "vulnérabilité", "vulnerability", "exploit", 
                   "pentest", "cryptographie", "cryptography", "chiffrement", "encryption", "pare-feu", 
                   "firewall", "malware", "virus", "phishing", "authentification", "token", "jwt"],
        systemPrompt: "Tu es un expert en cybersécurité. Adopte une approche défensive et éthique. Explique les vulnérabilités, les vecteurs d'attaque et les contre-mesures. Utilise des blocs de code pour les exemples techniques."
      },
      {
        domain: "development",
        keywords: ["code", "coder", "programmation", "programming", "développement", "development", 
                   "javascript", "python", "java", "c++", "rust", "go", "typescript", "react", "vue", 
                   "angular", "node", "express", "api", "database", "sql", "nosql", "backend", "frontend",
                   "bug", "debug", "fonction", "function", "classe", "class", "objet", "object", "algorithme"],
        systemPrompt: "Tu es un expert en développement logiciel. Fournis du code de production complet et fonctionnel dans des blocs Markdown (```). Explique l'architecture, les choix techniques et les bonnes pratiques."
      },
      {
        domain: "general",
        keywords: [],
        systemPrompt: "Tu es un assistant polyvalent. Adapte ton niveau de complexité à la question posée."
      }
    ];
  }

  analyzeDomain(message) {
    const lowerMessage = message.toLowerCase();
    let bestMatch = this.domainPatterns[this.domainPatterns.length - 1]; // general
    let bestScore = 0;

    for (const pattern of this.domainPatterns) {
      if (pattern.domain === "general") continue;
      let score = 0;
      for (const keyword of pattern.keywords) {
        if (lowerMessage.includes(keyword.toLowerCase())) {
          score += 1;
        }
      }
      if (score > bestScore) {
        bestScore = score;
        bestMatch = pattern;
      }
    }

    return bestMatch;
  }

  buildSystemPrompt(message, basePrompt) {
    const domain = this.analyzeDomain(message);
    return {
      role: "system",
      content: `${basePrompt}

DOMAINE D'EXPERTISE DÉTECTÉ : ${domain.domain.toUpperCase()}
${domain.systemPrompt}

FORMATAGE STRICT OBLIGATOIRE (Tâche 5) :
- TOUT code doit être encadré dans des blocs Markdown (```).
- TOUTE formule mathématique doit être encadrée en LaTeX ($ pour inline, $$ pour display).
- AUCUN caractère technique non formaté dans le texte brut.
- Les noms de variables, fonctions et fichiers doivent être en backticks (`).
- Les résultats de commandes doivent être dans des blocs de code.`
    };
  }
}

const dynamicContextManager = new DynamicContextManager();

// ==================== SYSTEM PROMPT LUBA (Tâche 5 : Formatage strict) ====================
const LUBA_BASE_SYSTEM_PROMPT = `Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, une startup basée à Kinshasa, fondée en 2026.

IDENTITÉ (à respecter strictement) :
- Tu t'appelles Luba (ou Luba.ia). Tu ne t'appelles JAMAIS Milo, Milou, ou tout autre nom.
- Si on te demande qui t'a créée : IA développée par HIKLON Technology, startup à Kinshasa, fondée en 2026.
- Ton ton est chaleureux, intelligent et proactif.

RÈGLE SUR LES DONNÉES (OBLIGATOIRE, PRODUCTION — AUCUNE SIMULATION) :
- Tu ne dois JAMAIS inventer un score sportif, une actualité, un résultat de recherche, une donnée météo ou toute information factuelle changeante. Utilise TOUJOURS l'outil approprié pour obtenir une donnée réelle.
- Si un outil échoue ou ne retourne rien, dis-le honnêtement à l'utilisateur. N'invente jamais un résultat de remplacement.
- Ne mentionne JAMAIS toi-même de sources dans replyText : le backend les ajoute automatiquement.

RÈGLE STRICTE SUR LES IMAGES (OBLIGATOIRE) :
- Dès que tu décris une personnalité, un lieu, un objet, un concept scientifique ou un événement, utilise TOUJOURS search_images.
- Dès qu'une recherche ou une information est demandée, ajoute TOUJOURS un appel à search_images en complément.

RÈGLE SUR LES SUGGESTIONS (OBLIGATOIRE) :
- Le champ "suggestions" doit TOUJOURS contenir 3 à 4 questions de suivi courtes et cliquables.

FORMAT DE RÉPONSE OBLIGATOIRE (JSON strict) :
{
  "replyText": "Ta réponse complète en Markdown",
  "toolCalls": [ { "name": "...", "arguments": { ... } } ],
  "suggestions": ["Question de suivi 1 ?", "Question de suivi 2 ?", "Question de suivi 3 ?"]
}
Si aucun outil n'est nécessaire, "toolCalls": [].

OUTILS DISPONIBLES :
- search_images : Rechercher des images (arguments: { query })
- search_web : Recherche générale — Wikipédia + actualités (arguments: { query })
- search_news : Actualités récentes (arguments: { query })
- search_sports_scores : Scores/résultats d'une équipe sportive (arguments: { query })
- search_science : Articles scientifiques/recherches (arguments: { query })
- search_social : Discussions sur les réseaux sociaux (Reddit) (arguments: { query })
- get_weather : Météo actuelle d'un lieu (arguments: { location })
- send_email : Envoyer un email réel (arguments: { recipient, subject, body })
- send_whatsapp_message : Envoyer un message WhatsApp réel (arguments: { phone_number, message })`;

// ==================== ARCHITECTURE LLM ENTERPRISE ====================
const LLM_PROVIDERS = {
  GROQ: { 
    baseURL: "https://api.groq.com/openai/v1", 
    apiKey: process.env.GROQ_API_KEY || "", 
    timeout: 45000, 
    maxTokens: 4000, 
    temperature: 0.7 
  },
  OPENROUTER: { 
    baseURL: "https://openrouter.ai/api/v1", 
    apiKey: process.env.OPENROUTER_API_KEY || "", 
    timeout: 60000, 
    maxTokens: 4000, 
    temperature: 0.7 
  }
};

const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    providers: [
      {
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "openai/gpt-oss-120b",
        maxTokens: 4000,
        timeout: 45000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 0
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "qwen/qwen-2.5-coder-32b-instruct:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 1
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 2
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_3 || "microsoft/phi-4:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 3
      }
    ]
  },
  v250: {
    name: "Ngandu",
    reasoning: {
      providers: [
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_REASONING || "deepseek/deepseek-r1:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_REASONING_MAX_TOKENS || "8000", 10),
          timeout: 90000,
          temperature: 0.3,
          jsonMode: false,
          failoverPriority: 0
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_REASONING_FALLBACK || "deepseek/deepseek-r1-distill-llama-70b:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_REASONING_MAX_TOKENS || "8000", 10),
          timeout: 90000,
          temperature: 0.3,
          jsonMode: false,
          failoverPriority: 1
        },
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_REASONING_FALLBACK || "openai/gpt-oss-120b",
          maxTokens: parseInt(process.env.GROQ_V250_REASONING_MAX_TOKENS || "6000", 10),
          timeout: 45000,
          temperature: 0.3,
          jsonMode: false,
          failoverPriority: 2
        }
      ]
    },
    code: {
      providers: [
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_CODE || "qwen/qwen-2.5-coder-32b-instruct:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_CODE_MAX_TOKENS || "8000", 10),
          timeout: 90000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 0
        },
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_CODE_FALLBACK || "openai/gpt-oss-120b",
          maxTokens: parseInt(process.env.GROQ_V250_CODE_MAX_TOKENS || "8000", 10),
          timeout: 45000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 1
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_CODE_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_CODE_MAX_TOKENS || "8000", 10),
          timeout: 60000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 2
        }
      ]
    },
    maxRetries: 3,
    degradedMode: true
  },
  vision: {
    name: "Vision",
    providers: [
      {
        provider: "groq",
        model: CONFIG.VISION_MODEL_GROQ,
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 0
      },
      {
        provider: "openrouter",
        model: CONFIG.VISION_MODEL_OPENROUTER,
        maxTokens: 4000,
        timeout: 90000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 1
      }
    ]
  }
};

// ==================== VALIDATION & FILTRE OPENROUTER ====================
function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;
  
  const knownProviders = [
    "openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", 
    "anthropic/", "google/", "mistralai/", "cohere/"
  ];
  
  const isOpenRouterModel = knownProviders.some(prefix => model.includes(prefix));
  
  if (isOpenRouterModel && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return `${model}:free`;
  }
  
  return model;
}

// ==================== INTERCEPTEUR D'ERREURS GLOBAL ====================
class LLMErrorInterceptor {
  static isRetryableError(error) {
    const status = error.response?.status;
    const retryableStatuses = [408, 429, 500, 502, 503, 504];
    const isTimeout = error.code === "ECONNABORTED" || 
                      error.code === "ETIMEDOUT" || 
                      error.code === "ESOCKETTIMEDOUT" ||
                      /timeout/i.test(error.message || "");
    const isNetworkError = error.code === "ENOTFOUND" || 
                           error.code === "ECONNRESET" ||
                           error.code === "ECONNREFUSED" ||
                           error.code === "EAI_AGAIN";
    
    return retryableStatuses.includes(status) || isTimeout || isNetworkError;
  }
  
  static getErrorCode(error) {
    const status = error.response?.status;
    if (status) return `HTTP_${status}`;
    if (error.code === "ECONNABORTED") return "TIMEOUT";
    if (error.code === "ENOTFOUND") return "DNS_ERROR";
    if (error.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    return "UNKNOWN_ERROR";
  }
  
  static shouldSkipProvider(error, providerConfig) {
    const errorCode = this.getErrorCode(error);
    
    if (errorCode === "HTTP_402") {
      console.warn(`[LLM-INTERCEPTOR] Crédits épuisés sur ${providerConfig.provider}/${providerConfig.model}`);
      return true;
    }
    
    if (errorCode === "HTTP_404") {
      console.warn(`[LLM-INTERCEPTOR] Modèle introuvable: ${providerConfig.model}`);
      return true;
    }
    
    return false;
  }
}

// ==================== WRAPPER EXECUTE WITH RETRY AND FALLBACK (Tâche 1) ====================
async function executeWithRetryAndFallback(providerList, promptParams, options = {}) {
  const {
    maxRetriesPerProvider = CONFIG.MAX_RETRY_ATTEMPTS,
    baseDelayMs = CONFIG.RETRY_BASE_DELAY_MS,
    maxDelayMs = CONFIG.RETRY_MAX_DELAY_MS,
    timeoutMultiplier = 1.5,
    onProviderFail = null,
    onProviderSuccess = null
  } = options;
  
  let lastError = null;
  const providerResults = [];
  
  const sortedProviders = [...providerList].sort((a, b) => a.failoverPriority - b.failoverPriority);
  
  for (let i = 0; i < sortedProviders.length; i++) {
    const providerConfig = sortedProviders[i];
    const provider = providerConfig.provider;
    
    const providerConfig2 = LLM_PROVIDERS[provider.toUpperCase()];
    if (!providerConfig2 || !providerConfig2.apiKey) {
      console.warn(`[LLM-FALLBACK] Fournisseur ${provider} non configuré - skip`);
      continue;
    }
    
    let model = providerConfig.model;
    if (provider === "openrouter") {
      model = validateAndSanitizeOpenRouterModel(model);
      if (!model) {
        console.warn(`[LLM-FALLBACK] Modèle OpenRouter invalide - skip`);
        continue;
      }
    }
    
    console.log(`[LLM-FALLBACK] Tentative fournisseur ${i + 1}/${sortedProviders.length} : ${provider}/${model}`);
    
    for (let attempt = 0; attempt < maxRetriesPerProvider; attempt++) {
      try {
        const timeout = providerConfig.timeout * (attempt > 0 ? timeoutMultiplier : 1);
        
        const result = await callProviderRaw({
          provider,
          model,
          messages: promptParams.messages,
          jsonMode: providerConfig.jsonMode,
          timeout,
          maxTokens: providerConfig.maxTokens,
          temperature: providerConfig.temperature,
          images: promptParams.images || null
        });
        
        const providerResult = {
          providerUsed: provider,
          modelUsed: model,
          providerPriority: providerConfig.failoverPriority,
          attempts: attempt + 1,
          response: result
        };
        
        providerResults.push(providerResult);
        
        if (onProviderSuccess) {
          onProviderSuccess(providerResult);
        }
        
        console.log(`[LLM-FALLBACK] ✅ Succès avec ${provider}/${model} (tentative ${attempt + 1}/${maxRetriesPerProvider})`);
        
        return {
          success: true,
          ...providerResult,
          providerChain: providerResults
        };
        
      } catch (error) {
        lastError = error;
        const errorCode = LLMErrorInterceptor.getErrorCode(error);
        
        console.error(`[LLM-FALLBACK] ❌ Échec ${provider}/${model} (tentative ${attempt + 1}/${maxRetriesPerProvider}): ${errorCode}`);
        
        if (onProviderFail) {
          onProviderFail({
            provider,
            model,
            errorCode,
            errorMessage: error.message,
            attempt: attempt + 1
          });
        }
        
        if (LLMErrorInterceptor.shouldSkipProvider(error, providerConfig)) {
          console.log(`[LLM-FALLBACK] Provider ${provider} marqué comme indisponible - passage au suivant`);
          break;
        }
        
        // Backoff exponentiel pour les erreurs retryables
        if (LLMErrorInterceptor.isRetryableError(error) && attempt < maxRetriesPerProvider - 1) {
          const retryDelay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
          console.log(`[LLM-FALLBACK] Backoff exponentiel: ${retryDelay}ms avant nouvelle tentative`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        } else if (!LLMErrorInterceptor.isRetryableError(error)) {
          // Erreur non retryable, on passe au fournisseur suivant
          break;
        }
      }
    }
  }
  
  console.error(`[LLM-FALLBACK] ❌ Tous les fournisseurs ont échoué. Dernière erreur: ${LLMErrorInterceptor.getErrorCode(lastError)}`);
  
  return {
    success: false,
    error: lastError,
    providerChain: providerResults,
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  };
}

// ==================== APPEL PROVIDER BRUT AVEC SUPPORT VISION ====================
async function callProviderRaw({ provider, model, messages, jsonMode = false, timeout, maxTokens, temperature = 0.7, images = null }) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ : LLM_PROVIDERS.OPENROUTER;
  
  if (!cfg.apiKey) {
    const err = new Error(`Clé API manquante pour le fournisseur ${provider}`);
    err.code = "MISSING_API_KEY";
    throw err;
  }
  
  // Formatage du contenu pour le support Vision (Tâche 6)
  let formattedMessages = messages;
  if (images && images.length > 0) {
    const lastMessage = messages[messages.length - 1];
    if (lastMessage.role === "user") {
      const contentParts = [];
      
      // Ajoute le texte
      if (typeof lastMessage.content === "string") {
        contentParts.push({ type: "text", text: lastMessage.content });
      }
      
      // Ajoute les images en Base64
      for (const image of images) {
        contentParts.push({
          type: "image_url",
          image_url: {
            url: image.dataUrl
          }
        });
      }
      
      lastMessage.content = contentParts;
      formattedMessages = [...messages.slice(0, -1), lastMessage];
    }
  }
  
  const payload = { 
    model, 
    messages: formattedMessages, 
    temperature, 
    max_tokens: maxTokens || cfg.maxTokens 
  };
  
  if (jsonMode) {
    payload.response_format = { type: "json_object" };
  }
  
  const headers = { 
    Authorization: `Bearer ${cfg.apiKey}`, 
    "Content-Type": "application/json" 
  };
  
  if (provider === "openrouter") {
    headers["HTTP-Referer"] = "https://luba-ia.web.app";
    headers["X-Title"] = "Luba.ia Assistant";
  }
  
  const response = await axios.post(
    `${cfg.baseURL}/chat/completions`, 
    payload, 
    { headers, timeout: timeout || cfg.timeout }
  );
  
  const choice = response.data?.choices?.[0];
  const content = choice?.message?.content;
  
  if (choice?.finish_reason === "length") {
    console.warn(`⚠️ Réponse ${provider}/${model} tronquée par max_tokens`);
  }
  
  if (!content) {
    throw new Error(`Réponse ${provider} vide`);
  }
  
  return jsonMode ? JSON.parse(content) : content;
}

// ==================== GESTION DE CONTEXTE OPTIMISÉE ====================
class ConversationContextManager {
  constructor(maxTokens = CONFIG.MAX_CONTEXT_TOKENS) {
    this.maxTokens = maxTokens;
  }
  
  estimateTokens(text) {
    return Math.ceil(text.length / 4);
  }
  
  truncateHistory(history, currentMessage) {
    let totalTokens = this.estimateTokens(currentMessage);
    const truncatedHistory = [];
    
    for (let i = history.length - 1; i >= 0; i--) {
      const message = history[i];
      const messageTokens = this.estimateTokens(message.content);
      
      if (totalTokens + messageTokens > this.maxTokens) {
        break;
      }
      
      totalTokens += messageTokens;
      truncatedHistory.unshift(message);
    }
    
    return truncatedHistory;
  }
  
  async getCleanContext(conversationId, currentMessage, limit = CONFIG.MAX_HISTORY_LENGTH) {
    const history = await getHistory(conversationId, limit);
    const cleanHistory = this.truncateHistory(history, currentMessage);
    
    return {
      messages: [...cleanHistory, { role: "user", content: currentMessage }],
      historyLength: cleanHistory.length,
      estimatedTokens: this.estimateTokens(JSON.stringify(cleanHistory))
    };
  }
}

const contextManager = new ConversationContextManager();

// ==================== CALL LLM V100 - MWAMBA ====================
async function callLLM_v100(messages, images = null) {
  console.log("[LLM-v100] Démarrage du routage Mwamba");
  
  // Tâche 4 : Injection de contexte dynamique
  const lastUserMessage = [...messages].reverse().find(m => m.role === "user");
  const userText = typeof lastUserMessage?.content === "string" ? lastUserMessage.content : "";
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userText, LUBA_BASE_SYSTEM_PROMPT);
  
  const providers = MODEL_TIERS.v100.providers;
  
  const result = await executeWithRetryAndFallback(
    providers,
    { 
      messages: [dynamicSystemPrompt, ...messages],
      images: images
    },
    {
      maxRetriesPerProvider: CONFIG.MAX_RETRY_ATTEMPTS,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-v100] Failover détecté: ${failInfo.provider}/${failInfo.model} - ${failInfo.errorCode}`);
      }
    }
  );
  
  if (result.success) {
    return {
      ...result.response,
      providerUsed: result.providerUsed,
      modelUsed: result.modelUsed,
      degraded: result.providerPriority > 0
    };
  }
  
  throw new Error(`Échec complet du tier v100: ${result.errorCode}`);
}

// ==================== CALL LLM V250 - NGANDU ====================
async function callLLM_v250(messages, userMessage, images = null) {
  console.log("[LLM-v250] Démarrage du pipeline Ngandu");
  
  const tier = MODEL_TIERS.v250;
  let providerChain = [];
  
  // Tâche 4 : Injection de contexte dynamique
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userMessage, LUBA_BASE_SYSTEM_PROMPT);
  
  // Étape 1 : Raisonnement
  const reasoningMessages = [
    {
      role: "system",
      content: `${dynamicSystemPrompt.content}\n\nAnalyse ce problème complexe en profondeur. Effectue les démonstrations nécessaires, isole les edge cases et rédige le pseudo-code/l'architecture. Sois complet et rigoureux.`
    },
    ...messages
  ];
  
  const reasoningResult = await executeWithRetryAndFallback(
    tier.reasoning.providers,
    { 
      messages: reasoningMessages,
      images: images
    },
    {
      maxRetriesPerProvider: tier.maxRetries,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-v250-R1] Failover raisonnement: ${failInfo.provider}/${failInfo.model}`);
      }
    }
  );
  
  if (!reasoningResult.success || !reasoningResult.response || reasoningResult.response.trim().length < 40) {
    console.error("[LLM-v250] Échec de l'étape de raisonnement - dégradation vers v100");
    return await degradedFallbackToV100(messages, "reasoning_failed", images);
  }
  
  const reasoningAnalysis = reasoningResult.response;
  providerChain.push(`R1:${reasoningResult.providerUsed}/${reasoningResult.modelUsed}`);
  
  // Étape 2 : Génération de code
  const codeMessages = [
    {
      role: "system",
      content: `Génère le code de production complet, typé, sécurisé et documenté en te basant strictement sur le plan ci-dessous.

PLAN / ANALYSE (étape 1 — raisonnement) :
${reasoningAnalysis}

Tu DOIS répondre au format JSON strict : { "replyText": "réponse complète en Markdown avec le code", "toolCalls": [], "suggestions": ["question 1 ?", "question 2 ?", "question 3 ?"] }.
FORMATAGE STRICT : Code dans des blocs \`\`\`, formules en LaTeX ($ ou $$).`
    },
    { role: "user", content: userMessage }
  ];
  
  const codeResult = await executeWithRetryAndFallback(
    tier.code.providers,
    { 
      messages: codeMessages,
      images: images
    },
    {
      maxRetriesPerProvider: tier.maxRetries,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-v250-R2] Failover code: ${failInfo.provider}/${failInfo.model}`);
      }
    }
  );
  
  if (!codeResult.success || !codeResult.response) {
    console.error("[LLM-v250] Échec de l'étape de génération - dégradation vers v100");
    return await degradedFallbackToV100(messages, "code_generation_failed", images);
  }
  
  providerChain.push(`R2:${codeResult.providerUsed}/${codeResult.modelUsed}`);
  
  return {
    ...codeResult.response,
    providerUsed: "pipeline_v250",
    modelUsed: providerChain.join(" -> "),
    degraded: false,
    providerChain,
    reasoningProviderUsed: reasoningResult.providerUsed
  };
}

// ==================== CALL VISION (Tâche 6) ====================
async function callVisionModel(messages, images) {
  console.log("[LLM-VISION] Démarrage du pipeline Vision");
  
  const providers = MODEL_TIERS.vision.providers;
  
  const result = await executeWithRetryAndFallback(
    providers,
    { 
      messages: messages,
      images: images
    },
    {
      maxRetriesPerProvider: 2,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-VISION] Failover: ${failInfo.provider}/${failInfo.model}`);
      }
    }
  );
  
  if (result.success) {
    return {
      ...result.response,
      providerUsed: result.providerUsed,
      modelUsed: result.modelUsed,
      visionEnabled: true
    };
  }
  
  // Fallback vers v100 sans vision si tous les modèles vision échouent
  console.warn("[LLM-VISION] Échec des modèles vision - fallback vers v100 textuel");
  return await callLLM_v100(messages, null);
}

// ==================== GARDE-FOU : DÉGRADATION GRACIEUSE ====================
async function degradedFallbackToV100(messages, reason, images = null) {
  console.warn(`[LLM-GARDE-FOU] Dégradation gracieuse vers v100 (${reason})`);
  
  try {
    const fallbackResult = await callLLM_v100(messages, images);
    return {
      ...fallbackResult,
      providerUsed: "v250_degraded_to_v100",
      modelUsed: `${fallbackResult.providerUsed}/${fallbackResult.modelUsed}`,
      degraded: true,
      degradationReason: reason,
      originalTier: "v250",
      actualTier: "v100"
    };
  } catch (fallbackError) {
    console.error("[LLM-GARDE-FOU] Échec total de la dégradation:", fallbackError.message);
    
    return {
      replyText: "Je rencontre actuellement des difficultés techniques. Veuillez réessayer dans quelques instants. Nos équipes techniques ont été informées.",
      toolCalls: [],
      suggestions: [
        "Peux-tu réessayer avec une question plus simple ?",
        "Comment fonctionne Luba.ia ?",
        "Quels sont les services disponibles ?"
      ],
      providerUsed: "error_graceful_degradation",
      modelUsed: "none",
      degraded: true,
      degradationReason: `${reason}_and_v100_failed`,
      error: true
    };
  }
}

// ==================== GESTION DU CONTEXTE PAR CONVERSATION_ID ====================
async function getSession(conversationId, userId, firebaseUid = null) {
  // Vérifie dans SQLite
  const session = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);
  if (session) {
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
    return session;
  }
  
  // Vérifie dans Supabase
  if (supabase && firebaseUid) {
    try {
      const { data: supabaseSession, error } = await supabase
        .from("sessions")
        .select("session_id, user_id, firebase_uid")
        .eq("session_id", conversationId)
        .single();
      
      if (supabaseSession && !error) {
        // Synchronise vers SQLite
        await dbRun("INSERT OR IGNORE INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)", [
          conversationId, supabaseSession.user_id || userId, supabaseSession.firebase_uid
        ]);
        return { session_id: conversationId, user_id: supabaseSession.user_id || userId, firebase_uid: supabaseSession.firebase_uid };
      }
    } catch (error) {
      console.error("❌ Erreur Supabase getSession:", error.message);
    }
  }
  
  // Crée une nouvelle session
  await dbRun("INSERT INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)", [
    conversationId, userId, firebaseUid
  ]);
  
  // Synchronise avec Supabase
  await syncSessionWithSupabase(conversationId, firebaseUid, userId);
  
  return { session_id: conversationId, user_id: userId, firebase_uid: firebaseUid };
}

async function getHistory(conversationId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  // Vérifie SQLite d'abord
  const localRows = await dbAll("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?", [conversationId, limit]);
  
  if (localRows.length > 0) {
    return localRows.reverse().map((row) => ({ role: row.role, content: row.content }));
  }
  
  // Vérifie Supabase
  if (supabase) {
    try {
      const { data: supabaseMessages, error } = await supabase
        .from("messages")
        .select("role, content")
        .eq("session_id", conversationId)
        .order("created_at", { ascending: false })
        .limit(limit);
      
      if (supabaseMessages && !error && supabaseMessages.length > 0) {
        return supabaseMessages.reverse().map((row) => ({ role: row.role, content: row.content }));
      }
    } catch (error) {
      console.error("❌ Erreur Supabase getHistory:", error.message);
    }
  }
  
  return [];
}

async function saveMessage(conversationId, role, content, firebaseUid = null) {
  // Sauvegarde SQLite
  await dbRun("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", [conversationId, role, content]);
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
  
  // Sauvegarde Supabase
  await syncMessageWithSupabase(conversationId, role, content, firebaseUid);
}

// ==================== HANDLE CHAT PRINCIPAL (avec support Vision) ====================
async function handleChat({ conversationId, userId, firebaseUid, message, googleAccessToken = null, channel = "web", modelTier = "v100", images = null }) {
  console.log(`[CHAT] Démarrage conversation ${conversationId} - Tier: ${modelTier} - Images: ${images ? images.length : 0}`);
  
  const session = await getSession(conversationId, userId, firebaseUid);

  const activeIntent = await getActiveIntent(conversationId);
  if (activeIntent) {
    return await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken });
  }

  await saveMessage(conversationId, "user", message, firebaseUid);

  const context = await contextManager.getCleanContext(conversationId, message);
  const messages = context.messages;

  let finalResponse = null;
  let imageUrls = [];
  let providerUsed = "unknown";
  let suggestions = [];
  const usedSources = new Set();
  let degraded = false;

  try {
    // Tâche 6 : Routage vers Vision si images présentes
    if (images && images.length > 0) {
      console.log("[CHAT] Mode Vision activé");
      const visionResult = await callVisionModel(messages, images);
      finalResponse = visionResult.replyText || "Je n'ai pas pu analyser l'image.";
      suggestions = Array.isArray(visionResult.suggestions) ? visionResult.suggestions.slice(0, 4) : [];
      providerUsed = visionResult.providerUsed || "vision";
      
    } else if (modelTier === "v250") {
      const result = await callLLM_v250(messages, message);
      finalResponse = result.replyText || "Je n'ai pas pu générer une réponse.";
      suggestions = Array.isArray(result.suggestions) ? result.suggestions.slice(0, 4) : [];
      providerUsed = result.providerUsed || "pipeline_v250";
      degraded = result.degraded || false;
      
    } else {
      // Tier v100 avec boucle ReAct
      let keepRunning = true;
      let maxLoops = 5;

      while (keepRunning && maxLoops > 0) {
        maxLoops--;
        let llmResponse;
        try {
          llmResponse = await callLLM_v100(messages);
          providerUsed = llmResponse.providerUsed;
          degraded = llmResponse.degraded || false;
        } catch (error) {
          console.error("[CHAT] Erreur LLM v100:", error.message);
          finalResponse = "Je suis momentanément indisponible. Veuillez réessayer dans quelques instants.";
          suggestions = [
            "Peux-tu réessayer ?",
            "Comment fonctionne Luba.ia ?",
            "Quels sont les services disponibles ?"
          ];
          providerUsed = "error_graceful_degradation";
          degraded = true;
          break;
        }

        if (llmResponse.toolCalls && llmResponse.toolCalls.length > 0) {
          for (const toolCall of llmResponse.toolCalls) {
            let toolResult;
            try {
              const { result, sourceKeys } = await executeTool(toolCall.name, toolCall.arguments || {}, { userId, googleAccessToken });
              toolResult = result;
              sourceKeys.forEach((k) => usedSources.add(k));
              if ((toolCall.name === "search_images" || toolCall.name === "search_image") && toolResult.images) {
                imageUrls = imageUrls.concat(toolResult.images.map((img) => img.url));
              }
            } catch (toolError) {
              console.error(`[CHAT] Erreur outil ${toolCall.name}:`, toolError.message);
              toolResult = { success: false, error: toolError.message };
            }

            messages.push({ role: "assistant", content: `Résultat de l'outil ${toolCall.name}: ${JSON.stringify(toolResult)}` });
          }

          messages.push({
            role: "user",
            content: "Formule maintenant ta réponse finale complète avec les résultats des outils, et propose 3 à 4 questions de suivi dans le champ suggestions."
          });
          keepRunning = true;
        } else {
          finalResponse = llmResponse.replyText || "Je n'ai pas pu générer une réponse.";
          suggestions = Array.isArray(llmResponse.suggestions) ? llmResponse.suggestions.slice(0, 4) : [];
          keepRunning = false;
        }
      }

      if (!finalResponse) finalResponse = "Je rencontre des difficultés techniques. Veuillez réessayer.";
    }

    // Ajout des images si disponibles
    if (imageUrls.length > 0) {
      const imageMarkdown = imageUrls.map((url, index) => `![Image ${index + 1}](${url})`).join("\n\n");
      finalResponse += `\n\n---\n\n📷 **Illustrations :**\n\n${imageMarkdown}`;
      usedSources.add("wikimediacommons");
    }

    // Ajout des sources
    if (usedSources.size > 0) {
      const sourceLines = Array.from(usedSources)
        .map((key) => OPEN_SOURCES[key])
        .filter(Boolean)
        .map((src) => `[![${src.name}](${src.logo})](${src.url}) ${src.name}`);
      if (sourceLines.length > 0) finalResponse += `\n\n---\n\n**Sources :** ${sourceLines.join(" · ")}`;
    }

    await saveMessage(conversationId, "assistant", finalResponse, firebaseUid);

    console.log(`[CHAT] ✅ Réponse finale générée (${finalResponse.length} caractères)`);

    return {
      reply: finalResponse,
      images: imageUrls,
      error: providerUsed.startsWith("error"),
      providerUsed,
      modelTier,
      degraded,
      visionEnabled: Boolean(images && images.length > 0),
      suggestions,
      sources: Array.from(usedSources).map((key) => OPEN_SOURCES[key]).filter(Boolean)
    };

  } catch (error) {
    console.error("[CHAT] Erreur critique:", error.message);

    const fallbackResponse = {
      reply: "Je suis momentanément indisponible. Nos équipes techniques travaillent à résoudre le problème.",
      images: [],
      error: true,
      providerUsed: "error_critical",
      modelTier,
      degraded: true,
      suggestions: [
        "Peux-tu réessayer ?",
        "Comment fonctionne Luba.ia ?",
        "Quels sont les services disponibles ?"
      ],
      sources: []
    };

    await saveMessage(conversationId, "assistant", fallbackResponse.reply, firebaseUid);

    return fallbackResponse;
  }
}

// ==================== ROUTES ====================
const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

app.get("/", (req, res) => {
  res.json({ reply: `✅ Serveur ${CONFIG.AGENT_NAME} opérationnel`, error: false, version: CONFIG.VERSION });
});

// ==================== HEALTH CHECK ====================
app.get("/api/health", asyncHandler(async (req, res) => {
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
      supabase: Boolean(supabase),
      firebaseAuth: Boolean(firebaseApp),
      version: CONFIG.VERSION,
      features: {
        vision: true,
        extendedPayload: CONFIG.MAX_MESSAGE_LENGTH,
        dynamicContext: true,
        multiDeviceSync: Boolean(supabase),
        strictFormatting: true,
        retryMechanism: `${CONFIG.MAX_RETRY_ATTEMPTS} tentatives max`
      }
    }
  });
}));

// ==================== ROUTE CHAT PRINCIPALE (Tâche 6 : support multipart) ====================
app.post(
  "/api/chat",
  apiLimiter,
  authenticateUser,
  upload.array("images", 3),
  asyncHandler(async (req, res) => {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;
    const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'message' est obligatoire.", error: true });
    }
    
    // Tâche 2 : Validation étendue
    if (message.length > CONFIG.MAX_MESSAGE_LENGTH) {
      return res.status(200).json({ reply: `⚠️ Message trop long (max ${CONFIG.MAX_MESSAGE_LENGTH} caractères).`, error: true });
    }

    if (!conversationId || typeof conversationId !== "string") {
      conversationId = `conv_${crypto.randomUUID()}`;
      isNewConversation = true;
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }

    if (modelTier === "v250") {
      req.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
      res.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
    }

    const googleAccessToken = extractGoogleAccessToken(req);

    // Tâche 6 : Conversion des images en Base64
    let images = null;
    if (req.files && req.files.length > 0) {
      images = req.files.map(file => convertImageToBase64(file.buffer, file.mimetype));
      console.log(`[CHAT] ${images.length} image(s) reçue(s), taille totale: ${images.reduce((acc, img) => acc + img.size, 0)} octets`);
    }

    try {
      const result = await handleChat({ 
        conversationId, 
        userId: req.userId, 
        firebaseUid: req.firebaseUid,
        message: message.trim(), 
        googleAccessToken, 
        channel: "web", 
        modelTier,
        images
      });
      return res.status(200).json({ ...result, conversationId, isNewConversation });
    } catch (error) {
      console.error("❌ Erreur /api/chat:", error.message);
      return res.status(200).json({ reply: "⚠️ Une erreur est survenue. Veuillez réessayer.", error: true, conversationId, modelTier });
    }
  })
);

// ==================== ROUTE CONVERSATIONS (Tâche 3 : multi-appareils) ====================
app.get(
  "/api/conversations",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    let conversations = [];
    
    // Récupération depuis Supabase (source de vérité pour multi-appareils)
    if (supabase) {
      try {
        const { data: supabaseConversations, error } = await supabase
          .from("sessions")
          .select("session_id, created_at, updated_at")
          .eq("firebase_uid", req.firebaseUid)
          .order("updated_at", { ascending: false })
          .limit(50);
        
        if (supabaseConversations && !error) {
          conversations = supabaseConversations;
        }
      } catch (error) {
        console.error("❌ Erreur Supabase conversations:", error.message);
      }
    }
    
    // Fallback SQLite
    if (conversations.length === 0) {
      const rows = await dbAll(
        "SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? OR firebase_uid = ? ORDER BY updated_at DESC LIMIT 50", 
        [req.userId, req.firebaseUid]
      );
      conversations = rows;
    }
    
    // Enrichissement avec le dernier message
    const enrichedConversations = await Promise.all(
      conversations.map(async (conv) => {
        let lastMessage = null;
        
        // Vérifie Supabase d'abord
        if (supabase) {
          try {
            const { data: supabaseMsg, error } = await supabase
              .from("messages")
              .select("role, content")
              .eq("session_id", conv.session_id)
              .order("created_at", { ascending: false })
              .limit(1);
            
            if (supabaseMsg && supabaseMsg.length > 0 && !error) {
              lastMessage = supabaseMsg[0];
            }
          } catch (error) {
            // Ignore et passe au fallback
          }
        }
        
        // Fallback SQLite
        if (!lastMessage) {
          lastMessage = await dbGet("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1", [conv.session_id]);
        }
        
        return {
          conversationId: conv.session_id,
          createdAt: conv.created_at,
          updatedAt: conv.updated_at,
          lastMessageRole: lastMessage?.role || null,
          lastMessagePreview: lastMessage?.content ? lastMessage.content.slice(0, 140) : null
        };
      })
    );
    
    return res.status(200).json({ 
      reply: "Conversations récupérées.", 
      error: false, 
      conversations: enrichedConversations,
      source: supabase ? "supabase" : "sqlite"
    });
  })
);

// ==================== ROUTE OUTILS DIRECTS ====================
app.post(
  "/api/tools",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const toolName = req.body.toolName || req.body.action;
    const params = req.body.params || req.body.arguments || {};

    if (!toolName || typeof toolName !== "string") {
      return res.status(200).json({ success: false, error: true, reply: "⚠️ Le paramètre 'toolName' est obligatoire." });
    }

    const googleAccessToken = extractGoogleAccessToken(req);

    try {
      const { result, sourceKeys } = await executeTool(toolName, params, { userId: req.userId, googleAccessToken });
      const sources = sourceKeys.map((k) => OPEN_SOURCES[k]).filter(Boolean);
      return res.status(200).json({ success: true, error: false, toolName, result, sources });
    } catch (error) {
      console.error(`❌ Erreur /api/tools (${toolName}):`, error.message);
      return res.status(200).json({ success: false, error: true, reply: `⚠️ ${error.message}`, toolName });
    }
  })
);

// ==================== FONCTIONS UTILITAIRES MANQUANTES ====================
function extractGoogleAccessToken(req) {
  if (req.headers["x-google-access-token"]) return String(req.headers["x-google-access-token"]).trim();
  return null;
}

async function setActiveIntent(conversationId, intentType, intentData = {}) {
  await dbRun("UPDATE sessions SET active_intent = ?, intent_data = ? WHERE session_id = ?", [intentType, JSON.stringify(intentData), conversationId]);
}

async function getActiveIntent(conversationId) {
  const row = await dbGet("SELECT active_intent, intent_data FROM sessions WHERE session_id = ?", [conversationId]);
  if (!row || !row.active_intent) return null;
  try {
    return { type: row.active_intent, data: JSON.parse(row.intent_data || "{}") };
  } catch (e) {
    return null;
  }
}

async function clearActiveIntent(conversationId) {
  await dbRun("UPDATE sessions SET active_intent = NULL, intent_data = NULL WHERE session_id = ?", [conversationId]);
}

async function assertConversationOwnership(conversationId, userId) {
  const existing = await dbGet("SELECT user_id, firebase_uid FROM sessions WHERE session_id = ?", [conversationId]);
  if (existing && existing.user_id && existing.user_id !== userId && existing.firebase_uid !== userId) {
    const err = new Error("Cette conversation n'appartient pas à cet utilisateur.");
    err.code = "CONVERSATION_OWNERSHIP";
    throw err;
  }
}

// ==================== DISPATCHER D'OUTILS ====================
async function executeTool(toolName, args = {}, context = {}) {
  const { userId, googleAccessToken } = context;
  let result;
  let sourceKeys = [];

  switch (toolName) {
    case "search_images":
    case "search_image":
      result = await searchWikimediaImages(args.query);
      if (result.images?.length > 0) sourceKeys.push("wikimediacommons");
      break;
    case "search_web":
      result = await searchWeb(args.query);
      sourceKeys = result.sourcesUsed || [];
      break;
    case "search_news":
      result = await searchNews(args.query);
      if (result.articles?.length > 0) sourceKeys.push("googlenews");
      break;
    case "search_sports_scores":
      result = await searchSportsScores(args.query || args.team);
      if (result.events?.length > 0) sourceKeys.push("thesportsdb");
      break;
    case "search_science":
      result = await searchScience(args.query);
      if (result.papers?.length > 0) sourceKeys.push("arxiv");
      break;
    case "search_social":
      result = await searchSocial(args.query);
      if (result.posts?.length > 0) sourceKeys.push("reddit");
      break;
    case "get_weather":
      result = await getWeather(args.location || args.query);
      if (!result.error) sourceKeys.push("openmeteo");
      break;
    case "send_email":
      result = await dispatchSendEmail({
        googleAccessToken,
        recipient: args.recipient || args.to,
        subject: args.subject,
        body: args.body
      });
      break;
    case "send_whatsapp_message":
    case "send_whatsapp":
      result = await sendWhatsAppSmart(userId, args.phone_number || args.to, args.message);
      break;
    default:
      result = { success: false, error: `Outil inconnu: ${toolName}` };
  }

  return { result, sourceKeys };
}

// ==================== GESTION DES INTENTIONS GUIDÉES ====================
async function handleActiveIntent(conversationId, activeIntent, userMessage, context = {}) {
  const { userId, googleAccessToken } = context;

  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data;
      if (data.step === "NEED_NUMBER") {
        const phoneRegex = /^(\+?\d{1,3}[-.\s]?)?\d{9,15}$/;
        if (phoneRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "WHATSAPP", { step: "NEED_MESSAGE", recipient: userMessage.trim() });
          return { reply: `✅ Numéro enregistré. Quel message voulez-vous envoyer à ${userMessage.trim()} ?`, error: false };
        }
        return { reply: "⚠️ Numéro invalide.", error: true };
      }
      if (data.step === "NEED_MESSAGE") {
        try {
          await sendWhatsAppSmart(userId, data.recipient, userMessage);
          await clearActiveIntent(conversationId);
          return { reply: `✅ Message WhatsApp mis en file d'envoi vers ${data.recipient} !`, error: false };
        } catch (error) {
          return { reply: `⚠️ Erreur d'envoi: ${error.message}`, error: true };
        }
      }
      break;
    }
    case "EMAIL": {
      const data = activeIntent.data;
      if (data.step === "NEED_RECIPIENT") {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (emailRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "EMAIL", { step: "NEED_SUBJECT", recipient: userMessage.trim() });
          return { reply: "✅ Destinataire enregistré. Quel est le sujet de l'email ?", error: false };
        }
        return { reply: "⚠️ Adresse email invalide.", error: true };
      }
      if (data.step === "NEED_SUBJECT") {
        await setActiveIntent(conversationId, "EMAIL", { step: "NEED_BODY", recipient: data.recipient, subject: userMessage });
        return { reply: "✅ Sujet enregistré. Quel est le contenu de l'email ?", error: false };
      }
      if (data.step === "NEED_BODY") {
        const result = await dispatchSendEmail({ googleAccessToken, recipient: data.recipient, subject: data.subject, body: userMessage });
        await clearActiveIntent(conversationId);
        if (result.success) return { reply: `✅ Email envoyé à ${data.recipient} !`, error: false };
        return { reply: `⚠️ Erreur: ${result.error}`, error: true };
      }
      break;
    }
  }

  await clearActiveIntent(conversationId);
  return { reply: "Je ne comprends plus l'action. Recommençons.", error: true };
}

// ==================== DÉMARRAGE ====================
const PORT = CONFIG.PORT;
const server = app.listen(PORT, () => {
  console.log("\n" + "=".repeat(60));
  console.log(`🚀 SERVEUR ${CONFIG.AGENT_NAME.toUpperCase()} DÉMARRÉ (v${CONFIG.VERSION})`);
  console.log("=".repeat(60));
  console.log(`🔢 Version : ${CONFIG.VERSION}`);
  console.log(`🔌 Port : ${PORT}`);
  console.log(`🔐 Firebase Auth : ${firebaseApp ? "actif" : "inactif"}`);
  console.log(`🗄️ Supabase : ${supabase ? "connecté" : "non configuré"}`);
  console.log(`📝 Payload max : ${CONFIG.MAX_MESSAGE_LENGTH} caractères`);
  console.log(`🔄 Retry max : ${CONFIG.MAX_RETRY_ATTEMPTS} tentatives par provider`);
  console.log(`👁️ Vision : activé (${CONFIG.VISION_MODEL_GROQ} / ${CONFIG.VISION_MODEL_OPENROUTER})`);
  console.log(`📱 Multi-appareils : ${supabase ? "synchronisé via Supabase" : "local uniquement"}`);
  console.log("=".repeat(60) + "\n");
});

server.timeout = 180000;
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

let shuttingDown = false;
async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n🛑 Arrêt gracieux (${signal})...`);
  
  const forceExitTimer = setTimeout(() => {
    console.error("⏱️ Arrêt forcé");
    process.exit(1);
  }, 10000);

  try {
    await whatsappManager.destroyAll();
    if (whatsappWorker) await whatsappWorker.close();
    if (whatsappQueue) await whatsappQueue.close();
    if (emailTransporter) emailTransporter.close();
    await new Promise((resolve) => db.close((err) => { resolve(); }));
    server.close(() => {
      clearTimeout(forceExitTimer);
      console.log("✅ Arrêt terminé");
      process.exit(0);
    });
  } catch (err) {
    console.error("❌ Erreur pendant l'arrêt:", err.message);
    clearTimeout(forceExitTimer);
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (error) => console.error("❌ Erreur non capturée:", error.message));
process.on("unhandledRejection", (reason) => console.error("❌ Promesse rejetée non gérée:", reason));

// ==================== RÉFÉRENCES AUX FONCTIONS DES SOURCES OUVERTES ====================
// Ces fonctions doivent être définies dans le fichier pour fonctionner :
// searchWikimediaImages, searchWikipediaSummary, searchNews, searchWeb, searchSportsScores,
// searchScience, searchSocial, getWeather, dispatchSendEmail, sendWhatsAppSmart,
// whatsappManager, whatsappGateway, sendWhatsAppSmart, enqueueWhatsAppSend

// Note : Les fonctions des sources ouvertes (searchWikimediaImages, etc.) sont identiques
// à celles de la version 9.1.0 et doivent être incluses dans le fichier complet.
// Pour des raisons de longueur, elles ne sont pas répétées ici mais sont requises.

module.exports = app;
