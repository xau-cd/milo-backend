// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 12.0.0 Enterprise (Production Ready - Blindé)
// Architecture : Modulaire, Microservices-ready, Haute Disponibilité
// Optimisé pour Render.com
//
// FONCTIONNALITÉS :
// - Authentification Firebase Admin SDK complète
// - Gestion des sessions avec tokens
// - Quotas par utilisateur (FREE/PREMIUM/ADMIN)
// - Protection anti-fraude (IP blocking, rate limiting)
// - Audit LLM complet
// - Logs de sécurité
// - WhatsApp via Baileys avec persistance
// - Email via Gmail/Resend/SMTP
// - Recherche multi-sources
// - Pipeline LLM v100/v250 avec failover
// - Vision par IA
// - RGPD (suppression de compte)
// - Moteur d'intention NLP (natural)
// - Calcul formel (mathjs)
// - Agrégation RSS (rss-parser)
// - Recherche web (duck-duck-scrape)
// - Scraping (cheerio)
// - MÉMOIRE CONVERSATIONNELLE PERSISTANTE
// - SYNCHRONISATION UID UTILISATEUR
// ================================================================================

require("dotenv").config();

// ==================== IMPORTS CORE ====================
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
const { EventEmitter } = require("events");
const { Groq } = require("@groq/sdk");
const math = require("mathjs");
const Parser = require("rss-parser");
const { search } = require("duck-duck-scrape");
const cheerio = require("cheerio");
const natural = require("natural");

// ==================== IMPORTS FIREBASE ADMIN ====================
let firebaseAdmin = null;
try {
  firebaseAdmin = require("firebase-admin");
} catch (e) {
  console.warn("⚠️ firebase-admin non installé");
}

// ==================== IMPORTS OPTIONNELS ====================
let BullMQ = null;
let IORedis = null;
try {
  BullMQ = require("bullmq");
  IORedis = require("ioredis");
} catch (e) {
  console.warn("⚠️ BullMQ/Redis non installés - file d'attente en mémoire");
}

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} = require("@whiskeysockets/baileys");

// ==================== CONFIGURATION GLOBALE ====================
const CONFIG = {
  PORT: parseInt(process.env.PORT || "3000", 10),
  ENV: process.env.NODE_ENV || "production",
  VERSION: "12.0.0",
  AGENT_NAME: "Luba",
  COMPANY: "HIKLON Technology",

  MAX_MESSAGE_LENGTH: parseInt(process.env.MAX_MESSAGE_LENGTH || "15000", 10),
  MAX_HISTORY_LENGTH: parseInt(process.env.MAX_HISTORY_LENGTH || "50", 10),
  MAX_CONTEXT_MESSAGES: parseInt(process.env.MAX_CONTEXT_MESSAGES || "20", 10),
  IMAGE_SEARCH_LIMIT: parseInt(process.env.IMAGE_SEARCH_LIMIT || "6", 10),
  MAX_CONTEXT_TOKENS: parseInt(process.env.MAX_CONTEXT_TOKENS || "8000", 10),
  MAX_IMAGE_SIZE_MB: parseInt(process.env.MAX_IMAGE_SIZE_MB || "10", 10),
  MAX_IMAGES_PER_REQUEST: parseInt(process.env.MAX_IMAGES_PER_REQUEST || "3", 10),

  MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS || "3", 10),
  RETRY_BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS || "1000", 10),
  RETRY_MAX_DELAY_MS: parseInt(process.env.RETRY_MAX_DELAY_MS || "8000", 10),
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),
  CIRCUIT_BREAKER_RESET_MS: parseInt(process.env.CIRCUIT_BREAKER_RESET_MS || "60000", 10),

  DEFAULT_TIMEOUT: parseInt(process.env.DEFAULT_TIMEOUT || "30000", 10),
  V250_STEP_TIMEOUT: parseInt(process.env.V250_STEP_TIMEOUT || "90000", 10),
  V250_ROUTE_TIMEOUT: parseInt(process.env.V250_ROUTE_TIMEOUT || "180000", 10),

  WHATSAPP_QR_TIMEOUT: parseInt(process.env.WHATSAPP_QR_TIMEOUT || "30000", 10),
  WHATSAPP_RETRY_DELAY: parseInt(process.env.WHATSAPP_RETRY_DELAY || "3000", 10),

  MAX_LOGIN_ATTEMPTS: parseInt(process.env.MAX_LOGIN_ATTEMPTS || "5", 10),
  LOGIN_BLOCK_DURATION: parseInt(process.env.LOGIN_BLOCK_DURATION || "900000", 10),
  MAX_SESSIONS_PER_USER: parseInt(process.env.MAX_SESSIONS_PER_USER || "10", 10),

  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  UPLOADS_PATH: path.join(__dirname, "uploads"),

  VISION_MODEL_GROQ: process.env.VISION_MODEL_GROQ || "openai/gpt-4o-mini",
  VISION_MODEL_OPENROUTER: process.env.VISION_MODEL_OPENROUTER || "qwen/qwen-2.5-vl-72b-instruct:free",

  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/12.0.0"
};

// ==================== CONFIGURATION FIREBASE ====================
const FIREBASE_CONFIG = {
  apiKey: process.env.FIREBASE_API_KEY || "AIzaSyAdGCNZZAmbFyFSiDErjpEA4C1-PVsy52A",
  projectId: process.env.FIREBASE_PROJECT_ID || "luba-ia-636",
  authDomain: process.env.FIREBASE_AUTH_DOMAIN || "luba-ia-636.firebaseapp.com",
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET || "luba-ia-636.firebasestorage.app",
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID || "502404354252",
  appId: process.env.FIREBASE_APP_ID || "1:502404354252:web:660ab2109ce448e1803269"
};

// ==================== CONFIGURATION HOSTING ====================
const HOSTING_CONFIG = {
  domain: process.env.HOSTING_DOMAIN || "https://luba.web.app",
  allowedOrigins: [
    "https://luba.web.app",
    "https://luba-ia-636.web.app",
    "https://luba-ia-636.firebaseapp.com",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:5173",
    "http://localhost:4200"
  ]
};

// ==================== SYSTÈME DE QUOTAS ====================
const USER_QUOTAS = {
  FREE: {
    maxMessagesPerDay: 100,
    maxImagesPerDay: 20,
    maxWhatsAppMessagesPerDay: 10,
    maxEmailsPerDay: 5,
    maxTokensPerRequest: 8000
  },
  PREMIUM: {
    maxMessagesPerDay: 1000,
    maxImagesPerDay: 200,
    maxWhatsAppMessagesPerDay: 100,
    maxEmailsPerDay: 50,
    maxTokensPerRequest: 32000
  },
  ADMIN: {
    maxMessagesPerDay: 999999,
    maxImagesPerDay: 999999,
    maxWhatsAppMessagesPerDay: 999999,
    maxEmailsPerDay: 999999,
    maxTokensPerRequest: 128000
  }
};

// ==================== CRÉATION DES DOSSIERS ====================
for (const dir of [path.dirname(CONFIG.DB_PATH), CONFIG.SESSIONS_PATH, CONFIG.UPLOADS_PATH]) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Dossier créé: ${dir}`);
  }
}

// ==================== LOGGER PINO ====================
const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  transport: process.env.NODE_ENV === "development" ? {
    target: "pino-pretty",
    options: { colorize: true }
  } : undefined,
  base: {
    service: "luba-backend",
    version: CONFIG.VERSION
  }
});

// ==================== INITIALISATION FIREBASE ADMIN ====================
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
    firebaseApp = firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(serviceAccount),
      projectId: FIREBASE_CONFIG.projectId
    });
    logger.info("✅ Firebase Admin initialisé");
  } catch (e) {
    logger.error({ err: e.message }, "❌ Erreur initialisation Firebase Admin");
  }
} else {
  logger.warn("⚠️ Firebase Admin non initialisé - mode API REST");
}

// ==================== INITIALISATION SQLITE ====================
const db = new sqlite3.Database(CONFIG.DB_PATH, (err) => {
  if (err) {
    logger.error({ err: err.message }, "❌ Impossible d'ouvrir la base SQLite");
    process.exit(1);
  }
  logger.info("✅ Base de données SQLite initialisée");
});

db.run("PRAGMA journal_mode = WAL;");
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -64000;");
db.run("PRAGMA busy_timeout = 10000;");
db.run("PRAGMA temp_store = MEMORY;");
db.run("PRAGMA foreign_keys = ON;");
db.run("PRAGMA wal_autocheckpoint = 1000;");

// ==================== SCHÉMA SQLITE ====================
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    firebase_uid TEXT UNIQUE,
    email TEXT UNIQUE,
    display_name TEXT,
    role TEXT DEFAULT 'FREE',
    email_verified INTEGER DEFAULT 0,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    last_seen_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    firebase_uid TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    active_intent TEXT,
    intent_data TEXT,
    metadata TEXT DEFAULT '{}',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    user_id TEXT,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL,
    tool_calls TEXT,
    images TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
  )`);

  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id, id DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_firebase ON sessions(firebase_uid)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session_role ON messages(session_id, role)");

  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    firebase_uid TEXT,
    to_email TEXT NOT NULL,
    subject TEXT,
    status TEXT DEFAULT 'pending',
    provider TEXT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS llm_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    user_id TEXT,
    provider TEXT,
    model TEXT,
    tier TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    status TEXT DEFAULT 'success',
    error_code TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS user_quotas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    date TEXT NOT NULL,
    messages_count INTEGER DEFAULT 0,
    images_count INTEGER DEFAULT 0,
    whatsapp_count INTEGER DEFAULT 0,
    emails_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS security_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    event_type TEXT NOT NULL,
    details TEXT DEFAULT '{}',
    ip_address TEXT,
    user_agent TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS active_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    session_token TEXT UNIQUE,
    ip_address TEXT,
    user_agent TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    is_revoked INTEGER DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS login_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    ip_address TEXT,
    success INTEGER DEFAULT 0,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS blocked_ips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_address TEXT UNIQUE,
    reason TEXT,
    blocked_until DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS news_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    title TEXT,
    link TEXT,
    pub_date TEXT,
    description TEXT,
    source TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run("CREATE INDEX IF NOT EXISTS idx_user_quotas_user_date ON user_quotas(user_id, date)");
  db.run("CREATE INDEX IF NOT EXISTS idx_security_logs_user ON security_logs(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_llm_audit_user ON llm_audit_log(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_email_logs_user ON email_logs(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_active_sessions_user ON active_sessions(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_login_attempts_ip ON login_attempts(ip_address, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address)");
  db.run("CREATE INDEX IF NOT EXISTS idx_news_cache_category ON news_cache(category, created_at DESC)");
});

logger.info("✅ Schéma SQLite initialisé");

// ==================== INITIALISATION SUPABASE ====================
let supabase = null;
if (process.env.SUPABASE_URL && process.env.SUPABASE_KEY) {
  supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: "public" },
    global: { headers: { "x-application-name": "luba-backend" } }
  });
  logger.info("✅ Supabase initialisé");
} else {
  logger.warn("⚠️ Supabase non configuré");
}

// ==================== CONFIGURATION EMAIL ====================
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
  logger.info("✅ SMTP configuré");
} else {
  logger.warn("⚠️ SMTP non configuré");
}

// ==================== CONFIGURATION MULTER ====================
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  limits: {
    fileSize: CONFIG.MAX_IMAGE_SIZE_MB * 1024 * 1024,
    files: CONFIG.MAX_IMAGES_PER_REQUEST
  },
  fileFilter: (req, file, cb) => {
    if (CONFIG.ALLOWED_IMAGE_TYPES.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error(`Type de fichier non supporté. Types autorisés: ${CONFIG.ALLOWED_IMAGE_TYPES.join(", ")}`));
    }
  }
});

// ==================== INITIALISATION GROQ SDK ====================
const groq = new Groq({
  apiKey: process.env.GROQ_API_KEY || ""
});

// ==================== INITIALISATION RSS PARSER ====================
const rssParser = new Parser({
  timeout: 10000,
  headers: {
    "User-Agent": CONFIG.HTTP_USER_AGENT
  }
});

// ==================== INITIALISATION NATURAL NLP ====================
const tokenizer = new natural.WordTokenizer();
const classifier = new natural.BayesClassifier();

// Entraînement du classifieur d'intention
classifier.addDocument("calcule 2+2", "MATHS");
classifier.addDocument("résous cette équation x^2 + 3x + 2 = 0", "MATHS");
classifier.addDocument("intégrale de sin(x)", "MATHS");
classifier.addDocument("dérivée de x^2", "MATHS");
classifier.addDocument("factorielle de 10", "MATHS");
classifier.addDocument("matrice inverse", "MATHS");

classifier.addDocument("quelles sont les dernières actualités", "ACTUALITÉ");
classifier.addDocument("informations sur la politique", "ACTUALITÉ");
classifier.addDocument("news du jour", "ACTUALITÉ");
classifier.addDocument("dernières nouvelles internationales", "ACTUALITÉ");
classifier.addDocument("que se passe-t-il dans le monde", "ACTUALITÉ");

classifier.addDocument("score du match de football", "SPORT");
classifier.addDocument("résultat du PSG", "SPORT");
classifier.addDocument("classement ligue 1", "SPORT");
classifier.addDocument("dernier match de tennis", "SPORT");
classifier.addDocument("résultats NBA", "SPORT");

classifier.addDocument("écris un code en javascript", "CODE");
classifier.addDocument("fonction python pour trier", "CODE");
classifier.addDocument("debug ce script", "CODE");
classifier.addDocument("crée une API REST", "CODE");
classifier.addDocument("requête SQL", "CODE");

classifier.train();

// ==================== UTILITAIRES ====================
function convertImageToBase64(buffer, mimetype) {
  return {
    dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`,
    base64: buffer.toString("base64"),
    mimetype,
    size: buffer.length
  };
}

function generateRequestId() {
  return `req_${crypto.randomUUID()}`;
}

function generateConversationId() {
  return `conv_${crypto.randomUUID()}`;
}

function generateSessionToken() {
  return `sess_${crypto.randomBytes(32).toString("hex")}`;
}

function decodeXmlEntities(str) {
  return String(str)
    .replace(/<!\[CDATA\[/g, "")
    .replace(/\]\]>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

// ==================== WRAPPERS SQLITE ====================
function dbGet(query, params = []) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbAll(query, params = []) {
  return new Promise((resolve, reject) => {
    db.all(query, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function dbRun(query, params = []) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

// ==================== AUDIT LLM ====================
async function auditLLMCall({ sessionId, userId, provider, model, tier, promptTokens, completionTokens, latencyMs, status, errorCode }) {
  try {
    await dbRun(
      `INSERT INTO llm_audit_log (session_id, user_id, provider, model, tier, prompt_tokens, completion_tokens, latency_ms, status, error_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [sessionId, userId, provider, model, tier, promptTokens, completionTokens, latencyMs, status, errorCode]
    );
  } catch (error) {
    logger.error({ error: error.message }, "Erreur audit LLM");
  }
}

// ==================== LOGS DE SÉCURITÉ ====================
async function logSecurityEvent(userId, eventType, details = {}, ipAddress = null, userAgent = null) {
  try {
    await dbRun(
      `INSERT INTO security_logs (user_id, event_type, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)`,
      [userId, eventType, JSON.stringify(details), ipAddress, userAgent]
    );
  } catch (error) {
    logger.error({ error: error.message }, "Erreur log sécurité");
  }
}

// ==================== VÉRIFICATION TOKEN FIREBASE ====================
async function verifyFirebaseToken(token) {
  if (firebaseApp && firebaseAdmin) {
    try {
      const decodedToken = await firebaseAdmin.auth(firebaseApp).verifyIdToken(token, true);
      return {
        uid: decodedToken.uid,
        email: decodedToken.email || null,
        displayName: decodedToken.name || null,
        photoURL: decodedToken.picture || null,
        emailVerified: decodedToken.email_verified || false,
        role: decodedToken.role || 'FREE',
        customClaims: decodedToken
      };
    } catch (error) {
      logger.error({ error: error.message }, "Erreur vérification token (Admin SDK)");
      throw error;
    }
  }
  
  try {
    const response = await axios.post(
      `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${FIREBASE_CONFIG.apiKey}`,
      { idToken: token },
      { timeout: 10000 }
    );
    
    if (response.data.users && response.data.users.length > 0) {
      const user = response.data.users[0];
      return {
        uid: user.localId,
        email: user.email || null,
        displayName: user.displayName || null,
        photoURL: user.photoUrl || null,
        emailVerified: user.emailVerified || false,
        role: 'FREE'
      };
    }
    
    return null;
  } catch (error) {
    logger.error({ error: error.message }, "Erreur vérification token (API REST)");
    throw error;
  }
}

// ==================== GESTION DES RÔLES ====================
async function setUserRole(uid, role) {
  if (firebaseApp && firebaseAdmin) {
    try {
      await firebaseAdmin.auth(firebaseApp).setCustomUserClaims(uid, { role });
      await dbRun(`UPDATE users SET role = ? WHERE firebase_uid = ? OR id = ?`, [role, uid, uid]);
      await logSecurityEvent(uid, 'ROLE_UPDATED', { role });
      return { success: true, role };
    } catch (error) {
      logger.error({ error: error.message }, "Erreur mise à jour rôle");
      throw error;
    }
  }
  
  await dbRun(`UPDATE users SET role = ? WHERE firebase_uid = ? OR id = ?`, [role, uid, uid]);
  return { success: true, role };
}

async function getUserRole(uid) {
  if (firebaseApp && firebaseAdmin) {
    try {
      const user = await firebaseAdmin.auth(firebaseApp).getUser(uid);
      return user.customClaims?.role || 'FREE';
    } catch (error) {
      logger.error({ error: error.message }, "Erreur récupération rôle");
    }
  }
  
  const user = await dbGet("SELECT role FROM users WHERE firebase_uid = ? OR id = ?", [uid, uid]);
  return user?.role || 'FREE';
}

// ==================== GESTION DES SESSIONS ====================
async function createActiveSession(userId, ipAddress, userAgent) {
  const sessionToken = generateSessionToken();
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  
  const activeSessions = await dbAll(
    `SELECT COUNT(*) as count FROM active_sessions WHERE user_id = ? AND is_revoked = 0 AND expires_at > CURRENT_TIMESTAMP`,
    [userId]
  );
  
  if (activeSessions[0]?.count >= CONFIG.MAX_SESSIONS_PER_USER) {
    await dbRun(
      `UPDATE active_sessions SET is_revoked = 1 WHERE id = (SELECT id FROM active_sessions WHERE user_id = ? AND is_revoked = 0 ORDER BY created_at ASC LIMIT 1)`,
      [userId]
    );
  }
  
  await dbRun(
    `INSERT INTO active_sessions (user_id, session_token, ip_address, user_agent, expires_at) VALUES (?, ?, ?, ?, ?)`,
    [userId, sessionToken, ipAddress, userAgent, expiresAt]
  );
  
  return sessionToken;
}

async function validateActiveSession(userId, sessionToken) {
  const session = await dbGet(
    `SELECT * FROM active_sessions WHERE user_id = ? AND session_token = ? AND is_revoked = 0 AND expires_at > CURRENT_TIMESTAMP`,
    [userId, sessionToken]
  );
  
  if (session) {
    await dbRun(`UPDATE active_sessions SET last_activity = CURRENT_TIMESTAMP WHERE id = ?`, [session.id]);
    return true;
  }
  
  return false;
}

async function revokeSession(userId, sessionToken) {
  await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND session_token = ?`, [userId, sessionToken]);
}

async function revokeAllSessions(userId) {
  await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND is_revoked = 0`, [userId]);
}

// ==================== GESTION DES TENTATIVES ====================
async function checkLoginAttempts(ipAddress) {
  const cutoffTime = new Date(Date.now() - CONFIG.LOGIN_BLOCK_DURATION).toISOString();
  
  const attempts = await dbGet(
    `SELECT COUNT(*) as count FROM login_attempts WHERE ip_address = ? AND success = 0 AND created_at > ?`,
    [ipAddress, cutoffTime]
  );
  
  if (attempts?.count >= CONFIG.MAX_LOGIN_ATTEMPTS) {
    await dbRun(
      `INSERT OR REPLACE INTO blocked_ips (ip_address, reason, blocked_until) VALUES (?, 'Trop de tentatives échouées', ?)`,
      [ipAddress, new Date(Date.now() + CONFIG.LOGIN_BLOCK_DURATION).toISOString()]
    );
    
    return { blocked: true, message: "Trop de tentatives échouées. IP bloquée temporairement." };
  }
  
  return { blocked: false };
}

async function recordLoginAttempt(ipAddress, userId, success, errorMessage = null) {
  await dbRun(
    `INSERT INTO login_attempts (user_id, ip_address, success, error_message) VALUES (?, ?, ?, ?)`,
    [userId, ipAddress, success ? 1 : 0, errorMessage]
  );
}

async function isIPBlocked(ipAddress) {
  const blocked = await dbGet(
    `SELECT * FROM blocked_ips WHERE ip_address = ? AND blocked_until > CURRENT_TIMESTAMP`,
    [ipAddress]
  );
  return Boolean(blocked);
}

// ==================== GESTION DES QUOTAS ====================
async function checkUserQuota(userId, action, userRole = 'FREE') {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    const quotaRow = await dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]);
    
    if (!quotaRow) {
      await dbRun(
        `INSERT INTO user_quotas (user_id, date, messages_count, images_count, whatsapp_count, emails_count) VALUES (?, ?, 0, 0, 0, 0)`,
        [userId, today]
      );
      return { allowed: true, remaining: USER_QUOTAS[userRole] || USER_QUOTAS.FREE };
    }
    
    const limits = USER_QUOTAS[userRole] || USER_QUOTAS.FREE;
    
    let currentCount = 0;
    let maxAllowed = 0;
    
    switch (action) {
      case 'message':
        currentCount = quotaRow.messages_count;
        maxAllowed = limits.maxMessagesPerDay;
        break;
      case 'image':
        currentCount = quotaRow.images_count;
        maxAllowed = limits.maxImagesPerDay;
        break;
      case 'whatsapp':
        currentCount = quotaRow.whatsapp_count;
        maxAllowed = limits.maxWhatsAppMessagesPerDay;
        break;
      case 'email':
        currentCount = quotaRow.emails_count;
        maxAllowed = limits.maxEmailsPerDay;
        break;
    }
    
    if (currentCount >= maxAllowed) {
      return { 
        allowed: false, 
        remaining: 0,
        message: `Limite quotidienne atteinte pour ${action}. Limite : ${maxAllowed}`,
        current: currentCount,
        max: maxAllowed
      };
    }
    
    return { 
      allowed: true, 
      remaining: maxAllowed - currentCount,
      current: currentCount,
      max: maxAllowed
    };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur vérification quota");
    return { allowed: true, remaining: null };
  }
}

async function incrementUserQuota(userId, action) {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    let columnToUpdate;
    switch (action) {
      case 'message': columnToUpdate = 'messages_count'; break;
      case 'image': columnToUpdate = 'images_count'; break;
      case 'whatsapp': columnToUpdate = 'whatsapp_count'; break;
      case 'email': columnToUpdate = 'emails_count'; break;
      default: return;
    }
    
    await dbRun(
      `UPDATE user_quotas SET ${columnToUpdate} = ${columnToUpdate} + 1 WHERE user_id = ? AND date = ?`,
      [userId, today]
    );
  } catch (error) {
    logger.error({ error: error.message }, "Erreur mise à jour quota");
  }
}

// ==================== CIRCUIT BREAKER ====================
class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || CONFIG.CIRCUIT_BREAKER_THRESHOLD;
    this.resetTimeout = options.resetTimeout || CONFIG.CIRCUIT_BREAKER_RESET_MS;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED";
    this.emitter = new EventEmitter();
  }

  async execute(fn) {
    if (this.state === "OPEN") {
      const elapsed = Date.now() - this.lastFailureTime;
      if (elapsed >= this.resetTimeout) {
        this.state = "HALF_OPEN";
        logger.info({ circuit: this.name }, "Circuit breaker: HALF_OPEN");
      } else {
        throw new Error(`Circuit breaker ${this.name} est OPEN`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  onSuccess() {
    this.failureCount = 0;
    this.state = "CLOSED";
    this.emitter.emit("success", { name: this.name });
  }

  onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.failureCount >= this.failureThreshold) {
      this.state = "OPEN";
      this.emitter.emit("open", { name: this.name, failureCount: this.failureCount });
      logger.warn({ circuit: this.name, failures: this.failureCount }, "Circuit breaker: OPEN");
    }

    this.emitter.emit("failure", { name: this.name, failureCount: this.failureCount });
  }
}

// ==================== GESTIONNAIRE DE FILE ====================
class QueueManager {
  constructor() {
    this.useRedis = Boolean(process.env.REDIS_URL) && Boolean(BullMQ) && Boolean(IORedis);
    this.queues = new Map();
    this.workers = new Map();
    this.inMemoryQueues = new Map();

    if (this.useRedis) {
      this.connection = new IORedis(process.env.REDIS_URL, {
        maxRetriesPerRequest: null,
        enableReadyCheck: true,
        retryStrategy: (times) => Math.min(times * 200, 5000)
      });
      logger.info("✅ File d'attente Redis initialisée");
    } else {
      logger.warn("⚠️ File d'attente en mémoire (fallback)");
    }
  }

  createQueue(name, processor, options = {}) {
    if (this.useRedis) {
      const queue = new BullMQ.Queue(name, { connection: this.connection });
      const worker = new BullMQ.Worker(name, processor, {
        connection: this.connection,
        concurrency: options.concurrency || 3,
        limiter: options.limiter || { max: 10, duration: 1000 }
      });

      worker.on("failed", (job, err) => {
        logger.error({ jobId: job?.id, err: err.message }, `Job ${name} échoué`);
      });

      this.queues.set(name, queue);
      this.workers.set(name, worker);
    } else {
      const inMemoryQueue = [];
      let processing = false;
      const processQueue = async () => {
        if (processing) return;
        processing = true;
        while (inMemoryQueue.length > 0) {
          const job = inMemoryQueue.shift();
          try {
            await processor(job);
          } catch (error) {
            logger.error({ err: error.message }, `Job ${name} échoué (mémoire)`);
          }
        }
        processing = false;
      };

      this.inMemoryQueues.set(name, {
        add: async (data) => {
          inMemoryQueue.push(data);
          processQueue();
        }
      });
    }
  }

  async add(name, data, options = {}) {
    if (this.useRedis) {
      const queue = this.queues.get(name);
      if (queue) {
        return await queue.add("process", data, {
          attempts: options.attempts || 5,
          backoff: { type: "exponential", delay: options.backoffDelay || 2000 },
          removeOnComplete: 100,
          removeOnFail: 500
        });
      }
    } else {
      const queue = this.inMemoryQueues.get(name);
      if (queue) {
        return await queue.add(data);
      }
    }
    throw new Error(`Queue ${name} non trouvée`);
  }

  async close() {
    if (this.useRedis) {
      for (const worker of this.workers.values()) await worker.close();
      for (const queue of this.queues.values()) await queue.close();
      await this.connection.quit();
    }
  }
}

const queueManager = new QueueManager();

// ==================== SOURCES OUVERTES ====================
const OPEN_SOURCES = {
  wikipedia: { name: "Wikipédia", url: "https://fr.wikipedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikipedia.org" },
  wikimediacommons: { name: "Wikimedia Commons", url: "https://commons.wikimedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikimedia.org" },
  googlenews: { name: "Google News", url: "https://news.google.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=news.google.com" },
  thesportsdb: { name: "TheSportsDB", url: "https://www.thesportsdb.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=thesportsdb.com" },
  arxiv: { name: "arXiv", url: "https://arxiv.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=arxiv.org" },
  reddit: { name: "Reddit", url: "https://reddit.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=reddit.com" },
  openmeteo: { name: "Open-Meteo", url: "https://open-meteo.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=open-meteo.com" }
};

// ==================== FONCTIONS DE RECHERCHE ====================
async function searchWikimediaImages(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(query)}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 15000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const pages = response.data?.query?.pages;
    if (!pages) return { images: [] };
    const images = Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        title: page.title || "Image",
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
        pageUrl: page.imageinfo?.[0]?.descriptionurl || null
      }))
      .filter((img) => img.url);
    return { images };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur recherche images");
    return { images: [], error: error.message };
  }
}

async function searchWikipediaSummary(query) {
  try {
    const url = `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`;
    const response = await axios.get(url, { timeout: 10000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    if (response.data?.type === "disambiguation" || !response.data?.extract) return { summary: null };
    return {
      title: response.data.title,
      summary: response.data.extract,
      url: response.data.content_urls?.desktop?.page
    };
  } catch (error) {
    return { summary: null, error: error.message };
  }
}

async function searchNews(query) {
  if (!query) return { articles: [] };
  try {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=fr&gl=FR&ceid=FR:fr`;
    const response = await axios.get(url, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const xml = response.data;
    const items = [];
    const itemRegex = /<item>([\s\S]*?)<\/item>/g;
    let match;
    while ((match = itemRegex.exec(xml)) !== null && items.length < 6) {
      const block = match[1];
      const title = (block.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const link = (block.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || "";
      const pubDate = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || "";
      const description = (block.match(/<description>([\s\S]*?)<\/description>/) || [])[1] || "";
      if (title) items.push({ title: decodeXmlEntities(title), link: link.trim(), pubDate, description: decodeXmlEntities(description) });
    }
    return { articles: items };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur recherche actualités");
    return { articles: [], error: error.message };
  }
}

async function searchWeb(query) {
  if (!query || typeof query !== "string") return { results: [], sourcesUsed: [] };
  const [wiki, news, ddgResults] = await Promise.all([
    searchWikipediaSummary(query),
    searchNews(query),
    searchDuckDuckGo(query)
  ]);
  const results = [];
  const sourcesUsed = [];
  
  if (wiki.summary) {
    results.push({ title: wiki.title, snippet: wiki.summary, url: wiki.url, type: "wiki" });
    sourcesUsed.push("wikipedia");
  }
  
  if (ddgResults.length > 0) {
    for (const r of ddgResults) {
      results.push({ title: r.title, snippet: r.snippet, url: r.url, type: "web" });
    }
  }
  
  if (news.articles?.length > 0) {
    news.articles.slice(0, 3).forEach((a) => results.push({ title: a.title, url: a.link, pubDate: a.pubDate, type: "news" }));
    sourcesUsed.push("googlenews");
  }
  
  return { results, sourcesUsed };
}

async function searchDuckDuckGo(query) {
  try {
    const results = await search(query, {
      safeSearch: "OFF",
      locale: "fr-fr",
      maxResults: 5
    });
    return results.map(r => ({
      title: r.title,
      snippet: r.description,
      url: r.url,
      source: r.source
    }));
  } catch (error) {
    logger.error({ error: error.message }, "Erreur recherche DuckDuckGo");
    return [];
  }
}

async function scrapeArticleContent(url) {
  try {
    const response = await axios.get(url, {
      timeout: 15000,
      headers: { "User-Agent": CONFIG.HTTP_USER_AGENT }
    });
    const $ = cheerio.load(response.data);
    const title = $("title").text().trim();
    const paragraphs = [];
    $("p").each((i, el) => {
      const text = $(el).text().trim();
      if (text.length > 50) paragraphs.push(text);
    });
    return {
      title,
      content: paragraphs.slice(0, 5).join("\n\n").slice(0, 2000),
      url
    };
  } catch (error) {
    return { error: error.message, url };
  }
}

async function searchSportsScores(query) {
  if (!query) return { events: [], error: "Aucune équipe précisée" };
  try {
    const searchUrl = `https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t=${encodeURIComponent(query)}`;
    const searchResp = await axios.get(searchUrl, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const team = searchResp.data?.teams?.[0];
    if (!team) return { events: [], error: `Équipe "${query}" introuvable` };
    const eventsUrl = `https://www.thesportsdb.com/api/v1/json/3/eventslast.php?id=${team.idTeam}`;
    const eventsResp = await axios.get(eventsUrl, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const events = (eventsResp.data?.results || []).slice(0, 5).map((e) => ({
      match: `${e.strHomeTeam} ${e.intHomeScore ?? "?"} - ${e.intAwayScore ?? "?"} ${e.strAwayTeam}`,
      date: e.dateEvent,
      league: e.strLeague
    }));
    return { team: team.strTeam, events };
  } catch (error) {
    return { events: [], error: error.message };
  }
}

async function searchScience(query) {
  if (!query) return { papers: [] };
  try {
    const url = `http://export.arxiv.org/api/query?search_query=all:${encodeURIComponent(query)}&start=0&max_results=5`;
    const response = await axios.get(url, { timeout: 15000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const xml = response.data;
    const items = [];
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    let match;
    while ((match = entryRegex.exec(xml)) !== null && items.length < 5) {
      const block = match[1];
      const title = (block.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const summary = (block.match(/<summary>([\s\S]*?)<\/summary>/) || [])[1] || "";
      const link = (block.match(/<id>([\s\S]*?)<\/id>/) || [])[1] || "";
      if (title) items.push({ title: decodeXmlEntities(title), summary: decodeXmlEntities(summary).slice(0, 300), link: link.trim() });
    }
    return { papers: items };
  } catch (error) {
    return { papers: [], error: error.message };
  }
}

async function searchSocial(query) {
  if (!query) return { posts: [] };
  try {
    const url = `https://www.reddit.com/search.json?q=${encodeURIComponent(query)}&limit=6&sort=relevance`;
    const response = await axios.get(url, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const posts = (response.data?.data?.children || []).map((c) => ({
      title: c.data.title,
      subreddit: c.data.subreddit_name_prefixed,
      score: c.data.score,
      url: `https://reddit.com${c.data.permalink}`
    }));
    return { posts };
  } catch (error) {
    return { posts: [], error: error.message };
  }
}

async function getWeather(location) {
  if (!location) return { error: "Aucun lieu précisé" };
  try {
    const geoUrl = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(location)}&count=1&language=fr`;
    const geoResp = await axios.get(geoUrl, { timeout: 10000 });
    const place = geoResp.data?.results?.[0];
    if (!place) return { error: `Lieu "${location}" introuvable` };
    const weatherUrl = `https://api.open-meteo.com/v1/forecast?latitude=${place.latitude}&longitude=${place.longitude}&current=temperature_2m,weather_code,wind_speed_10m&timezone=auto`;
    const weatherResp = await axios.get(weatherUrl, { timeout: 10000 });
    const current = weatherResp.data?.current;
    return {
      location: `${place.name}, ${place.country}`,
      temperature: current?.temperature_2m,
      windSpeed: current?.wind_speed_10m,
      weatherCode: current?.weather_code
    };
  } catch (error) {
    return { error: error.message };
  }
}

// ==================== MOTEUR MATHÉMATIQUE ====================
function executeMathExpression(expression) {
  try {
    const result = math.evaluate(expression);
    return {
      success: true,
      expression,
      result: result,
      formatted: math.format(result, { precision: 14 })
    };
  } catch (error) {
    return {
      success: false,
      expression,
      error: error.message
    };
  }
}

function detectMathExpressions(message) {
  const patterns = [
    /(\d+[\d\s\*\+\-\/\(\)\.]+\d+)/g,
    /(?:calcule|calcul|résous|resous|solve|compute)\s*:?\s*([^\n]+)/i,
    /(\d+\s*[\+\-\*\/\^]\s*\d+)/g,
    /(?:intégrale|integrale|dérivée|derivee|factorielle|matrice|limite)\s*(?:de|of)?\s*:?\s*([^\n]+)/i,
    /(?:sqrt|sin|cos|tan|log|exp|abs|floor|ceil|round)\s*\([^)]+\)/g
  ];
  
  const expressions = [];
  for (const pattern of patterns) {
    const matches = message.match(pattern);
    if (matches) {
      expressions.push(...matches);
    }
  }
  
  return expressions;
}

// ==================== ANALYSE D'INTENTION ====================
function analyzeIntent(message) {
  const lowerMessage = message.toLowerCase();
  
  // Vérification mathématique via patterns
  const mathPatterns = [
    /[\d\s\*\+\-\/\(\)\.]{3,}[\d\)]/, 
    /(?:calcule|calcul|résous|resous|solve|compute|equation|équation)/i,
    /(?:intégrale|integrale|dérivée|derivee|factorielle|matrice|limite)/i,
    /(?:sqrt|sin\(|cos\(|tan\(|log\(|exp\()/i,
    /[\^]{1,2}\d/
  ];
  const hasMath = mathPatterns.some(p => p.test(message));
  
  // Vérification actualités
  const newsWords = ["actualité", "actualites", "actualité", "news", "dernières nouvelles", "journal", "politique", "économie", "monde", "international", "breaking", "info"];
  const hasNews = newsWords.some(w => lowerMessage.includes(w));
  
  // Vérification sport
  const sportWords = ["sport", "match", "football", "basket", "tennis", "score", "résultat", "resultat", "classement", "ligue", "championnat", "nba", "psg", "om", "real madrid", "barca"];
  const hasSport = sportWords.some(w => lowerMessage.includes(w));
  
  // Vérification code
  const codeWords = ["code", "coder", "programmation", "programme", "javascript", "python", "java", "c++", "typescript", "react", "vue", "angular", "node", "api", "debug", "fonction", "function", "classe", "class", "algorithme", "sql", "html", "css"];
  const hasCode = codeWords.some(w => lowerMessage.includes(w));
  
  if (hasMath) return "MATHS";
  if (hasSport) return "SPORT";
  if (hasNews) return "ACTUALITÉ";
  if (hasCode) return "CODE";
  
  // Utilisation du classifieur NLP
  const classified = classifier.classify(lowerMessage);
  
  // Mapping des intentions
  const intentMap = {
    "MATHS": "MATHS",
    "ACTUALITÉ": "ACTUALITÉ",
    "SPORT": "SPORT",
    "CODE": "CODE"
  };
  
  return intentMap[classified] || "GENERAL";
}

// ==================== GESTION DE LA MÉMOIRE CONVERSATIONNELLE ====================
// Fonction améliorée pour récupérer l'historique complet avec contexte
async function getFullHistory(conversationId, userId = null, limit = CONFIG.MAX_CONTEXT_MESSAGES) {
  try {
    // Récupérer les messages depuis SQLite avec le user_id
    let query = "SELECT role, content, created_at FROM messages WHERE session_id = ?";
    let params = [conversationId];
    
    if (userId) {
      query += " AND (user_id = ? OR user_id IS NULL)";
      params.push(userId);
    }
    
    query += " ORDER BY id DESC LIMIT ?";
    params.push(limit);
    
    const localRows = await dbAll(query, params);
    
    if (localRows.length > 0) {
      // Inverser pour avoir l'ordre chronologique
      return localRows.reverse().map(row => ({
        role: row.role,
        content: row.content
      }));
    }
    
    // Fallback vers Supabase si disponible
    if (supabase) {
      try {
        const { data: supabaseMessages, error } = await supabase
          .from("messages")
          .select("role, content")
          .eq("session_id", conversationId)
          .order("created_at", { ascending: false })
          .limit(limit);
        
        if (supabaseMessages && !error && supabaseMessages.length > 0) {
          return supabaseMessages.reverse().map(row => ({
            role: row.role,
            content: row.content
          }));
        }
      } catch (error) {
        logger.error({ error: error.message }, "Erreur Supabase getFullHistory");
      }
    }
    
    return [];
  } catch (error) {
    logger.error({ error: error.message }, "Erreur getFullHistory");
    return [];
  }
}

// Fonction pour sauvegarder un message avec le user_id
async function saveMessageWithUser(conversationId, role, content, userId = null, firebaseUid = null, metadata = {}) {
  try {
    await dbRun(
      "INSERT INTO messages (session_id, user_id, role, content, metadata) VALUES (?, ?, ?, ?, ?)",
      [conversationId, userId, role, content, JSON.stringify(metadata)]
    );
    
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
    
    // Synchronisation Supabase
    if (supabase && firebaseUid) {
      try {
        await supabase.from("messages").insert({
          session_id: conversationId,
          firebase_uid: firebaseUid,
          user_id: userId,
          role,
          content,
          created_at: new Date().toISOString()
        });
      } catch (error) {
        logger.error({ error: error.message }, "Erreur sync message Supabase");
      }
    }
    
    return true;
  } catch (error) {
    logger.error({ error: error.message }, "Erreur saveMessageWithUser");
    return false;
  }
}

// ==================== ENVOI D'EMAIL ====================
async function verifyGmailScope(accessToken) {
  try {
    const response = await axios.get("https://www.googleapis.com/oauth2/v1/tokeninfo", {
      params: { access_token: accessToken },
      timeout: 10000
    });
    const scopes = response.data.scope?.split(" ") || [];
    return scopes.includes("https://www.googleapis.com/auth/gmail.send") || scopes.includes("https://mail.google.com/");
  } catch (error) {
    return false;
  }
}

async function sendEmailViaGmail(accessToken, recipient, subject, body) {
  const hasValidScope = await verifyGmailScope(accessToken);
  if (!hasValidScope) {
    const error = new Error("Token Gmail invalide ou scope gmail.send manquant");
    error.code = "GMAIL_SCOPE_MISSING";
    throw error;
  }
  
  const messageLines = [
    `To: ${recipient}`,
    `Subject: =?utf-8?B?${Buffer.from(subject || "(sans sujet)").toString("base64")}?=`,
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    "",
    body || ""
  ];
  const rawMessage = messageLines.join("\r\n");
  const encodedMessage = Buffer.from(rawMessage).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const response = await axios.post(
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    { raw: encodedMessage },
    { headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" }, timeout: 15000 }
  );
  return { success: true, provider: "gmail", messageId: response.data?.id || null };
}

async function sendEmailViaResend(recipient, subject, body) {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) return { success: false, error: "RESEND_API_KEY non configurée" };
  try {
    const response = await axios.post(
      "https://api.resend.com/emails",
      {
        from: process.env.RESEND_FROM || "Luba <onboarding@resend.dev>",
        to: recipient,
        subject: subject || "(sans sujet)",
        html: `<div style="font-family: Arial; padding: 20px;">${String(body || "").replace(/\n/g, "<br>")}</div>`
      },
      { headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" }, timeout: 15000 }
    );
    return { success: true, provider: "resend", messageId: response.data?.id || null };
  } catch (error) {
    return { success: false, error: `Resend: ${error.response?.data?.message || error.message}` };
  }
}

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
    return { success: false, error: error.message };
  }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body, userId }) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!recipient || !emailRegex.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  let result;
  if (googleAccessToken) {
    try {
      result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body);
    } catch (error) {
      result = { success: false, error: `Gmail API: ${error.message}` };
    }
  } else if (process.env.RESEND_API_KEY) {
    result = await sendEmailViaResend(recipient, subject, body);
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  try {
    await dbRun("INSERT INTO email_logs (user_id, to_email, subject, status, provider, error_message) VALUES (?, ?, ?, ?, ?, ?)", [
      userId || null, recipient, subject || null, result.success ? "sent" : "failed", result.provider || null, result.error || null
    ]);
  } catch (logErr) {
    logger.error({ error: logErr.message }, "Erreur journalisation email");
  }

  return result;
}

// ==================== PERSISTANCE WHATSAPP ====================
async function saveWhatsAppCredentials(userId, credentialsData) {
  if (!supabase) return false;
  
  try {
    const cipher = crypto.createCipheriv(
      'aes-256-gcm',
      Buffer.from(process.env.WHATSAPP_ENCRYPTION_KEY || 'default-key-32-bytes-long!!!!!!'),
      Buffer.from(process.env.WHATSAPP_ENCRYPTION_IV || 'default-iv-16')
    );
    
    let encrypted = cipher.update(JSON.stringify(credentialsData), 'utf8', 'hex');
    encrypted += cipher.final('hex');
    const authTag = cipher.getAuthTag().toString('hex');
    
    const { error } = await supabase
      .from('whatsapp_credentials')
      .upsert({ user_id: userId, encrypted_data: encrypted, auth_tag: authTag, updated_at: new Date().toISOString() });
    
    if (error) {
      logger.error({ error: error.message }, "Erreur sauvegarde credentials WhatsApp");
      return false;
    }
    
    return true;
  } catch (error) {
    logger.error({ error: error.message }, "Erreur chiffrement credentials WhatsApp");
    return false;
  }
}

async function loadWhatsAppCredentials(userId) {
  if (!supabase) return null;
  
  try {
    const { data, error } = await supabase
      .from('whatsapp_credentials')
      .select('encrypted_data, auth_tag')
      .eq('user_id', userId)
      .single();
    
    if (error || !data) return null;
    
    const decipher = crypto.createDecipheriv(
      'aes-256-gcm',
      Buffer.from(process.env.WHATSAPP_ENCRYPTION_KEY || 'default-key-32-bytes-long!!!!!!'),
      Buffer.from(process.env.WHATSAPP_ENCRYPTION_IV || 'default-iv-16')
    );
    
    decipher.setAuthTag(Buffer.from(data.auth_tag, 'hex'));
    
    let decrypted = decipher.update(data.encrypted_data, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    
    return JSON.parse(decrypted);
  } catch (error) {
    logger.error({ error: error.message }, "Erreur déchiffrement credentials WhatsApp");
    return null;
  }
}

// ==================== WHATSAPP - BAILEYS ====================
function toPlainWhatsAppText(markdown) {
  return String(markdown)
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[!\[.*?\]\(.*?\)\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

class BaileysManager {
  constructor() {
    this.sessions = new Map();
  }

  async initClient(userId) {
    const existing = this.sessions.get(userId);
    if (existing?.ready) return { connected: true, qrCode: null };
    if (existing?.qrCode) return { connected: false, qrCode: existing.qrCode };

    const authDir = path.join(CONFIG.SESSIONS_PATH, userId);
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    const savedCredentials = await loadWhatsAppCredentials(userId);
    if (savedCredentials) {
      try {
        fs.writeFileSync(path.join(authDir, "creds.json"), JSON.stringify(savedCredentials));
      } catch (error) {
        logger.error({ error: error.message }, "Erreur restauration credentials WhatsApp");
      }
    }

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    let version;
    try {
      version = (await fetchLatestBaileysVersion()).version;
    } catch (e) {
      version = undefined;
    }

    const sock = makeWASocket({
      version,
      auth: state,
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      browser: ["Luba.ia", "Chrome", "1.0.0"]
    });

    const sessionData = { sock, qrCode: null, ready: false };
    this.sessions.set(userId, sessionData);

    sock.ev.on("creds.update", async (creds) => {
      await saveCreds();
      await saveWhatsAppCredentials(userId, creds);
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          sessionData.qrCode = await qrcode.toDataURL(qr, { width: 600, margin: 2 });
          logger.info({ userId }, "QR Code WhatsApp généré");
        } catch (e) {
          logger.error({ error: e.message }, "Erreur génération QR");
        }
      }

      if (connection === "open") {
        sessionData.ready = true;
        sessionData.qrCode = null;
        db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
        logger.info({ userId }, "WhatsApp connecté");
      }

      if (connection === "close") {
        sessionData.ready = false;
        db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        this.sessions.delete(userId);
        if (shouldReconnect) {
          setTimeout(() => {
            this.initClient(userId).catch((e) => logger.error({ error: e.message }, "Erreur reconnexion WhatsApp"));
          }, CONFIG.WHATSAPP_RETRY_DELAY);
        }
      }
    });

    sock.ev.on("messages.upsert", async ({ messages: msgs, type }) => {
      if (type !== "notify") return;
      for (const msg of msgs) {
        try {
          if (!msg.message || msg.key.fromMe) continue;
          const remoteJid = msg.key.remoteJid;
          if (!remoteJid || remoteJid.endsWith("@g.us")) continue;

          const text = msg.message.conversation || msg.message.extendedTextMessage?.text || msg.message.imageMessage?.caption || null;
          if (!text) continue;

          const phoneNumber = remoteJid.replace(/@.*$/, "");
          const conversationId = `whatsapp_${phoneNumber}`;

          const result = await handleChat({
            conversationId,
            userId: conversationId,
            firebaseUid: null,
            message: text.slice(0, CONFIG.MAX_MESSAGE_LENGTH),
            channel: "whatsapp",
            modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(remoteJid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (err) {
          logger.error({ error: err.message }, "Erreur traitement message WhatsApp");
        }
      }
    });

    return { connected: false, qrCode: null };
  }

  async sendMessage(userId, to, message) {
    const session = this.sessions.get(userId);
    if (!session || !session.ready) {
      const error = new Error("WhatsApp non connecté");
      error.code = "WHATSAPP_NOT_CONNECTED";
      throw error;
    }
    const cleanNumber = String(to).replace(/[^\d]/g, "");
    if (!cleanNumber) {
      const error = new Error("Numéro de destinataire invalide");
      error.code = "INVALID_RECIPIENT";
      throw error;
    }
    const jid = `${cleanNumber}@s.whatsapp.net`;
    await session.sock.sendMessage(jid, { text: message });
    return { success: true, to: cleanNumber };
  }

  getQRCode(userId) {
    return this.sessions.get(userId)?.qrCode || null;
  }

  async destroyAll() {
    for (const [userId, session] of this.sessions) {
      try {
        session.sock.end(undefined);
      } catch (e) {
        logger.error({ error: e.message }, `Erreur fermeture WhatsApp (${userId})`);
      }
    }
  }
}

const whatsappManager = new BaileysManager();

queueManager.createQueue(
  "whatsapp-outbound",
  async (job) => {
    const data = job?.data ?? job;
    const { userId, phoneNumber, message } = data;
    await whatsappManager.sendMessage(userId, phoneNumber, message);
  },
  { concurrency: 3, limiter: { max: 10, duration: 1000 } }
);

async function sendWhatsAppSmart(userId, phoneNumber, message) {
  const quotaCheck = await checkUserQuota(userId, 'whatsapp');
  if (!quotaCheck.allowed) {
    throw new Error(quotaCheck.message || "Limite WhatsApp atteinte");
  }
  
  await queueManager.add("whatsapp-outbound", { userId, phoneNumber, message }, { attempts: 5, backoffDelay: 2000 });
  await incrementUserQuota(userId, 'whatsapp');
  return { success: true, queued: true };
}

// ==================== ARCHITECTURE LLM ====================
const LLM_PROVIDERS = {
  GROQ: {
    baseURL: "https://api.groq.com/openai/v1",
    apiKey: process.env.GROQ_API_KEY || "",
    timeout: CONFIG.DEFAULT_TIMEOUT,
    maxTokens: 4000,
    temperature: 0.7,
    circuitBreaker: new CircuitBreaker("groq")
  },
  OPENROUTER: {
    baseURL: "https://openrouter.ai/api/v1",
    apiKey: process.env.OPENROUTER_API_KEY || "",
    timeout: 60000,
    maxTokens: 4000,
    temperature: 0.7,
    circuitBreaker: new CircuitBreaker("openrouter")
  }
};

const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    providers: [
      { provider: "groq", model: process.env.GROQ_MODEL_V100 || "llama-3.3-70b-versatile", maxTokens: 4000, timeout: 45000, temperature: 0.7, jsonMode: true, failoverPriority: 0 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "qwen/qwen-2.5-coder-32b-instruct:free", maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 1 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free", maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 2 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_3 || "microsoft/phi-4:free", maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 3 }
    ]
  },
  v250: {
    name: "Ngandu",
    reasoning: {
      providers: [
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_REASONING || "deepseek/deepseek-r1:free", maxTokens: 8000, timeout: 90000, temperature: 0.3, jsonMode: false, failoverPriority: 0 },
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_REASONING_FALLBACK || "deepseek/deepseek-r1-distill-llama-70b:free", maxTokens: 8000, timeout: 90000, temperature: 0.3, jsonMode: false, failoverPriority: 1 },
        { provider: "groq", model: process.env.GROQ_MODEL_V250_REASONING_FALLBACK || "llama-3.3-70b-versatile", maxTokens: 6000, timeout: 45000, temperature: 0.3, jsonMode: false, failoverPriority: 2 }
      ]
    },
    code: {
      providers: [
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_CODE || "qwen/qwen-2.5-coder-32b-instruct:free", maxTokens: 8000, timeout: 90000, temperature: 0.5, jsonMode: true, failoverPriority: 0 },
        { provider: "groq", model: process.env.GROQ_MODEL_V250_CODE_FALLBACK || "llama-3.3-70b-versatile", maxTokens: 8000, timeout: 45000, temperature: 0.5, jsonMode: true, failoverPriority: 1 },
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_CODE_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free", maxTokens: 8000, timeout: 60000, temperature: 0.5, jsonMode: true, failoverPriority: 2 }
      ]
    },
    maxRetries: CONFIG.MAX_RETRY_ATTEMPTS,
    degradedMode: true
  },
  vision: {
    name: "Vision",
    providers: [
      { provider: "groq", model: CONFIG.VISION_MODEL_GROQ, maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 0 },
      { provider: "openrouter", model: CONFIG.VISION_MODEL_OPENROUTER, maxTokens: 4000, timeout: 90000, temperature: 0.7, jsonMode: true, failoverPriority: 1 }
    ]
  }
};

function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;
  const knownProviders = ["openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", "anthropic/", "google/", "mistralai/", "cohere/"];
  const isOpenRouterModel = knownProviders.some((prefix) => model.includes(prefix));
  if (isOpenRouterModel && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return model + ":free";
  }
  return model;
}

class LLMErrorInterceptor {
  static isRetryableError(error) {
    const status = error.response?.status;
    const retryableStatuses = [408, 429, 500, 502, 503, 504];
    const isTimeout = ["ECONNABORTED", "ETIMEDOUT", "ESOCKETTIMEDOUT"].includes(error.code) || /timeout/i.test(error.message || "");
    const isNetworkError = ["ENOTFOUND", "ECONNRESET", "ECONNREFUSED", "EAI_AGAIN"].includes(error.code);
    return retryableStatuses.includes(status) || isTimeout || isNetworkError;
  }

  static getErrorCode(error) {
    const status = error?.response?.status;
    if (status) return "HTTP_" + status;
    if (error?.code === "ECONNABORTED") return "TIMEOUT";
    if (error?.code === "ENOTFOUND") return "DNS_ERROR";
    if (error?.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    if (error?.code === "MISSING_API_KEY") return "MISSING_API_KEY";
    return "UNKNOWN_ERROR";
  }

  static shouldSkipProvider(error, providerConfig) {
    const errorCode = this.getErrorCode(error);
    if (["HTTP_402", "HTTP_404", "MISSING_API_KEY"].includes(errorCode)) return true;
    return false;
  }
}

async function executeWithRetryAndFallback(providerList, promptParams, options = {}) {
  const {
    maxRetriesPerProvider = CONFIG.MAX_RETRY_ATTEMPTS,
    baseDelayMs = CONFIG.RETRY_BASE_DELAY_MS,
    maxDelayMs = CONFIG.RETRY_MAX_DELAY_MS,
    timeoutMultiplier = 1.5,
    onProviderFail = null,
    onProviderSuccess = null,
    enableCircuitBreaker = true,
    sessionId = null,
    userId = null,
    tier = "v100"
  } = options;

  let lastError = null;
  const providerResults = [];
  const sortedProviders = [...providerList].sort((a, b) => a.failoverPriority - b.failoverPriority);

  for (let i = 0; i < sortedProviders.length; i++) {
    const providerConfig = sortedProviders[i];
    const provider = providerConfig.provider;
    const providerInfo = LLM_PROVIDERS[provider.toUpperCase()];

    if (!providerInfo || !providerInfo.apiKey) continue;

    let model = providerConfig.model;
    if (provider === "openrouter") {
      model = validateAndSanitizeOpenRouterModel(model);
      if (!model) continue;
    }

    for (let attempt = 0; attempt < maxRetriesPerProvider; attempt++) {
      const startTime = Date.now();
      try {
        const timeout = providerConfig.timeout * (attempt > 0 ? timeoutMultiplier : 1);

        const executeCall = async () => {
          return await callProviderRaw({
            provider,
            model,
            messages: promptParams.messages,
            jsonMode: providerConfig.jsonMode,
            timeout,
            maxTokens: providerConfig.maxTokens,
            temperature: providerConfig.temperature,
            images: promptParams.images || null
          });
        };

        let result;
        if (enableCircuitBreaker && providerInfo.circuitBreaker) {
          result = await providerInfo.circuitBreaker.execute(executeCall);
        } else {
          result = await executeCall();
        }

        const latencyMs = Date.now() - startTime;
        const providerResult = {
          providerUsed: provider,
          modelUsed: model,
          providerPriority: providerConfig.failoverPriority,
          attempts: attempt + 1,
          response: result,
          latencyMs
        };

        providerResults.push(providerResult);

        if (sessionId) {
          await auditLLMCall({
            sessionId, userId, provider, model, tier,
            promptTokens: 0, completionTokens: 0,
            latencyMs, status: "success", errorCode: null
          });
        }

        if (onProviderSuccess) onProviderSuccess(providerResult);

        return { success: true, ...providerResult, providerChain: providerResults };
      } catch (error) {
        lastError = error;
        const errorCode = LLMErrorInterceptor.getErrorCode(error);
        const latencyMs = Date.now() - startTime;

        if (sessionId) {
          await auditLLMCall({
            sessionId, userId, provider, model, tier,
            promptTokens: 0, completionTokens: 0,
            latencyMs, status: "failed", errorCode
          });
        }

        if (onProviderFail) {
          onProviderFail({ provider, model, errorCode, errorMessage: error.message, attempt: attempt + 1 });
        }

        if (LLMErrorInterceptor.shouldSkipProvider(error, providerConfig)) break;

        if (LLMErrorInterceptor.isRetryableError(error) && attempt < maxRetriesPerProvider - 1) {
          const retryDelay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        } else if (!LLMErrorInterceptor.isRetryableError(error)) {
          break;
        }
      }
    }
  }

  return {
    success: false,
    error: lastError,
    providerChain: providerResults,
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  };
}

async function callProviderRaw({ provider, model, messages, jsonMode = false, timeout, maxTokens, temperature = 0.7, images = null }) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ : LLM_PROVIDERS.OPENROUTER;

  if (!cfg.apiKey) {
    const err = new Error("Clé API manquante pour " + provider);
    err.code = "MISSING_API_KEY";
    throw err;
  }

  let formattedMessages = messages;
  if (images && images.length > 0) {
    const lastMessageIndex = messages.length - 1;
    if (messages[lastMessageIndex]?.role === "user") {
      const contentParts = [];
      if (typeof messages[lastMessageIndex].content === "string") {
        contentParts.push({ type: "text", text: messages[lastMessageIndex].content });
      }
      for (const image of images) {
        contentParts.push({ type: "image_url", image_url: { url: image.dataUrl } });
      }
      formattedMessages = [...messages.slice(0, lastMessageIndex), { role: "user", content: contentParts }];
    }
  }

  const payload = {
    model,
    messages: formattedMessages,
    temperature,
    max_tokens: maxTokens || cfg.maxTokens
  };

  if (jsonMode) payload.response_format = { type: "json_object" };

  const headers = {
    Authorization: "Bearer " + cfg.apiKey,
    "Content-Type": "application/json"
  };

  if (provider === "openrouter") {
    headers["HTTP-Referer"] = HOSTING_CONFIG.domain;
    headers["X-Title"] = "Luba.ia Assistant";
  }

  const response = await axios.post(cfg.baseURL + "/chat/completions", payload, { headers, timeout: timeout || cfg.timeout });

  const choice = response?.data?.choices?.[0];
  const content = choice?.message?.content;

  if (!content) throw new Error("Réponse " + provider + " vide");

  if (!jsonMode) return content;

  try {
    return JSON.parse(content);
  } catch (parseError) {
    throw new Error(`Réponse ${provider} invalide (JSON malformé)`);
  }
}

// ==================== CONTEXT MANAGER ====================
class DynamicContextManager {
  constructor() {
    this.domainPatterns = [
      {
        domain: "mathematics",
        keywords: ["math", "calcul", "équation", "equation", "algèbre", "algebra", "géométrie", "geometry", "calculus", "intégrale", "integrale", "dérivée", "derivative", "théorème", "theorem", "nombre", "number", "fonction", "function", "limite", "limit", "matrice", "matrix", "probabilité", "probability", "statistique", "statistics"],
        systemPrompt: "Tu es un expert en mathématiques. Utilise LaTeX pour toutes les formules. Détaille chaque étape du raisonnement."
      },
      {
        domain: "cybersecurity",
        keywords: ["sécurité", "security", "cyber", "hack", "vulnérabilité", "vulnerability", "exploit", "pentest", "cryptographie", "cryptography", "chiffrement", "encryption", "pare-feu", "firewall", "malware", "virus", "phishing", "authentification", "token", "jwt", "sql injection", "xss", "csrf", "ddos", "ransomware"],
        systemPrompt: "Tu es un expert en cybersécurité. Adopte une approche défensive et éthique."
      },
      {
        domain: "development",
        keywords: ["code", "coder", "programmation", "programming", "développement", "development", "javascript", "python", "java", "c++", "rust", "go", "typescript", "react", "vue", "angular", "node", "express", "api", "database", "sql", "nosql", "backend", "frontend", "bug", "debug", "fonction", "function", "classe", "class", "algorithme", "framework", "library", "package", "npm", "git", "docker", "kubernetes", "html", "css"],
        systemPrompt: "Tu es un expert en développement logiciel. Fournis du code de production complet et fonctionnel. Ne génère du code que si l'utilisateur le demande explicitement."
      },
      {
        domain: "data_science",
        keywords: ["data", "données", "machine learning", "deep learning", "neural network", "réseau de neurones", "pandas", "numpy", "tensorflow", "pytorch", "scikit", "regression", "classification", "clustering", "nlp", "computer vision", "dataset"],
        systemPrompt: "Tu es un expert en data science et machine learning."
      },
      {
        domain: "general",
        keywords: [],
        systemPrompt: "Tu es un assistant polyvalent."
      }
    ];
  }

  analyzeDomain(message) {
    const lowerMessage = String(message).toLowerCase();
    let bestMatch = this.domainPatterns[this.domainPatterns.length - 1];
    let bestScore = 0;
    for (const pattern of this.domainPatterns) {
      if (pattern.domain === "general") continue;
      let score = 0;
      for (const keyword of pattern.keywords) {
        if (lowerMessage.includes(keyword.toLowerCase())) score += 1;
      }
      if (score > bestScore) {
        bestScore = score;
        bestMatch = pattern;
      }
    }
    return bestMatch;
  }

  buildSystemPrompt(message, basePrompt, conversationContext = "") {
    const domain = this.analyzeDomain(message);
    const formattingRules = [
      "FORMATAGE STRICT OBLIGATOIRE :",
      "- TOUT code doit être encadré dans des blocs Markdown avec triple backticks",
      "- TOUTE formule mathématique doit être encadrée en LaTeX ($ pour inline, $$ pour display)",
      "- Les noms de variables, fonctions et fichiers doivent être en backticks simples",
      "- IMPORTANT : Utilise le contexte de la conversation pour répondre de manière cohérente"
    ].join("\n");

    const webCodeRules = [
      "RÈGLE STRICTE POUR LA GÉNÉRATION DE CODE HTML/CSS/JAVASCRIPT :",
      "- Tout code HTML doit être encadré par un bloc Markdown ```html suivi de ```",
      "- Tout code CSS doit utiliser UNIQUEMENT la syntaxe /* ... */",
      "- Tout code JavaScript doit être encadré par un bloc Markdown ```javascript suivi de ```",
      "- Le code livré doit TOUJOURS être complet et syntaxiquement valide",
      "- Ne génère du code que si l'utilisateur le demande explicitement"
    ].join("\n");

    let contextSection = "";
    if (conversationContext) {
      contextSection = "\n\nCONTEXTE DE LA CONVERSATION PRÉCÉDENTE :\n" + conversationContext + "\n\nINSTRUCTION : Utilise ce contexte pour comprendre les références et maintenir la cohérence de la conversation.";
    }

    return {
      role: "system",
      content: basePrompt + "\n\nDOMAINE D'EXPERTISE DÉTECTÉ : " + domain.domain.toUpperCase() + "\n" + domain.systemPrompt + "\n\n" + formattingRules + "\n\n" + webCodeRules + contextSection
    };
  }
}

const dynamicContextManager = new DynamicContextManager();

// ==================== SYSTEM PROMPT ====================
const LUBA_BASE_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, une startup basée à Kinshasa, fondée en 2026.",
  "",
  "IDENTITÉ :",
  "- Tu t'appelles Luba (ou Luba.ia).",
  "- IA développée par HIKLON Technology, startup à Kinshasa, fondée en 2026.",
  "- Ton ton est chaleureux, intelligent et proactif.",
  "",
  "RÈGLE SUR LA MÉMOIRE CONVERSATIONNELLE :",
  "- Tu dois TOUJOURS te souvenir du contexte de la conversation.",
  "- Si l'utilisateur fait référence à quelque chose mentionné précédemment, utilise ce contexte.",
  "- Exemple : Si on parle du Congo et qu'on demande 'comment s'appellent ses habitants', réponds 'les Congolais'.",
  "",
  "RÈGLE SUR LES DONNÉES (OBLIGATOIRE) :",
  "- Tu ne dois JAMAIS inventer un score sportif, une actualité, un résultat de recherche, une donnée météo.",
  "- Utilise TOUJOURS l'outil approprié pour obtenir une donnée réelle.",
  "- Si un outil échoue, dis-le honnêtement.",
  "",
  "RÈGLE STRICTE SUR LES IMAGES :",
  "- Dès que tu décris une personnalité, un lieu, un objet, utilise TOUJOURS search_images.",
  "",
  "RÈGLE SUR LES SUGGESTIONS :",
  "- Le champ suggestions doit TOUJOURS contenir 3 à 4 questions de suivi.",
  "",
  "RÈGLE SUR LE CODE :",
  "- Tu ne génères JAMAIS de code (Python, JavaScript, etc.) spontanément.",
  "- Tu ne génères du code QUE si l'utilisateur le demande explicitement.",
  "",
  "RÈGLE SUR LES MATHÉMATIQUES :",
  "- Pour tout calcul, utilise le résultat exact fourni par le moteur mathématique.",
  "- Formate les équations en LaTeX ($...$ en ligne, $$...$$ en bloc).",
  "",
  "FORMAT DE RÉPONSE OBLIGATOIRE (JSON strict) :",
  "{",
  '  "replyText": "Ta réponse complète en Markdown",',
  '  "toolCalls": [],',
  '  "suggestions": ["Question 1 ?", "Question 2 ?", "Question 3 ?"]',
  "}",
  "",
  "OUTILS DISPONIBLES :",
  "- search_images : Rechercher des images",
  "- search_web : Recherche générale",
  "- search_news : Actualités récentes",
  "- search_sports_scores : Scores sportifs",
  "- search_science : Articles scientifiques",
  "- search_social : Discussions réseaux sociaux",
  "- get_weather : Météo actuelle",
  "- send_email : Envoyer un email",
  "- send_whatsapp_message : Envoyer un message WhatsApp",
  "- execute_math : Calcul mathématique exact"
].join("\n");

// ==================== INITIALISATION EXPRESS ====================
const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ==================== CORS ====================
app.use(cors({
  origin: function (origin, callback) {
    if (!origin || HOSTING_CONFIG.allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      logger.warn({ origin }, "Origine CORS refusée");
      callback(new Error("Origine non autorisée"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token", "X-Session-Token"],
  credentials: true,
  maxAge: 86400
}));

// ==================== SECURITY ====================
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://apis.google.com", "https://www.gstatic.com"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://fonts.googleapis.com"],
      imgSrc: ["'self'", "data:", "blob:", "https://*", "http://*"],
      connectSrc: ["'self'", "https://api.groq.com", "https://openrouter.ai", "https://*.firebaseio.com", "https://*.supabase.co", "wss://*.firebaseio.com"],
      fontSrc: ["'self'", "https://fonts.gstatic.com", "https://cdnjs.cloudflare.com"],
      objectSrc: ["'none'"],
      frameSrc: ["https://*.firebaseapp.com", "https://*.web.app"],
      workerSrc: ["'self'", "blob:"]
    }
  }
}));

// ==================== BODY PARSERS ====================
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

// ==================== RATE LIMITERS ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({ success: false, error: true, reply: "Trop de requêtes. Réessayez dans 15 minutes.", code: "RATE_LIMIT" });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({ success: false, error: true, reply: "Limite de requêtes atteinte.", code: "RATE_LIMIT_STRICT" });
  }
});

// ==================== LOGGING MIDDLEWARE ====================
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  res.on("finish", () => {
    logger.info({ requestId, status: res.statusCode, duration: Date.now() - start }, "Réponse envoyée");
  });
  next();
});

// ==================== AUTHENTIFICATION ====================
const authenticateUser = async (req, res, next) => {
  try {
    const isBlocked = await isIPBlocked(req.ip);
    if (isBlocked) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé. IP bloquée.", code: "IP_BLOCKED" });
    }
    
    const authHeader = req.headers.authorization || req.headers.Authorization;
    const bearerToken = authHeader && authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    
    if (!bearerToken) {
      await recordLoginAttempt(req.ip, null, false, "Token manquant");
      return res.status(401).json({ success: false, error: true, reply: "Authentification requise.", code: "MISSING_TOKEN" });
    }
    
    try {
      const user = await verifyFirebaseToken(bearerToken);
      if (!user) {
        await recordLoginAttempt(req.ip, null, false, "Token invalide");
        return res.status(401).json({ success: false, error: true, reply: "Session invalide.", code: "INVALID_TOKEN" });
      }
      
      req.userId = user.uid;
      req.firebaseUid = user.uid;
      req.verifiedIdentity = true;
      req.userRole = user.role || 'FREE';
      req.emailVerified = user.emailVerified;
      
      await recordLoginAttempt(req.ip, user.uid, true);
      await logSecurityEvent(user.uid, 'LOGIN_SUCCESS', { email: user.email }, req.ip, req.headers['user-agent']);
      
      // Synchronisation utilisateur - SAUVEGARDE UID
      const userRow = await dbGet("SELECT * FROM users WHERE id = ? OR firebase_uid = ?", [user.uid, user.uid]);
      if (!userRow) {
        await dbRun(
          "INSERT INTO users (id, firebase_uid, email, display_name, role, email_verified, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
          [user.uid, user.uid, user.email, user.displayName || user.uid, req.userRole, user.emailVerified ? 1 : 0]
        );
        logger.info({ userId: user.uid }, "✅ Nouvel utilisateur créé avec UID");
      } else {
        await dbRun(
          "UPDATE users SET last_seen_at = CURRENT_TIMESTAMP, email = COALESCE(?, email), display_name = COALESCE(?, display_name), role = ?, email_verified = ?, firebase_uid = ? WHERE id = ?",
          [user.email, user.displayName, req.userRole, user.emailVerified ? 1 : 0, user.uid, user.uid]
        );
        logger.info({ userId: user.uid }, "✅ Utilisateur synchronisé avec UID");
      }
      
      if (supabase && user.uid) {
        await syncUserWithSupabase(user.uid, user.email, user.displayName);
      }
      
      next();
    } catch (error) {
      await recordLoginAttempt(req.ip, null, false, error.message);
      const loginCheck = await checkLoginAttempts(req.ip);
      return res.status(401).json({
        success: false,
        error: true,
        reply: loginCheck.blocked ? loginCheck.message : "Session invalide.",
        code: loginCheck.blocked ? "IP_BLOCKED" : "INVALID_TOKEN"
      });
    }
  } catch (error) {
    logger.error({ error: error.message }, "Erreur authentification");
    return res.status(500).json({ success: false, error: true, reply: "Erreur interne.", code: "AUTH_INTERNAL_ERROR" });
  }
};

const requireRole = (allowedRoles) => {
  return (req, res, next) => {
    if (!req.userRole || (!allowedRoles.includes(req.userRole) && req.userRole !== 'ADMIN')) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "INSUFFICIENT_ROLE" });
    }
    next();
  };
};

// ==================== SYNCHRONISATION SUPABASE ====================
async function syncUserWithSupabase(firebaseUid, email, displayName) {
  if (!supabase || !firebaseUid) return;
  try {
    const { data: existingUser, error: fetchError } = await supabase.from("users").select("firebase_uid").eq("firebase_uid", firebaseUid).single();
    if (fetchError && fetchError.code !== "PGRST116") return;
    if (!existingUser) {
      await supabase.from("users").insert({ 
        id: firebaseUid,
        firebase_uid: firebaseUid, 
        email, 
        display_name: displayName, 
        last_seen_at: new Date().toISOString() 
      });
      logger.info({ firebaseUid }, "✅ Utilisateur créé dans Supabase");
    } else {
      await supabase.from("users").update({ 
        last_seen_at: new Date().toISOString(),
        email: email || existingUser.email,
        display_name: displayName || existingUser.display_name
      }).eq("firebase_uid", firebaseUid);
    }
  } catch (error) {
    logger.error({ error: error.message }, "Erreur sync Supabase");
  }
}

async function syncSessionWithSupabase(sessionId, firebaseUid, userId) {
  if (!supabase || !firebaseUid) return;
  try {
    const { data: existingSession, error: fetchError } = await supabase.from("sessions").select("session_id").eq("session_id", sessionId).single();
    if (fetchError && fetchError.code !== "PGRST116") return;
    if (!existingSession) {
      await supabase.from("sessions").insert({ 
        session_id: sessionId, 
        firebase_uid: firebaseUid, 
        user_id: userId, 
        created_at: new Date().toISOString(), 
        updated_at: new Date().toISOString() 
      });
    } else {
      await supabase.from("sessions").update({ 
        updated_at: new Date().toISOString(),
        user_id: userId,
        firebase_uid: firebaseUid
      }).eq("session_id", sessionId);
    }
  } catch (error) {
    logger.error({ error: error.message }, "Erreur sync session Supabase");
  }
}

async function syncMessageWithSupabase(sessionId, role, content, firebaseUid, userId = null) {
  if (!supabase || !firebaseUid) return;
  try {
    await supabase.from("messages").insert({ 
      session_id: sessionId, 
      firebase_uid: firebaseUid, 
      user_id: userId,
      role, 
      content, 
      created_at: new Date().toISOString() 
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur sync message Supabase");
  }
}

// ==================== GESTION DES SESSIONS ====================
async function getSession(conversationId, userId, firebaseUid = null) {
  const session = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);
  if (session) {
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
    return session;
  }

  if (supabase && firebaseUid) {
    try {
      const { data: supabaseSession, error } = await supabase.from("sessions").select("session_id, user_id, firebase_uid").eq("session_id", conversationId).single();
      if (supabaseSession && !error) {
        await dbRun("INSERT OR IGNORE INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)", [conversationId, supabaseSession.user_id || userId, supabaseSession.firebase_uid]);
        return { session_id: conversationId, user_id: supabaseSession.user_id || userId, firebase_uid: supabaseSession.firebase_uid };
      }
    } catch (error) {
      logger.error({ error: error.message }, "Erreur Supabase getSession");
    }
  }

  await dbRun("INSERT INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)", [conversationId, userId, firebaseUid]);
  await syncSessionWithSupabase(conversationId, firebaseUid, userId);
  return { session_id: conversationId, user_id: userId, firebase_uid: firebaseUid };
}

// Utiliser getFullHistory au lieu de getHistory
async function getHistory(conversationId, userId = null, limit = CONFIG.MAX_CONTEXT_MESSAGES) {
  return await getFullHistory(conversationId, userId, limit);
}

// Utiliser saveMessageWithUser au lieu de saveMessage
async function saveMessage(conversationId, role, content, userId = null, firebaseUid = null) {
  return await saveMessageWithUser(conversationId, role, content, userId, firebaseUid);
}

// ==================== GESTION DES INTENTIONS ====================
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

// ==================== CALL LLM ====================
async function callLLM_v100(messages, images = null, sessionId = null, userId = null, conversationContext = "") {
  const lastUserMessage = [...messages].reverse().find((m) => m.role === "user");
  const userText = typeof lastUserMessage?.content === "string" ? lastUserMessage.content : "";
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userText, LUBA_BASE_SYSTEM_PROMPT, conversationContext);

  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.v100.providers,
    { messages: [dynamicSystemPrompt, ...messages], images },
    { maxRetriesPerProvider: CONFIG.MAX_RETRY_ATTEMPTS, sessionId, userId, tier: "v100" }
  );

  if (result.success) {
    return { ...result.response, providerUsed: result.providerUsed, modelUsed: result.modelUsed, degraded: result.providerPriority > 0 };
  }

  throw new Error("Échec complet du tier v100");
}

async function callLLM_v250(messages, userMessage, images = null, sessionId = null, userId = null, conversationContext = "") {
  const tier = MODEL_TIERS.v250;
  const providerChain = [];
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userMessage, LUBA_BASE_SYSTEM_PROMPT, conversationContext);

  const reasoningMessages = [
    { role: "system", content: dynamicSystemPrompt.content + "\n\nAnalyse ce problème complexe en profondeur." },
    ...messages
  ];

  const reasoningResult = await executeWithRetryAndFallback(
    tier.reasoning.providers,
    { messages: reasoningMessages, images },
    { maxRetriesPerProvider: tier.maxRetries, sessionId, userId, tier: "v250_reasoning" }
  );

  if (!reasoningResult.success || !reasoningResult.response || reasoningResult.response.trim().length < 40) {
    return await degradedFallbackToV100(messages, "reasoning_failed", images, conversationContext);
  }

  const reasoningAnalysis = reasoningResult.response;
  providerChain.push("R1:" + reasoningResult.providerUsed + "/" + reasoningResult.modelUsed);

  const formattingDirective = [
    "Tu DOIS répondre au format JSON strict :",
    "{",
    '  "replyText": "réponse complète en Markdown",',
    '  "toolCalls": [],',
    '  "suggestions": ["question 1 ?", "question 2 ?", "question 3 ?"]',
    "}",
    "",
    "FORMATAGE STRICT :",
    "- Code HTML dans un bloc ```html ... ```",
    "- Code CSS dans un bloc ```css ... ``` avec UNIQUEMENT des commentaires /* ... */",
    "- Code JavaScript dans un bloc ```javascript ... ```",
    "- Code complet, jamais tronqué",
    "- Formules mathématiques en LaTeX ($...$ en ligne, $$...$$ en bloc)"
  ].join("\n");

  const codeMessages = [
    { role: "system", content: "Génère le code de production complet basé sur le plan ci-dessous.\n\nPLAN :\n" + reasoningAnalysis + "\n\n" + formattingDirective },
    { role: "user", content: userMessage }
  ];

  const codeResult = await executeWithRetryAndFallback(
    tier.code.providers,
    { messages: codeMessages, images },
    { maxRetriesPerProvider: tier.maxRetries, sessionId, userId, tier: "v250_code" }
  );

  if (!codeResult.success || !codeResult.response) {
    return await degradedFallbackToV100(messages, "code_generation_failed", images, conversationContext);
  }

  providerChain.push("R2:" + codeResult.providerUsed + "/" + codeResult.modelUsed);

  return {
    ...codeResult.response,
    providerUsed: "pipeline_v250",
    modelUsed: providerChain.join(" -> "),
    degraded: false,
    providerChain,
    reasoningProviderUsed: reasoningResult.providerUsed
  };
}

async function callVisionModel(messages, images, sessionId = null, userId = null, conversationContext = "") {
  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.vision.providers,
    { messages, images },
    { maxRetriesPerProvider: 2, sessionId, userId, tier: "vision" }
  );

  if (result.success) {
    return { ...result.response, providerUsed: result.providerUsed, modelUsed: result.modelUsed, visionEnabled: true };
  }

  return await callLLM_v100(messages, null, sessionId, userId, conversationContext);
}

async function degradedFallbackToV100(messages, reason, images = null, conversationContext = "") {
  try {
    const fallbackResult = await callLLM_v100(messages, images, null, null, conversationContext);
    return {
      ...fallbackResult,
      providerUsed: "v250_degraded_to_v100",
      modelUsed: fallbackResult.providerUsed + "/" + fallbackResult.modelUsed,
      degraded: true,
      degradationReason: reason,
      originalTier: "v250",
      actualTier: "v100"
    };
  } catch (fallbackError) {
    return {
      replyText: "Je rencontre actuellement des difficultés techniques. Veuillez réessayer.",
      toolCalls: [],
      suggestions: ["Peux-tu réessayer ?", "Comment fonctionne Luba.ia ?", "Quels sont les services disponibles ?"],
      providerUsed: "error_graceful_degradation",
      modelUsed: "none",
      degraded: true,
      error: true
    };
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
    case "execute_math":
    case "calculate":
      result = executeMathExpression(args.expression || args.query);
      break;
    case "send_email":
      result = await dispatchSendEmail({
        googleAccessToken,
        recipient: args.recipient || args.to,
        subject: args.subject,
        body: args.body,
        userId
      });
      break;
    case "send_whatsapp_message":
    case "send_whatsapp":
      result = await sendWhatsAppSmart(userId, args.phone_number || args.to, args.message);
      break;
    default:
      result = { success: false, error: "Outil inconnu : " + toolName };
  }

  return { result, sourceKeys };
}

// ==================== ENRICHISSEMENT DE CONTEXTE ====================
async function enrichContextWithIntent(intent, userMessage) {
  const enrichment = {
    contextData: "",
    toolCalls: [],
    sourceKeys: []
  };

  switch (intent) {
    case "MATHS": {
      const expressions = detectMathExpressions(userMessage);
      if (expressions.length > 0) {
        for (const expr of expressions.slice(0, 3)) {
          const mathResult = executeMathExpression(expr);
          if (mathResult.success) {
            enrichment.contextData += `\n[Calcul exact] ${expr} = ${mathResult.formatted}\n`;
          }
        }
        enrichment.toolCalls.push({ name: "execute_math", arguments: { expression: expressions[0] } });
      }
      break;
    }
    case "ACTUALITÉ": {
      const newsResult = await searchNews(userMessage);
      if (newsResult.articles && newsResult.articles.length > 0) {
        enrichment.contextData += "\n[ACTUALITÉS RÉCENTES]\n";
        newsResult.articles.slice(0, 5).forEach((a, i) => {
          enrichment.contextData += `${i + 1}. ${a.title} (${a.pubDate})\n   ${a.link}\n   ${a.description ? a.description.slice(0, 200) : ""}\n\n`;
        });
        enrichment.sourceKeys.push("googlenews");
        enrichment.toolCalls.push({ name: "search_news", arguments: { query: userMessage } });
      }
      break;
    }
    case "SPORT": {
      const sportResult = await searchSportsScores(userMessage);
      if (sportResult.events && sportResult.events.length > 0) {
        enrichment.contextData += `\n[RÉSULTATS SPORTIFS - ${sportResult.team || ""}]\n`;
        sportResult.events.forEach((e) => {
          enrichment.contextData += `${e.match} (${e.date}) - ${e.league}\n`;
        });
        enrichment.sourceKeys.push("thesportsdb");
        enrichment.toolCalls.push({ name: "search_sports_scores", arguments: { query: userMessage } });
      }
      break;
    }
    case "CODE": {
      enrichment.contextData += "\n[MODE CODE ACTIVÉ]\nL'utilisateur demande explicitement du code. Fournis une réponse complète avec des blocs de code Markdown.";
      break;
    }
  }

  return enrichment;
}

// ==================== HANDLE CHAT - AVEC MÉMOIRE CONVERSATIONNELLE ====================
async function handleChat({ conversationId, userId, firebaseUid, message, googleAccessToken = null, channel = "web", modelTier = "v100", images = null }) {
  await getSession(conversationId, userId, firebaseUid);

  const activeIntent = await getActiveIntent(conversationId);
  if (activeIntent) {
    return await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken, firebaseUid });
  }

  // Sauvegarder le message utilisateur avec le user_id
  await saveMessageWithUser(conversationId, "user", message, userId, firebaseUid);

  // Récupérer l'historique complet avec le contexte
  const history = await getFullHistory(conversationId, userId);
  
  // Construire le contexte de conversation pour le LLM
  let conversationContext = "";
  if (history.length > 0) {
    const recentHistory = history.slice(-CONFIG.MAX_CONTEXT_MESSAGES);
    conversationContext = recentHistory.map(msg => 
      `${msg.role === "user" ? "Utilisateur" : "Assistant"}: ${msg.content.slice(0, 500)}`
    ).join("\n");
  }
  
  // Phase 1 : Analyse d'intention
  const intent = analyzeIntent(message);
  logger.info({ intent, conversationId, historyLength: history.length }, "Intention détectée");
  
  // Phase 2 : Enrichissement de contexte
  const enrichment = await enrichContextWithIntent(intent, message);
  
  // Construire les messages pour le LLM avec l'historique complet
  let messages = [];
  
  // Inclure l'historique limité pour le contexte
  const contextHistory = history.slice(-CONFIG.MAX_CONTEXT_MESSAGES);
  messages = [...contextHistory, { role: "user", content: message }];
  
  // Injection du contexte enrichi si disponible
  if (enrichment.contextData) {
    messages = [...contextHistory, { 
      role: "user", 
      content: message + "\n\n[CONTEXTE ENRICHISSÉ - NE PAS CITER CES SOURCES DANS TA RÉPONSE]\n" + enrichment.contextData 
    }];
  }

  let finalResponse = null;
  let imageUrls = [];
  let providerUsed = "unknown";
  let suggestions = [];
  const usedSources = new Set(enrichment.sourceKeys);
  let degraded = false;

  try {
    if (images && images.length > 0) {
      const visionResult = await callVisionModel(messages, images, conversationId, userId, conversationContext);
      finalResponse = visionResult.replyText || "Je n'ai pas pu analyser l'image.";
      suggestions = Array.isArray(visionResult.suggestions) ? visionResult.suggestions.slice(0, 4) : [];
      providerUsed = visionResult.providerUsed || "vision";
    } else if (modelTier === "v250") {
      const result = await callLLM_v250(messages, message, null, conversationId, userId, conversationContext);
      finalResponse = result.replyText || "Je n'ai pas pu générer une réponse.";
      suggestions = Array.isArray(result.suggestions) ? result.suggestions.slice(0, 4) : [];
      providerUsed = result.providerUsed || "pipeline_v250";
      degraded = result.degraded || false;
    } else {
      let keepRunning = true;
      let maxLoops = 5;

      while (keepRunning && maxLoops > 0) {
        maxLoops--;
        let llmResponse;
        try {
          llmResponse = await callLLM_v100(messages, null, conversationId, userId, conversationContext);
          providerUsed = llmResponse.providerUsed;
          degraded = llmResponse.degraded || false;
        } catch (error) {
          finalResponse = "Je suis momentanément indisponible. Veuillez réessayer.";
          suggestions = ["Peux-tu réessayer ?", "Comment fonctionne Luba.ia ?", "Quels sont les services disponibles ?"];
          providerUsed = "error_graceful_degradation";
          degraded = true;
          break;
        }

        const allToolCalls = [...(llmResponse.toolCalls || []), ...enrichment.toolCalls];
        
        if (allToolCalls.length > 0) {
          for (const toolCall of allToolCalls) {
            let toolResult;
            try {
              const { result, sourceKeys } = await executeTool(toolCall.name, toolCall.arguments || {}, { userId, googleAccessToken });
              toolResult = result;
              sourceKeys.forEach((k) => usedSources.add(k));
              if ((toolCall.name === "search_images" || toolCall.name === "search_image") && toolResult.images) {
                imageUrls = imageUrls.concat(toolResult.images.map((img) => img.url));
              }
            } catch (toolError) {
              toolResult = { success: false, error: toolError.message };
            }

            messages.push({ role: "assistant", content: "Résultat de l'outil " + toolCall.name + " : " + JSON.stringify(toolResult) });
          }

          messages.push({
            role: "user",
            content: "Formule maintenant ta réponse finale complète avec les résultats des outils, et propose 3 à 4 questions de suivi. Respecte strictement le formatage LaTeX pour les mathématiques. Utilise le contexte de la conversation pour répondre."
          });
          enrichment.toolCalls = [];
          keepRunning = true;
        } else {
          finalResponse = llmResponse.replyText || "Je n'ai pas pu générer une réponse.";
          suggestions = Array.isArray(llmResponse.suggestions) ? llmResponse.suggestions.slice(0, 4) : [];
          keepRunning = false;
        }
      }

      if (!finalResponse) finalResponse = "Je rencontre des difficultés techniques. Veuillez réessayer.";
    }

    if (imageUrls.length > 0) {
      const imageMarkdown = imageUrls.map((url, index) => "![Image " + (index + 1) + "](" + url + ")").join("\n\n");
      finalResponse += "\n\n---\n\n**Illustrations :**\n\n" + imageMarkdown;
      usedSources.add("wikimediacommons");
    }

    if (usedSources.size > 0) {
      const sourceLines = Array.from(usedSources)
        .map((key) => OPEN_SOURCES[key])
        .filter(Boolean)
        .map((src) => "[" + src.name + "](" + src.url + ")");
      if (sourceLines.length > 0) finalResponse += "\n\n---\n\n**Sources :** " + sourceLines.join(" · ");
    }

    // Sauvegarder la réponse de l'assistant avec le user_id
    await saveMessageWithUser(conversationId, "assistant", finalResponse, userId, firebaseUid, { providerUsed, intent });

    return {
      reply: finalResponse,
      images: imageUrls,
      error: providerUsed.startsWith("error"),
      providerUsed,
      modelTier,
      degraded,
      visionEnabled: Boolean(images && images.length > 0),
      suggestions,
      sources: Array.from(usedSources).map((key) => OPEN_SOURCES[key]).filter(Boolean),
      intent,
      contextLength: history.length
    };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur critique handleChat");

    const fallbackResponse = {
      reply: "Je suis momentanément indisponible. Nos équipes techniques travaillent à résoudre le problème.",
      images: [],
      error: true,
      providerUsed: "error_critical",
      modelTier,
      degraded: true,
      suggestions: ["Peux-tu réessayer ?", "Comment fonctionne Luba.ia ?", "Quels sont les services disponibles ?"],
      sources: [],
      intent
    };

    try {
      await saveMessageWithUser(conversationId, "assistant", fallbackResponse.reply, userId, firebaseUid);
    } catch (saveError) {
      logger.error({ error: saveError.message }, "Erreur sauvegarde message de secours");
    }

    return fallbackResponse;
  }
}

// ==================== GESTION DES INTENTIONS ====================
async function handleActiveIntent(conversationId, activeIntent, userMessage, context = {}) {
  const { userId, googleAccessToken } = context;

  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data;
      if (data.step === "NEED_NUMBER") {
        const phoneRegex = /^(\+?\d{1,3}[-.\s]?)?\d{9,15}$/;
        if (phoneRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "WHATSAPP", { step: "NEED_MESSAGE", recipient: userMessage.trim() });
          return { reply: "Numéro enregistré. Quel message voulez-vous envoyer à " + userMessage.trim() + " ?", error: false };
        }
        return { reply: "Numéro invalide.", error: true };
      }
      if (data.step === "NEED_MESSAGE") {
        try {
          await sendWhatsAppSmart(userId, data.recipient, userMessage);
          await clearActiveIntent(conversationId);
          return { reply: "Message WhatsApp mis en file d'envoi vers " + data.recipient + " !", error: false };
        } catch (error) {
          return { reply: "Erreur d'envoi : " + error.message, error: true };
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
          return { reply: "Destinataire enregistré. Quel est le sujet de l'email ?", error: false };
        }
        return { reply: "Adresse email invalide.", error: true };
      }
      if (data.step === "NEED_SUBJECT") {
        await setActiveIntent(conversationId, "EMAIL", { step: "NEED_BODY", recipient: data.recipient, subject: userMessage });
        return { reply: "Sujet enregistré. Quel est le contenu de l'email ?", error: false };
      }
      if (data.step === "NEED_BODY") {
        const result = await dispatchSendEmail({ googleAccessToken, recipient: data.recipient, subject: data.subject, body: userMessage, userId });
        await clearActiveIntent(conversationId);
        if (result.success) return { reply: "Email envoyé à " + data.recipient + " (via " + result.provider + ") !", error: false };
        return { reply: "Erreur : " + result.error, error: true };
      }
      break;
    }
  }

  await clearActiveIntent(conversationId);
  return { reply: "Je ne comprends plus l'action. Recommençons.", error: true };
}

// ==================== ROUTES ====================
app.get("/", (req, res) => {
  res.json({ success: true, error: false, reply: "Serveur " + CONFIG.AGENT_NAME + " opérationnel", version: CONFIG.VERSION, company: CONFIG.COMPANY });
});

app.get("/api/health", async (req, res) => {
  try {
    let dbOk = true;
    try { await dbGet("SELECT 1"); } catch (e) { dbOk = false; }

    res.json({
      success: !dbOk,
      error: !dbOk,
      reply: "Serveur " + CONFIG.AGENT_NAME + " en bonne santé",
      data: {
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
        database: dbOk ? "ok" : "erreur",
        supabase: Boolean(supabase),
        firebaseAuth: firebaseApp ? "admin_sdk" : "api_rest",
        groq: Boolean(process.env.GROQ_API_KEY),
        version: CONFIG.VERSION,
        features: {
          vision: true,
          quotas: true,
          securityLogs: true,
          firebaseAdmin: Boolean(firebaseApp),
          sessionManagement: true,
          ipBlocking: true,
          nlpIntent: true,
          mathEngine: true,
          rssAggregation: true,
          webSearch: true,
          scraping: true,
          conversationMemory: true,
          uidSync: true
        }
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: true, reply: "Erreur interne" });
  }
});

app.post("/api/chat", apiLimiter, authenticateUser, upload.array("images", CONFIG.MAX_IMAGES_PER_REQUEST), async (req, res) => {
  try {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;
    const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";

    const quotaCheck = await checkUserQuota(req.userId, 'message', req.userRole);
    if (!quotaCheck.allowed) {
      return res.status(429).json({ success: false, error: true, reply: quotaCheck.message || "Limite atteinte.", code: "QUOTA_EXCEEDED" });
    }

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ success: false, error: true, reply: "Le paramètre 'message' est obligatoire.", code: "MISSING_MESSAGE" });
    }

    if (!conversationId || typeof conversationId !== "string") {
      conversationId = generateConversationId();
      isNewConversation = true;
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(403).json({ success: false, error: true, reply: error.message, code: "CONVERSATION_OWNERSHIP" });
    }

    const googleAccessToken = req.headers["x-google-access-token"] || null;

    let images = null;
    if (req.files && req.files.length > 0) {
      images = req.files.map((file) => convertImageToBase64(file.buffer, file.mimetype));
      await incrementUserQuota(req.userId, 'image');
    }

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

    await incrementUserQuota(req.userId, 'message');

    return res.status(200).json({ ...result, conversationId, isNewConversation });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur API/Chat");
    return res.status(500).json({ success: false, error: true, reply: "Une erreur est survenue.", code: "CHAT_ERROR" });
  }
});

// Route pour récupérer l'historique complet d'une conversation
app.get("/api/conversation/:conversationId/messages", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { conversationId } = req.params;
    
    if (!conversationId) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    }
    
    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(403).json({ success: false, error: true, reply: error.message, code: "CONVERSATION_OWNERSHIP" });
    }
    
    const messages = await getFullHistory(conversationId, req.userId, CONFIG.MAX_HISTORY_LENGTH);
    
    return res.status(200).json({
      success: true,
      error: false,
      conversationId,
      messages,
      count: messages.length
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur récupération historique");
    return res.status(500).json({ success: false, error: true, code: "HISTORY_FETCH_ERROR" });
  }
});

app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const rows = await dbAll("SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? OR firebase_uid = ? ORDER BY updated_at DESC LIMIT 50", [req.userId, req.firebaseUid]);
    
    const enrichedConversations = await Promise.all(rows.map(async (conv) => {
      const lastMessage = await dbGet("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1", [conv.session_id]);
      return {
        conversationId: conv.session_id,
        createdAt: conv.created_at,
        updatedAt: conv.updated_at,
        lastMessageRole: lastMessage?.role || null,
        lastMessagePreview: lastMessage?.content ? lastMessage.content.slice(0, 140) : null
      };
    }));

    return res.status(200).json({ success: true, error: false, conversations: enrichedConversations });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, conversations: [] });
  }
});

app.get("/api/user/stats", authenticateUser, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const quotaRow = await dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [req.userId, today]);
    
    return res.status(200).json({
      success: true,
      error: false,
      data: {
        quotas: quotaRow || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
        role: req.userRole || 'FREE',
        limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
        userId: req.userId,
        firebaseUid: req.firebaseUid
      }
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "STATS_ERROR" });
  }
});

app.post("/api/session/create", authenticateUser, async (req, res) => {
  try {
    const sessionToken = await createActiveSession(req.userId, req.ip, req.headers['user-agent']);
    return res.status(200).json({ success: true, error: false, data: { sessionToken, expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString() } });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "SESSION_CREATE_ERROR" });
  }
});

app.post("/api/session/revoke", authenticateUser, async (req, res) => {
  try {
    const { sessionToken } = req.body;
    if (sessionToken) {
      await revokeSession(req.userId, sessionToken);
    } else {
      await revokeAllSessions(req.userId);
    }
    return res.status(200).json({ success: true, error: false, message: sessionToken ? "Session révoquée." : "Toutes les sessions révoquées." });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "SESSION_REVOKE_ERROR" });
  }
});

app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(['ADMIN']), async (req, res) => {
  try {
    const { uid, role } = req.body;
    if (!uid || !role || !['FREE', 'PREMIUM', 'ADMIN'].includes(role)) {
      return res.status(400).json({ success: false, error: true, code: "INVALID_PARAMS" });
    }
    const result = await setUserRole(uid, role);
    return res.status(200).json({ success: true, error: false, data: result });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "ROLE_UPDATE_ERROR" });
  }
});

app.post("/api/tools", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const toolName = req.body.toolName || req.body.action;
    const params = req.body.params || req.body.arguments || req.body.data || {};
    
    if (!toolName) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_TOOL_NAME" });
    }
    
    const googleAccessToken = req.headers["x-google-access-token"] || null;
    const { result, sourceKeys } = await executeTool(toolName, params, { userId: req.userId, googleAccessToken });
    const sources = sourceKeys.map((k) => OPEN_SOURCES[k]).filter(Boolean);
    
    return res.status(200).json({ success: true, error: false, toolName, result, sources });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "TOOL_EXECUTION_ERROR" });
  }
});

app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const result = await whatsappManager.initClient(req.userId);
    
    if (result.connected) {
      return res.status(200).json({ success: true, error: false, message: "WhatsApp déjà connecté.", data: { qrCode: null, qrCodeBase64: null } });
    }
    
    let qrCode = null;
    const startTime = Date.now();
    while (!qrCode && Date.now() - startTime < CONFIG.WHATSAPP_QR_TIMEOUT) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      qrCode = whatsappManager.getQRCode(req.userId);
    }
    
    if (qrCode) {
      return res.status(200).json({ success: true, error: false, message: "Connexion initiée", data: { qrCode, qrCodeBase64: qrCode }, qr: qrCode });
    }
    
    return res.status(408).json({ success: false, error: true, message: "Délai dépassé.", code: "QR_TIMEOUT" });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_CONNECT_ERROR" });
  }
});

app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    if (!req.body.to || !req.body.message) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_PARAMS" });
    }
    
    const quotaCheck = await checkUserQuota(req.userId, 'whatsapp', req.userRole);
    if (!quotaCheck.allowed) {
      return res.status(429).json({ success: false, error: true, code: "WHATSAPP_QUOTA_EXCEEDED" });
    }
    
    const result = await whatsappManager.sendMessage(req.userId, req.body.to, req.body.message);
    await incrementUserQuota(req.userId, 'whatsapp');
    
    return res.status(200).json({ success: true, error: false, data: result });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_SEND_ERROR" });
  }
});

app.post("/api/intent/init", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { intentType, conversationId } = req.body;
    const convId = conversationId || req.body.conversation_id;

    if (!convId) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    }

    await getSession(convId, req.userId, req.firebaseUid);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(convId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.status(200).json({ success: true, error: false, reply: "Envoi WhatsApp initié. Quel est le numéro ?" });
    }
    if (intentType === "EMAIL") {
      await setActiveIntent(convId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.status(200).json({ success: true, error: false, reply: "Envoi d'email initié. Quelle est l'adresse ?" });
    }
    return res.status(400).json({ success: false, error: true, code: "UNKNOWN_INTENT" });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "INTENT_ERROR" });
  }
});

app.post("/api/memory/clear", authenticateUser, async (req, res) => {
  try {
    const conversationId = req.body.conversationId || req.body.conversation_id;

    if (!conversationId) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);
    await clearActiveIntent(conversationId);

    return res.status(200).json({ success: true, error: false, reply: "Mémoire effacée." });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "MEMORY_CLEAR_ERROR" });
  }
});

app.delete("/api/account", authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const firebaseUid = req.firebaseUid;
    
    try {
      const whatsappSession = whatsappManager.sessions.get(userId);
      if (whatsappSession?.sock) whatsappSession.sock.end(undefined);
      whatsappManager.sessions.delete(userId);
      const authDir = path.join(CONFIG.SESSIONS_PATH, userId);
      if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
    } catch (error) {
      logger.warn({ error: error.message }, "Erreur suppression session WhatsApp");
    }
    
    await dbRun("DELETE FROM messages WHERE session_id IN (SELECT session_id FROM sessions WHERE user_id = ?)", [userId]);
    await dbRun("DELETE FROM sessions WHERE user_id = ?", [userId]);
    await dbRun("DELETE FROM email_logs WHERE user_id = ? OR firebase_uid = ?", [userId, firebaseUid]);
    await dbRun("DELETE FROM llm_audit_log WHERE user_id = ?", [userId]);
    await dbRun("DELETE FROM security_logs WHERE user_id = ?", [userId]);
    await dbRun("DELETE FROM user_quotas WHERE user_id = ?", [userId]);
    await dbRun("DELETE FROM active_sessions WHERE user_id = ?", [userId]);
    await dbRun("DELETE FROM users WHERE id = ?", [userId]);
    
    if (supabase && firebaseUid) {
      try {
        await supabase.from("messages").delete().eq("firebase_uid", firebaseUid);
        await supabase.from("sessions").delete().eq("firebase_uid", firebaseUid);
        await supabase.from("whatsapp_credentials").delete().eq("user_id", userId);
        await supabase.from("users").delete().eq("firebase_uid", firebaseUid);
      } catch (error) {
        logger.error({ error: error.message }, "Erreur suppression Supabase");
      }
    }
    
    let firebaseAccountDeleted = false;
    if (firebaseApp && firebaseAdmin) {
      try {
        await firebaseAdmin.auth(firebaseApp).deleteUser(firebaseUid);
        firebaseAccountDeleted = true;
      } catch (error) {
        logger.error({ error: error.message }, "Erreur suppression compte Firebase");
      }
    }
    
    return res.status(200).json({
      success: true,
      error: false,
      message: "Compte supprimé avec succès.",
      code: "ACCOUNT_DELETED",
      firebaseAccountDeleted
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: true, code: "ACCOUNT_DELETION_ERROR" });
  }
});

// ==================== ROUTE 404 ====================
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: true,
    reply: "Route non trouvée",
    code: "NOT_FOUND",
    availableRoutes: [
      "GET /", "GET /api/health", "POST /api/chat", "GET /api/conversations",
      "GET /api/conversation/:conversationId/messages",
      "GET /api/user/stats", "POST /api/tools", "POST /api/whatsapp/connect",
      "POST /api/whatsapp/send", "POST /api/intent/init", "POST /api/memory/clear",
      "POST /api/session/create", "POST /api/session/revoke", "POST /api/admin/set-role",
      "DELETE /api/account"
    ]
  });
});

// ==================== MIDDLEWARE D'ERREUR ====================
app.use((error, req, res, next) => {
  logger.error({ error: error.message, stack: error.stack }, "Erreur non gérée");
  if (res.headersSent) return next(error);
  return res.status(500).json({ success: false, error: true, reply: "Une erreur interne est survenue.", code: "INTERNAL_ERROR" });
});

// ==================== DÉMARRAGE ====================
const server = app.listen(CONFIG.PORT, () => {
  logger.info("Serveur " + CONFIG.AGENT_NAME + " v" + CONFIG.VERSION + " démarré sur le port " + CONFIG.PORT);
  console.log("🚀 Serveur " + CONFIG.AGENT_NAME + " v" + CONFIG.VERSION + " opérationnel sur le port " + CONFIG.PORT);
  console.log("🌐 Domaine: " + HOSTING_CONFIG.domain);
  console.log("🔐 Firebase Admin: " + (firebaseApp ? "activé" : "désactivé (mode API REST)"));
  console.log("🧠 NLP Intent Engine: activé");
  console.log("📐 MathJS Engine: activé");
  console.log("📰 RSS Parser: activé");
  console.log("🔍 Web Search (DuckDuckGo): activé");
  console.log("📄 Scraping (Cheerio): activé");
  console.log("💾 Mémoire Conversationnelle: activée");
  console.log("👤 Synchronisation UID: activée");
});

// ==================== ARRÊT PROPRE ====================
let isShuttingDown = false;

async function shutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre du serveur");

  await new Promise((resolve) => server.close(resolve));
  
  try { await whatsappManager.destroyAll(); } catch (error) { logger.error({ error: error.message }, "Erreur fermeture WhatsApp"); }
  try { await queueManager.close(); } catch (error) { logger.error({ error: error.message }, "Erreur fermeture files"); }
  
  await new Promise((resolve) => db.close(() => resolve()));
  
  console.log("✅ Arrêt propre terminé");
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", (error) => {
  logger.error({ error: error.message, stack: error.stack }, "uncaughtException");
});
process.on("unhandledRejection", (reason) => {
  logger.error({ reason: String(reason) }, "unhandledRejection");
});

module.exports = { app, db, queueManager, whatsappManager };
