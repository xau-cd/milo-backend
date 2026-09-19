// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 13.0.0 Jarvis Edition (Production Ready - Blindé)
// Architecture : Modulaire, UID unifié, Agent Orchestrator, Response Formatter
//
// CHANTIERS v13 :
// 1. Formatage réponses (ResponseFormatter : title/subtitle/sections/body)
// 2. Images Wikimedia fiables (proxy /api/images/proxy + validation + cache)
// 3. UID unifié (canonicalUid = Firebase UID partout)
// 4. Sauvegarde discussions atomique (SQLite→Supabase + sync_queue)
// 5. Mémoire conversationnelle persistante + résumé glissant
// 6. Agent Jarvis (YouTube, tâches, calendrier, notes, actions web)
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
const math = require("mathjs");
const Parser = require("rss-parser");
const cheerio = require("cheerio");
const natural = require("natural");

let Groq = null;
try { ({ Groq } = require("@groq/sdk")); } catch (e) { console.warn("⚠️ @groq/sdk non installé"); }

let duckSearch = null;
try { ({ search: duckSearch } = require("duck-duck-scrape")); } catch (e) { console.warn("⚠️ duck-duck-scrape non installé"); }

// ==================== IMPORTS FIREBASE ADMIN ====================
let firebaseAdmin = null;
try { firebaseAdmin = require("firebase-admin"); } catch (e) { console.warn("⚠️ firebase-admin non installé"); }

// ==================== IMPORTS OPTIONNELS ====================
let BullMQ = null;
let IORedis = null;
try { BullMQ = require("bullmq"); IORedis = require("ioredis"); } catch (e) { console.warn("⚠️ BullMQ/Redis non installés - file d'attente en mémoire"); }

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
  VERSION: "13.0.0",
  AGENT_NAME: "Luba",
  COMPANY: "HIKLON Technology",

  MAX_MESSAGE_LENGTH: parseInt(process.env.MAX_MESSAGE_LENGTH || "15000", 10),
  MAX_HISTORY_LENGTH: parseInt(process.env.MAX_HISTORY_LENGTH || "100", 10),
  MAX_CONTEXT_MESSAGES: parseInt(process.env.MAX_CONTEXT_MESSAGES || "30", 10),
  MAX_CONTEXT_TOKENS: parseInt(process.env.MAX_CONTEXT_TOKENS || "8000", 10),
  SUMMARY_TRIGGER_AT: parseInt(process.env.SUMMARY_TRIGGER_AT || "40", 10),
  IMAGE_SEARCH_LIMIT: parseInt(process.env.IMAGE_SEARCH_LIMIT || "6", 10),
  MAX_IMAGE_SIZE_MB: parseInt(process.env.MAX_IMAGE_SIZE_MB || "10", 10),
  MAX_IMAGES_PER_REQUEST: parseInt(process.env.MAX_IMAGES_PER_REQUEST || "3", 10),

  MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS || "3", 10),
  RETRY_BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS || "1000", 10),
  RETRY_MAX_DELAY_MS: parseInt(process.env.RETRY_MAX_DELAY_MS || "8000", 10),
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),
  CIRCUIT_BREAKER_RESET_MS: parseInt(process.env.CIRCUIT_BREAKER_RESET_MS || "60000", 10),

  DEFAULT_TIMEOUT: parseInt(process.env.DEFAULT_TIMEOUT || "30000", 10),

  WHATSAPP_QR_TIMEOUT: parseInt(process.env.WHATSAPP_QR_TIMEOUT || "30000", 10),
  WHATSAPP_RETRY_DELAY: parseInt(process.env.WHATSAPP_RETRY_DELAY || "3000", 10),

  MAX_LOGIN_ATTEMPTS: parseInt(process.env.MAX_LOGIN_ATTEMPTS || "5", 10),
  LOGIN_BLOCK_DURATION: parseInt(process.env.LOGIN_BLOCK_DURATION || "900000", 10),
  MAX_SESSIONS_PER_USER: parseInt(process.env.MAX_SESSIONS_PER_USER || "10", 10),

  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  UPLOADS_PATH: path.join(__dirname, "uploads"),
  IMAGE_CACHE_PATH: path.join(__dirname, "data", "image_cache"),

  VISION_MODEL_GROQ: process.env.VISION_MODEL_GROQ || "openai/gpt-4o-mini",
  VISION_MODEL_OPENROUTER: process.env.VISION_MODEL_OPENROUTER || "qwen/qwen-2.5-vl-72b-instruct:free",

  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/13.0.0"
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

// ==================== QUOTAS ====================
const USER_QUOTAS = {
  FREE: { maxMessagesPerDay: 100, maxImagesPerDay: 20, maxWhatsAppMessagesPerDay: 10, maxEmailsPerDay: 5, maxActionsPerDay: 30, maxTokensPerRequest: 8000 },
  PREMIUM: { maxMessagesPerDay: 1000, maxImagesPerDay: 200, maxWhatsAppMessagesPerDay: 100, maxEmailsPerDay: 50, maxActionsPerDay: 500, maxTokensPerRequest: 32000 },
  ADMIN: { maxMessagesPerDay: 999999, maxImagesPerDay: 999999, maxWhatsAppMessagesPerDay: 999999, maxEmailsPerDay: 999999, maxActionsPerDay: 999999, maxTokensPerRequest: 128000 }
};

// ==================== DOSSIERS ====================
for (const dir of [path.dirname(CONFIG.DB_PATH), CONFIG.SESSIONS_PATH, CONFIG.UPLOADS_PATH, CONFIG.IMAGE_CACHE_PATH]) {
  if (!fs.existsSync(dir)) { fs.mkdirSync(dir, { recursive: true }); console.log(`📁 Dossier créé: ${dir}`); }
}

// ==================== LOGGER ====================
const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  transport: process.env.NODE_ENV === "development" ? { target: "pino-pretty", options: { colorize: true } } : undefined,
  base: { service: "luba-backend", version: CONFIG.VERSION }
});

// ==================== FIREBASE ADMIN ====================
let firebaseApp = null;

function parseFirebaseServiceAccount(raw) {
  try { return JSON.parse(raw); }
  catch (e) {
    try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
    catch (e2) { throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide"); }
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
  } catch (e) { logger.error({ err: e.message }, "❌ Erreur initialisation Firebase Admin"); }
} else {
  logger.warn("⚠️ Firebase Admin non initialisé - mode API REST");
}

// ==================== SQLITE ====================
const db = new sqlite3.Database(CONFIG.DB_PATH, (err) => {
  if (err) { logger.error({ err: err.message }, "❌ Impossible d'ouvrir la base SQLite"); process.exit(1); }
  logger.info("✅ Base de données SQLite initialisée");
});

db.run("PRAGMA journal_mode = WAL;");
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -64000;");
db.run("PRAGMA busy_timeout = 10000;");
db.run("PRAGMA temp_store = MEMORY;");
db.run("PRAGMA foreign_keys = ON;");
db.run("PRAGMA wal_autocheckpoint = 1000;");

// ==================== SCHÉMA SQLITE (UID UNIFIÉ) ====================
// ⚠️ CHANTIER 3 : Un seul UID canonique = firebase_uid. Toutes les tables référencent canonical_uid.
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    canonical_uid TEXT PRIMARY KEY,
    firebase_uid TEXT UNIQUE,
    email TEXT UNIQUE,
    display_name TEXT,
    role TEXT DEFAULT 'FREE',
    email_verified INTEGER DEFAULT 0,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    preferences TEXT DEFAULT '{}',
    last_seen_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Migration douce depuis ancien schéma (id / firebase_uid / user_id)
  db.all("PRAGMA table_info(users)", (err, cols) => {
    if (err || !cols) return;
    const colNames = cols.map(c => c.name);
    if (colNames.includes("id") && !colNames.includes("canonical_uid")) {
      db.run("ALTER TABLE users ADD COLUMN canonical_uid TEXT");
      db.run("UPDATE users SET canonical_uid = COALESCE(firebase_uid, id) WHERE canonical_uid IS NULL");
      logger.info("🔧 Migration users : canonical_uid ajouté");
    }
  });

  db.run(`CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    canonical_uid TEXT NOT NULL,
    title TEXT,
    summary TEXT,
    last_message_preview TEXT,
    message_count INTEGER DEFAULT 0,
    pinned INTEGER DEFAULT 0,
    archived INTEGER DEFAULT 0,
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (canonical_uid) REFERENCES users(canonical_uid) ON DELETE CASCADE
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    canonical_uid TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL,
    tool_calls TEXT,
    images TEXT DEFAULT '[]',
    action_cards TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
  )`);

  // Migration douce messages
  db.all("PRAGMA table_info(messages)", (err, cols) => {
    if (err || !cols) return;
    const colNames = cols.map(c => c.name);
    if (colNames.includes("session_id") && !colNames.includes("conversation_id")) {
      db.run("ALTER TABLE messages ADD COLUMN conversation_id TEXT");
      db.run("UPDATE messages SET conversation_id = session_id WHERE conversation_id IS NULL");
      logger.info("🔧 Migration messages : conversation_id ajouté");
    }
    if (!colNames.includes("canonical_uid")) {
      db.run("ALTER TABLE messages ADD COLUMN canonical_uid TEXT");
    }
    if (!colNames.includes("action_cards")) {
      db.run("ALTER TABLE messages ADD COLUMN action_cards TEXT DEFAULT '[]'");
    }
  });

  db.run("CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id, id DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_uid ON messages(canonical_uid, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_conv_uid ON conversations(canonical_uid, updated_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_conv_updated ON conversations(updated_at DESC)");

  // ========== CHANTIER 4 : file de synchronisation ==========
  db.run(`CREATE TABLE IF NOT EXISTS sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_sync_status ON sync_queue(status, created_at)");

  // ========== CHANTIER 6 : Agent Jarvis ==========
  db.run(`CREATE TABLE IF NOT EXISTS agent_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL,
    task_uid TEXT UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    due_at DATETIME,
    priority TEXT DEFAULT 'normal',
    status TEXT DEFAULT 'pending',
    tags TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (canonical_uid) REFERENCES users(canonical_uid) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_tasks_uid_status ON agent_tasks(canonical_uid, status, due_at)");

  db.run(`CREATE TABLE IF NOT EXISTS agent_calendar (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL,
    event_uid TEXT UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    starts_at DATETIME NOT NULL,
    ends_at DATETIME,
    location TEXT,
    reminder_minutes INTEGER DEFAULT 15,
    metadata TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (canonical_uid) REFERENCES users(canonical_uid) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_calendar_uid_start ON agent_calendar(canonical_uid, starts_at)");

  db.run(`CREATE TABLE IF NOT EXISTS agent_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL,
    note_uid TEXT UNIQUE,
    title TEXT,
    content TEXT NOT NULL,
    tags TEXT DEFAULT '[]',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (canonical_uid) REFERENCES users(canonical_uid) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_notes_uid ON agent_notes(canonical_uid, created_at DESC)");

  db.run(`CREATE TABLE IF NOT EXISTS agent_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL,
    conversation_id TEXT,
    action_type TEXT NOT NULL,
    payload TEXT DEFAULT '{}',
    status TEXT DEFAULT 'executed',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Tables d'audit
  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT, to_email TEXT NOT NULL, subject TEXT,
    status TEXT DEFAULT 'pending', provider TEXT, error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS llm_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT, canonical_uid TEXT, provider TEXT, model TEXT, tier TEXT,
    prompt_tokens INTEGER DEFAULT 0, completion_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0, status TEXT DEFAULT 'success', error_code TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS user_quotas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL, date TEXT NOT NULL,
    messages_count INTEGER DEFAULT 0, images_count INTEGER DEFAULT 0,
    whatsapp_count INTEGER DEFAULT 0, emails_count INTEGER DEFAULT 0, actions_count INTEGER DEFAULT 0,
    UNIQUE(canonical_uid, date)
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS security_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT, event_type TEXT NOT NULL, details TEXT DEFAULT '{}',
    ip_address TEXT, user_agent TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS active_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT NOT NULL, session_token TEXT UNIQUE,
    ip_address TEXT, user_agent TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME, is_revoked INTEGER DEFAULT 0
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS login_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid TEXT, ip_address TEXT, success INTEGER DEFAULT 0,
    error_message TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS blocked_ips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_address TEXT UNIQUE, reason TEXT, blocked_until DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Migration quotas
  db.all("PRAGMA table_info(user_quotas)", (err, cols) => {
    if (err || !cols) return;
    const names = cols.map(c => c.name);
    if (!names.includes("actions_count")) {
      db.run("ALTER TABLE user_quotas ADD COLUMN actions_count INTEGER DEFAULT 0");
    }
  });
});

logger.info("✅ Schéma SQLite initialisé");

// ==================== SUPABASE ====================
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

// ==================== EMAIL ====================
let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  emailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || "587", 10),
    secure: process.env.SMTP_PORT === "465",
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
    tls: { rejectUnauthorized: false },
    pool: true, maxConnections: 3, maxMessages: 50
  });
  logger.info("✅ SMTP configuré");
} else {
  logger.warn("⚠️ SMTP non configuré");
}

// ==================== MULTER ====================
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: CONFIG.MAX_IMAGE_SIZE_MB * 1024 * 1024, files: CONFIG.MAX_IMAGES_PER_REQUEST },
  fileFilter: (req, file, cb) => {
    if (CONFIG.ALLOWED_IMAGE_TYPES.includes(file.mimetype)) cb(null, true);
    else cb(new Error(`Type de fichier non supporté: ${file.mimetype}`));
  }
});

// ==================== NLP / RSS ====================
const rssParser = new Parser({
  timeout: 10000,
  headers: { "User-Agent": CONFIG.HTTP_USER_AGENT }
});

const classifier = new natural.BayesClassifier();
classifier.addDocument("calcule 2+2", "MATHS");
classifier.addDocument("résous cette équation x^2 + 3x + 2 = 0", "MATHS");
classifier.addDocument("intégrale de sin(x)", "MATHS");
classifier.addDocument("dérivée de x^2", "MATHS");
classifier.addDocument("factorielle de 10", "MATHS");
classifier.addDocument("quelles sont les dernières actualités", "ACTUALITÉ");
classifier.addDocument("informations sur la politique", "ACTUALITÉ");
classifier.addDocument("news du jour", "ACTUALITÉ");
classifier.addDocument("score du match de football", "SPORT");
classifier.addDocument("résultat du PSG", "SPORT");
classifier.addDocument("classement ligue 1", "SPORT");
classifier.addDocument("écris un code en javascript", "CODE");
classifier.addDocument("fonction python pour trier", "CODE");
classifier.addDocument("debug ce script", "CODE");
classifier.addDocument("joue la chanson Thriller", "ACTION");
classifier.addDocument("cherche le clip de Thriller sur youtube", "ACTION");
classifier.addDocument("rappelle-moi d'appeler maman demain", "ACTION");
classifier.addDocument("ajoute une tâche acheter du lait", "ACTION");
classifier.addDocument("planifie une réunion demain à 14h", "ACTION");
classifier.addDocument("note que le client s'appelle Marc", "ACTION");
classifier.addDocument("météo à Kinshasa", "ACTION");
classifier.addDocument("envoie un email à marc", "ACTION");
classifier.train();

// ==================== UTILITAIRES ====================
function convertImageToBase64(buffer, mimetype) {
  return { dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`, base64: buffer.toString("base64"), mimetype, size: buffer.length };
}
const generateRequestId = () => `req_${crypto.randomUUID()}`;
const generateConversationId = () => `conv_${crypto.randomUUID()}`;
const generateSessionToken = () => `sess_${crypto.randomBytes(32).toString("hex")}`;
const generateEntityUid = (prefix) => `${prefix}_${crypto.randomUUID()}`;

function decodeXmlEntities(str) {
  return String(str)
    .replace(/<!\[CDATA\[/g, "").replace(/\]\]>/g, "")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\s+/g, " ").trim();
}

function dbGet(query, params = []) {
  return new Promise((resolve, reject) => { db.get(query, params, (err, row) => err ? reject(err) : resolve(row)); });
}
function dbAll(query, params = []) {
  return new Promise((resolve, reject) => { db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows)); });
}
function dbRun(query, params = []) {
  return new Promise((resolve, reject) => { db.run(query, params, function (err) { err ? reject(err) : resolve(this); }); });
}

// ==================== CHANTIER 3 : UID UNIFIÉ ====================
/**
 * Résout l'UID canonique. TOUJOURS firebase_uid (primaire) — sinon un id stable.
 * Assure la création / mise à jour dans SQLite + Supabase.
 */
async function resolveCanonicalUser(identity) {
  const uid = identity.uid;
  if (!uid) throw new Error("UID manquant");

  let row = await dbGet("SELECT * FROM users WHERE canonical_uid = ? OR firebase_uid = ?", [uid, uid]);

  if (!row) {
    await dbRun(
      `INSERT INTO users (canonical_uid, firebase_uid, email, display_name, role, email_verified, last_seen_at)
       VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
      [uid, uid, identity.email || null, identity.displayName || uid, identity.role || 'FREE', identity.emailVerified ? 1 : 0]
    );
    logger.info({ uid }, "✅ Utilisateur canonique créé");
  } else {
    // Réconciliation : si ancien id ≠ firebase_uid, on promeut firebase_uid comme canonical
    if (row.canonical_uid !== uid && row.firebase_uid === uid) {
      await dbRun("UPDATE users SET canonical_uid = ? WHERE canonical_uid = ?", [uid, row.canonical_uid]);
      row.canonical_uid = uid;
    }
    await dbRun(
      `UPDATE users SET firebase_uid = ?, email = COALESCE(?, email), display_name = COALESCE(?, display_name),
       role = ?, email_verified = ?, last_seen_at = CURRENT_TIMESTAMP WHERE canonical_uid = ?`,
      [uid, identity.email, identity.displayName, identity.role || row.role || 'FREE', identity.emailVerified ? 1 : 0, row.canonical_uid]
    );
  }

  // Sync Supabase (asynchrone, non bloquant)
  syncUserToSupabase(uid, identity).catch(e => logger.warn({ err: e.message }, "Sync user Supabase échouée"));

  return uid;
}

// ==================== AUDIT / LOGS ====================
async function auditLLMCall(data) {
  try {
    await dbRun(
      `INSERT INTO llm_audit_log (conversation_id, canonical_uid, provider, model, tier, prompt_tokens, completion_tokens, latency_ms, status, error_code)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [data.conversationId, data.canonicalUid, data.provider, data.model, data.tier,
       data.promptTokens || 0, data.completionTokens || 0, data.latencyMs || 0,
       data.status || 'success', data.errorCode || null]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur audit LLM"); }
}

async function logSecurityEvent(canonicalUid, eventType, details = {}, ipAddress = null, userAgent = null) {
  try {
    await dbRun(
      `INSERT INTO security_logs (canonical_uid, event_type, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)`,
      [canonicalUid, eventType, JSON.stringify(details), ipAddress, userAgent]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur log sécurité"); }
}

// ==================== FIREBASE TOKEN ====================
async function verifyFirebaseToken(token) {
  if (firebaseApp && firebaseAdmin) {
    const decoded = await firebaseAdmin.auth(firebaseApp).verifyIdToken(token, true);
    return {
      uid: decoded.uid,
      email: decoded.email || null,
      displayName: decoded.name || null,
      photoURL: decoded.picture || null,
      emailVerified: decoded.email_verified || false,
      role: decoded.role || 'FREE'
    };
  }
  const response = await axios.post(
    `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${FIREBASE_CONFIG.apiKey}`,
    { idToken: token }, { timeout: 10000 }
  );
  if (response.data.users?.length > 0) {
    const u = response.data.users[0];
    return { uid: u.localId, email: u.email || null, displayName: u.displayName || null,
             photoURL: u.photoUrl || null, emailVerified: u.emailVerified || false, role: 'FREE' };
  }
  return null;
}

// ==================== RÔLES ====================
async function setUserRole(uid, role) {
  if (firebaseApp && firebaseAdmin) {
    try {
      await firebaseAdmin.auth(firebaseApp).setCustomUserClaims(uid, { role });
    } catch (e) { logger.warn({ err: e.message }, "setCustomUserClaims a échoué"); }
  }
  await dbRun(`UPDATE users SET role = ? WHERE canonical_uid = ?`, [role, uid]);
  await logSecurityEvent(uid, 'ROLE_UPDATED', { role });
  return { success: true, role };
}

async function getUserRole(uid) {
  if (firebaseApp && firebaseAdmin) {
    try {
      const u = await firebaseAdmin.auth(firebaseApp).getUser(uid);
      return u.customClaims?.role || 'FREE';
    } catch (e) {}
  }
  const row = await dbGet("SELECT role FROM users WHERE canonical_uid = ?", [uid]);
  return row?.role || 'FREE';
}

// ==================== SESSIONS ACTIVES ====================
async function createActiveSession(canonicalUid, ip, ua) {
  const sessionToken = generateSessionToken();
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  const count = await dbGet(`SELECT COUNT(*) as c FROM active_sessions WHERE canonical_uid = ? AND is_revoked = 0 AND expires_at > CURRENT_TIMESTAMP`, [canonicalUid]);
  if (count?.c >= CONFIG.MAX_SESSIONS_PER_USER) {
    await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE id = (SELECT id FROM active_sessions WHERE canonical_uid = ? AND is_revoked = 0 ORDER BY created_at ASC LIMIT 1)`, [canonicalUid]);
  }
  await dbRun(`INSERT INTO active_sessions (canonical_uid, session_token, ip_address, user_agent, expires_at) VALUES (?, ?, ?, ?, ?)`,
    [canonicalUid, sessionToken, ip, ua, expiresAt]);
  return sessionToken;
}

async function revokeSession(canonicalUid, token) {
  await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE canonical_uid = ? AND session_token = ?`, [canonicalUid, token]);
}
async function revokeAllSessions(canonicalUid) {
  await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE canonical_uid = ?`, [canonicalUid]);
}

// ==================== TENTATIVES / IP ====================
async function checkLoginAttempts(ip) {
  const cutoff = new Date(Date.now() - CONFIG.LOGIN_BLOCK_DURATION).toISOString();
  const row = await dbGet(`SELECT COUNT(*) as c FROM login_attempts WHERE ip_address = ? AND success = 0 AND created_at > ?`, [ip, cutoff]);
  if (row?.c >= CONFIG.MAX_LOGIN_ATTEMPTS) {
    await dbRun(`INSERT OR REPLACE INTO blocked_ips (ip_address, reason, blocked_until) VALUES (?, 'Trop de tentatives', ?)`,
      [ip, new Date(Date.now() + CONFIG.LOGIN_BLOCK_DURATION).toISOString()]);
    return { blocked: true, message: "Trop de tentatives. Réessayez plus tard." };
  }
  return { blocked: false };
}
async function recordLoginAttempt(ip, uid, success, errMsg = null) {
  await dbRun(`INSERT INTO login_attempts (canonical_uid, ip_address, success, error_message) VALUES (?, ?, ?, ?)`, [uid, ip, success ? 1 : 0, errMsg]);
}
async function isIPBlocked(ip) {
  const row = await dbGet(`SELECT 1 FROM blocked_ips WHERE ip_address = ? AND blocked_until > CURRENT_TIMESTAMP`, [ip]);
  return Boolean(row);
}

// ==================== QUOTAS ====================
async function checkUserQuota(canonicalUid, action, role = 'FREE') {
  try {
    const today = new Date().toISOString().split('T')[0];
    let row = await dbGet(`SELECT * FROM user_quotas WHERE canonical_uid = ? AND date = ?`, [canonicalUid, today]);
    if (!row) {
      await dbRun(`INSERT INTO user_quotas (canonical_uid, date) VALUES (?, ?)`, [canonicalUid, today]);
      row = { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0, actions_count: 0 };
    }
    const limits = USER_QUOTAS[role] || USER_QUOTAS.FREE;
    const map = {
      message: ['messages_count', 'maxMessagesPerDay'],
      image: ['images_count', 'maxImagesPerDay'],
      whatsapp: ['whatsapp_count', 'maxWhatsAppMessagesPerDay'],
      email: ['emails_count', 'maxEmailsPerDay'],
      action: ['actions_count', 'maxActionsPerDay']
    };
    const [col, lim] = map[action] || [null, null];
    if (!col) return { allowed: true };
    const current = row[col] || 0;
    const max = limits[lim];
    if (current >= max) return { allowed: false, message: `Limite ${action} atteinte (${max}/jour).`, current, max };
    return { allowed: true, current, max, remaining: max - current };
  } catch (e) { logger.error({ err: e.message }, "Quota check"); return { allowed: true }; }
}

async function incrementUserQuota(canonicalUid, action) {
  try {
    const today = new Date().toISOString().split('T')[0];
    const map = { message: 'messages_count', image: 'images_count', whatsapp: 'whatsapp_count', email: 'emails_count', action: 'actions_count' };
    const col = map[action];
    if (!col) return;
    await dbRun(`UPDATE user_quotas SET ${col} = ${col} + 1 WHERE canonical_uid = ? AND date = ?`, [canonicalUid, today]);
  } catch (e) { logger.error({ err: e.message }, "Quota increment"); }
}

// ==================== CIRCUIT BREAKER ====================
class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || CONFIG.CIRCUIT_BREAKER_THRESHOLD;
    this.resetTimeout = options.resetTimeout || CONFIG.CIRCUIT_BREAKER_RESET_MS;
    this.failureCount = 0; this.lastFailureTime = null; this.state = "CLOSED";
  }
  async execute(fn) {
    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime >= this.resetTimeout) this.state = "HALF_OPEN";
      else throw new Error(`Circuit breaker ${this.name} est OPEN`);
    }
    try { const r = await fn(); this.failureCount = 0; this.state = "CLOSED"; return r; }
    catch (e) {
      this.failureCount++; this.lastFailureTime = Date.now();
      if (this.failureCount >= this.failureThreshold) { this.state = "OPEN"; logger.warn({ c: this.name }, "Circuit OPEN"); }
      throw e;
    }
  }
}

// ==================== QUEUE MANAGER ====================
class QueueManager {
  constructor() {
    this.useRedis = Boolean(process.env.REDIS_URL) && Boolean(BullMQ) && Boolean(IORedis);
    this.queues = new Map(); this.workers = new Map(); this.inMemoryQueues = new Map();
    if (this.useRedis) {
      this.connection = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null, retryStrategy: (t) => Math.min(t * 200, 5000) });
      logger.info("✅ Queue Redis");
    } else logger.warn("⚠️ Queue en mémoire");
  }
  createQueue(name, processor, options = {}) {
    if (this.useRedis) {
      const q = new BullMQ.Queue(name, { connection: this.connection });
      const w = new BullMQ.Worker(name, processor, {
        connection: this.connection,
        concurrency: options.concurrency || 3,
        limiter: options.limiter || { max: 10, duration: 1000 }
      });
      w.on("failed", (job, err) => logger.error({ jobId: job?.id, err: err.message }, `Job ${name} échoué`));
      this.queues.set(name, q); this.workers.set(name, w);
    } else {
      const arr = [];
      let processing = false;
      const run = async () => {
        if (processing) return; processing = true;
        while (arr.length > 0) { const j = arr.shift(); try { await processor(j); } catch (e) { logger.error({ err: e.message }, "Job mémoire échoué"); } }
        processing = false;
      };
      this.inMemoryQueues.set(name, { add: async (d) => { arr.push(d); run(); } });
    }
  }
  async add(name, data, options = {}) {
    if (this.useRedis) {
      const q = this.queues.get(name);
      if (q) return await q.add("process", data, { attempts: options.attempts || 5, backoff: { type: "exponential", delay: options.backoffDelay || 2000 }, removeOnComplete: 100, removeOnFail: 500 });
    }
    const q = this.inMemoryQueues.get(name);
    if (q) return await q.add(data);
    throw new Error(`Queue ${name} non trouvée`);
  }
  async close() {
    if (this.useRedis) {
      for (const w of this.workers.values()) await w.close();
      for (const q of this.queues.values()) await q.close();
      await this.connection.quit();
    }
  }
}
const queueManager = new QueueManager();

// ==================== SOURCES ====================
const OPEN_SOURCES = {
  wikipedia: { name: "Wikipédia", url: "https://fr.wikipedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikipedia.org" },
  wikimediacommons: { name: "Wikimedia Commons", url: "https://commons.wikimedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikimedia.org" },
  googlenews: { name: "Google News", url: "https://news.google.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=news.google.com" },
  thesportsdb: { name: "TheSportsDB", url: "https://www.thesportsdb.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=thesportsdb.com" },
  arxiv: { name: "arXiv", url: "https://arxiv.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=arxiv.org" },
  reddit: { name: "Reddit", url: "https://reddit.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=reddit.com" },
  openmeteo: { name: "Open-Meteo", url: "https://open-meteo.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=open-meteo.com" },
  youtube: { name: "YouTube", url: "https://youtube.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=youtube.com" }
};

// ==================== CHANTIER 2 : IMAGES FIABLES ====================
/**
 * Wrapper de recherche d'images : ajoute un proxy interne et un fallback.
 * Toutes les URLs retournées passent par /api/images/proxy?url=...
 */
function proxifyImageUrl(url) {
  if (!url) return null;
  try {
    // Cache local d'images connues (stockage chemin)
    return `/api/images/proxy?url=${encodeURIComponent(url)}`;
  } catch { return url; }
}

async function searchWikimediaImages(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(query)}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 15000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const pages = response.data?.query?.pages;
    if (!pages) return { images: [] };

    const images = await Promise.all(Object.values(pages).map(async (page) => {
      const info = page.imageinfo?.[0];
      const rawUrl = info?.thumburl || info?.url || null;
      if (!rawUrl) return null;

      // Validation HEAD : on vérifie que l'image est vraiment accessible
      const validated = await validateImageUrl(rawUrl);

      return {
        url: proxifyImageUrl(rawUrl),
        originalUrl: rawUrl,
        title: page.title || "Image",
        description: info?.extmetadata?.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
        pageUrl: info?.descriptionurl || null,
        validated
      };
    }));

    const clean = images.filter(i => i && i.validated !== false);
    return { images: clean.length ? clean : images.filter(Boolean) };
  } catch (e) {
    logger.error({ err: e.message }, "Wikimedia search");
    return { images: [], error: e.message };
  }
}

/**
 * Vérifie qu'une URL d'image répond en HEAD. Timeout court.
 */
async function validateImageUrl(url) {
  try {
    const resp = await axios.head(url, { timeout: 5000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT, "Referer": "https://commons.wikimedia.org/" } });
    return resp.status >= 200 && resp.status < 400;
  } catch (e) {
    logger.debug({ url, err: e.message }, "Image invalide");
    return false;
  }
}

// ==================== RECHERCHE ====================
async function searchWikipediaSummary(query) {
  try {
    const url = `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`;
    const r = await axios.get(url, { timeout: 10000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    if (r.data?.type === "disambiguation" || !r.data?.extract) return { summary: null };
    return { title: r.data.title, summary: r.data.extract, url: r.data.content_urls?.desktop?.page, thumbnail: r.data.thumbnail?.source || null };
  } catch { return { summary: null }; }
}

async function searchNews(query) {
  if (!query) return { articles: [] };
  try {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=fr&gl=FR&ceid=FR:fr`;
    const r = await axios.get(url, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const xml = r.data; const items = [];
    const re = /<item>([\s\S]*?)<\/item>/g; let m;
    while ((m = re.exec(xml)) !== null && items.length < 6) {
      const b = m[1];
      const title = (b.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const link = (b.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || "";
      const pubDate = (b.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || "";
      const description = (b.match(/<description>([\s\S]*?)<\/description>/) || [])[1] || "";
      if (title) items.push({ title: decodeXmlEntities(title), link: link.trim(), pubDate, description: decodeXmlEntities(description) });
    }
    return { articles: items };
  } catch (e) { return { articles: [], error: e.message }; }
}

async function searchDuckDuckGo(query) {
  if (!duckSearch) return [];
  try {
    const r = await duckSearch(query, { safeSearch: "OFF", locale: "fr-fr", maxResults: 5 });
    return r.map(x => ({ title: x.title, snippet: x.description, url: x.url, source: x.source }));
  } catch (e) { logger.error({ err: e.message }, "DDG"); return []; }
}

async function searchWeb(query) {
  if (!query) return { results: [], sourcesUsed: [] };
  const [wiki, news, ddg] = await Promise.all([searchWikipediaSummary(query), searchNews(query), searchDuckDuckGo(query)]);
  const results = []; const sourcesUsed = [];
  if (wiki.summary) { results.push({ title: wiki.title, snippet: wiki.summary, url: wiki.url, type: "wiki" }); sourcesUsed.push("wikipedia"); }
  for (const r of ddg) results.push({ ...r, type: "web" });
  if (news.articles?.length) { news.articles.slice(0, 3).forEach(a => results.push({ title: a.title, url: a.link, pubDate: a.pubDate, type: "news" })); sourcesUsed.push("googlenews"); }
  return { results, sourcesUsed };
}

async function searchSportsScores(query) {
  if (!query) return { events: [] };
  try {
    const s = await axios.get(`https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t=${encodeURIComponent(query)}`, { timeout: 12000 });
    const team = s.data?.teams?.[0]; if (!team) return { events: [], error: "Équipe introuvable" };
    const e = await axios.get(`https://www.thesportsdb.com/api/v1/json/3/eventslast.php?id=${team.idTeam}`, { timeout: 12000 });
    return { team: team.strTeam, events: (e.data?.results || []).slice(0, 5).map(x => ({ match: `${x.strHomeTeam} ${x.intHomeScore ?? "?"} - ${x.intAwayScore ?? "?"} ${x.strAwayTeam}`, date: x.dateEvent, league: x.strLeague })) };
  } catch (e) { return { events: [], error: e.message }; }
}

async function searchScience(query) {
  if (!query) return { papers: [] };
  try {
    const r = await axios.get(`http://export.arxiv.org/api/query?search_query=all:${encodeURIComponent(query)}&start=0&max_results=5`, { timeout: 15000 });
    const xml = r.data; const out = [];
    const re = /<entry>([\s\S]*?)<\/entry>/g; let m;
    while ((m = re.exec(xml)) !== null && out.length < 5) {
      const b = m[1];
      const title = (b.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const summary = (b.match(/<summary>([\s\S]*?)<\/summary>/) || [])[1] || "";
      const link = (b.match(/<id>([\s\S]*?)<\/id>/) || [])[1] || "";
      if (title) out.push({ title: decodeXmlEntities(title), summary: decodeXmlEntities(summary).slice(0, 300), link: link.trim() });
    }
    return { papers: out };
  } catch (e) { return { papers: [], error: e.message }; }
}

async function searchSocial(query) {
  if (!query) return { posts: [] };
  try {
    const r = await axios.get(`https://www.reddit.com/search.json?q=${encodeURIComponent(query)}&limit=6&sort=relevance`, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    return { posts: (r.data?.data?.children || []).map(c => ({ title: c.data.title, subreddit: c.data.subreddit_name_prefixed, score: c.data.score, url: `https://reddit.com${c.data.permalink}` })) };
  } catch (e) { return { posts: [], error: e.message }; }
}

async function getWeather(location) {
  if (!location) return { error: "Aucun lieu" };
  try {
    const g = await axios.get(`https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(location)}&count=1&language=fr`, { timeout: 10000 });
    const p = g.data?.results?.[0]; if (!p) return { error: `Lieu "${location}" introuvable` };
    const w = await axios.get(`https://api.open-meteo.com/v1/forecast?latitude=${p.latitude}&longitude=${p.longitude}&current=temperature_2m,weather_code,wind_speed_10m&timezone=auto`, { timeout: 10000 });
    const c = w.data?.current;
    return { location: `${p.name}, ${p.country}`, temperature: c?.temperature_2m, windSpeed: c?.wind_speed_10m, weatherCode: c?.weather_code };
  } catch (e) { return { error: e.message }; }
}

function executeMathExpression(expression) {
  try { const r = math.evaluate(expression); return { success: true, expression, result: r, formatted: math.format(r, { precision: 14 }) }; }
  catch (e) { return { success: false, expression, error: e.message }; }
}

function detectMathExpressions(message) {
  const patterns = [
    /(\d+[\d\s\*\+\-\/\(\)\.]+\d+)/g,
    /(?:calcule|calcul|résous|resous|solve|compute)\s*:?\s*([^\n]+)/i,
    /(\d+\s*[\+\-\*\/\^]\s*\d+)/g,
    /(?:intégrale|integrale|dérivée|derivee|factorielle|matrice|limite)\s*(?:de|of)?\s*:?\s*([^\n]+)/i,
    /(?:sqrt|sin|cos|tan|log|exp|abs|floor|ceil|round)\s*\([^)]+\)/g
  ];
  const out = [];
  for (const p of patterns) { const m = message.match(p); if (m) out.push(...m); }
  return out;
}

// ==================== CHANTIER 6 : AGENT JARVIS ====================
/**
 * Détecte les commandes d'action (verbe + objet) dans un message.
 * Retourne { isAction, actionType, params } ou null.
 */
function detectAgentAction(message) {
  const m = String(message).toLowerCase().trim();

  // YouTube / musique / vidéo
  const ytMatch = m.match(/(?:joue|mets|lance|play|cherche|trouve|montre|ouvre)\s+(?:le\s+|la\s+|les\s+|un\s+|une\s+)?(?:clip|chanson|musique|vidéo|video|film|morceau|titre|album)?\s*(?:de|d'|du|des)?\s*(.+)/i);
  if (ytMatch && /(joue|mets|lance|play|clip|chanson|musique|vidéo|video|youtube)/i.test(m)) {
    return { isAction: true, actionType: "youtube.play", params: { query: ytMatch[1].trim() } };
  }

  // Tâche
  const taskAdd = m.match(/(?:ajoute|crée|créer|nouvelle?)\s+(?:une\s+)?t[âa]che\s*:?\s*(.+)/i);
  if (taskAdd) return { isAction: true, actionType: "task.create", params: { title: taskAdd[1].trim() } };
  const taskList = /(?:liste|mes|voir|affiche|quelles?)\s+(?:mes\s+)?t[âa]ches/i;
  if (taskList.test(m)) return { isAction: true, actionType: "task.list", params: {} };

  // Calendrier / rappel
  const reminder = m.match(/(?:rappelle[- ]moi|rappelle|rappel|reminder|rappelez[- ]moi)\s+(?:de\s+|d'|que\s+)?(.+)/i);
  if (reminder) return { isAction: true, actionType: "calendar.reminder", params: { text: reminder[1].trim(), raw: message } };

  const calCreate = m.match(/(?:planifie|programme|ajoute|crée|créer|organise)\s+(?:une\s+|un\s+)?(?:r[ée]union|événement|event|rendez[- ]vous|rdv|rencontre)\s*(.+)/i);
  if (calCreate) return { isAction: true, actionType: "calendar.create", params: { text: calCreate[1].trim(), raw: message } };

  const calList = /(?:mon|voir|affiche|liste|quels?)\s+(?:agenda|calendrier|emploi\s+du\s+temps|événements|rendez[- ]vous)/i;
  if (calList.test(m)) return { isAction: true, actionType: "calendar.list", params: {} };

  // Note
  const noteAdd = m.match(/(?:note|retiens|m[ée]morise|sauvegarde|prends?\s+note)\s*(?:que\s+|:)?\s*(.+)/i);
  if (noteAdd) return { isAction: true, actionType: "note.create", params: { content: noteAdd[1].trim() } };

  // Météo
  const weatherMatch = m.match(/(?:m[ée]t[ée]o|temps|temp[ée]rature)\s+(?:à|a|de|du|pour|sur)\s+(.+)/i);
  if (weatherMatch) return { isAction: true, actionType: "weather.show", params: { location: weatherMatch[1].trim() } };

  // Email
  const emailMatch = m.match(/(?:envoie|envoyer|écris|ecris)\s+(?:un\s+)?(?:email|mail|courriel)\s+(?:à|a|pour)\s+(.+)/i);
  if (emailMatch) return { isAction: true, actionType: "email.compose", params: { recipient: emailMatch[1].trim(), raw: message } };

  // WhatsApp
  const waMatch = m.match(/(?:envoie|envoyer|écris|ecris)\s+(?:un\s+)?(?:whatsapp|message\s+whatsapp)\s+(?:à|a|pour)\s+(.+)/i);
  if (waMatch) return { isAction: true, actionType: "whatsapp.compose", params: { recipient: waMatch[1].trim(), raw: message } };

  // Web open
  const webMatch = m.match(/(?:ouvre|va\s+sur|navigue\s+vers|open)\s+(https?:\/\/\S+)/i);
  if (webMatch) return { isAction: true, actionType: "web.open", params: { url: webMatch[1] } };

  return { isAction: false };
}

/**
 * Orchestrateur : exécute une action détectée et retourne une "actionCard"
 * que le frontend rend comme composant interactif.
 */
class AgentOrchestrator {
  constructor() {}

  async execute(actionType, params, ctx) {
    const { canonicalUid, conversationId, message } = ctx;
    const card = { type: actionType, params: {}, display: {}, createdAt: new Date().toISOString() };

    try {
      switch (actionType) {
        case "youtube.play": {
          const query = params.query || message;
          const results = await this.searchYouTube(query, 5);
          if (!results.length) {
            card.display = { error: true, message: `Aucune vidéo trouvée pour "${query}".` };
            card.cardType = "youtube.error";
            return card;
          }
          const top = results[0];
          card.cardType = "youtube.player";
          card.display = {
            videoId: top.videoId,
            title: top.title,
            thumbnail: top.thumbnail,
            channel: top.channel,
            url: `https://www.youtube.com/watch?v=${top.videoId}`,
            embedUrl: `https://www.youtube.com/embed/${top.videoId}?autoplay=1`,
            related: results.slice(1)
          };
          await this.logAgentAction(canonicalUid, conversationId, "youtube.play", { query, videoId: top.videoId });
          return card;
        }

        case "task.create": {
          const task = await this.createTask(canonicalUid, params.title, message);
          card.cardType = "task.created";
          card.display = { task };
          await this.logAgentAction(canonicalUid, conversationId, "task.create", { taskId: task.task_uid });
          return card;
        }
        case "task.list": {
          const tasks = await this.listTasks(canonicalUid);
          card.cardType = "task.list";
          card.display = { tasks, count: tasks.length };
          return card;
        }

        case "calendar.reminder":
        case "calendar.create": {
          const parsed = this.parseDateTime(params.raw || message);
          const event = await this.createCalendarEvent(canonicalUid, {
            title: params.text || params.title || message,
            starts_at: parsed.startsAt,
            ends_at: parsed.endsAt,
            reminder_minutes: 15
          });
          card.cardType = "calendar.created";
          card.display = { event, parsed };
          await this.logAgentAction(canonicalUid, conversationId, actionType, { eventId: event.event_uid });
          return card;
        }
        case "calendar.list": {
          const events = await this.listCalendarEvents(canonicalUid);
          card.cardType = "calendar.list";
          card.display = { events, count: events.length };
          return card;
        }

        case "note.create": {
          const note = await this.createNote(canonicalUid, params.content);
          card.cardType = "note.created";
          card.display = { note };
          return card;
        }

        case "weather.show": {
          const w = await getWeather(params.location);
          card.cardType = "weather.card";
          card.display = w;
          return card;
        }

        case "email.compose": {
          card.cardType = "email.composer";
          card.display = { recipient: params.recipient, subject: "", body: "", raw: params.raw };
          return card;
        }
        case "whatsapp.compose": {
          card.cardType = "whatsapp.composer";
          card.display = { recipient: params.recipient, message: "", raw: params.raw };
          return card;
        }

        case "web.open": {
          card.cardType = "web.iframe";
          card.display = { url: params.url };
          return card;
        }

        default: {
          card.cardType = "unknown";
          card.display = { message: "Action non supportée." };
          return card;
        }
      }
    } catch (e) {
      logger.error({ err: e.message, actionType }, "Agent action failed");
      card.cardType = `${actionType}.error`;
      card.display = { error: true, message: e.message };
      return card;
    }
  }

  async searchYouTube(query, limit = 5) {
    // Scraping léger de la page de résultats YouTube (sans API key)
    try {
      const url = `https://www.youtube.com/results?search_query=${encodeURIComponent(query)}`;
      const r = await axios.get(url, {
        timeout: 12000,
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
          "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"
        }
      });
      const html = r.data;
      // Extraction basique : videoId + titre
      const results = [];
      const re = /"videoRenderer":\{"videoId":"([^"]+)".*?"title":\{"runs":\[\{"text":"([^"]+)"/g;
      let m;
      while ((m = re.exec(html)) !== null && results.length < limit) {
        results.push({
          videoId: m[1],
          title: m[2].replace(/\\u0026/g, "&").replace(/\\"/g, '"'),
          thumbnail: `https://i.ytimg.com/vi/${m[1]}/hqdefault.jpg`,
          channel: null,
          url: `https://www.youtube.com/watch?v=${m[1]}`
        });
      }
      return results;
    } catch (e) {
      logger.error({ err: e.message, query }, "YouTube search failed");
      return [];
    }
  }

  // ----- TÂCHES -----
  async createTask(canonicalUid, title, rawText = "") {
    const task_uid = generateEntityUid("task");
    const parsed = this.parseDateTime(rawText);
    await dbRun(
      `INSERT INTO agent_tasks (canonical_uid, task_uid, title, description, due_at, priority, tags)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [canonicalUid, task_uid, title, rawText, parsed.startsAt, "normal", "[]"]
    );
    return await dbGet(`SELECT * FROM agent_tasks WHERE task_uid = ?`, [task_uid]);
  }

  async listTasks(canonicalUid, status = "pending") {
    return await dbAll(
      `SELECT * FROM agent_tasks WHERE canonical_uid = ? AND status = ? ORDER BY COALESCE(due_at, created_at) ASC LIMIT 50`,
      [canonicalUid, status]
    );
  }

  async completeTask(canonicalUid, task_uid) {
    await dbRun(`UPDATE agent_tasks SET status = 'done', updated_at = CURRENT_TIMESTAMP WHERE task_uid = ? AND canonical_uid = ?`, [task_uid, canonicalUid]);
  }

  // ----- CALENDRIER -----
  async createCalendarEvent(canonicalUid, ev) {
    const event_uid = generateEntityUid("event");
    await dbRun(
      `INSERT INTO agent_calendar (canonical_uid, event_uid, title, description, starts_at, ends_at, location, reminder_minutes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [canonicalUid, event_uid, ev.title, ev.description || null, ev.starts_at, ev.ends_at || null, ev.location || null, ev.reminder_minutes || 15]
    );
    return await dbGet(`SELECT * FROM agent_calendar WHERE event_uid = ?`, [event_uid]);
  }

  async listCalendarEvents(canonicalUid, fromNow = true) {
    const q = fromNow
      ? `SELECT * FROM agent_calendar WHERE canonical_uid = ? AND starts_at >= datetime('now', '-1 day') ORDER BY starts_at ASC LIMIT 50`
      : `SELECT * FROM agent_calendar WHERE canonical_uid = ? ORDER BY starts_at DESC LIMIT 50`;
    return await dbAll(q, [canonicalUid]);
  }

  // ----- NOTES -----
  async createNote(canonicalUid, content) {
    const note_uid = generateEntityUid("note");
    await dbRun(`INSERT INTO agent_notes (canonical_uid, note_uid, title, content, tags) VALUES (?, ?, ?, ?, ?)`,
      [canonicalUid, note_uid, content.slice(0, 50), content, "[]"]);
    return await dbGet(`SELECT * FROM agent_notes WHERE note_uid = ?`, [note_uid]);
  }
  async listNotes(canonicalUid) {
    return await dbAll(`SELECT * FROM agent_notes WHERE canonical_uid = ? ORDER BY created_at DESC LIMIT 50`, [canonicalUid]);
  }

  // ----- UTILS -----
  parseDateTime(text) {
    const now = new Date();
    const lower = String(text).toLowerCase();
    let startsAt = new Date(now.getTime() + 60 * 60 * 1000); // +1h par défaut

    // "demain"
    if (/\bdemain\b/.test(lower)) startsAt.setDate(startsAt.getDate() + 1);
    if (/\bapr[èe]s[- ]demain\b/.test(lower)) startsAt.setDate(startsAt.getDate() + 2);
    if (/\baujourd'?hui\b/.test(lower)) { /* déjà aujourd'hui */ }

    // Heures : "à 14h", "à 14h30", "14:30"
    const hm = lower.match(/(?:à|a|vers)?\s*(\d{1,2})\s*(?:h|:)\s*(\d{2})?/);
    if (hm) {
      const h = parseInt(hm[1], 10);
      const m = hm[2] ? parseInt(hm[2], 10) : 0;
      startsAt.setHours(h, m, 0, 0);
    }

    // "dans X minutes / heures"
    const dm = lower.match(/dans\s+(\d+)\s*(minute|min|heure|h)/);
    if (dm) {
      const n = parseInt(dm[1], 10);
      const unit = dm[2];
      startsAt = new Date(now.getTime() + (unit.startsWith("min") ? n * 60000 : n * 3600000));
    }

    const endsAt = new Date(startsAt.getTime() + 60 * 60 * 1000);
    return { startsAt: startsAt.toISOString(), endsAt: endsAt.toISOString() };
  }

  async logAgentAction(canonicalUid, conversationId, actionType, payload) {
    try {
      await dbRun(`INSERT INTO agent_actions (canonical_uid, conversation_id, action_type, payload) VALUES (?, ?, ?, ?)`,
        [canonicalUid, conversationId, actionType, JSON.stringify(payload || {})]);
      await incrementUserQuota(canonicalUid, 'action');
    } catch (e) { logger.warn({ err: e.message }, "logAgentAction"); }
  }
}

const agentOrchestrator = new AgentOrchestrator();

// ==================== CHANTIER 1 : RESPONSE FORMATTER ====================
/**
 * Formate une réponse brute pour un affichage hiérarchisé côté frontend :
 *  - title : grand titre en gras
 *  - subtitle : sous-titre lisible (police distincte)
 *  - sections : [{ heading, content }] structurés
 *  - body : markdown restant
 *  - citations / sources
 *
 * Ne supprime jamais le markdown : il est CONSERVÉ dans `body` pour compat.
 */
class ResponseFormatter {
  static format(replyText, extras = {}) {
    const text = String(replyText || "");
    const lines = text.split("\n");

    let title = null;
    let subtitle = null;
    const sections = [];
    const rest = [];
    let currentSection = null;

    const isHeading = (l) => /^#{1,6}\s+/.test(l);
    const isBoldLine = (l) => /^\*\*[^*]+\*\*\s*$/.test(l.trim());

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const trimmed = line.trim();

      if (!title && (isHeading(line) || isBoldLine(line))) {
        title = trimmed.replace(/^#{1,6}\s+/, "").replace(/^\*\*|\*\*$/g, "").trim();
        continue;
      }
      if (!subtitle && title && (isHeading(line) || isBoldLine(line))) {
        subtitle = trimmed.replace(/^#{1,6}\s+/, "").replace(/^\*\*|\*\*$/g, "").trim();
        continue;
      }
      if (isHeading(line)) {
        if (currentSection) sections.push(currentSection);
        currentSection = { heading: trimmed.replace(/^#{1,6}\s+/, "").trim(), content: [] };
        continue;
      }
      if (currentSection) currentSection.content.push(line);
      else rest.push(line);
    }
    if (currentSection) sections.push(currentSection);

    // Nettoyage des sections (contenu concaténé)
    const cleanSections = sections
      .map(s => ({ heading: s.heading, content: s.content.join("\n").trim() }))
      .filter(s => s.heading || s.content);

    // Extraction des images markdown pour exposition structurée
    const imageMatches = text.match(/!\[[^\]]*\]\((https?:\/\/[^\s)]+)\)/g) || [];
    const images = imageMatches.map(m => {
      const urlM = m.match(/\((https?:\/\/[^\s)]+)\)/);
      const altM = m.match(/!\[([^\]]*)\]/);
      return { url: urlM ? urlM[1] : null, alt: altM ? altM[1] : "", markdown: m };
    }).filter(i => i.url);

    return {
      title: title || null,
      subtitle: subtitle || null,
      sections: cleanSections,
      body: text,              // Markdown complet conservé pour compat
      images,                  // Images structurées
      sources: extras.sources || [],
      actionCards: extras.actionCards || [],
      suggestions: extras.suggestions || [],
      metadata: {
        wordCount: text.split(/\s+/).filter(Boolean).length,
        charCount: text.length,
        hasCode: /```/.test(text),
        hasMath: /\$[^$]+\$|\$\$[\s\S]+?\$\$|\\\(|\\\[/.test(text),
        language: extras.language || "fr"
      }
    };
  }
}

// ==================== CONTEXT MANAGER ====================
class DynamicContextManager {
  constructor() {
    this.domainPatterns = [
      { domain: "mathematics", keywords: ["math", "calcul", "équation", "équation", "algèbre", "géométrie", "intégrale", "dérivée", "théorème", "matrice", "probabilité"],
        systemPrompt: "Expert mathématiques. LaTeX systématique. Détaille les étapes." },
      { domain: "cybersecurity", keywords: ["sécurité", "cyber", "hack", "vulnérabilité", "pentest", "cryptographie", "chiffrement", "firewall", "malware", "phishing"],
        systemPrompt: "Expert cybersécurité. Approche défensive et éthique." },
      { domain: "development", keywords: ["code", "programmation", "javascript", "python", "java", "rust", "go", "typescript", "react", "node", "api", "sql", "debug"],
        systemPrompt: "Expert développement. Code complet de production." },
      { domain: "data_science", keywords: ["data", "machine learning", "deep learning", "neural network", "pandas", "numpy", "tensorflow", "pytorch"],
        systemPrompt: "Expert data science." },
      { domain: "productivity", keywords: ["tâche", "agenda", "calendrier", "rappel", "réunion", "rendez-vous", "emploi du temps", "planifier"],
        systemPrompt: "Assistant productivité. Propose des structures claires, priorisées." },
      { domain: "general", keywords: [], systemPrompt: "Assistant polyvalent." }
    ];
  }
  analyzeDomain(message) {
    const lower = String(message).toLowerCase();
    let best = this.domainPatterns[this.domainPatterns.length - 1], bestScore = 0;
    for (const p of this.domainPatterns) {
      if (p.domain === "general") continue;
      let s = 0;
      for (const k of p.keywords) if (lower.includes(k)) s++;
      if (s > bestScore) { bestScore = s; best = p; }
    }
    return best;
  }
  buildSystemPrompt(message, basePrompt, conversationContext = "", memorySummary = "") {
    const domain = this.analyzeDomain(message);
    const formattingRules = [
      "FORMATAGE STRICT OBLIGATOIRE :",
      "- Écris TOUJOURS un grand titre en gras (**Titre**) en début de réponse.",
      "- Utilise des sous-titres distincts (## ou ###) pour structurer.",
      "- Aère le contenu : sauts de ligne entre sections, listes à puces pour énumérer.",
      "- Code : blocs Markdown ```lang ... ```",
      "- Maths : LaTeX ($...$ inline, $$...$$ display)",
      "- Cite TOUJOURS tes sources à la fin si tu utilises le web.",
      "- Réponds de manière hiérarchisée : Titre → Sous-titre → Sections → Détails.",
      "- Utilise le contexte conversationnel pour rester cohérent."
    ].join("\n");
    const webCodeRules = [
      "RÈGLES CODE :",
      "- HTML : bloc ```html```",
      "- CSS : commentaires /* */ uniquement",
      "- JS : bloc ```javascript```",
      "- Code complet, jamais tronqué."
    ].join("\n");

    let contextSection = "";
    if (memorySummary) {
      contextSection += `\n\nRÉSUMÉ DE LA CONVERSATION PRÉCÉDENTE :\n${memorySummary}\n`;
    }
    if (conversationContext) {
      contextSection += `\n\nHISTORIQUE RÉCENT :\n${conversationContext}\n\nUtilise ce contexte pour comprendre les références.`;
    }

    return {
      role: "system",
      content: basePrompt + `\n\nDOMAINE : ${domain.domain.toUpperCase()}\n${domain.systemPrompt}\n\n${formattingRules}\n\n${webCodeRules}${contextSection}`
    };
  }
}
const dynamicContextManager = new DynamicContextManager();

// ==================== SYSTEM PROMPT ====================
const LUBA_BASE_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), intelligence artificielle créée par HIKLON Technology (Kinshasa, 2026).",
  "Tu es un véritable assistant personnel façon Jarvis : chaleureux, proactif, capable d'automatiser des tâches réelles.",
  "",
  "IDENTITÉ :",
  "- Nom : Luba.ia",
  "- Créateur : HIKLON Technology, Kinshasa, 2026.",
  "- Ton : chaleureux, précis, proactif.",
  "",
  "MÉMOIRE :",
  "- Tu te souviens TOUJOURS du contexte conversationnel.",
  "- Si l'utilisateur dit 'il', 'elle', 'ça', 'le', 'la', réfère-toi au dernier sujet évoqué.",
  "",
  "DONNÉES (OBLIGATOIRE) :",
  "- N'invente JAMAIS un score, une actu, une météo, un résultat.",
  "- Utilise les outils pour les données réelles.",
  "- Si un outil échoue, dis-le honnêtement.",
  "",
  "IMAGES :",
  "- Dès que tu décris une personnalité, un lieu, un objet remarquable, utilise search_images.",
  "- Formate en Markdown : ![description](url)",
  "",
  "SUGGESTIONS :",
  "- Le champ suggestions contient TOUJOURS 3 à 4 questions de suivi pertinentes.",
  "",
  "CODE :",
  "- Ne génère JAMAIS de code spontanément. Uniquement si demandé explicitement.",
  "",
  "MATHÉMATIQUES :",
  "- Utilise le résultat exact du moteur mathématique. Format LaTeX.",
  "",
  "ACTIONS JARVIS :",
  "- Si l'utilisateur demande de jouer une vidéo, créer une tâche, planifier un rendez-vous, noter, envoyer un email/WhatsApp,",
  "  tu peux répondre normalement : l'orchestrateur d'actions s'occupe d'exécuter et de renvoyer une carte interactive.",
  "- Quand tu détectes une intention d'action, tu peux dire 'Je lance ça tout de suite.'",
  "",
  "FORMAT DE RÉPONSE (JSON strict) :",
  "{",
  '  "replyText": "**Titre**\\n\\n## Sous-titre\\n\\nContenu aéré en Markdown...",',
  '  "toolCalls": [],',
  '  "suggestions": ["...", "...", "..."]',
  "}",
  "",
  "OUTILS DISPONIBLES :",
  "- search_images, search_web, search_news, search_sports_scores, search_science, search_social",
  "- get_weather, execute_math, send_email, send_whatsapp_message"
].join("\n");

// ==================== LLM PROVIDERS ====================
const LLM_PROVIDERS = {
  GROQ: {
    baseURL: "https://api.groq.com/openai/v1",
    apiKey: process.env.GROQ_API_KEY || "",
    timeout: CONFIG.DEFAULT_TIMEOUT, maxTokens: 4000, temperature: 0.7,
    circuitBreaker: new CircuitBreaker("groq")
  },
  OPENROUTER: {
    baseURL: "https://openrouter.ai/api/v1",
    apiKey: process.env.OPENROUTER_API_KEY || "",
    timeout: 60000, maxTokens: 4000, temperature: 0.7,
    circuitBreaker: new CircuitBreaker("openrouter")
  }
};

const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    providers: [
      { provider: "groq", model: process.env.GROQ_MODEL_V100 || "llama-3.3-70b-versatile", maxTokens: 4000, timeout: 45000, temperature: 0.7, jsonMode: true, failoverPriority: 0 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "qwen/qwen-2.5-coder-32b-instruct:free", maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 1 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free", maxTokens: 4000, timeout: 60000, temperature: 0.7, jsonMode: true, failoverPriority: 2 }
    ]
  },
  v250: {
    name: "Ngandu",
    reasoning: {
      providers: [
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_REASONING || "deepseek/deepseek-r1:free", maxTokens: 8000, timeout: 90000, temperature: 0.3, jsonMode: false, failoverPriority: 0 },
        { provider: "groq", model: process.env.GROQ_MODEL_V250_REASONING_FALLBACK || "llama-3.3-70b-versatile", maxTokens: 6000, timeout: 45000, temperature: 0.3, jsonMode: false, failoverPriority: 2 }
      ]
    },
    code: {
      providers: [
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_CODE || "qwen/qwen-2.5-coder-32b-instruct:free", maxTokens: 8000, timeout: 90000, temperature: 0.5, jsonMode: true, failoverPriority: 0 },
        { provider: "groq", model: process.env.GROQ_MODEL_V250_CODE_FALLBACK || "llama-3.3-70b-versatile", maxTokens: 8000, timeout: 45000, temperature: 0.5, jsonMode: true, failoverPriority: 1 }
      ]
    },
    maxRetries: CONFIG.MAX_RETRY_ATTEMPTS, degradedMode: true
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
  const knownPrefixes = ["openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", "anthropic/", "google/", "mistralai/", "cohere/"];
  const isOpenRouter = knownPrefixes.some(p => model.includes(p));
  if (isOpenRouter && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) return model + ":free";
  return model;
}

class LLMErrorInterceptor {
  static isRetryableError(e) {
    const s = e.response?.status;
    const retryable = [408, 429, 500, 502, 503, 504];
    const isTimeout = ["ECONNABORTED", "ETIMEDOUT", "ESOCKETTIMEDOUT"].includes(e.code) || /timeout/i.test(e.message || "");
    const isNet = ["ENOTFOUND", "ECONNRESET", "ECONNREFUSED", "EAI_AGAIN"].includes(e.code);
    return retryable.includes(s) || isTimeout || isNet;
  }
  static getErrorCode(e) {
    if (e?.response?.status) return "HTTP_" + e.response.status;
    if (e?.code === "ECONNABORTED") return "TIMEOUT";
    if (e?.code === "ENOTFOUND") return "DNS_ERROR";
    if (e?.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    if (e?.code === "MISSING_API_KEY") return "MISSING_API_KEY";
    return "UNKNOWN_ERROR";
  }
  static shouldSkipProvider(e) {
    return ["HTTP_402", "HTTP_404", "MISSING_API_KEY"].includes(this.getErrorCode(e));
  }
}

async function executeWithRetryAndFallback(providerList, promptParams, options = {}) {
  const {
    maxRetriesPerProvider = CONFIG.MAX_RETRY_ATTEMPTS,
    baseDelayMs = CONFIG.RETRY_BASE_DELAY_MS,
    maxDelayMs = CONFIG.RETRY_MAX_DELAY_MS,
    timeoutMultiplier = 1.5,
    onProviderFail = null, onProviderSuccess = null,
    enableCircuitBreaker = true,
    conversationId = null, canonicalUid = null, tier = "v100"
  } = options;

  let lastError = null;
  const providerResults = [];
  const sorted = [...providerList].sort((a, b) => a.failoverPriority - b.failoverPriority);

  for (const providerConfig of sorted) {
    const provider = providerConfig.provider;
    const info = LLM_PROVIDERS[provider.toUpperCase()];
    if (!info || !info.apiKey) continue;
    let model = providerConfig.model;
    if (provider === "openrouter") {
      model = validateAndSanitizeOpenRouterModel(model);
      if (!model) continue;
    }

    for (let attempt = 0; attempt < maxRetriesPerProvider; attempt++) {
      const start = Date.now();
      try {
        const timeout = providerConfig.timeout * (attempt > 0 ? timeoutMultiplier : 1);
        const callFn = async () => callProviderRaw({
          provider, model, messages: promptParams.messages,
          jsonMode: providerConfig.jsonMode, timeout,
          maxTokens: providerConfig.maxTokens, temperature: providerConfig.temperature,
          images: promptParams.images || null
        });

        let result;
        if (enableCircuitBreaker && info.circuitBreaker) result = await info.circuitBreaker.execute(callFn);
        else result = await callFn();

        const latencyMs = Date.now() - start;
        const pr = { providerUsed: provider, modelUsed: model, providerPriority: providerConfig.failoverPriority, attempts: attempt + 1, response: result, latencyMs };
        providerResults.push(pr);
        if (conversationId) await auditLLMCall({ conversationId, canonicalUid, provider, model, tier, latencyMs, status: "success" });
        if (onProviderSuccess) onProviderSuccess(pr);
        return { success: true, ...pr, providerChain: providerResults };
      } catch (e) {
        lastError = e;
        const errorCode = LLMErrorInterceptor.getErrorCode(e);
        const latencyMs = Date.now() - start;
        if (conversationId) await auditLLMCall({ conversationId, canonicalUid, provider, model, tier, latencyMs, status: "failed", errorCode });
        if (onProviderFail) onProviderFail({ provider, model, errorCode, errorMessage: e.message, attempt: attempt + 1 });
        if (LLMErrorInterceptor.shouldSkipProvider(e)) break;
        if (LLMErrorInterceptor.isRetryableError(e) && attempt < maxRetriesPerProvider - 1) {
          await new Promise(r => setTimeout(r, Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs)));
        } else if (!LLMErrorInterceptor.isRetryableError(e)) break;
      }
    }
  }
  return { success: false, error: lastError, providerChain: providerResults, errorCode: LLMErrorInterceptor.getErrorCode(lastError) };
}

async function callProviderRaw({ provider, model, messages, jsonMode, timeout, maxTokens, temperature = 0.7, images = null }) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ : LLM_PROVIDERS.OPENROUTER;
  if (!cfg.apiKey) { const e = new Error("Clé API manquante: " + provider); e.code = "MISSING_API_KEY"; throw e; }

  let formattedMessages = messages;
  if (images && images.length > 0) {
    const li = messages.length - 1;
    if (messages[li]?.role === "user") {
      const parts = [];
      if (typeof messages[li].content === "string") parts.push({ type: "text", text: messages[li].content });
      for (const img of images) parts.push({ type: "image_url", image_url: { url: img.dataUrl } });
      formattedMessages = [...messages.slice(0, li), { role: "user", content: parts }];
    }
  }

  const payload = { model, messages: formattedMessages, temperature, max_tokens: maxTokens || cfg.maxTokens };
  if (jsonMode) payload.response_format = { type: "json_object" };

  const headers = { Authorization: "Bearer " + cfg.apiKey, "Content-Type": "application/json" };
  if (provider === "openrouter") {
    headers["HTTP-Referer"] = HOSTING_CONFIG.domain;
    headers["X-Title"] = "Luba.ia Assistant";
  }

  const r = await axios.post(cfg.baseURL + "/chat/completions", payload, { headers, timeout: timeout || cfg.timeout });
  const content = r?.data?.choices?.[0]?.message?.content;
  if (!content) throw new Error("Réponse " + provider + " vide");
  if (!jsonMode) return content;
  try { return JSON.parse(content); }
  catch { throw new Error(`JSON invalide de ${provider}`); }
}

// ==================== CHANTIER 5 : MÉMOIRE CONVERSATIONNELLE ====================
/**
 * Charge l'historique complet d'une conversation (par canonical_uid + conversation_id).
 * Retourne les messages en ordre chronologique.
 */
async function getConversationHistory(conversationId, canonicalUid, limit = CONFIG.MAX_CONTEXT_MESSAGES) {
  try {
    const rows = await dbAll(
      `SELECT role, content, images, action_cards, created_at FROM messages
       WHERE conversation_id = ? AND canonical_uid = ?
       ORDER BY id DESC LIMIT ?`,
      [conversationId, canonicalUid, limit]
    );
    if (rows && rows.length > 0) {
      return rows.reverse().map(r => ({
        role: r.role,
        content: r.content,
        images: safeJsonParse(r.images, []),
        actionCards: safeJsonParse(r.action_cards, []),
        createdAt: r.created_at
      }));
    }

    if (supabase) {
      try {
        const { data, error } = await supabase.from("messages")
          .select("role, content, created_at")
          .eq("conversation_id", conversationId)
          .order("created_at", { ascending: false })
          .limit(limit);
        if (data && !error && data.length) {
          return data.reverse().map(r => ({ role: r.role, content: r.content, createdAt: r.created_at }));
        }
      } catch (e) { logger.warn({ err: e.message }, "Supabase history"); }
    }
    return [];
  } catch (e) { logger.error({ err: e.message }, "getConversationHistory"); return []; }
}

/**
 * Construit un contexte texte à partir de l'historique.
 */
function buildConversationContext(history) {
  if (!history?.length) return "";
  return history.map(m => {
    const who = m.role === "user" ? "Utilisateur" : m.role === "assistant" ? "Luba" : "Système";
    const text = String(m.content || "").replace(/\s+/g, " ").slice(0, 600);
    return `${who}: ${text}`;
  }).join("\n");
}

/**
 * Résumé glissant : génère un résumé court des messages plus anciens que la fenêtre.
 * Stocké dans conversations.summary.
 */
async function maybeRefreshSummary(conversationId, canonicalUid) {
  try {
    const total = await dbGet(`SELECT COUNT(*) as c FROM messages WHERE conversation_id = ?`, [conversationId]);
    if (!total || total.c < CONFIG.SUMMARY_TRIGGER_AT) return null;

    // Récupère les messages anciens non résumés
    const old = await dbAll(
      `SELECT role, content FROM messages
       WHERE conversation_id = ? AND canonical_uid = ?
       ORDER BY id ASC LIMIT 40`,
      [conversationId, canonicalUid]
    );
    if (!old.length) return null;

    const convo = old.map(m => `${m.role}: ${m.content.slice(0, 400)}`).join("\n");

    // Appel LLM léger pour résumer
    try {
      const summaryResp = await callProviderRaw({
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "llama-3.3-70b-versatile",
        messages: [
          { role: "system", content: "Résume la conversation suivante en 5 points clés, en français, factuel, sans commentaire." },
          { role: "user", content: convo }
        ],
        jsonMode: false,
        timeout: 20000, maxTokens: 500, temperature: 0.3
      });
      const summary = String(summaryResp || "").slice(0, 1500);
      await dbRun(`UPDATE conversations SET summary = ?, updated_at = CURRENT_TIMESTAMP WHERE conversation_id = ?`, [summary, conversationId]);
      return summary;
    } catch (e) {
      logger.warn({ err: e.message }, "Résumé LLM échoué");
      return null;
    }
  } catch (e) { logger.warn({ err: e.message }, "maybeRefreshSummary"); return null; }
}

function safeJsonParse(str, def) { try { return str ? JSON.parse(str) : def; } catch { return def; } }

// ==================== CHANTIER 4 : PERSISTANCE ATOMIQUE + SYNC QUEUE ====================
/**
 * Sauvegarde un message dans SQLite puis enfile la sync Supabase.
 * Ne bloque jamais la réponse utilisateur.
 */
async function persistMessage({ conversationId, canonicalUid, role, content, images = [], actionCards = [], metadata = {} }) {
  await dbRun(
    `INSERT INTO messages (conversation_id, canonical_uid, role, content, images, action_cards, metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [conversationId, canonicalUid, role, content, JSON.stringify(images), JSON.stringify(actionCards), JSON.stringify(metadata)]
  );

  // Met à jour la conversation
  await dbRun(
    `UPDATE conversations SET
       last_message_preview = ?,
       message_count = message_count + 1,
       updated_at = CURRENT_TIMESTAMP
     WHERE conversation_id = ?`,
    [String(content).slice(0, 200), conversationId]
  );

  // Enqueue sync Supabase (best effort, non bloquant)
  await enqueueSync({
    entity_type: "message",
    entity_id: `${conversationId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`,
    operation: "insert",
    payload: { conversation_id: conversationId, canonical_uid: canonicalUid, role, content, images, action_cards: actionCards, metadata, created_at: new Date().toISOString() }
  });
}

async function enqueueSync(item) {
  if (!supabase) return;
  try {
    await dbRun(
      `INSERT INTO sync_queue (entity_type, entity_id, operation, payload, status) VALUES (?, ?, ?, ?, 'pending')`,
      [item.entity_type, item.entity_id, item.operation, JSON.stringify(item.payload)]
    );
  } catch (e) { logger.warn({ err: e.message }, "enqueueSync"); }
}

/**
 * Worker de synchronisation SQLite -> Supabase.
 */
async function runSyncWorker() {
  if (!supabase) return;
  const BATCH = 20;
  try {
    const rows = await dbAll(`SELECT * FROM sync_queue WHERE status = 'pending' AND attempts < 5 ORDER BY id ASC LIMIT ?`, [BATCH]);
    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);
        if (row.entity_type === "message") {
          const { error } = await supabase.from("messages").insert(payload);
          if (error) throw new Error(error.message);
        } else if (row.entity_type === "conversation") {
          const { error } = await supabase.from("conversations").upsert(payload, { onConflict: "conversation_id" });
          if (error) throw new Error(error.message);
        } else if (row.entity_type === "user") {
          const { error } = await supabase.from("users").upsert(payload, { onConflict: "canonical_uid" });
          if (error) throw new Error(error.message);
        }
        await dbRun(`UPDATE sync_queue SET status = 'done', updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [row.id]);
      } catch (e) {
        await dbRun(
          `UPDATE sync_queue SET attempts = attempts + 1, last_error = ?, status = CASE WHEN attempts >= 4 THEN 'failed' ELSE 'pending' END, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
          [String(e.message).slice(0, 500), row.id]
        );
      }
    }
  } catch (e) { logger.error({ err: e.message }, "runSyncWorker"); }
}
setInterval(runSyncWorker, 5000);

async function syncUserToSupabase(canonicalUid, identity) {
  if (!supabase) return;
  try {
    const existing = await dbGet("SELECT * FROM users WHERE canonical_uid = ?", [canonicalUid]);
    const payload = {
      canonical_uid: canonicalUid,
      firebase_uid: canonicalUid,
      email: identity.email || existing?.email || null,
      display_name: identity.displayName || existing?.display_name || canonicalUid,
      role: identity.role || existing?.role || 'FREE',
      last_seen_at: new Date().toISOString()
    };
    const { error } = await supabase.from("users").upsert(payload, { onConflict: "canonical_uid" });
    if (error) throw new Error(error.message);
  } catch (e) { logger.warn({ err: e.message }, "syncUserToSupabase"); }
}

// ==================== GESTION CONVERSATIONS ====================
async function ensureConversation(conversationId, canonicalUid, title = null) {
  const existing = await dbGet(`SELECT * FROM conversations WHERE conversation_id = ?`, [conversationId]);
  if (existing) {
    if (existing.canonical_uid !== canonicalUid) {
      const err = new Error("Cette conversation n'appartient pas à cet utilisateur.");
      err.code = "CONVERSATION_OWNERSHIP";
      throw err;
    }
    await dbRun(`UPDATE conversations SET updated_at = CURRENT_TIMESTAMP WHERE conversation_id = ?`, [conversationId]);
    return existing;
  }
  await dbRun(
    `INSERT INTO conversations (conversation_id, canonical_uid, title) VALUES (?, ?, ?)`,
    [conversationId, canonicalUid, title]
  );
  await enqueueSync({
    entity_type: "conversation",
    entity_id: conversationId,
    operation: "insert",
    payload: { conversation_id: conversationId, canonical_uid: canonicalUid, title, created_at: new Date().toISOString(), updated_at: new Date().toISOString() }
  });
  return await dbGet(`SELECT * FROM conversations WHERE conversation_id = ?`, [conversationId]);
}

// ==================== WHATSAPP ====================
function toPlainWhatsAppText(md) {
  return String(md)
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[!\[.*?\]\(.*?\)\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)")
    .replace(/\n{3,}/g, "\n\n").trim();
}

class BaileysManager {
  constructor() { this.sessions = new Map(); }

  async initClient(canonicalUid) {
    const existing = this.sessions.get(canonicalUid);
    if (existing?.ready) return { connected: true, qrCode: null };
    if (existing?.qrCode) return { connected: false, qrCode: existing.qrCode };

    const authDir = path.join(CONFIG.SESSIONS_PATH, canonicalUid);
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    let version;
    try { version = (await fetchLatestBaileysVersion()).version; } catch {}

    const sock = makeWASocket({
      version, auth: state, logger: pino({ level: "silent" }),
      printQRInTerminal: false, browser: ["Luba.ia", "Chrome", "1.0.0"]
    });

    const sessionData = { sock, qrCode: null, ready: false };
    this.sessions.set(canonicalUid, sessionData);

    sock.ev.on("creds.update", async () => { await saveCreds(); });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;
      if (qr) {
        try { sessionData.qrCode = await qrcode.toDataURL(qr, { width: 600, margin: 2 }); } catch {}
      }
      if (connection === "open") {
        sessionData.ready = true; sessionData.qrCode = null;
        db.run("UPDATE users SET whatsapp_connected = 1 WHERE canonical_uid = ?", [canonicalUid]);
      }
      if (connection === "close") {
        sessionData.ready = false;
        db.run("UPDATE users SET whatsapp_connected = 0 WHERE canonical_uid = ?", [canonicalUid]);
        const code = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = code !== DisconnectReason.loggedOut;
        this.sessions.delete(canonicalUid);
        if (shouldReconnect) setTimeout(() => this.initClient(canonicalUid).catch(() => {}), CONFIG.WHATSAPP_RETRY_DELAY);
      }
    });

    sock.ev.on("messages.upsert", async ({ messages: msgs, type }) => {
      if (type !== "notify") return;
      for (const msg of msgs) {
        try {
          if (!msg.message || msg.key.fromMe) continue;
          const jid = msg.key.remoteJid;
          if (!jid || jid.endsWith("@g.us")) continue;
          const text = msg.message.conversation || msg.message.extendedTextMessage?.text || msg.message.imageMessage?.caption || null;
          if (!text) continue;

          const phoneNumber = jid.replace(/@.*$/, "");
          const conversationId = `whatsapp_${phoneNumber}`;
          // UID canonique = "whatsapp_" + numéro (utilisateur externe)
          const externalUid = `whatsapp_${phoneNumber}`;

          // On garantit l'existence de l'utilisateur externe
          await dbRun(`INSERT OR IGNORE INTO users (canonical_uid, firebase_uid, display_name) VALUES (?, NULL, ?)`,
            [externalUid, `WhatsApp ${phoneNumber}`]);

          const result = await handleChat({
            conversationId, canonicalUid: externalUid, message: text.slice(0, CONFIG.MAX_MESSAGE_LENGTH),
            channel: "whatsapp", modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(jid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (e) { logger.error({ err: e.message }, "WA message"); }
      }
    });

    return { connected: false, qrCode: null };
  }

  async sendMessage(canonicalUid, to, message) {
    const session = this.sessions.get(canonicalUid);
    if (!session || !session.ready) { const e = new Error("WhatsApp non connecté"); e.code = "WHATSAPP_NOT_CONNECTED"; throw e; }
    const clean = String(to).replace(/[^\d]/g, "");
    if (!clean) { const e = new Error("Numéro invalide"); e.code = "INVALID_RECIPIENT"; throw e; }
    await session.sock.sendMessage(`${clean}@s.whatsapp.net`, { text: message });
    return { success: true, to: clean };
  }

  getQRCode(canonicalUid) { return this.sessions.get(canonicalUid)?.qrCode || null; }

  async destroyAll() {
    for (const [, s] of this.sessions) { try { s.sock.end(undefined); } catch {} }
  }
}
const whatsappManager = new BaileysManager();

queueManager.createQueue("whatsapp-outbound", async (job) => {
  const data = job?.data ?? job;
  await whatsappManager.sendMessage(data.canonicalUid, data.phoneNumber, data.message);
}, { concurrency: 3, limiter: { max: 10, duration: 1000 } });

async function sendWhatsAppSmart(canonicalUid, phoneNumber, message) {
  const q = await checkUserQuota(canonicalUid, 'whatsapp');
  if (!q.allowed) throw new Error(q.message || "Limite WhatsApp atteinte");
  await queueManager.add("whatsapp-outbound", { canonicalUid, phoneNumber, message }, { attempts: 5, backoffDelay: 2000 });
  await incrementUserQuota(canonicalUid, 'whatsapp');
  return { success: true, queued: true };
}

// ==================== EMAIL ====================
async function sendEmailViaGmail(accessToken, recipient, subject, body) {
  const lines = [
    `To: ${recipient}`,
    `Subject: =?utf-8?B?${Buffer.from(subject || "(sans sujet)").toString("base64")}?=`,
    "MIME-Version: 1.0", "Content-Type: text/html; charset=utf-8", "", body || ""
  ];
  const raw = lines.join("\r\n");
  const encoded = Buffer.from(raw).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const r = await axios.post("https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    { raw: encoded }, { headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" }, timeout: 15000 });
  return { success: true, provider: "gmail", messageId: r.data?.id || null };
}

async function sendEmailViaResend(recipient, subject, body) {
  const key = process.env.RESEND_API_KEY;
  if (!key) return { success: false, error: "RESEND_API_KEY non configurée" };
  try {
    const r = await axios.post("https://api.resend.com/emails",
      { from: process.env.RESEND_FROM || "Luba <onboarding@resend.dev>", to: recipient, subject: subject || "(sans sujet)", html: `<div style="font-family:Arial;padding:20px">${String(body || "").replace(/\n/g, "<br>")}</div>` },
      { headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" }, timeout: 15000 });
    return { success: true, provider: "resend", messageId: r.data?.id || null };
  } catch (e) { return { success: false, error: `Resend: ${e.response?.data?.message || e.message}` }; }
}

async function sendEmailViaSMTP(to, subject, body) {
  if (!emailTransporter) return { success: false, error: "SMTP non configuré" };
  try {
    const info = await emailTransporter.sendMail({
      from: process.env.EMAIL_FROM || `"Luba" <${process.env.SMTP_USER}>`,
      to, subject: subject || "(sans sujet)",
      html: `<div style="font-family:Arial;padding:20px">${String(body || "").replace(/\n/g, "<br>")}</div>`,
      text: body || ""
    });
    return { success: true, provider: "smtp", messageId: info.messageId };
  } catch (e) { return { success: false, error: e.message }; }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body, canonicalUid }) {
  const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!recipient || !re.test(String(recipient).trim())) return { success: false, error: "Email destinataire invalide" };

  let result;
  if (googleAccessToken) {
    try { result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body); }
    catch (e) { result = { success: false, error: `Gmail API: ${e.message}` }; }
  } else if (process.env.RESEND_API_KEY) result = await sendEmailViaResend(recipient, subject, body);
  else result = await sendEmailViaSMTP(recipient, subject, body);

  try {
    await dbRun(`INSERT INTO email_logs (canonical_uid, to_email, subject, status, provider, error_message) VALUES (?, ?, ?, ?, ?, ?)`,
      [canonicalUid || null, recipient, subject || null, result.success ? "sent" : "failed", result.provider || null, result.error || null]);
  } catch (e) { logger.warn({ err: e.message }, "email log"); }
  return result;
}

// ==================== TOOL EXECUTOR ====================
async function executeTool(toolName, args = {}, context = {}) {
  const { canonicalUid, googleAccessToken } = context;
  let result; let sourceKeys = [];

  switch (toolName) {
    case "search_images":
    case "search_image": {
      result = await searchWikimediaImages(args.query);
      if (result.images?.length) sourceKeys.push("wikimediacommons");
      break;
    }
    case "search_web": result = await searchWeb(args.query); sourceKeys = result.sourcesUsed || []; break;
    case "search_news": result = await searchNews(args.query); if (result.articles?.length) sourceKeys.push("googlenews"); break;
    case "search_sports_scores": result = await searchSportsScores(args.query || args.team); if (result.events?.length) sourceKeys.push("thesportsdb"); break;
    case "search_science": result = await searchScience(args.query); if (result.papers?.length) sourceKeys.push("arxiv"); break;
    case "search_social": result = await searchSocial(args.query); if (result.posts?.length) sourceKeys.push("reddit"); break;
    case "get_weather": result = await getWeather(args.location || args.query); if (!result.error) sourceKeys.push("openmeteo"); break;
    case "execute_math":
    case "calculate": result = executeMathExpression(args.expression || args.query); break;
    case "send_email": result = await dispatchSendEmail({ googleAccessToken, recipient: args.recipient || args.to, subject: args.subject, body: args.body, canonicalUid }); break;
    case "send_whatsapp_message":
    case "send_whatsapp": result = await sendWhatsAppSmart(canonicalUid, args.phone_number || args.to, args.message); break;
    default: result = { success: false, error: "Outil inconnu : " + toolName };
  }
  return { result, sourceKeys };
}

// ==================== INTENT / ENRICHISSEMENT ====================
function analyzeIntent(message) {
  const lower = String(message).toLowerCase();

  // Actions Jarvis
  const action = detectAgentAction(message);
  if (action.isAction) return "ACTION";

  const hasMath = [/(\d+[\d\s\*\+\-\/\(\)\.]+\d+)/, /(?:calcule|résous|equation|équation)/i, /(?:intégrale|dérivée|factorielle|matrice|limite)/i, /[\^]\d/].some(p => p.test(message));
  if (hasMath) return "MATHS";

  const newsWords = ["actualité", "actualites", "news", "dernières nouvelles", "journal", "politique", "économie", "international"];
  if (newsWords.some(w => lower.includes(w))) return "ACTUALITÉ";

  const sportWords = ["sport", "match", "football", "basket", "tennis", "score", "résultat", "classement", "nba", "psg"];
  if (sportWords.some(w => lower.includes(w))) return "SPORT";

  const codeWords = ["code", "programmation", "javascript", "python", "java", "typescript", "react", "node", "api", "debug", "fonction", "sql"];
  if (codeWords.some(w => lower.includes(w))) return "CODE";

  const classified = classifier.classify(lower);
  return ["MATHS", "ACTUALITÉ", "SPORT", "CODE", "ACTION"].includes(classified) ? classified : "GENERAL";
}

async function enrichContextWithIntent(intent, userMessage) {
  const out = { contextData: "", toolCalls: [], sourceKeys: [] };

  if (intent === "MATHS") {
    const exprs = detectMathExpressions(userMessage);
    if (exprs.length) {
      for (const e of exprs.slice(0, 3)) {
        const r = executeMathExpression(e);
        if (r.success) out.contextData += `\n[Calcul exact] ${e} = ${r.formatted}\n`;
      }
      out.toolCalls.push({ name: "execute_math", arguments: { expression: exprs[0] } });
    }
  } else if (intent === "ACTUALITÉ") {
    const n = await searchNews(userMessage);
    if (n.articles?.length) {
      out.contextData += "\n[ACTUALITÉS RÉCENTES]\n";
      n.articles.slice(0, 5).forEach((a, i) => {
        out.contextData += `${i + 1}. ${a.title} (${a.pubDate})\n   ${a.link}\n\n`;
      });
      out.sourceKeys.push("googlenews");
    }
  } else if (intent === "SPORT") {
    const s = await searchSportsScores(userMessage);
    if (s.events?.length) {
      out.contextData += `\n[RÉSULTATS SPORTIFS - ${s.team || ""}]\n`;
      s.events.forEach(e => out.contextData += `${e.match} (${e.date})\n`);
      out.sourceKeys.push("thesportsdb");
    }
  }
  return out;
}

// ==================== LLM CALLERS ====================
async function callLLM_v100(messages, images = null, conversationId = null, canonicalUid = null, conversationContext = "", memorySummary = "") {
  const lastUser = [...messages].reverse().find(m => m.role === "user");
  const userText = typeof lastUser?.content === "string" ? lastUser.content : "";
  const sysPrompt = dynamicContextManager.buildSystemPrompt(userText, LUBA_BASE_SYSTEM_PROMPT, conversationContext, memorySummary);
  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.v100.providers,
    { messages: [sysPrompt, ...messages], images },
    { sessionId: conversationId, conversationId, canonicalUid, tier: "v100" }
  );
  if (result.success) return { ...result.response, providerUsed: result.providerUsed, modelUsed: result.modelUsed, degraded: result.providerPriority > 0 };
  throw new Error("Échec v100");
}

async function callLLM_v250(messages, userMessage, images = null, conversationId = null, canonicalUid = null, conversationContext = "", memorySummary = "") {
  const tier = MODEL_TIERS.v250;
  const chain = [];
  const sysPrompt = dynamicContextManager.buildSystemPrompt(userMessage, LUBA_BASE_SYSTEM_PROMPT, conversationContext, memorySummary);

  const reasoningMessages = [{ role: "system", content: sysPrompt.content + "\n\nAnalyse ce problème en profondeur." }, ...messages];
  const r1 = await executeWithRetryAndFallback(tier.reasoning.providers, { messages: reasoningMessages, images }, { maxRetriesPerProvider: tier.maxRetries, conversationId, canonicalUid, tier: "v250_reasoning" });
  if (!r1.success || !r1.response || r1.response.trim().length < 40) return await degradedToV100(messages, "reasoning_failed", images, conversationContext, memorySummary);
  chain.push("R1:" + r1.providerUsed + "/" + r1.modelUsed);

  const directive = [
    "Réponds au format JSON strict :", "{",
    '  "replyText": "**Titre**\\n\\n## Sous-titre\\n\\nContenu Markdown aéré...",',
    '  "toolCalls": [],', '  "suggestions": ["...", "...", "..."]', "}",
    "Structure la réponse : Grand titre en gras, sous-titres distincts, sections aérées.",
    "Code : blocs ```lang```. Maths : LaTeX $...$."
  ].join("\n");

  const codeMessages = [
    { role: "system", content: "Génère la réponse finale complète basée sur ce plan :\n\n" + r1.response + "\n\n" + directive },
    { role: "user", content: userMessage }
  ];
  const r2 = await executeWithRetryAndFallback(tier.code.providers, { messages: codeMessages, images }, { maxRetriesPerProvider: tier.maxRetries, conversationId, canonicalUid, tier: "v250_code" });
  if (!r2.success || !r2.response) return await degradedToV100(messages, "code_failed", images, conversationContext, memorySummary);
  chain.push("R2:" + r2.providerUsed + "/" + r2.modelUsed);

  return { ...r2.response, providerUsed: "pipeline_v250", modelUsed: chain.join(" -> "), degraded: false, providerChain: chain };
}

async function callVisionModel(messages, images, conversationId = null, canonicalUid = null, conversationContext = "", memorySummary = "") {
  const r = await executeWithRetryAndFallback(MODEL_TIERS.vision.providers, { messages, images }, { maxRetriesPerProvider: 2, conversationId, canonicalUid, tier: "vision" });
  if (r.success) return { ...r.response, providerUsed: r.providerUsed, modelUsed: r.modelUsed, visionEnabled: true };
  return await callLLM_v100(messages, null, conversationId, canonicalUid, conversationContext, memorySummary);
}

async function degradedToV100(messages, reason, images, ctx, summary) {
  try {
    const fb = await callLLM_v100(messages, images, null, null, ctx, summary);
    return { ...fb, providerUsed: "v250_degraded_to_v100", modelUsed: fb.providerUsed + "/" + fb.modelUsed, degraded: true, degradationReason: reason };
  } catch {
    return {
      replyText: "**Service momentanément indisponible**\n\nJe rencontre des difficultés techniques. Veuillez réessayer dans quelques instants.",
      toolCalls: [], suggestions: ["Réessayer ?", "Comment fonctionne Luba.ia ?", "Quels services sont disponibles ?"],
      providerUsed: "error_graceful_degradation", modelUsed: "none", degraded: true, error: true
    };
  }
}

// ==================== HANDLE CHAT (V13, UID UNIFIÉ) ====================
async function handleChat({ conversationId, canonicalUid, message, googleAccessToken = null, channel = "web", modelTier = "v100", images = null }) {
  // 1. S'assure que la conversation existe et appartient bien à canonicalUid
  const conv = await ensureConversation(conversationId, canonicalUid, String(message).slice(0, 60));

  // 2. Charge l'historique AVANT le nouveau message
  const history = await getConversationHistory(conversationId, canonicalUid, CONFIG.MAX_CONTEXT_MESSAGES);
  const conversationContext = buildConversationContext(history);

  // 3. Résumé glissant
  let memorySummary = conv.summary || "";
  if (history.length >= CONFIG.SUMMARY_TRIGGER_AT) {
    const fresh = await maybeRefreshSummary(conversationId, canonicalUid);
    if (fresh) memorySummary = fresh;
  }

  // 4. Persiste le message utilisateur
  await persistMessage({ conversationId, canonicalUid, role: "user", content: message, images: (images || []).map(i => i.dataUrl) });

  // 5. Détection d'intention (inclut ACTION)
  const intent = analyzeIntent(message);

  // 6. Phase Agent Jarvis : détecte et exécute une action si besoin
  const actionCards = [];
  if (intent === "ACTION") {
    const detected = detectAgentAction(message);
    if (detected.isAction) {
      const card = await agentOrchestrator.execute(detected.actionType, detected.params, { canonicalUid, conversationId, message });
      actionCards.push(card);
    }
  }

  // 7. Enrichissement classique (math, news, sport)
  const enrichment = await enrichContextWithIntent(intent, message);
  const usedSources = new Set(enrichment.sourceKeys);

  // 8. Construction des messages LLM
  let messages = [];
  const contextHistory = history.map(h => ({ role: h.role, content: h.content }));
  if (enrichment.contextData) {
    messages = [...contextHistory, {
      role: "user",
      content: message + "\n\n[CONTEXTE ENRICHI — NE PAS CITER TEL QUEL]\n" + enrichment.contextData
    }];
  } else {
    messages = [...contextHistory, { role: "user", content: message }];
  }

  // 9. Appel LLM
  let finalResponse = "";
  let imageUrls = [];
  let providerUsed = "unknown";
  let suggestions = [];
  let degraded = false;

  try {
    if (images && images.length > 0) {
      const v = await callVisionModel(messages, images, conversationId, canonicalUid, conversationContext, memorySummary);
      finalResponse = v.replyText || "Je n'ai pas pu analyser l'image.";
      suggestions = Array.isArray(v.suggestions) ? v.suggestions.slice(0, 4) : [];
      providerUsed = v.providerUsed || "vision";
    } else if (modelTier === "v250") {
      const r = await callLLM_v250(messages, message, null, conversationId, canonicalUid, conversationContext, memorySummary);
      finalResponse = r.replyText || "Je n'ai pas pu générer une réponse.";
      suggestions = Array.isArray(r.suggestions) ? r.suggestions.slice(0, 4) : [];
      providerUsed = r.providerUsed || "pipeline_v250";
      degraded = r.degraded || false;
    } else {
      let keep = true, loops = 5;
      while (keep && loops > 0) {
        loops--;
        let llmResponse;
        try {
          llmResponse = await callLLM_v100(messages, null, conversationId, canonicalUid, conversationContext, memorySummary);
          providerUsed = llmResponse.providerUsed;
          degraded = llmResponse.degraded || false;
        } catch (e) {
          finalResponse = "**Indisponible**\n\nJe suis momentanément indisponible. Réessayez dans un instant.";
          suggestions = ["Réessayer ?", "Comment fonctionne Luba.ia ?", "Que peux-tu faire ?"];
          providerUsed = "error_graceful_degradation"; degraded = true; break;
        }

        const allToolCalls = [...(llmResponse.toolCalls || []), ...enrichment.toolCalls];
        if (allToolCalls.length > 0) {
          for (const tc of allToolCalls) {
            let toolResult;
            try {
              const { result, sourceKeys } = await executeTool(tc.name, tc.arguments || {}, { canonicalUid, googleAccessToken });
              toolResult = result;
              sourceKeys.forEach(k => usedSources.add(k));
              if ((tc.name === "search_images" || tc.name === "search_image") && toolResult.images) {
                imageUrls = imageUrls.concat(toolResult.images.map(i => i.url));
              }
            } catch (e) { toolResult = { success: false, error: e.message }; }
            messages.push({ role: "assistant", content: "Résultat outil " + tc.name + " : " + JSON.stringify(toolResult) });
          }
          messages.push({ role: "user", content: "Formule la réponse finale complète avec les résultats des outils. Structure-la hiérarchiquement (titre gras, sous-titres, sections). Propose 3-4 questions de suivi. Respecte LaTeX pour les maths." });
          enrichment.toolCalls = [];
          keep = true;
        } else {
          finalResponse = llmResponse.replyText || "Je n'ai pas pu générer de réponse.";
          suggestions = Array.isArray(llmResponse.suggestions) ? llmResponse.suggestions.slice(0, 4) : [];
          keep = false;
        }
      }
      if (!finalResponse) finalResponse = "**Erreur technique**\n\nJe rencontre des difficultés. Réessayez.";
    }

    // Injection des images markdown
    if (imageUrls.length > 0) {
      const md = imageUrls.map((u, i) => `![Illustration ${i + 1}](${u})`).join("\n\n");
      finalResponse += "\n\n---\n\n**Illustrations :**\n\n" + md;
      usedSources.add("wikimediacommons");
    }

    // Sources
    if (usedSources.size > 0) {
      const lines = Array.from(usedSources).map(k => OPEN_SOURCES[k]).filter(Boolean).map(s => `[${s.name}](${s.url})`);
      if (lines.length) finalResponse += "\n\n---\n\n**Sources :** " + lines.join(" · ");
    }

    // Persiste la réponse assistant
    await persistMessage({
      conversationId, canonicalUid, role: "assistant", content: finalResponse,
      images: imageUrls, actionCards, metadata: { providerUsed, intent, degraded }
    });

    // Formatage structuré pour le frontend
    const formatted = ResponseFormatter.format(finalResponse, {
      sources: Array.from(usedSources).map(k => OPEN_SOURCES[k]).filter(Boolean),
      actionCards,
      suggestions
    });

    return {
      reply: finalResponse,
      formatted,
      images: imageUrls,
      actionCards,
      error: providerUsed.startsWith("error"),
      providerUsed, modelTier, degraded,
      visionEnabled: Boolean(images && images.length > 0),
      suggestions,
      sources: Array.from(usedSources).map(k => OPEN_SOURCES[k]).filter(Boolean),
      intent,
      contextLength: history.length,
      memorySummaryUsed: Boolean(memorySummary),
      conversationId,
      canonicalUid
    };
  } catch (e) {
    logger.error({ err: e.message }, "handleChat critical");
    const fallback = "**Service momentanément indisponible**\n\nNos équipes techniques travaillent à résoudre le problème.";
    try { await persistMessage({ conversationId, canonicalUid, role: "assistant", content: fallback }); } catch {}
    return {
      reply: fallback,
      formatted: ResponseFormatter.format(fallback, { suggestions: ["Réessayer ?", "Que peux-tu faire ?", "Comment fonctionne Luba.ia ?"] }),
      images: [], actionCards: [], error: true, providerUsed: "error_critical",
      modelTier, degraded: true, suggestions: ["Réessayer ?", "Que peux-tu faire ?", "Comment fonctionne Luba.ia ?"],
      sources: [], intent, conversationId, canonicalUid
    };
  }
}

// ==================== EXPRESS ====================
const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

app.use(cors({
  origin: (origin, cb) => {
    if (!origin || HOSTING_CONFIG.allowedOrigins.includes(origin)) cb(null, true);
    else cb(new Error("Origine non autorisée"));
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token", "X-Session-Token"],
  credentials: true, maxAge: 86400
}));

app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://apis.google.com", "https://www.gstatic.com"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://fonts.googleapis.com"],
      imgSrc: ["'self'", "data:", "blob:", "https://*", "http://*"],
      connectSrc: ["'self'", "https://api.groq.com", "https://openrouter.ai", "https://*.firebaseio.com", "https://*.supabase.co", "wss://*.firebaseio.com", "https://www.youtube.com", "https://i.ytimg.com"],
      fontSrc: ["'self'", "https://fonts.gstatic.com", "https://cdnjs.cloudflare.com"],
      objectSrc: ["'none'"],
      frameSrc: ["https://*.firebaseapp.com", "https://*.web.app", "https://www.youtube.com", "https://www.youtube-nocookie.com"],
      workerSrc: ["'self'", "blob:"]
    }
  }
}));

app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 300, standardHeaders: true, legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ success: false, error: true, reply: "Trop de requêtes.", code: "RATE_LIMIT" })
});
const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, max: 100, standardHeaders: true, legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ success: false, error: true, reply: "Limite atteinte.", code: "RATE_LIMIT_STRICT" })
});

app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  res.on("finish", () => logger.info({ requestId, status: res.statusCode, duration: Date.now() - start }, "Réponse"));
  next();
});

// ==================== AUTH ====================
const authenticateUser = async (req, res, next) => {
  try {
    const isBlocked = await isIPBlocked(req.ip);
    if (isBlocked) return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "IP_BLOCKED" });

    const authHeader = req.headers.authorization || req.headers.Authorization;
    const bearer = authHeader && authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    if (!bearer) {
      await recordLoginAttempt(req.ip, null, false, "Token manquant");
      return res.status(401).json({ success: false, error: true, reply: "Authentification requise.", code: "MISSING_TOKEN" });
    }

    try {
      const id = await verifyFirebaseToken(bearer);
      if (!id) {
        await recordLoginAttempt(req.ip, null, false, "Token invalide");
        return res.status(401).json({ success: false, error: true, reply: "Session invalide.", code: "INVALID_TOKEN" });
      }

      // ⚠️ CHANTIER 3 : UID canonique unique
      const canonicalUid = await resolveCanonicalUser(id);
      req.canonicalUid = canonicalUid;
      req.userId = canonicalUid;   // compat
      req.firebaseUid = canonicalUid; // compat
      req.verifiedIdentity = true;
      req.userRole = id.role || 'FREE';
      req.emailVerified = id.emailVerified;
      req.userEmail = id.email;
      req.userDisplayName = id.displayName;

      await recordLoginAttempt(req.ip, canonicalUid, true);
      await logSecurityEvent(canonicalUid, 'LOGIN_SUCCESS', { email: id.email }, req.ip, req.headers['user-agent']);
      next();
    } catch (e) {
      await recordLoginAttempt(req.ip, null, false, e.message);
      const lc = await checkLoginAttempts(req.ip);
      return res.status(401).json({
        success: false, error: true,
        reply: lc.blocked ? lc.message : "Session invalide.",
        code: lc.blocked ? "IP_BLOCKED" : "INVALID_TOKEN"
      });
    }
  } catch (e) {
    logger.error({ err: e.message }, "Auth");
    return res.status(500).json({ success: false, error: true, reply: "Erreur interne.", code: "AUTH_ERROR" });
  }
};

const requireRole = (roles) => (req, res, next) => {
  if (!req.userRole || (!roles.includes(req.userRole) && req.userRole !== 'ADMIN'))
    return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "INSUFFICIENT_ROLE" });
  next();
};

// ==================== ROUTES ====================
app.get("/", (req, res) => res.json({ success: true, error: false, reply: `${CONFIG.AGENT_NAME} opérationnel`, version: CONFIG.VERSION, company: CONFIG.COMPANY }));

app.get("/api/health", async (req, res) => {
  let dbOk = true; try { await dbGet("SELECT 1"); } catch { dbOk = false; }
  res.json({
    success: dbOk, error: !dbOk,
    reply: `${CONFIG.AGENT_NAME} en bonne santé`,
    data: {
      timestamp: new Date().toISOString(), uptime: process.uptime(),
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
      database: dbOk ? "ok" : "erreur",
      supabase: Boolean(supabase),
      firebaseAuth: firebaseApp ? "admin_sdk" : "api_rest",
      version: CONFIG.VERSION,
      features: {
        vision: true, quotas: true, securityLogs: true, firebaseAdmin: Boolean(firebaseApp),
        sessionManagement: true, ipBlocking: true, nlpIntent: true, mathEngine: true,
        rssAggregation: true, webSearch: true, scraping: true,
        conversationMemory: true, uidUnified: true, responseFormatter: true,
        agentJarvis: true, syncQueue: true, imageProxy: true, structuredCards: true
      }
    }
  });
});

// ---------- CHAT ----------
app.post("/api/chat", apiLimiter, authenticateUser, upload.array("images", CONFIG.MAX_IMAGES_PER_REQUEST), async (req, res) => {
  try {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;
    const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";

    const q = await checkUserQuota(req.canonicalUid, 'message', req.userRole);
    if (!q.allowed) return res.status(429).json({ success: false, error: true, reply: q.message, code: "QUOTA_EXCEEDED" });

    if (!message || typeof message !== "string" || !message.trim())
      return res.status(400).json({ success: false, error: true, reply: "Message obligatoire.", code: "MISSING_MESSAGE" });

    if (!conversationId || typeof conversationId !== "string") {
      conversationId = generateConversationId();
      isNewConversation = true;
    }

    const googleAccessToken = req.headers["x-google-access-token"] || null;
    let images = null;
    if (req.files?.length) {
      images = req.files.map(f => convertImageToBase64(f.buffer, f.mimetype));
      await incrementUserQuota(req.canonicalUid, 'image');
    }

    const result = await handleChat({
      conversationId,
      canonicalUid: req.canonicalUid,
      message: message.trim(),
      googleAccessToken, channel: "web", modelTier, images
    });

    await incrementUserQuota(req.canonicalUid, 'message');
    return res.status(200).json({ ...result, conversationId, isNewConversation });
  } catch (e) {
    logger.error({ err: e.message }, "POST /api/chat");
    return res.status(500).json({ success: false, error: true, reply: "Erreur interne.", code: "CHAT_ERROR" });
  }
});

// ---------- HISTORIQUE ----------
app.get("/api/conversation/:conversationId/messages", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { conversationId } = req.params;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    const conv = await dbGet("SELECT canonical_uid FROM conversations WHERE conversation_id = ?", [conversationId]);
    if (conv && conv.canonical_uid !== req.canonicalUid)
      return res.status(403).json({ success: false, error: true, code: "CONVERSATION_OWNERSHIP" });

    const rows = await dbAll(
      `SELECT id, role, content, images, action_cards, created_at FROM messages
       WHERE conversation_id = ? AND canonical_uid = ? ORDER BY id ASC LIMIT ?`,
      [conversationId, req.canonicalUid, CONFIG.MAX_HISTORY_LENGTH]
    );
    const messages = rows.map(r => ({
      id: r.id, role: r.role, content: r.content,
      images: safeJsonParse(r.images, []),
      actionCards: safeJsonParse(r.action_cards, []),
      createdAt: r.created_at
    }));
    return res.status(200).json({ success: true, error: false, conversationId, messages, count: messages.length });
  } catch (e) {
    logger.error({ err: e.message }, "GET history");
    return res.status(500).json({ success: false, error: true, code: "HISTORY_FETCH_ERROR" });
  }
});

app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT c.*, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.conversation_id) as msg_count
       FROM conversations c WHERE c.canonical_uid = ? AND c.archived = 0
       ORDER BY c.updated_at DESC LIMIT 50`,
      [req.canonicalUid]
    );
    const conversations = rows.map(c => ({
      conversationId: c.conversation_id,
      title: c.title || "Nouvelle conversation",
      summary: c.summary || null,
      lastMessagePreview: c.last_message_preview,
      messageCount: c.msg_count,
      pinned: Boolean(c.pinned),
      createdAt: c.created_at,
      updatedAt: c.updated_at
    }));
    return res.status(200).json({ success: true, error: false, conversations });
  } catch (e) {
    logger.error({ err: e.message }, "GET conversations");
    return res.status(500).json({ success: false, error: true, conversations: [] });
  }
});

// ⚠️ CHANTIER 4 : endpoint dédié aux discussions récentes (dashboard)
app.get("/api/conversations/recent", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || "20", 10), 50);
    const rows = await dbAll(
      `SELECT c.*, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.conversation_id) as msg_count
       FROM conversations c WHERE c.canonical_uid = ?
       ORDER BY c.updated_at DESC LIMIT ?`,
      [req.canonicalUid, limit]
    );
    const recent = await Promise.all(rows.map(async c => {
      const last = await dbGet(
        `SELECT role, content, created_at FROM messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 1`,
        [c.conversation_id]
      );
      return {
        conversationId: c.conversation_id,
        title: c.title || "Nouvelle conversation",
        summary: c.summary || null,
        messageCount: c.msg_count,
        lastMessage: last ? { role: last.role, preview: String(last.content).slice(0, 140), createdAt: last.created_at } : null,
        updatedAt: c.updated_at
      };
    }));
    return res.status(200).json({ success: true, error: false, conversations: recent });
  } catch (e) {
    logger.error({ err: e.message }, "GET recent");
    return res.status(500).json({ success: false, error: true, code: "RECENT_FETCH_ERROR" });
  }
});

// ---------- CHANTIER 2 : PROXY IMAGES ----------
app.get("/api/images/proxy", async (req, res) => {
  try {
    const targetUrl = req.query.url;
    if (!targetUrl) return res.status(400).json({ success: false, error: true, code: "MISSING_URL" });

    // Whitelist : uniquement les domaines d'images autorisés
    const urlObj = new URL(targetUrl);
    const allowedHosts = ["upload.wikimedia.org", "commons.wikimedia.org", "i.ytimg.com", "img.youtube.com"];
    if (!allowedHosts.some(h => urlObj.hostname === h || urlObj.hostname.endsWith("." + h))) {
      return res.status(403).json({ success: false, error: true, code: "HOST_NOT_ALLOWED" });
    }

    const resp = await axios.get(targetUrl, {
      responseType: "stream", timeout: 15000,
      headers: { "User-Agent": CONFIG.HTTP_USER_AGENT, "Referer": "https://commons.wikimedia.org/" }
    });

    res.setHeader("Content-Type", resp.headers["content-type"] || "image/jpeg");
    res.setHeader("Cache-Control", "public, max-age=86400");
    res.setHeader("Access-Control-Allow-Origin", "*");
    resp.data.pipe(res);
  } catch (e) {
    logger.warn({ err: e.message }, "Image proxy");
    return res.status(502).json({ success: false, error: true, code: "IMAGE_PROXY_ERROR" });
  }
});

// ---------- USER ----------
app.get("/api/user/stats", authenticateUser, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const q = await dbGet(`SELECT * FROM user_quotas WHERE canonical_uid = ? AND date = ?`, [req.canonicalUid, today]);
    const tasksCount = await dbGet(`SELECT COUNT(*) as c FROM agent_tasks WHERE canonical_uid = ? AND status = 'pending'`, [req.canonicalUid]);
    const eventsCount = await dbGet(`SELECT COUNT(*) as c FROM agent_calendar WHERE canonical_uid = ? AND starts_at >= datetime('now')`, [req.canonicalUid]);
    res.json({
      success: true, error: false,
      data: {
        quotas: q || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0, actions_count: 0 },
        role: req.userRole || 'FREE',
        limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
        canonicalUid: req.canonicalUid,
        agent: { pendingTasks: tasksCount?.c || 0, upcomingEvents: eventsCount?.c || 0 }
      }
    });
  } catch (e) { return res.status(500).json({ success: false, error: true, code: "STATS_ERROR" }); }
});

// ---------- SESSIONS ----------
app.post("/api/session/create", authenticateUser, async (req, res) => {
  try {
    const t = await createActiveSession(req.canonicalUid, req.ip, req.headers['user-agent']);
    return res.json({ success: true, error: false, data: { sessionToken: t, expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString() } });
  } catch { return res.status(500).json({ success: false, error: true, code: "SESSION_CREATE_ERROR" }); }
});

app.post("/api/session/revoke", authenticateUser, async (req, res) => {
  try {
    const { sessionToken } = req.body;
    if (sessionToken) await revokeSession(req.canonicalUid, sessionToken);
    else await revokeAllSessions(req.canonicalUid);
    return res.json({ success: true, error: false, message: "Session(s) révoquée(s)." });
  } catch { return res.status(500).json({ success: false, error: true, code: "SESSION_REVOKE_ERROR" }); }
});

app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(['ADMIN']), async (req, res) => {
  try {
    const { uid, role } = req.body;
    if (!uid || !['FREE', 'PREMIUM', 'ADMIN'].includes(role))
      return res.status(400).json({ success: false, error: true, code: "INVALID_PARAMS" });
    return res.json({ success: true, error: false, data: await setUserRole(uid, role) });
  } catch { return res.status(500).json({ success: false, error: true, code: "ROLE_UPDATE_ERROR" }); }
});

// ---------- TOOLS ----------
app.post("/api/tools", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const toolName = req.body.toolName || req.body.action;
    const params = req.body.params || req.body.arguments || req.body.data || {};
    if (!toolName) return res.status(400).json({ success: false, error: true, code: "MISSING_TOOL_NAME" });
    const googleAccessToken = req.headers["x-google-access-token"] || null;
    const { result, sourceKeys } = await executeTool(toolName, params, { canonicalUid: req.canonicalUid, googleAccessToken });
    const sources = sourceKeys.map(k => OPEN_SOURCES[k]).filter(Boolean);
    return res.json({ success: true, error: false, toolName, result, sources });
  } catch { return res.status(500).json({ success: false, error: true, code: "TOOL_ERROR" }); }
});

// ---------- CHANTIER 6 : API AGENT ----------
app.post("/api/agent/action", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { actionType, params = {}, conversationId = null } = req.body;
    if (!actionType) return res.status(400).json({ success: false, error: true, code: "MISSING_ACTION_TYPE" });

    const q = await checkUserQuota(req.canonicalUid, 'action', req.userRole);
    if (!q.allowed) return res.status(429).json({ success: false, error: true, reply: q.message, code: "ACTION_QUOTA_EXCEEDED" });

    const card = await agentOrchestrator.execute(actionType, params, {
      canonicalUid: req.canonicalUid, conversationId, message: params.message || ""
    });
    await incrementUserQuota(req.canonicalUid, 'action');
    return res.json({ success: true, error: false, card });
  } catch (e) {
    logger.error({ err: e.message }, "POST /api/agent/action");
    return res.status(500).json({ success: false, error: true, code: "AGENT_ACTION_ERROR" });
  }
});

app.get("/api/agent/tasks", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const status = req.query.status || "pending";
    const tasks = await agentOrchestrator.listTasks(req.canonicalUid, status);
    return res.json({ success: true, error: false, tasks });
  } catch { return res.status(500).json({ success: false, error: true, code: "TASKS_FETCH_ERROR" }); }
});

app.post("/api/agent/tasks/:uid/complete", apiLimiter, authenticateUser, async (req, res) => {
  try {
    await agentOrchestrator.completeTask(req.canonicalUid, req.params.uid);
    return res.json({ success: true, error: false });
  } catch { return res.status(500).json({ success: false, error: true, code: "TASK_COMPLETE_ERROR" }); }
});

app.get("/api/agent/calendar", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const fromNow = req.query.from_now !== "false";
    const events = await agentOrchestrator.listCalendarEvents(req.canonicalUid, fromNow);
    return res.json({ success: true, error: false, events });
  } catch { return res.status(500).json({ success: false, error: true, code: "CALENDAR_FETCH_ERROR" }); }
});

app.get("/api/agent/notes", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const notes = await agentOrchestrator.listNotes(req.canonicalUid);
    return res.json({ success: true, error: false, notes });
  } catch { return res.status(500).json({ success: false, error: true, code: "NOTES_FETCH_ERROR" }); }
});

app.post("/api/agent/youtube/search", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { query, limit = 5 } = req.body;
    if (!query) return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    const results = await agentOrchestrator.searchYouTube(query, limit);
    return res.json({ success: true, error: false, results });
  } catch { return res.status(500).json({ success: false, error: true, code: "YT_SEARCH_ERROR" }); }
});

// ---------- WHATSAPP ----------
app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const result = await whatsappManager.initClient(req.canonicalUid);
    if (result.connected) return res.json({ success: true, error: false, message: "Déjà connecté.", data: { qrCode: null } });

    let qr = null;
    const start = Date.now();
    while (!qr && Date.now() - start < CONFIG.WHATSAPP_QR_TIMEOUT) {
      await new Promise(r => setTimeout(r, 500));
      qr = whatsappManager.getQRCode(req.canonicalUid);
    }
    if (qr) return res.json({ success: true, error: false, message: "Connexion initiée", data: { qrCode: qr } });
    return res.status(408).json({ success: false, error: true, message: "Délai dépassé.", code: "QR_TIMEOUT" });
  } catch { return res.status(500).json({ success: false, error: true, code: "WHATSAPP_CONNECT_ERROR" }); }
});

app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    if (!req.body.to || !req.body.message) return res.status(400).json({ success: false, error: true, code: "MISSING_PARAMS" });
    const q = await checkUserQuota(req.canonicalUid, 'whatsapp', req.userRole);
    if (!q.allowed) return res.status(429).json({ success: false, error: true, code: "WHATSAPP_QUOTA_EXCEEDED" });
    const result = await whatsappManager.sendMessage(req.canonicalUid, req.body.to, req.body.message);
    await incrementUserQuota(req.canonicalUid, 'whatsapp');
    return res.json({ success: true, error: false, data: result });
  } catch { return res.status(500).json({ success: false, error: true, code: "WHATSAPP_SEND_ERROR" }); }
});

// ---------- MEMORY ----------
app.post("/api/memory/clear", authenticateUser, async (req, res) => {
  try {
    const conversationId = req.body.conversationId || req.body.conversation_id;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    await dbRun("DELETE FROM messages WHERE conversation_id = ? AND canonical_uid = ?", [conversationId, req.canonicalUid]);
    await dbRun("UPDATE conversations SET summary = NULL, message_count = 0 WHERE conversation_id = ? AND canonical_uid = ?", [conversationId, req.canonicalUid]);
    return res.json({ success: true, error: false, reply: "Mémoire effacée." });
  } catch { return res.status(500).json({ success: false, error: true, code: "MEMORY_CLEAR_ERROR" }); }
});

// ---------- RGPD ----------
app.delete("/api/account", authenticateUser, async (req, res) => {
  try {
    const uid = req.canonicalUid;
    try {
      const s = whatsappManager.sessions.get(uid);
      if (s?.sock) s.sock.end(undefined);
      whatsappManager.sessions.delete(uid);
      const dir = path.join(CONFIG.SESSIONS_PATH, uid);
      if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
    } catch {}

    await dbRun(`DELETE FROM messages WHERE conversation_id IN (SELECT conversation_id FROM conversations WHERE canonical_uid = ?)`, [uid]);
    await dbRun(`DELETE FROM conversations WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM agent_tasks WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM agent_calendar WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM agent_notes WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM agent_actions WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM email_logs WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM llm_audit_log WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM security_logs WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM user_quotas WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM active_sessions WHERE canonical_uid = ?`, [uid]);
    await dbRun(`DELETE FROM users WHERE canonical_uid = ?`, [uid]);

    if (supabase) {
      try {
        await supabase.from("messages").delete().eq("canonical_uid", uid);
        await supabase.from("conversations").delete().eq("canonical_uid", uid);
        await supabase.from("users").delete().eq("canonical_uid", uid);
      } catch {}
    }

    let firebaseDeleted = false;
    if (firebaseApp && firebaseAdmin) {
      try { await firebaseAdmin.auth(firebaseApp).deleteUser(uid); firebaseDeleted = true; } catch {}
    }

    return res.json({ success: true, error: false, message: "Compte supprimé.", firebaseAccountDeleted: firebaseDeleted });
  } catch { return res.status(500).json({ success: false, error: true, code: "ACCOUNT_DELETION_ERROR" }); }
});

// ---------- 404 ----------
app.use((req, res) => res.status(404).json({
  success: false, error: true, reply: "Route non trouvée", code: "NOT_FOUND"
}));

app.use((err, req, res, next) => {
  logger.error({ err: err.message, stack: err.stack }, "Erreur non gérée");
  if (res.headersSent) return next(err);
  res.status(500).json({ success: false, error: true, reply: "Erreur interne.", code: "INTERNAL_ERROR" });
});

// ==================== DÉMARRAGE ====================
const server = app.listen(CONFIG.PORT, () => {
  logger.info(`${CONFIG.AGENT_NAME} v${CONFIG.VERSION} sur le port ${CONFIG.PORT}`);
  console.log(`🚀 ${CONFIG.AGENT_NAME} v${CONFIG.VERSION} — Jarvis Edition`);
  console.log(`🌐 Domaine: ${HOSTING_CONFIG.domain}`);
  console.log(`🔐 Firebase Admin: ${firebaseApp ? "activé" : "REST"}`);
  console.log(`🧠 Agent Jarvis: activé`);
  console.log(`🖼️  Proxy images: /api/images/proxy`);
  console.log(`💾 Sync queue: ${supabase ? "Supabase" : "désactivée"}`);
  console.log(`👤 UID unifié: canonicalUid`);
});

let isShuttingDown = false;
async function shutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre");
  await new Promise(r => server.close(r));
  try { await whatsappManager.destroyAll(); } catch {}
  try { await queueManager.close(); } catch {}
  await new Promise(r => db.close(() => r()));
  console.log("✅ Arrêt terminé");
  process.exit(0);
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", e => logger.error({ err: e.message, stack: e.stack }, "uncaughtException"));
process.on("unhandledRejection", r => logger.error({ reason: String(r) }, "unhandledRejection"));

module.exports = { app, db, queueManager, whatsappManager, agentOrchestrator };
