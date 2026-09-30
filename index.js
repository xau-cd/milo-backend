// ================================================================================
// LUBA BACKEND v15.0.0 — Enterprise Edition
// HIKLON TECHNOLOGIES · Kinshasa, RDC · 2026
// ================================================================================
// REFONTE v15 (voir historique de migration plus bas) :
//   ✅ Math : LaTeX brut (plus de JSON), normalizeMath, évaluateur isolé en Worker
//   ✅ Streaming SSE opt-in, timeouts stricts (AbortController partout)
//   ✅ Circuit breaker par (provider + modèle + clé), HALF_OPEN sérialisé
//   ✅ Auth : cache token 5 min, plus de checkRevoked par défaut, chemin critique allégé
//   ✅ DB : timestamps INTEGER (ms), outbox, mutex par conversation, ids UUID
//   ✅ Supabase = source de vérité, SQLite = cache + outbox ; {error} TOUJOURS vérifié
//   ✅ Intentions : tool calling natif, pré-routeur \b…\b, GENERAL par défaut
//   ✅ Images : allSettled, cache conditionnel, UA Wikimedia conforme, searchImages DDG
//   ✅ run_code : sandbox externe (Piston/Judge0/E2B), jamais d'eval/exec local
//   ✅ WhatsApp : creds complets + clés Signal, déconnexion propre, quota + liste blanche
// ================================================================================

require("dotenv").config();

// ==================== IMPORTS CORE ====================
const express        = require("express");
const cors           = require("cors");
const helmet         = require("helmet");
const rateLimit      = require("express-rate-limit");
const axios          = require("axios");
const qrcode         = require("qrcode");
const nodemailer     = require("nodemailer");
const sqlite3        = require("sqlite3").verbose();
const path           = require("path");
const fs             = require("fs");
const crypto         = require("crypto");
const pino           = require("pino");
const multer         = require("multer");
const { createClient } = require("@supabase/supabase-js");
const { EventEmitter } = require("events");
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");
const Parser         = require("rss-parser");
const FormData       = require("form-data");
const cheerio        = require("cheerio");
const { LRUCache }   = require("lru-cache");

// ==================== OPTIONNELS (non bloquants) ====================
let GoogleGenAI = null;
try { GoogleGenAI = require("@google/genai").GoogleGenAI; }
catch { console.warn("⚠️ @google/genai non installé — Gemini désactivé"); }

let firebaseAdmin = null;
try { firebaseAdmin = require("firebase-admin"); }
catch { console.warn("⚠️ firebase-admin non installé — mode REST seulement"); }

let BullMQ = null, IORedis = null;
try { BullMQ = require("bullmq"); IORedis = require("ioredis"); }
catch { console.warn("⚠️ BullMQ/Redis non installés — files en mémoire"); }

let ddgScrape = null;
try { ddgScrape = require("duck-duck-scrape"); }
catch { console.warn("⚠️ duck-duck-scrape non installé"); }

// ==================== CONFIGURATION ====================
const CONFIG = {
  PORT: parseInt(process.env.PORT || "3000", 10),
  ENV: process.env.NODE_ENV || "production",
  VERSION: "15.0.0",
  AGENT_NAME: "Luba",
  COMPANY: "HIKLON TECHNOLOGIES",

  // Tailles
  MAX_MESSAGE_LENGTH: parseInt(process.env.MAX_MESSAGE_LENGTH || "15000", 10),
  MAX_HISTORY_LENGTH: parseInt(process.env.MAX_HISTORY_LENGTH || "50", 10),
  MAX_CONTEXT_MESSAGES: parseInt(process.env.MAX_CONTEXT_MESSAGES || "20", 10),
  MAX_CONTEXT_TOKENS: parseInt(process.env.MAX_CONTEXT_TOKENS || "8000", 10),
  IMAGE_SEARCH_LIMIT: parseInt(process.env.IMAGE_SEARCH_LIMIT || "6", 10),
  MAX_IMAGE_SIZE_MB: parseInt(process.env.MAX_IMAGE_SIZE_MB || "10", 10),
  MAX_IMAGES_PER_REQUEST: parseInt(process.env.MAX_IMAGES_PER_REQUEST || "3", 10),

  // Retry / timeouts (⚠️ PHASE 1 : bornes strictes pour éviter les minutes de blocage)
  MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS || "2", 10),
  RETRY_BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS || "400", 10),
  RETRY_MAX_DELAY_MS: parseInt(process.env.RETRY_MAX_DELAY_MS || "2000", 10),
  CHAT_ATTEMPT_TIMEOUT_MS: parseInt(process.env.CHAT_ATTEMPT_TIMEOUT_MS || "10000", 10),
  CHAT_GLOBAL_TIMEOUT_MS: parseInt(process.env.CHAT_GLOBAL_TIMEOUT_MS || "40000", 10),
  V250_ROUTE_TIMEOUT_MS: parseInt(process.env.V250_ROUTE_TIMEOUT || "60000", 10),
  TOOL_TIMEOUT_MS: parseInt(process.env.TOOL_TIMEOUT_MS || "8000", 10),

  // Circuit breaker
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "4", 10),
  CIRCUIT_BREAKER_RESET_MS: parseInt(process.env.CIRCUIT_BREAKER_RESET_MS || "45000", 10),
  CIRCUIT_BREAKER_HALF_OPEN_MAX: 1, // une seule requête de test en HALF_OPEN

  // Auth
  AUTH_TOKEN_CACHE_TTL_MS: parseInt(process.env.AUTH_TOKEN_CACHE_TTL_MS || "300000", 10), // 5 min
  AUTH_CHECK_REVOKED: process.env.AUTH_CHECK_REVOKED === "true",
  MAX_LOGIN_ATTEMPTS: parseInt(process.env.MAX_LOGIN_ATTEMPTS || "20", 10),
  LOGIN_BLOCK_DURATION_MS: parseInt(process.env.LOGIN_BLOCK_DURATION || "900000", 10),
  MAX_SESSIONS_PER_USER: parseInt(process.env.MAX_SESSIONS_PER_USER || "10", 10),

  // Chemins
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  UPLOADS_PATH: path.join(__dirname, "uploads"),

  // Images
  IMAGE_CACHE_TTL_MS: parseInt(process.env.IMAGE_CACHE_TTL_MS || String(20 * 60 * 1000), 10),
  IMAGE_SOURCE_DEADLINE_MS: parseInt(process.env.IMAGE_SOURCE_DEADLINE_MS || "3500", 10),
  WIKIMEDIA_USER_AGENT:
    process.env.WIKIMEDIA_USER_AGENT ||
    `LubaAI/${"15.0.0"} (https://luba.web.app; ${process.env.CONTACT_EMAIL || "contact@luba.web.app"})`,

  // WhatsApp
  WHATSAPP_QR_TIMEOUT: parseInt(process.env.WHATSAPP_QR_TIMEOUT || "30000", 10),
  WHATSAPP_RETRY_DELAY: parseInt(process.env.WHATSAPP_RETRY_DELAY || "4000", 10),

  // Modèles vision (à valider via /api/health → modelAvailability.issues)
  VISION_MODEL_GROQ: process.env.VISION_MODEL_GROQ || "qwen/qwen3.6-27b",
  VISION_MODEL_OPENROUTER: process.env.VISION_MODEL_OPENROUTER || "inclusionai/ling-3.0-flash-vl:free",
  VISION_MODEL_GEMINI: process.env.VISION_MODEL_GEMINI || "gemini-3.6-flash",

  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],
  ALLOWED_AUDIO_TYPES: ["audio/mpeg","audio/mp4","audio/wav","audio/webm","audio/ogg","audio/m4a","audio/x-m4a","audio/aac"],
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || `LubaAI-App/${"15.0.0"}`,

  // Sandbox d'exécution de code (jamais d'eval local)
  CODE_SANDBOX_PROVIDER: process.env.CODE_SANDBOX_PROVIDER || "", // "piston" | "judge0" | "e2b" | ""
  PISTON_URL: process.env.PISTON_URL || "",
  JUDGE0_URL: process.env.JUDGE0_URL || "",
  E2B_API_KEY: process.env.E2B_API_KEY || ""
};

// ==================== FIREBASE CONFIG (aucun secret en dur) ====================
const FIREBASE_CONFIG = {
  apiKey: process.env.FIREBASE_API_KEY || null,
  projectId: process.env.FIREBASE_PROJECT_ID || "luba-ia-636",
  authDomain: process.env.FIREBASE_AUTH_DOMAIN || "luba-ia-636.firebaseapp.com",
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET || "luba-ia-636.firebasestorage.app",
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID || "502404354252",
  appId: process.env.FIREBASE_APP_ID || "1:502404354252:web:660ab2109ce448e1803269"
};

const HOSTING_CONFIG = {
  domain: process.env.HOSTING_DOMAIN || "https://luba.web.app",
  allowedOrigins: (process.env.ALLOWED_ORIGINS?.split(",").map(s => s.trim()).filter(Boolean)) || [
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
  FREE:    { maxMessagesPerDay: 100,    maxImagesPerDay: 20,    maxWhatsAppMessagesPerDay: 10,    maxEmailsPerDay: 5,    maxTokensPerRequest: 8000 },
  PREMIUM: { maxMessagesPerDay: 1000,   maxImagesPerDay: 200,   maxWhatsAppMessagesPerDay: 100,   maxEmailsPerDay: 50,   maxTokensPerRequest: 32000 },
  ADMIN:   { maxMessagesPerDay: 999999, maxImagesPerDay: 999999,maxWhatsAppMessagesPerDay: 999999,maxEmailsPerDay: 999999,maxTokensPerRequest: 128000 }
};

// ==================== DOSSIERS ====================
for (const dir of [path.dirname(CONFIG.DB_PATH), CONFIG.SESSIONS_PATH, CONFIG.UPLOADS_PATH]) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Dossier créé: ${dir}`);
  }
}

// ==================== LOGGER ====================
const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  transport: process.env.NODE_ENV === "development"
    ? { target: "pino-pretty", options: { colorize: true } }
    : undefined,
  base: { service: "luba-backend", version: CONFIG.VERSION },
  redact: {
    paths: [
      "req.headers.authorization",
      "req.headers['x-google-access-token']",
      "*.apiKey",
      "*.key",
      "*.token",
      "*.access_token",
      "*.refresh_token"
    ],
    censor: "[REDACTED]"
  }
});

// ==================== VALIDATION ENVIRONNEMENT (fail-fast) ====================
function validateEnvironment() {
  const problems = [];

  const hasFirebaseAdmin = Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const hasFirebaseRestKey = Boolean(process.env.FIREBASE_API_KEY);
  if (!hasFirebaseAdmin && !hasFirebaseRestKey) {
    problems.push("Aucune authentification Firebase configurée : définissez FIREBASE_SERVICE_ACCOUNT_JSON (recommandé) ou FIREBASE_API_KEY.");
  }

  const hasAnyLLMKey = Boolean(
    process.env.GROQ_API_KEY || process.env.OPENROUTER_API_KEY ||
    process.env.CEREBRAS_API_KEY || process.env.GEMINI_API_KEY
  );
  if (!hasAnyLLMKey) {
    problems.push("Aucune clé de provider LLM configurée (GROQ_API_KEY / OPENROUTER_API_KEY / CEREBRAS_API_KEY / GEMINI_API_KEY).");
  }

  if (CONFIG.ENV === "production") {
    const key = process.env.WHATSAPP_ENCRYPTION_KEY;
    const iv = process.env.WHATSAPP_ENCRYPTION_IV;
    if (!key || key.length < 32 || !iv || iv.length < 16) {
      problems.push("WHATSAPP_ENCRYPTION_KEY (32+) et WHATSAPP_ENCRYPTION_IV (16+) obligatoires en production.");
    }
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
      problems.push("SUPABASE_URL / SUPABASE_KEY absents : SQLite seul n'est PAS une persistance fiable en production.");
    }
  }

  if (problems.length > 0) {
    for (const p of problems) console.error("❌ CONFIG MANQUANTE : " + p);
    if (CONFIG.ENV === "production") {
      console.error("🛑 Démarrage interrompu (production).");
      process.exit(1);
    } else {
      console.warn("⚠️ Démarrage en mode dégradé.");
    }
  }
}
validateEnvironment();

// ==================== FIREBASE ADMIN INIT ====================
let firebaseApp = null;

function parseFirebaseServiceAccount(raw) {
  try { return JSON.parse(raw); }
  catch {
    try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
    catch { throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide"); }
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
  logger.warn("⚠️ Firebase Admin non initialisé — mode REST");
}

// ================================================================================
// ==================== BASE DE DONNÉES SQLITE (v2 — timestamps INTEGER) ========
// ================================================================================
// ⚠️ RÈGLE ABSOLUE v15 : tout timestamp est stocké en INTEGER = Date.now() (ms UTC).
// Cela élimine les faux négatifs de la v14 (comparaison textuelle ISO vs 'YYYY-MM-DD HH:MM:SS').
// ================================================================================

const db = new sqlite3.Database(CONFIG.DB_PATH, (err) => {
  if (err) { logger.error({ err: err.message }, "❌ Impossible d'ouvrir SQLite"); process.exit(1); }
  logger.info("✅ SQLite initialisé");
});

db.run("PRAGMA journal_mode = WAL;");
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -64000;");
db.run("PRAGMA busy_timeout = 10000;");
db.run("PRAGMA temp_store = MEMORY;");
db.run("PRAGMA foreign_keys = ON;");
db.run("PRAGMA wal_autocheckpoint = 1000;");

// ==================== SCHÉMA ====================
db.serialize(() => {
  // -------- USERS --------
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    firebase_uid TEXT UNIQUE,
    email TEXT UNIQUE,
    display_name TEXT,
    role TEXT DEFAULT 'FREE',
    email_verified INTEGER DEFAULT 0,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    last_seen_at INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);

  // -------- SESSIONS --------
  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    firebase_uid TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
    active_intent TEXT,
    intent_data TEXT,
    intent_expires_at INTEGER,
    metadata TEXT DEFAULT '{}',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    user_id TEXT,
    role TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool')),
    content TEXT NOT NULL,
    tool_calls TEXT,
    tool_call_id TEXT,
    images TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
  )`);

  // -------- INDEX --------
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_firebase ON sessions(firebase_uid)");

  // -------- AUDIT / LOGS --------
  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT, firebase_uid TEXT, to_email TEXT NOT NULL,
    subject TEXT, status TEXT DEFAULT 'pending', provider TEXT,
    error_message TEXT, created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS llm_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT, user_id TEXT, provider TEXT, model TEXT, tier TEXT,
    prompt_tokens INTEGER DEFAULT 0, completion_tokens INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0, status TEXT DEFAULT 'success', error_code TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS security_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT, event_type TEXT NOT NULL, details TEXT DEFAULT '{}',
    fingerprint TEXT, ip_address TEXT, user_agent TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);
  // ⚠️ FIX v15 : on remplace le LIKE '%fingerprint%' par une colonne indexée.
  db.run("CREATE INDEX IF NOT EXISTS idx_security_fingerprint ON security_logs(user_id, event_type, fingerprint)");
  db.run("CREATE INDEX IF NOT EXISTS idx_security_user_time ON security_logs(user_id, created_at DESC)");

  // -------- QUOTAS --------
  db.run(`CREATE TABLE IF NOT EXISTS user_quotas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL, date TEXT NOT NULL,
    messages_count INTEGER DEFAULT 0, images_count INTEGER DEFAULT 0,
    whatsapp_count INTEGER DEFAULT 0, emails_count INTEGER DEFAULT 0,
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
    UNIQUE(user_id, date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_user_quotas_user_date ON user_quotas(user_id, date)");

  // -------- SESSIONS ACTIVES --------
  db.run(`CREATE TABLE IF NOT EXISTS active_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    session_token_hash TEXT UNIQUE,
    ip_address TEXT, user_agent TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    last_activity INTEGER DEFAULT (strftime('%s','now')*1000),
    expires_at INTEGER, is_revoked INTEGER DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_active_sessions_user ON active_sessions(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_active_sessions_lookup ON active_sessions(user_id, session_token_hash, is_revoked, expires_at)");

  // -------- TENTATIVES DE LOGIN / IP BLOQUÉES --------
  db.run(`CREATE TABLE IF NOT EXISTS login_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT, ip_address TEXT, success INTEGER DEFAULT 0,
    error_message TEXT, created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_login_attempts_ip ON login_attempts(ip_address, created_at DESC)");

  db.run(`CREATE TABLE IF NOT EXISTS blocked_ips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_address TEXT UNIQUE, reason TEXT,
    strike_count INTEGER DEFAULT 1, blocked_until INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address, blocked_until)");

  // -------- MÉMOIRE LONG TERME --------
  db.run(`CREATE TABLE IF NOT EXISTS user_memory (
    user_id TEXT PRIMARY KEY, summary TEXT DEFAULT '',
    messages_since_update INTEGER DEFAULT 0,
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);

  // -------- TÂCHES (UUID côté serveur — ⚠️ FIX v15 : plus d'AUTOINCREMENT exposé) --------
  db.run(`CREATE TABLE IF NOT EXISTS user_tasks (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    title TEXT NOT NULL, notes TEXT,
    due_at INTEGER, status TEXT DEFAULT 'pending',
    notified_at INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id, status, due_at)");
  db.run("CREATE INDEX IF NOT EXISTS idx_user_tasks_due ON user_tasks(status, due_at)");

  // -------- NEWS CACHE --------
  db.run(`CREATE TABLE IF NOT EXISTS news_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL, title TEXT, link TEXT, pub_date TEXT,
    description TEXT, source TEXT, created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);

  // -------- ⚠️ OUTBOX (nouveau v15) — file d'écriture différée Supabase --------
  // Idempotence : clé (table_name, op, idempotency_key) → une seule exécution.
  db.run(`CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('insert','update','upsert','delete')),
    payload TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    next_attempt_at INTEGER DEFAULT (strftime('%s','now')*1000),
    status TEXT DEFAULT 'pending',
    created_at INTEGER DEFAULT (strftime('%s','now')*1000),
    updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
    UNIQUE(table_name, op, idempotency_key)
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(status, next_attempt_at)");

  // -------- TOKEN CACHE (⚠️ FIX v15 : cache process-local des tokens Firebase) --------
  // Évite verifyIdToken() à chaque requête (5 min de TTL).
  db.run(`CREATE TABLE IF NOT EXISTS token_cache (
    token_hash TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    email TEXT, display_name TEXT,
    email_verified INTEGER DEFAULT 0,
    role TEXT DEFAULT 'FREE',
    expires_at INTEGER NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);
  db.run("CREATE INDEX IF NOT EXISTS idx_token_cache_expiry ON token_cache(expires_at)");

  // -------- CONVERSATION MUTEX (⚠️ FIX v15 : verrou par conversation) --------
  db.run(`CREATE TABLE IF NOT EXISTS conversation_locks (
    conversation_id TEXT PRIMARY KEY,
    locked_until INTEGER NOT NULL,
    owner_request_id TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now')*1000)
  )`);
});

logger.info("✅ Schéma SQLite v2 initialisé");

// ================================================================================
// ==================== WRAPPERS DB ============================================
// ================================================================================

function dbGet(query, params = []) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => err ? reject(err) : resolve(row));
  });
}
function dbAll(query, params = []) {
  return new Promise((resolve, reject) => {
    db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows));
  });
}
function dbRun(query, params = []) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) { err ? reject(err) : resolve(this); });
  });
}
function dbExec(query) {
  return new Promise((resolve, reject) => {
    db.exec(query, (err) => err ? reject(err) : resolve());
  });
}

/**
 * Transaction atomique SQLite. Le callback reçoit les mêmes helpers dbGet/dbAll/dbRun
 * mais ils s'exécutent dans la transaction courante.
 */
async function dbTransaction(fn) {
  await dbExec("BEGIN IMMEDIATE");
  try {
    const result = await fn({ dbGet, dbAll, dbRun });
    await dbExec("COMMIT");
    return result;
  } catch (e) {
    try { await dbExec("ROLLBACK"); } catch {}
    throw e;
  }
}

// ================================================================================
// ==================== UTILITAIRES ============================================
// ================================================================================

function generateRequestId() { return `req_${crypto.randomUUID()}`; }
function generateConversationId() { return `conv_${crypto.randomUUID()}`; }
function generateSessionToken() { return `sess_${crypto.randomBytes(32).toString("hex")}`; }
function generateUUID() { return crypto.randomUUID(); }
function generateTaskId() { return `task_${crypto.randomUUID()}`; }

function sha256(input) {
  return crypto.createHash("sha256").update(String(input)).digest("hex");
}
function hashSessionToken(token) { return sha256(token); }

function nowMs() { return Date.now(); }

/** 🔒 Échappe le HTML (emails / affichage). Ne PAS utiliser sur le texte du chat. */
function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/**
 * 🧼 Nettoyage MINIMAL pour entrée LLM : ne mutile PAS le code (contrairement à v14).
 * On retire uniquement les caractères de contrôle dangereux + longueur.
 * L'échappement HTML est fait à la SORTIE (affichage/email), jamais à l'entrée.
 */
function sanitizeForLLM(input, maxLength = CONFIG.MAX_MESSAGE_LENGTH) {
  if (input === null || input === undefined) return "";
  let text = String(input);
  // Retire NUL, VERTICAL TAB, FF, et 0x0E-0x1F (garde \n \t \r)
  text = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
  // Limite la profondeur de répétition (anti-DoS par message géant)
  if (text.length > maxLength) text = text.slice(0, maxLength);
  return text.trim();
}

/** Nettoyage renforcé pour les champs structurés (nom, sujet d'email, etc.) */
function sanitizeStrict(input, maxLength = 500) {
  if (input === null || input === undefined) return "";
  let text = String(input).trim();
  // Caractères de contrôle (y compris \n)
  text = text.replace(/[\u0000-\u001F\u007F]/g, "");
  // Balises dangereuses
  text = text.replace(/<script[\s\S]*?<\/script>/gi, "");
  text = text.replace(/<iframe[\s\S]*?<\/iframe>/gi, "");
  if (text.length > maxLength) text = text.slice(0, maxLength);
  return text;
}

/**
 * 🧮 normalizeMath — ⚠️ PHASE 1 : corrige les caractères de contrôle introduits
 * par JSON.parse sur du LaTeX (\frac → U+000C, \theta → \t, etc.).
 * À appeler SYSTÉMATIQUEMENT sur la réponse LLM avant tout traitement.
 *
 * Transformations :
 *   - \( … \)   → $ … $
 *   - \[ … \]   → $$ … $$
 *   - Caractères de contrôle U+0000-U+001F (sauf \n \t) supprimés
 *   - Entités HTML (&amp;, &lt;, &gt;, &quot;, &#39;) décodées HORS blocs de code
 *   - Lignes vides multiples réduites
 */
function normalizeMath(input) {
  if (typeof input !== "string" || input.length === 0) return "";

  // Découpage par blocs de code (``` ... ```) → on ne touche pas l'intérieur
  const parts = [];
  const codeBlockRegex = /```[\s\S]*?```|`[^`\n]*`/g;
  let lastIndex = 0;
  let match;

  while ((match = codeBlockRegex.exec(input)) !== null) {
    if (match.index > lastIndex) {
      parts.push({ type: "text", content: input.slice(lastIndex, match.index) });
    }
    parts.push({ type: "code", content: match[0] });
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < input.length) {
    parts.push({ type: "text", content: input.slice(lastIndex) });
  }

  const processed = parts.map(({ type, content }) => {
    if (type === "code") return content;

    let text = content;
    // 1) Caractères de contrôle (⚠️ ceux que JSON.parse introduit dans LaTeX)
    text = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
    // 2) LaTeX : \( \) → $ $
    text = text.replace(/\\\(([\s\S]*?)\\\)/g, (_, inner) => `$${inner.trim()}$`);
    // 3) LaTeX : \[ \] → $$ $$
    text = text.replace(/\\\[([\s\S]*?)\\\]/g, (_, inner) => `$$${inner.trim()}$$`);
    // 4) Entités HTML résiduelles
    text = text
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&nbsp;/g, " ");
    // 5) Réduit les lignes vides multiples
    text = text.replace(/\n{3,}/g, "\n\n");
    return text;
  }).join("");

  return processed.trim();
}

/**
 * 🧠 Retire les balises <think>...</think> (raisonnement interne du modèle)
 * du texte destiné à l'utilisateur. Conserve éventuellement le contenu pour logs.
 */
function stripThinkTags(input) {
  if (typeof input !== "string") return { text: "", thinking: "" };
  const thinks = [];
  const text = input
    .replace(/<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/gi, (_, inner) => {
      thinks.push(inner.trim());
      return "";
    })
    .replace(/<think(?:ing)?>[\s\S]*$/i, "") // think non fermé → tronqué
    .trim();
  return { text, thinking: thinks.join("\n---\n") };
}

/** Décodage d'entités XML (RSS) */
function decodeXmlEntities(str) {
  return String(str)
    .replace(/<!\[CDATA\[/g, "").replace(/\]\]>/g, "")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, " ").trim();
}

/** Empreinte appareil (SHA-256 tronqué). */
function computeDeviceFingerprint(ip, userAgent) {
  return sha256(`${ip || "?"}::${userAgent || "?"}`).slice(0, 32);
}

/** Validation signature binaire image (anti-mimetype falsifié). */
function isValidImageSignature(buffer) {
  if (!buffer || buffer.length < 12) return false;
  const hex = buffer.subarray(0, 12).toString("hex");
  if (hex.startsWith("ffd8ff")) return true;                      // JPEG
  if (hex.startsWith("89504e470d0a1a0a")) return true;            // PNG
  if (hex.startsWith("47494638")) return true;                    // GIF
  if (hex.startsWith("52494646") &&
      buffer.subarray(8, 12).toString("ascii") === "WEBP") return true; // WEBP
  return false;
}

/** Conversion image → base64 (structure pour providers). */
function convertImageToBase64(buffer, mimetype) {
  return {
    dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`,
    base64: buffer.toString("base64"),
    mimetype,
    size: buffer.length
  };
}

/** Race avec deadline et fallback (pour appels réseau). */
function withDeadline(promise, deadlineMs, fallbackValue) {
  return Promise.race([
    promise,
    new Promise((resolve) => setTimeout(() => resolve(fallbackValue), deadlineMs).unref?.())
  ]);
}

/** allSettled avec deadline PAR PROMESSE (⚠️ PHASE 5 : ne jette plus tout si 1 traîne). */
async function allSettledWithDeadline(promises, deadlineMs) {
  const wrapped = promises.map((p) =>
    Promise.race([
      Promise.resolve(p).then(v => ({ status: "fulfilled", value: v }))
        .catch(e => ({ status: "rejected", reason: e })),
      new Promise((resolve) => setTimeout(() => resolve({ status: "timeout" }), deadlineMs).unref?.())
    ])
  );
  return Promise.all(wrapped);
}

/** Sleep simple. */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/** Backoff exponentiel avec jitter. */
function backoffDelay(attempt, base = 400, max = 8000) {
  const exp = Math.min(base * Math.pow(2, attempt), max);
  return exp + Math.floor(Math.random() * 200);
}

/** Regex email validée. */
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_REGEX = /^\+?[1-9]\d{6,14}$/; // E.164 souple

// ================================================================================
// ==================== DATES (⚠️ PHASE 2 : plus de comparaison textuelle) ======
// ================================================================================

/** Retourne le début du jour (UTC) en ms, utilisé pour les quotas. */
function todayKeyMs() {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

// ================================================================================
// ==================== ÉVALUATEUR MATH ISOLÉ (Worker) =========================
// ================================================================================
// ⚠️ PHASE 2 : math.evaluate() sur entrée utilisateur = DoS (zeros(1e9), factorielle géante).
// Solution : pool de Workers avec timeout strict + scope math restreint.

const MATH_WORKER_SOURCE = `
  const { parentPort, workerData } = require('worker_threads');
  const math = require('mathjs');

  // ⚠️ Instance restreinte : pas d'import, createUnit, parse arbitraire, eval.
  const safeMath = math.create({
    number: 'number',
    precision: 64,
    matrix: 'Matrix',
    predictable: true
  });
  // Bloque les fonctions connues pour être DoS
  safeMath.import({
    import: function () { throw new Error('import interdit'); },
    createUnit: function () { throw new Error('createUnit interdit'); },
    evaluate: function () { throw new Error('evaluate interdit dans le sandbox'); },
    parse: function () { throw new Error('parse interdit'); },
    simplify: function () { throw new Error('simplify interdit'); },
    derivative: function () { throw new Error('derivative interdit'); },
    zeros: function (n) {
      const size = Number(n);
      if (size > 10000) throw new Error('zeros trop grand');
      return math.zeros(size);
    },
    ones: function (n) {
      const size = Number(n);
      if (size > 10000) throw new Error('ones trop grand');
      return math.ones(size);
    },
    factorial: function (n) {
      const size = Number(n);
      if (size > 500) throw new Error('factorielle trop grande');
      return math.factorial(size);
    }
  }, { override: true });

  try {
    const expr = String(workerData.expression || '').slice(0, 2000);
    if (!expr) throw new Error('Expression vide');
    if (/[;]/.test(expr.replace(/\\\\(?!.)/g, ''))) {} // pas bloquant, juste info
    const result = safeMath.evaluate(expr);
    parentPort.postMessage({ ok: true, result: safeMath.format(result, { precision: 14 }) });
  } catch (e) {
    parentPort.postMessage({ ok: false, error: e.message || 'Erreur math' });
  }
`;

let mathWorkerPool = null;
function getMathWorkerPool(size = 2) {
  if (mathWorkerPool) return mathWorkerPool;
  mathWorkerPool = { size, available: [], busy: 0, queue: [] };
  return mathWorkerPool;
}

/** Exécute une expression math en Worker avec timeout strict (jamais dans le process principal). */
function evaluateMathSafe(expression, timeoutMs = 1500) {
  return new Promise((resolve) => {
    const expr = String(expression || "").slice(0, 2000);
    if (!expr.trim()) return resolve({ success: false, expression: expr, error: "Expression vide" });

    const worker = new Worker(MATH_WORKER_SOURCE, {
      eval: true,
      workerData: { expression: expr },
      resourceLimits: { maxOldGenerationSizeMb: 64, maxYoungGenerationSizeMb: 16 }
    });

    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        worker.terminate().catch(() => {});
        resolve({ success: false, expression: expr, error: "Expression trop lente (timeout)" });
      }
    }, timeoutMs);

    worker.on("message", (msg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      worker.terminate().catch(() => {});
      if (msg.ok) resolve({ success: true, expression: expr, result: msg.result, formatted: msg.result });
      else resolve({ success: false, expression: expr, error: msg.error });
    });

    worker.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ success: false, expression: expr, error: err.message });
    });

    worker.on("exit", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ success: false, expression: expr, error: `Worker sorti (code ${code})` });
    });
  });
}

/** Détection d'expressions mathématiques simples (regex, prompt-safe). */
function detectMathExpressions(message) {
  const patterns = [
    /(\d+(?:\.\d+)?(?:\s*[\+\-\*\/\^]\s*\d+(?:\.\d+)?)+)/g,
    /(?:calcule|calcul|résous|resous|solve|compute)\s*:?\s*([^\n]{1,200})/gi,
    /(?:sqrt|sin|cos|tan|log|exp|abs|floor|ceil|round)\s*\([^)]{1,100}\)/gi,
    /(?:intégrale|integrale|dérivée|derivee|factorielle|matrice|limite)\s*(?:de|of)?\s*:?\s*([^\n]{1,200})/gi
  ];
  const found = new Set();
  for (const p of patterns) {
    const matches = message.match(p);
    if (matches) matches.forEach(m => found.add(m.trim()));
  }
  return [...found];
}

// ================================================================================
// ==================== CIRCUIT BREAKER (par provider + modèle + clé) =========
// ================================================================================
// ⚠️ PHASE 1 : un 404 sur un modèle ne doit PAS ouvrir le breaker pour toute la clé.
// La clé = `${provider}:${model}:${keyLabel}`.

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold ?? CONFIG.CIRCUIT_BREAKER_THRESHOLD;
    this.resetTimeout = options.resetTimeout ?? CONFIG.CIRCUIT_BREAKER_RESET_MS;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED";
    this.halfOpenInFlight = 0;
    this.emitter = new EventEmitter();
  }

  canAttempt() {
    if (this.state === "CLOSED") return true;
    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime >= this.resetTimeout) {
        this.state = "HALF_OPEN";
        this.halfOpenInFlight = 0;
        logger.info({ circuit: this.name }, "Circuit HALF_OPEN");
        return true;
      }
      return false;
    }
    // HALF_OPEN : une seule requête de test à la fois
    if (this.halfOpenInFlight >= CONFIG.CIRCUIT_BREAKER_HALF_OPEN_MAX) return false;
    return true;
  }

  async execute(fn) {
    if (!this.canAttempt()) {
      const err = new Error(`Circuit ${this.name} OUVERT`);
      err.code = "CIRCUIT_OPEN";
      throw err;
    }
    const isHalfOpen = this.state === "HALF_OPEN";
    if (isHalfOpen) this.halfOpenInFlight++;

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    } finally {
      if (isHalfOpen) this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
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
    if (this.state === "HALF_OPEN" || this.failureCount >= this.failureThreshold) {
      this.state = "OPEN";
      this.emitter.emit("open", { name: this.name, failureCount: this.failureCount });
      logger.warn({ circuit: this.name, failures: this.failureCount }, "Circuit OPEN");
    }
    this.emitter.emit("failure", { name: this.name, failureCount: this.failureCount });
  }
}

/** Registre global des breakers (clé = provider:model:keyLabel). */
const circuitRegistry = new Map();
function getCircuit(provider, model, keyLabel) {
  const key = `${provider}:${model}:${keyLabel}`;
  let cb = circuitRegistry.get(key);
  if (!cb) {
    cb = new CircuitBreaker(key);
    circuitRegistry.set(key, cb);
  }
  return cb;
}

// ================================================================================
// ==================== CLIENT SUPABASE + OUTBOX ================================
// ================================================================================
// ⚠️ PHASE 3 : supabase-js ne LÈVE PAS, il renvoie {error}. TOUS les appels DOIVENT
// vérifier et loguer l'erreur. Les échecs sont mis en outbox pour rejeu.

let supabase = null;
if (process.env.SUPABASE_URL && process.env.SUPABASE_KEY) {
  supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: "public" },
    global: { headers: { "x-application-name": "luba-backend-v15" } }
  });
  logger.info("✅ Supabase initialisé (source de vérité)");
} else {
  logger.warn("⚠️ Supabase non configuré — SQLite local uniquement");
}

/**
 * 📤 Enfile une opération Supabase échouée pour rejeu avec backoff.
 * Idempotence : upsert sur clé stable OU clé composite (table, op, idempotency_key).
 */
async function enqueueOutbox(tableName, op, payload, idempotencyKey) {
  try {
    await dbRun(
      `INSERT INTO outbox (table_name, op, payload, idempotency_key, attempts, next_attempt_at, status)
       VALUES (?, ?, ?, ?, 0, ?, 'pending')
       ON CONFLICT(table_name, op, idempotency_key) DO UPDATE SET
         payload = excluded.payload,
         next_attempt_at = excluded.next_attempt_at,
         status = 'pending',
         updated_at = ?`,
      [tableName, op, JSON.stringify(payload), idempotencyKey, Date.now(), Date.now()]
    );
  } catch (e) {
    logger.error({ err: e.message, tableName, op }, "Erreur enqueue outbox");
  }
}

/**
 * Écrit dans Supabase. En cas d'erreur → outbox. Idempotency_key DOIT être stable
 * (par ex. `user_id:${id}:task_id:${taskId}`).
 */
async function supabaseWriteSafe({ table, op, payload, idempotencyKey, matchColumn = null, matchValue = null }) {
  if (!supabase) return { success: false, reason: "no_supabase" };
  try {
    let result;
    if (op === "insert")       result = await supabase.from(table).insert(payload);
    else if (op === "upsert")  result = await supabase.from(table).upsert(payload, { onConflict: matchColumn, ignoreDuplicates: false });
    else if (op === "update")  result = await supabase.from(table).update(payload).eq(matchColumn, matchValue);
    else if (op === "delete")  result = await supabase.from(table).delete().eq(matchColumn, matchValue);
    else return { success: false, reason: "unknown_op" };

    if (result.error) {
      logger.warn({ err: result.error.message, table, op }, "Supabase write failed → outbox");
      await enqueueOutbox(table, op, payload, idempotencyKey);
      return { success: false, error: result.error };
    }
    return { success: true };
  } catch (e) {
    logger.warn({ err: e.message, table, op }, "Supabase write exception → outbox");
    await enqueueOutbox(table, op, payload, idempotencyKey);
    return { success: false, error: e };
  }
}

/** Boucle de rejeu de l'outbox (backoff exponentiel, max 5 minutes entre tentatives). */
const OUTBOX_MAX_ATTEMPTS = 12;
const OUTBOX_BATCH_SIZE = 20;
async function processOutbox() {
  if (!supabase) return;
  let rows = [];
  try {
    rows = await dbAll(
      `SELECT * FROM outbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY next_attempt_at ASC LIMIT ?`,
      [Date.now(), OUTBOX_BATCH_SIZE]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur lecture outbox"); return; }

  for (const row of rows) {
    let payload;
    try { payload = JSON.parse(row.payload); }
    catch { await markOutboxDead(row.id, "payload JSON invalide"); continue; }

    try {
      let result;
      if (row.op === "insert")      result = await supabase.from(row.table_name).insert(payload);
      else if (row.op === "upsert") result = await supabase.from(row.table_name).upsert(payload, { ignoreDuplicates: false });
      else if (row.op === "update") result = await supabase.from(row.table_name).update(payload.data || payload).eq(payload.matchColumn, payload.matchValue);
      else if (row.op === "delete") result = await supabase.from(row.table_name).delete().eq(payload.matchColumn, payload.matchValue);

      if (result?.error) throw new Error(result.error.message);

      await dbRun(`UPDATE outbox SET status = 'done', updated_at = ? WHERE id = ?`, [Date.now(), row.id]);
    } catch (e) {
      const attempts = row.attempts + 1;
      if (attempts >= OUTBOX_MAX_ATTEMPTS) {
        await markOutboxDead(row.id, e.message);
      } else {
        const delay = Math.min(500 * Math.pow(2, attempts), 5 * 60 * 1000);
        await dbRun(
          `UPDATE outbox SET attempts = ?, last_error = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?`,
          [attempts, String(e.message).slice(0, 500), Date.now() + delay, Date.now(), row.id]
        );
      }
    }
  }
}

async function markOutboxDead(id, error) {
  await dbRun(
    `UPDATE outbox SET status = 'dead', last_error = ?, updated_at = ? WHERE id = ?`,
    [String(error).slice(0, 500), Date.now(), id]
  );
  logger.error({ outboxId: id, error }, "Outbox : opération abandonnée");
}

setInterval(processOutbox, 15000).unref?.();

// ================================================================================
// ==================== CACHE TOKEN AUTH (⚠️ PHASE 1 : 5 min TTL) ===============
// ================================================================================

const tokenCache = new LRUCache({
  max: 5000,
  ttl: CONFIG.AUTH_TOKEN_CACHE_TTL_MS,
  updateAgeOnGet: false
});

function cacheGetToken(token) {
  return tokenCache.get(sha256(token)) || null;
}
function cacheSetToken(token, user) {
  tokenCache.set(sha256(token), user);
}

// ================================================================================
// ==================== VÉRIFICATION FIREBASE ===================================
// ================================================================================

async function verifyFirebaseToken(token, { checkRevoked = CONFIG.AUTH_CHECK_REVOKED } = {}) {
  // 1) Cache mémoire (5 min) — évite verifyIdToken à chaque requête
  const cached = cacheGetToken(token);
  if (cached) return cached;

  // 2) Admin SDK
  if (firebaseApp && firebaseAdmin) {
    try {
      const decoded = await firebaseAdmin.auth(firebaseApp).verifyIdToken(token, checkRevoked);
      const user = {
        uid: decoded.uid,
        email: decoded.email || null,
        displayName: decoded.name || null,
        photoURL: decoded.picture || null,
        emailVerified: decoded.email_verified || false,
        role: decoded.role || "FREE",
        customClaims: decoded
      };
      cacheSetToken(token, user);
      return user;
    } catch (error) {
      logger.warn({ err: error.message, code: error.code }, "Échec vérif Firebase Admin");
      throw error;
    }
  }

  // 3) Repli REST
  if (!FIREBASE_CONFIG.apiKey) {
    const e = new Error("Aucune configuration Firebase disponible");
    e.code = "auth/configuration-not-found";
    throw e;
  }
  try {
    const response = await axios.post(
      `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${FIREBASE_CONFIG.apiKey}`,
      { idToken: token },
      { timeout: 8000 }
    );
    if (response.data.users && response.data.users.length > 0) {
      const u = response.data.users[0];
      const user = {
        uid: u.localId,
        email: u.email || null,
        displayName: u.displayName || null,
        photoURL: u.photoUrl || null,
        emailVerified: u.emailVerified || false,
        role: "FREE"
      };
      cacheSetToken(token, user);
      return user;
    }
    return null;
  } catch (error) {
    logger.warn({ err: error.message }, "Échec vérif Firebase REST");
    throw error;
  }
}

// ================================================================================
// ==================== LOGS SÉCURITÉ / AUDIT ===================================
// ================================================================================

async function logSecurityEvent(userId, eventType, details = {}, ipAddress = null, userAgent = null, fingerprint = null) {
  try {
    await dbRun(
      `INSERT INTO security_logs (user_id, event_type, details, fingerprint, ip_address, user_agent, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [userId, eventType, JSON.stringify(details), fingerprint, ipAddress, userAgent, Date.now()]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur log sécurité"); }
}

async function auditLLMCall({ sessionId, userId, provider, model, tier, promptTokens = 0, completionTokens = 0, latencyMs = 0, status, errorCode = null }) {
  try {
    await dbRun(
      `INSERT INTO llm_audit_log (session_id, user_id, provider, model, tier, prompt_tokens, completion_tokens, latency_ms, status, error_code, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [sessionId, userId, provider, model, tier, promptTokens, completionTokens, latencyMs, status, errorCode, Date.now()]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur audit LLM"); }
}

/**
 * Détection nouvel appareil (⚠️ PHASE 1 : plus de LIKE '%..%' → colonne fingerprint indexée).
 * Fire-and-forget, jamais bloquant.
 */
function detectAndLogNewDeviceAsync(userId, ip, userAgent) {
  setImmediate(async () => {
    try {
      const fingerprint = computeDeviceFingerprint(ip, userAgent);
      const existing = await dbGet(
        `SELECT id FROM security_logs WHERE user_id = ? AND event_type = 'DEVICE_SEEN' AND fingerprint = ? LIMIT 1`,
        [userId, fingerprint]
      );
      if (!existing) {
        await logSecurityEvent(userId, "NEW_DEVICE_DETECTED", { fingerprint }, ip, userAgent, fingerprint);
      }
      await logSecurityEvent(userId, "DEVICE_SEEN", {}, ip, userAgent, fingerprint);
    } catch (e) { logger.error({ err: e.message }, "Erreur détection appareil"); }
  });
}

// ================================================================================
// ==================== SESSIONS ACTIVES (Tokens hashés) ========================
// ================================================================================

async function createActiveSession(userId, ipAddress, userAgent) {
  const sessionToken = generateSessionToken();
  const expiresAt = Date.now() + 24 * 60 * 60 * 1000;

  const active = await dbGet(
    `SELECT COUNT(*) AS count FROM active_sessions WHERE user_id = ? AND is_revoked = 0 AND expires_at > ?`,
    [userId, Date.now()]
  );
  if ((active?.count || 0) >= CONFIG.MAX_SESSIONS_PER_USER) {
    // Révoque la plus ancienne
    await dbRun(
      `UPDATE active_sessions SET is_revoked = 1 WHERE id = (
         SELECT id FROM active_sessions WHERE user_id = ? AND is_revoked = 0
         ORDER BY created_at ASC LIMIT 1
       )`,
      [userId]
    );
  }

  await dbRun(
    `INSERT INTO active_sessions (user_id, session_token_hash, ip_address, user_agent, expires_at, created_at, last_activity)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [userId, hashSessionToken(sessionToken), ipAddress, userAgent, expiresAt, Date.now(), Date.now()]
  );

  return sessionToken;
}

async function validateActiveSession(userId, sessionToken) {
  const row = await dbGet(
    `SELECT * FROM active_sessions WHERE user_id = ? AND session_token_hash = ? AND is_revoked = 0 AND expires_at > ?`,
    [userId, hashSessionToken(sessionToken), Date.now()]
  );
  if (row) {
    await dbRun(`UPDATE active_sessions SET last_activity = ? WHERE id = ?`, [Date.now(), row.id]);
    return true;
  }
  return false;
}

async function revokeSession(userId, sessionToken) {
  await dbRun(
    `UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND session_token_hash = ?`,
    [userId, hashSessionToken(sessionToken)]
  );
}

async function revokeAllSessions(userId) {
  await dbRun(`UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND is_revoked = 0`, [userId]);
}

// ================================================================================
// ==================== PROTECTION ANTI-BRUTE-FORCE (dates en ms) ===============
// ================================================================================

async function checkLoginAttempts(ipAddress) {
  const cutoff = Date.now() - CONFIG.LOGIN_BLOCK_DURATION_MS;
  const row = await dbGet(
    `SELECT COUNT(*) AS count FROM login_attempts WHERE ip_address = ? AND success = 0 AND created_at > ?`,
    [ipAddress, cutoff]
  );

  if ((row?.count || 0) >= CONFIG.MAX_LOGIN_ATTEMPTS) {
    const existing = await dbGet(`SELECT strike_count FROM blocked_ips WHERE ip_address = ?`, [ipAddress]);
    const strikeCount = (existing?.strike_count || 0) + 1;
    const escalated = Math.min(CONFIG.LOGIN_BLOCK_DURATION_MS * Math.pow(2, strikeCount - 1), 24 * 60 * 60 * 1000);

    await dbRun(
      `INSERT INTO blocked_ips (ip_address, reason, strike_count, blocked_until, created_at)
       VALUES (?, 'Trop de tentatives échouées', ?, ?, ?)
       ON CONFLICT(ip_address) DO UPDATE SET
         reason = excluded.reason,
         strike_count = excluded.strike_count,
         blocked_until = excluded.blocked_until`,
      [ipAddress, strikeCount, Date.now() + escalated, Date.now()]
    );

    if (strikeCount >= 3) logger.warn({ ipAddress, strikeCount }, "🚨 IP récidiviste");
    return { blocked: true, message: "Trop de tentatives échouées. IP temporairement bloquée." };
  }
  return { blocked: false };
}

/** ⚠️ PHASE 2 : "Token manquant" n'est PAS un échec de login (ne bloque pas les IP partagées). */
async function recordLoginAttempt(ipAddress, userId, success, errorMessage = null, { countFailure = true } = {}) {
  if (!success && !countFailure) return;
  try {
    await dbRun(
      `INSERT INTO login_attempts (user_id, ip_address, success, error_message, created_at) VALUES (?, ?, ?, ?, ?)`,
      [userId, ipAddress, success ? 1 : 0, errorMessage ? String(errorMessage).slice(0, 500) : null, Date.now()]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur recordLoginAttempt"); }
}

async function isIPBlocked(ipAddress) {
  const row = await dbGet(
    `SELECT 1 FROM blocked_ips WHERE ip_address = ? AND blocked_until > ? LIMIT 1`,
    [ipAddress, Date.now()]
  );
  return Boolean(row);
}

// ================================================================================
// ==================== QUOTAS ==================================================
// ================================================================================

async function checkUserQuota(userId, action, userRole = "FREE") {
  try {
    const today = todayKeyMs();
    const limits = USER_QUOTAS[userRole] || USER_QUOTAS.FREE;
    const quota = await dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]);

    let current = 0, max = 0;
    switch (action) {
      case "message":  current = quota?.messages_count || 0; max = limits.maxMessagesPerDay; break;
      case "image":    current = quota?.images_count || 0;   max = limits.maxImagesPerDay;   break;
      case "whatsapp": current = quota?.whatsapp_count || 0; max = limits.maxWhatsAppMessagesPerDay; break;
      case "email":    current = quota?.emails_count || 0;   max = limits.maxEmailsPerDay;   break;
      default: return { allowed: true, remaining: null };
    }

    if (current >= max) {
      return { allowed: false, remaining: 0, current, max, message: `Limite atteinte pour ${action} (max : ${max}).` };
    }
    return { allowed: true, remaining: max - current, current, max };
  } catch (e) {
    logger.error({ err: e.message }, "Erreur checkUserQuota");
    // Fail-open prudent : si la DB est cassée, on laisse passer (mieux que bloquer tout le monde).
    return { allowed: true, remaining: null };
  }
}

async function incrementUserQuota(userId, action) {
  try {
    const today = todayKeyMs();
    const col = ({ message: "messages_count", image: "images_count", whatsapp: "whatsapp_count", email: "emails_count" })[action];
    if (!col) return;
    await dbRun(
      `INSERT INTO user_quotas (user_id, date, ${col}, updated_at)
       VALUES (?, ?, 1, ?)
       ON CONFLICT(user_id, date) DO UPDATE SET ${col} = ${col} + 1, updated_at = excluded.updated_at`,
      [userId, today, Date.now()]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur incrementUserQuota"); }
}

// ================================================================================
// ==================== MUTEX PAR CONVERSATION ==================================
// ================================================================================
// ⚠️ PHASE 3 : deux messages rapides ne doivent pas s'entrelacer.

async function acquireConversationLock(conversationId, ownerRequestId, ttlMs = 30000) {
  const now = Date.now();
  const expires = now + ttlMs;
  try {
    await dbRun(
      `INSERT INTO conversation_locks (conversation_id, locked_until, owner_request_id, created_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(conversation_id) DO UPDATE SET
         locked_until = excluded.locked_until,
         owner_request_id = excluded.owner_request_id
       WHERE conversation_locks.locked_until < ?`,
      [conversationId, expires, ownerRequestId, now, now]
    );
    const row = await dbGet(
      `SELECT owner_request_id, locked_until FROM conversation_locks WHERE conversation_id = ?`,
      [conversationId]
    );
    return Boolean(row && row.owner_request_id === ownerRequestId && row.locked_until >= now);
  } catch (e) {
    logger.error({ err: e.message }, "Erreur acquireConversationLock");
    return true; // fail-open
  }
}

async function releaseConversationLock(conversationId, ownerRequestId) {
  try {
    await dbRun(
      `UPDATE conversation_locks SET locked_until = 0 WHERE conversation_id = ? AND owner_request_id = ?`,
      [conversationId, ownerRequestId]
    );
  } catch (e) { logger.error({ err: e.message }, "Erreur releaseConversationLock"); }
}

// ================================================================================
// ==================== ENTITÉS (⚠️ PHASE 4 : extraction pour images/sport/YouTube)
// ================================================================================

/**
 * Extraction d'entité minimaliste : on cherche la partie la plus informative de la
 * requête utilisateur (nom propre, lieu, équipe, sujet) sans passer la phrase entière
 * aux API externes.
 *
 * Heuristique : on privilégie les séquences de mots capitalisés (hors début de phrase),
 * sinon on nettoie les verbes interrogatifs/déictiques.
 */
function extractEntity(message) {
  if (!message || typeof message !== "string") return "";
  let m = message.trim().replace(/[?!.,;:]+$/g, "");

  // Supprime les amorces d'interrogation usuelles
  m = m.replace(
    /^(qui est|c'?est qui|qui était|montre-moi|montre moi|cherche|trouve-moi|trouve moi|parle-moi de|parle moi de|donne-moi|donne moi|photo de|image de|clip de|vidéo de|video de|chanson de|à quoi ressemble|a quoi ressemble|quelle est|quel est|où se trouve|ou se trouve)\s+/i,
    ""
  ).trim();

  // Récupère la séquence de mots capitalisés la plus longue (au moins 1 mot)
  const capitalSeq = m.match(/\b([A-ZÀ-Ý][a-zà-ÿ]+(?:\s+[A-ZÀ-Ý][a-zà-ÿ]+){0,3})\b/g);
  if (capitalSeq && capitalSeq.length > 0) {
    return capitalSeq.sort((a, b) => b.length - a.length)[0];
  }

  // Repli : premier groupe nominal simple (max 60 chars)
  return m.split(/\s+/).slice(0, 8).join(" ").slice(0, 60);
}

// ================================================================================
// ==================== PRÉ-ROUTEUR D'INTENTION (⚠️ PHASE 4 : \b…\b) =============
// ================================================================================
// ⚠️ L'ancien analyzeIntent utilisait includes() : "comment" contenait "om", "capitale"
// contenait "api", "vue" → CODE, etc. On remplace par une correspondance de MOTS ENTIERS,
// normalisée (accents retirés), avec GENERAL par défaut. Le tool calling natif
// (Partie 2) prendra le relais pour les cas complexes.

function normalizeForMatch(s) {
  return String(s).toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function containsWholeWords(haystack, needles) {
  const h = " " + normalizeForMatch(haystack) + " ";
  for (const n of needles) {
    const token = normalizeForMatch(n);
    if (!token) continue;
    // Mot entier (espace avant/après, ou tiret / apostrophe en bord)
    const re = new RegExp(`(^|[\\s'\\-])${token.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}([\\s'\\-]|$)`);
    if (re.test(h)) return true;
  }
  return false;
}

const INTENT_KEYWORDS = {
  MATHS: ["calcule", "calculer", "resous", "equation", "integrale", "derivee", "factorielle", "matrice", "limite", "theoreme", "algebre"],
  ACTUALITE: ["actualite", "actualites", "news", "journal", "derniere", "dernieres", "presse"],
  SPORT: ["score", "match", "football", "basket", "tennis", "nba", "ligue", "championnat", "classement", "resultat"],
  CODE: ["code", "coder", "javascript", "python", "java", "typescript", "react", "angular", "vuejs", "nodejs", "sql", "algorithme", "bug", "debug"],
  VIDEO: ["clip", "video", "youtube", "chanson", "musique", "regarder", "ecouter"],
  TASK: ["rappelle-moi", "rappel", "tache", "taches", "planifie", "agenda", "rendez-vous"],
  PERSONNE: ["qui est", "photo de", "biographie de", "portrait de", "c'est qui"]
};

/**
 * Pré-routeur : renvoie { intent, entity } ou { intent: "GENERAL", entity }.
 * Ne renvoie JAMAIS SPORT/CODE/PERSONNE par effet de bord (mots entiers uniquement).
 */
function preRouteIntent(message) {
  const text = String(message || "").trim();
  const entity = extractEntity(text);

  if (!text) return { intent: "GENERAL", entity };

  if (containsWholeWords(text, INTENT_KEYWORDS.TASK))    return { intent: "TASK",     entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.VIDEO))   return { intent: "VIDEO",    entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.MATHS))   return { intent: "MATHS",    entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.SPORT))   return { intent: "SPORT",    entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.ACTUALITE)) return { intent: "ACTUALITE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.CODE))    return { intent: "CODE",     entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.PERSONNE))return { intent: "PERSONNE", entity };

  return { intent: "GENERAL", entity };
}

// ================================================================================
// ==================== REGISTRE DES PROVIDERS LLM ==============================
// ================================================================================

function buildKeyPool(keys, prefix) {
  return keys
    .filter((k) => typeof k === "string" && k.trim().length > 0)
    .map((apiKey, idx) => ({
      apiKey: apiKey.trim(),
      label: `${prefix}_key_${idx + 1}`
    }));
}

let geminiClient = null;
if (GoogleGenAI && process.env.GEMINI_API_KEY) {
  try {
    geminiClient = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    logger.info("✅ Client Gemini initialisé");
  } catch (e) {
    logger.error({ err: e.message }, "❌ Erreur init Gemini");
  }
} else if (!process.env.GEMINI_API_KEY) {
  logger.warn("⚠️ GEMINI_API_KEY absent — Gemini désactivé");
}

const LLM_PROVIDERS = {
  GROQ: {
    baseURL: "https://api.groq.com/openai/v1",
    defaultTimeout: 10000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool([process.env.GROQ_API_KEY, process.env.GROQ_API_KEY_2], "groq")
  },
  OPENROUTER: {
    baseURL: "https://openrouter.ai/api/v1",
    defaultTimeout: 12000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool(
      [process.env.OPENROUTER_API_KEY, process.env.OPENROUTER_API_KEY_2, process.env.OPENROUTER_API_KEY_3],
      "openrouter"
    )
  },
  CEREBRAS: {
    baseURL: "https://api.cerebras.ai/v1",
    defaultTimeout: 10000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool([process.env.CEREBRAS_API_KEY, process.env.CEREBRAS_API_KEY_2], "cerebras")
  },
  GEMINI: {
    baseURL: "https://generativelanguage.googleapis.com/v1beta",
    defaultTimeout: 12000,
    maxTokens: 8000,
    temperature: 0.7,
    isGemini: true,
    client: geminiClient,
    supportsTools: true,
    keyPool: buildKeyPool([process.env.GEMINI_API_KEY], "gemini")
  }
};

if (LLM_PROVIDERS.OPENROUTER.keyPool.length === 0) logger.warn("⚠️ Aucune clé OPENROUTER");
else logger.info(`✅ ${LLM_PROVIDERS.OPENROUTER.keyPool.length} clé(s) OpenRouter`);
if (LLM_PROVIDERS.GROQ.keyPool.length === 0) logger.warn("⚠️ Aucune clé GROQ");
else logger.info(`✅ ${LLM_PROVIDERS.GROQ.keyPool.length} clé(s) Groq`);
if (LLM_PROVIDERS.CEREBRAS.keyPool.length === 0) logger.warn("⚠️ Aucune clé CEREBRAS");
else logger.info(`✅ ${LLM_PROVIDERS.CEREBRAS.keyPool.length} clé(s) Cerebras`);

// ================================================================================
// ==================== TIERS DE MODÈLES ========================================
// ================================================================================
// ⚠️ PHASE 1 : jsonMode = false pour le chat conversationnel.
// Le chat répond en TEXTE BRUT + suggestions extraites par délimiteur.

const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    jsonMode: false, // ⚠️ v15 : texte brut (fin des U+000C / tabulations en maths)
    providers: [
      { provider: "groq",       model: process.env.GROQ_MODEL_V100 || "llama-3.3-70b-versatile",                   maxTokens: 4000, timeout: 10000, temperature: 0.7, failoverPriority: 0 },
      { provider: "gemini",     model: process.env.GEMINI_MODEL_V100 || "gemini-3.6-flash",                        maxTokens: 8000, timeout: 12000, temperature: 0.7, failoverPriority: 1 },
      { provider: "cerebras",   model: process.env.CEREBRAS_MODEL_V100 || "qwen-3.8-27b",                          maxTokens: 4000, timeout: 10000, temperature: 0.7, failoverPriority: 2 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "poolside/laguna-s-2.1:free", maxTokens: 4000, timeout: 12000, temperature: 0.7, failoverPriority: 3 },
      { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "nvidia/nemotron-3-super-120b:free", maxTokens: 4000, timeout: 12000, temperature: 0.7, failoverPriority: 4 }
    ]
  },
  v250: {
    name: "Ngandu",
    jsonMode: false,
    reasoning: {
      providers: [
        { provider: "gemini",     model: process.env.GEMINI_MODEL_V250_REASONING || "gemini-3.6-flash",                    maxTokens: 8000, timeout: 20000, temperature: 0.3, failoverPriority: 0 },
        { provider: "cerebras",   model: process.env.CEREBRAS_MODEL_V250_REASONING || "qwen-3.8-27b",                      maxTokens: 8000, timeout: 18000, temperature: 0.3, failoverPriority: 1, reasoningEffort: "high" },
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_REASONING || "nvidia/nemotron-3-super-120b:free", maxTokens: 8000, timeout: 20000, temperature: 0.3, failoverPriority: 2 }
      ]
    },
    code: {
      providers: [
        { provider: "openrouter", model: process.env.OPENROUTER_MODEL_V250_CODE || "poolside/laguna-s-2.1:free", maxTokens: 8000, timeout: 20000, temperature: 0.5, failoverPriority: 0 },
        { provider: "cerebras",   model: process.env.CEREBRAS_MODEL_V250_CODE || "qwen-3.8-27b",                 maxTokens: 8000, timeout: 15000, temperature: 0.5, failoverPriority: 1 },
        { provider: "groq",       model: process.env.GROQ_MODEL_V250_CODE_FALLBACK || "llama-3.3-70b-versatile", maxTokens: 8000, timeout: 15000, temperature: 0.5, failoverPriority: 2 }
      ]
    },
    maxRetries: 2
  },
  vision: {
    name: "Vision",
    jsonMode: false,
    providers: [
      { provider: "gemini",     model: CONFIG.VISION_MODEL_GEMINI,     maxTokens: 4000, timeout: 15000, temperature: 0.7, failoverPriority: 0 },
      { provider: "openrouter", model: CONFIG.VISION_MODEL_OPENROUTER, maxTokens: 4000, timeout: 20000, temperature: 0.7, failoverPriority: 1 },
      { provider: "groq",       model: CONFIG.VISION_MODEL_GROQ,       maxTokens: 4000, timeout: 15000, temperature: 0.7, failoverPriority: 2 }
    ]
  }
};

// ================================================================================
// ==================== VALIDATION / SANITIZE MODÈLES OPENROUTER ================
// ================================================================================

function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;
  const knownPrefixes = ["openai/","qwen/","meta-llama/","deepseek/","microsoft/","anthropic/","google/","mistralai/","cohere/","nvidia/","poolside/","inclusionai/"];
  const isORModel = knownPrefixes.some(p => model.includes(p));
  if (isORModel && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return model + ":free";
  }
  return model;
}

// ================================================================================
// ==================== INTERCEPTEUR ERREURS LLM ================================
// ================================================================================

class LLMErrorInterceptor {
  static isRetryableError(error) {
    const status = error?.response?.status;
    const retryableStatuses = [408, 429, 500, 502, 503, 504];
    const isTimeout = ["ECONNABORTED","ETIMEDOUT","ESOCKETTIMEDOUT","ABORT_ERR"].includes(error?.code) || /timeout/i.test(error?.message || "");
    const isNetwork = ["ENOTFOUND","ECONNRESET","ECONNREFUSED","EAI_AGAIN"].includes(error?.code);
    return retryableStatuses.includes(status) || isTimeout || isNetwork;
  }

  static getErrorCode(error) {
    const status = error?.response?.status;
    if (status) return `HTTP_${status}`;
    if (error?.code === "ECONNABORTED" || error?.code === "ABORT_ERR") return "TIMEOUT";
    if (error?.code === "ENOTFOUND") return "DNS_ERROR";
    if (error?.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    if (error?.code === "MISSING_API_KEY") return "MISSING_API_KEY";
    if (error?.code === "CIRCUIT_OPEN") return "CIRCUIT_OPEN";
    return "UNKNOWN_ERROR";
  }

  /** Providers à retirer du routage pour cette requête (ne pas insister). */
  static shouldSkipProvider(error) {
    const code = this.getErrorCode(error);
    return ["HTTP_400","HTTP_401","HTTP_403","HTTP_402","HTTP_404","MISSING_API_KEY"].includes(code);
  }

  /** ⚠️ PHASE 1 : 429/5xx/timeout → rotation IMMÉDIATE vers la clé/modèle suivant. */
  static shouldRotateImmediately(error) {
    const code = this.getErrorCode(error);
    return ["HTTP_429","HTTP_500","HTTP_502","HTTP_503","HTTP_504","TIMEOUT","DNS_ERROR","CONNECTION_REFUSED","CIRCUIT_OPEN"].includes(code);
  }
}

// ================================================================================
// ==================== SÉLECTION D'UN MESSAGE AMICAL POUR L'UTILISATEUR ========
// ================================================================================

function userFacingErrorMessage(error) {
  const code = LLMErrorInterceptor.getErrorCode(error);
  if (code === "HTTP_429") return "Je suis très sollicité. Réessayez dans quelques instants.";
  if (code === "TIMEOUT" || code === "HTTP_504") return "Je mets un peu trop de temps à répondre. Réessayez, ça devrait aller mieux.";
  if (code === "CIRCUIT_OPEN") return "Je suis temporairement indisponible. Réessayez dans une minute.";
  return "Je suis momentanément indisponible. Réessayez dans un instant.";
}

// ================================================================================
// ==================== VÉRIFICATION DISPONIBILITÉ MODÈLES ======================
// ================================================================================

let modelAvailabilityReport = { checkedAt: null, issues: [], ok: true };

function collectConfiguredOpenRouterModels() {
  const s = new Set();
  const collect = (list) => { for (const p of list) if (p.provider === "openrouter") s.add(p.model); };
  collect(MODEL_TIERS.v100.providers);
  collect(MODEL_TIERS.v250.reasoning.providers);
  collect(MODEL_TIERS.v250.code.providers);
  collect(MODEL_TIERS.vision.providers);
  return [...s];
}

async function checkModelAvailability() {
  const issues = [];

  // OpenRouter
  try {
    const r = await axios.get("https://openrouter.ai/api/v1/models", { timeout: 8000 });
    const catalog = r.data?.data || [];
    const freeIds = new Set(catalog
      .filter(m => m.id?.endsWith(":free") || (m.pricing?.prompt === "0" && m.pricing?.completion === "0"))
      .map(m => m.id));
    const allIds = new Set(catalog.map(m => m.id));
    for (const model of collectConfiguredOpenRouterModels()) {
      if (!allIds.has(model)) issues.push({ provider: "openrouter", model, problem: "INTROUVABLE" });
      else if (!freeIds.has(model)) issues.push({ provider: "openrouter", model, problem: "PAYANT" });
    }
  } catch (e) { logger.warn({ err: e.message }, "Vérif OpenRouter impossible"); }

  // Groq
  try {
    if (LLM_PROVIDERS.GROQ.keyPool.length > 0) {
      const r = await axios.get("https://api.groq.com/openai/v1/models", {
        timeout: 8000,
        headers: { Authorization: `Bearer ${LLM_PROVIDERS.GROQ.keyPool[0].apiKey}` }
      });
      const ids = new Set((r.data?.data || []).map(m => m.id));
      const configured = new Set([
        ...MODEL_TIERS.v100.providers.filter(p => p.provider === "groq").map(p => p.model),
        ...MODEL_TIERS.v250.reasoning.providers.filter(p => p.provider === "groq").map(p => p.model),
        ...MODEL_TIERS.v250.code.providers.filter(p => p.provider === "groq").map(p => p.model),
        CONFIG.VISION_MODEL_GROQ
      ]);
      for (const model of configured) if (!ids.has(model)) issues.push({ provider: "groq", model, problem: "INTROUVABLE" });
    }
  } catch (e) { logger.warn({ err: e.message }, "Vérif Groq impossible"); }

  // Cerebras
  try {
    if (LLM_PROVIDERS.CEREBRAS.keyPool.length > 0) {
      const r = await axios.get("https://api.cerebras.ai/v1/models", {
        timeout: 8000,
        headers: { Authorization: `Bearer ${LLM_PROVIDERS.CEREBRAS.keyPool[0].apiKey}` }
      });
      const ids = new Set((r.data?.data || []).map(m => m.id));
      const configured = new Set([
        ...MODEL_TIERS.v100.providers.filter(p => p.provider === "cerebras").map(p => p.model),
        ...MODEL_TIERS.v250.reasoning.providers.filter(p => p.provider === "cerebras").map(p => p.model),
        ...MODEL_TIERS.v250.code.providers.filter(p => p.provider === "cerebras").map(p => p.model)
      ]);
      for (const model of configured) if (!ids.has(model)) issues.push({ provider: "cerebras", model, problem: "INTROUVABLE" });
    }
  } catch (e) { logger.warn({ err: e.message }, "Vérif Cerebras impossible"); }

  // Gemini
  if (geminiClient && process.env.GEMINI_API_KEY) {
    try {
      const r = await axios.get("https://generativelanguage.googleapis.com/v1beta/models", {
        timeout: 8000,
        headers: { "x-goog-api-key": process.env.GEMINI_API_KEY }
      });
      const ids = new Set((r.data?.models || []).map(m => m.name?.replace("models/", "")));
      const configured = new Set([
        ...MODEL_TIERS.v100.providers.filter(p => p.provider === "gemini").map(p => p.model),
        ...MODEL_TIERS.v250.reasoning.providers.filter(p => p.provider === "gemini").map(p => p.model),
        CONFIG.VISION_MODEL_GEMINI
      ]);
      for (const model of configured) if (!ids.has(model)) issues.push({ provider: "gemini", model, problem: "INTROUVABLE" });
    } catch (e) { logger.warn({ err: e.message }, "Vérif Gemini impossible"); }
  }

  modelAvailabilityReport = { checkedAt: new Date().toISOString(), issues, ok: issues.length === 0 };
  if (issues.length > 0) logger.warn({ issues }, "🚨 Modèles problématiques — voir GET /api/health → modelAvailability");
  else logger.info("✅ Tous les modèles vérifiés");
}

checkModelAvailability();
setInterval(checkModelAvailability, 6 * 60 * 60 * 1000).unref?.();

// ================================================================================
// ==================== EMAIL (transport) =======================================
// ================================================================================

let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  const tlsRejectUnauthorized = process.env.SMTP_TLS_REJECT_UNAUTHORIZED !== "false";
  emailTransporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || "587", 10),
    secure: process.env.SMTP_PORT === "465",
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
    // ⚠️ PHASE 2 : rejectUnauthorized:true par défaut ; à ne désactiver qu'en dev explicite.
    tls: { rejectUnauthorized: tlsRejectUnauthorized },
    pool: true, maxConnections: 3, maxMessages: 50
  });
  logger.info("✅ SMTP configuré");
} else {
  logger.warn("⚠️ SMTP non configuré");
}

// ================================================================================
// ==================== MULTER ==================================================
// ================================================================================

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: CONFIG.MAX_IMAGE_SIZE_MB * 1024 * 1024, files: CONFIG.MAX_IMAGES_PER_REQUEST },
  fileFilter: (req, file, cb) => {
    if (CONFIG.ALLOWED_IMAGE_TYPES.includes(file.mimetype)) cb(null, true);
    else cb(new Error(`Type non supporté. Autorisés: ${CONFIG.ALLOWED_IMAGE_TYPES.join(", ")}`));
  }
});

const uploadAudio = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (CONFIG.ALLOWED_AUDIO_TYPES.includes(file.mimetype)) cb(null, true);
    else cb(new Error("Type audio non supporté."));
  }
});

// ================================================================================
// ==================== RSS PARSER ==============================================
// ================================================================================

const rssParser = new Parser({
  timeout: 8000,
  headers: { "User-Agent": CONFIG.HTTP_USER_AGENT }
});

// ================================================================================
// ==================== SOURCES OUVERTES ========================================
// ================================================================================

const OPEN_SOURCES = {
  wikipedia:        { name: "Wikipédia",         url: "https://fr.wikipedia.org",       logo: "https://www.google.com/s2/favicons?sz=64&domain=wikipedia.org" },
  wikimediacommons: { name: "Wikimedia Commons", url: "https://commons.wikimedia.org",  logo: "https://www.google.com/s2/favicons?sz=64&domain=wikimedia.org" },
  googlenews:       { name: "Google News",       url: "https://news.google.com",        logo: "https://www.google.com/s2/favicons?sz=64&domain=news.google.com" },
  thesportsdb:      { name: "TheSportsDB",       url: "https://www.thesportsdb.com",    logo: "https://www.google.com/s2/favicons?sz=64&domain=thesportsdb.com" },
  arxiv:            { name: "arXiv",             url: "https://arxiv.org",              logo: "https://www.google.com/s2/favicons?sz=64&domain=arxiv.org" },
  reddit:           { name: "Reddit",            url: "https://reddit.com",             logo: "https://www.google.com/s2/favicons?sz=64&domain=reddit.com" },
  openmeteo:        { name: "Open-Meteo",        url: "https://open-meteo.com",         logo: "https://www.google.com/s2/favicons?sz=64&domain=open-meteo.com" },
  youtube:          { name: "YouTube",           url: "https://youtube.com",            logo: "https://www.google.com/s2/favicons?sz=64&domain=youtube.com" },
  duckduckgo:       { name: "DuckDuckGo",        url: "https://duckduckgo.com",         logo: "https://www.google.com/s2/favicons?sz=64&domain=duckduckgo.com" }
};

// ================================================================================
// ==================== CACHE IMAGES (⚠️ PHASE 5 : ne jamais cacher un vide) ====
// ================================================================================

const imageSearchCache = new LRUCache({
  max: 500,
  ttl: CONFIG.IMAGE_CACHE_TTL_MS
});

function getCachedImages(key) { return imageSearchCache.get(key) || null; }
function setCachedImages(key, value) {
  // ⚠️ On ne met en cache QUE si on a au moins 1 image valide.
  if (!value?.images || value.images.length === 0) return;
  imageSearchCache.set(key, value);
}

// ================================================================================
// ==================== FILE MANAGER (BullMQ ou mémoire) ========================
// ================================================================================

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
      logger.info("✅ Redis initialisé");
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
      const inMemory = [];
      let processing = false;
      const processNext = async () => {
        if (processing) return;
        processing = true;
        while (inMemory.length > 0) {
          const job = inMemory.shift();
          try { await processor(job); }
          catch (e) { logger.error({ err: e.message }, `Job ${name} échoué`); }
        }
        processing = false;
      };
      this.inMemoryQueues.set(name, {
        add: async (data) => { inMemory.push(data); processNext(); }
      });
    }
  }

  async add(name, data, options = {}) {
    if (this.useRedis) {
      const q = this.queues.get(name);
      if (q) {
        return await q.add("process", data, {
          attempts: options.attempts || 5,
          backoff: { type: "exponential", delay: options.backoffDelay || 2000 },
          removeOnComplete: 100, removeOnFail: 500
        });
      }
    } else {
      const q = this.inMemoryQueues.get(name);
      if (q) return await q.add(data);
    }
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

// File bornée pour la mémoire long terme (⚠️ PHASE 1 : jamais en concurrence avec le chat)
queueManager.createQueue("memory-summary", async (job) => {
  const data = job?.data ?? job;
  await runMemorySummary(data);
}, { concurrency: 1, limiter: { max: 1, duration: 2000 } });

// ⚠️ Signature remplie en Partie 3 (mémoire long terme) — stub ici pour éviter les crashes.
let runMemorySummary = async () => {};

// ================================================================================
// ==================== ARRÊT PROPRE (⚠️ PHASE 2 : exit(1) sur uncaught) =======
// ================================================================================

let isShuttingDown = false;

async function shutdown(signal, exitCode = 0) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre du serveur");

  try { await new Promise((resolve) => global.__luba_server?.close(resolve)); } catch {}
  try { await queueManager.close(); } catch (e) { logger.error({ err: e.message }, "Erreur fermeture files"); }
  try { await new Promise((resolve) => db.close(() => resolve())); } catch {}

  console.log("✅ Arrêt propre terminé");
  process.exit(exitCode);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

process.on("uncaughtException", (error) => {
  logger.fatal({ err: error.message, stack: error.stack }, "uncaughtException — arrêt du process");
  // ⚠️ PHASE 2 : ne JAMAIS continuer dans un état indéfini.
  shutdown("uncaughtException", 1);
});

process.on("unhandledRejection", (reason) => {
  logger.error({ reason: String(reason) }, "unhandledRejection");
  // Pas d'exit : rejet non critique, on logue et on continue.
});

// ================================================================================
// ==================== EXPORTS (pour tests + Partie 2 à 4) ====================
// ================================================================================

module.exports = {
  // Config
  CONFIG, FIREBASE_CONFIG, HOSTING_CONFIG, USER_QUOTAS, OPEN_SOURCES, MODEL_TIERS,

  // Infra
  logger, db, dbGet, dbAll, dbRun, dbExec, dbTransaction,
  queueManager, supabase,

  // Sécurité
  sanitizeForLLM, sanitizeStrict, escapeHtml, normalizeMath, stripThinkTags,
  isValidImageSignature, computeDeviceFingerprint, hashSessionToken, sha256,
  EMAIL_REGEX, PHONE_REGEX,

  // Sessions & quotas
  createActiveSession, validateActiveSession, revokeSession, revokeAllSessions,
  checkLoginAttempts, recordLoginAttempt, isIPBlocked,
  checkUserQuota, incrementUserQuota,
  acquireConversationLock, releaseConversationLock,

  // Auth
  verifyFirebaseToken, cacheGetToken, cacheSetToken, tokenCache,

  // IA
  LLM_PROVIDERS, MODEL_TIERS, CircuitBreaker, getCircuit, LLMErrorInterceptor,
  userFacingErrorMessage, validateAndSanitizeOpenRouterModel,
  evaluateMathSafe, detectMathExpressions,

  // Intentions & entités
  preRouteIntent, extractEntity, normalizeForMatch, containsWholeWords,

  // Supabase / outbox
  supabaseWriteSafe, enqueueOutbox, processOutbox,

  // Utils
  generateRequestId, generateConversationId, generateSessionToken, generateUUID, generateTaskId,
  nowMs, todayKeyMs, withDeadline, allSettledWithDeadline, sleep, backoffDelay,

  // Model availability
  modelAvailabilityReport, checkModelAvailability,

  // Mémoire long terme (rempli en Partie 3)
  setRunMemorySummary: (fn) => { runMemorySummary = fn; }
};
// ================================================================================
// ==================== PARTIE 2 : AUTH DURCI · TOOL CALLING · SANDBOX ==========
// ================================================================================
// Sommaire :
//   §2.1  Limites de corps (JSON 1 Mo par défaut, 20 Mo sur uploads seulement)
//   §2.2  Rate limiters (memory + Redis opt-in) + strictLimiter
//   §2.3  authenticateUser durci (cache 5 min, fire-and-forget, path critique court)
//   §2.4  requireRole + vérifications admin
//   §2.5  Schémas JSON des OUTILS (native tool calling)
//   §2.6  Exécuteur d'outils (dispatch, whitelist, paramètres validés)
//   §2.7  Outils à effet de bord + confirmation
//   §2.8  run_code SANDBOX (Piston / Judge0 / E2B) — jamais d'eval local
//   §2.9  Boucle agent bornée (write → run → read error → fix)
//   §2.10 Appels LLM natifs (tools + tool_calls)
//   §2.11 Scheduler de rappels (due_at)
//   §2.12 WhatsApp durci (creds complets, déconnexion, whitelist, quota)
//   §2.13 Enregistrement des tâches de fond (queues)
// ================================================================================

const util = require("util");

// ================================================================================
// §2.1 — LIMITES DE CORPS (⚠️ PHASE 2 : 20 Mo avant auth = DoS)
// ================================================================================

/** Body JSON générique : 1 Mo (le vrai contenu passe par multipart pour les images). */
const jsonBodySmall = express.json({ limit: "1mb" });
/** Body JSON large : réservé aux routes qui en ont besoin (upload base64 non utilisé ici). */
const jsonBodyLarge = express.json({ limit: "20mb" });
const urlEncoded = express.urlencoded({ extended: true, limit: "1mb" });

function mountBodyParsers(app) {
  // ⚠️ RÈGLE : par défaut, TOUTES les routes reçoivent le body "small".
  // Les routes multipart (multer) gèrent leur propre limite fichier.
  app.use((req, res, next) => {
    // Ne parse PAS les multipart ici : multer s'en occupe.
    const ct = req.headers["content-type"] || "";
    if (ct.startsWith("multipart/form-data")) return next();
    // Routes qui ont explicitement besoin du gros body (déclarer en amont si besoin) :
    if (req.path === "/api/import/conversations") return jsonBodyLarge(req, res, next);
    return jsonBodySmall(req, res, next);
  });
  app.use(urlEncoded);
}

// ================================================================================
// §2.2 — RATE LIMITERS
// ================================================================================
// ⚠️ En multi-instance, il FAUT un store partagé (Redis). Sinon la limite réelle
//    est multipliée par le nombre d'instances. Le store Redis est branché si
//    REDIS_URL est présent (optionnel, silencieux sinon).

let redisRateLimitStore = null;
try {
  if (process.env.REDIS_URL && IORedis) {
    const RedisStore = require("rate-limit-redis");
    const rl = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
    redisRateLimitStore = new RedisStore({ sendCommand: (...args) => rl.call(...args) });
    logger.info("✅ Rate-limit Redis activé");
  }
} catch (e) {
  logger.warn({ err: e.message }, "⚠️ rate-limit-redis indisponible — stores mémoire");
}

function makeLimiter({ windowMs, max, code, message, keyGenerator }) {
  return rateLimit({
    windowMs,
    max,
    standardHeaders: true,
    legacyHeaders: false,
    ...(redisRateLimitStore ? { store: redisRateLimitStore } : {}),
    ...(keyGenerator ? { keyGenerator } : {}),
    handler: (req, res) => {
      logger.warn({ ip: req.ip, path: req.path, code }, "Rate limit dépassé");
      res.status(429).json({ success: false, error: true, reply: message, code });
    }
  });
}

const apiLimiter    = makeLimiter({ windowMs: 15 * 60 * 1000, max: 200, code: "RATE_LIMIT",        message: "Trop de requêtes. Réessayez dans 15 minutes." });
const strictLimiter = makeLimiter({ windowMs: 60 * 60 * 1000, max:  50, code: "RATE_LIMIT_STRICT", message: "Limite de requêtes atteinte." });
const chatLimiter   = makeLimiter({ windowMs: 60 * 1000,       max:  30, code: "RATE_LIMIT_CHAT",   message: "Trop de messages. Patientez un instant." });
const authLimiter   = makeLimiter({ windowMs: 15 * 60 * 1000, max:  60, code: "RATE_LIMIT_AUTH",   message: "Trop de tentatives d'authentification." });
const toolLimiter   = makeLimiter({ windowMs: 60 * 1000,       max:  40, code: "RATE_LIMIT_TOOL",   message: "Trop d'appels d'outils." });

// ================================================================================
// §2.3 — authenticateUser DURCI
// ================================================================================
// ⚠️ PHASE 1 : on sort du chemin critique les écritures (log success, device, sync Supabase).
// Le cache LRU évite verifyIdToken à chaque requête.

const AUTH_ERROR_MESSAGES = {
  MISSING_TOKEN:  "Authentification requise.",
  INVALID_TOKEN:  "Session invalide.",
  TOKEN_EXPIRED:  "Session expirée, veuillez vous reconnecter.",
  IP_BLOCKED:     "Accès refusé.",
  INTERNAL:       "Erreur d'authentification."
};

async function fastUpsertUser(uid, user, userRole) {
  // ⚠️ FIX v14 : ne JAMAIS écraser le rôle avec FREE en mode REST.
  // En Admin SDK, le rôle vient des custom claims (source de vérité) → on peut l'écrire.
  const hasAdmin = Boolean(firebaseApp && firebaseAdmin);
  try {
    const existing = await dbGet("SELECT id, role FROM users WHERE id = ?", [uid]);
    if (!existing) {
      await dbRun(
        `INSERT INTO users (id, firebase_uid, email, display_name, role, email_verified, last_seen_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [uid, uid, user.email, user.displayName || uid, userRole, user.emailVerified ? 1 : 0, Date.now(), Date.now(), Date.now()]
      );
      logger.info({ userId: uid }, "✅ Nouvel utilisateur créé");
    } else {
      // ⚠️ FIX : en REST, on n'écrase PAS le rôle existant (il était remis à FREE à chaque req).
      const newRole = hasAdmin ? userRole : (existing.role || "FREE");
      await dbRun(
        `UPDATE users SET last_seen_at = ?, email = COALESCE(?, email),
          display_name = COALESCE(?, display_name), role = ?, email_verified = ?, firebase_uid = ?
         WHERE id = ?`,
        [Date.now(), user.email, user.displayName, newRole, user.emailVerified ? 1 : 0, uid, uid]
      );
    }
  } catch (e) { logger.error({ err: e.message }, "Erreur fastUpsertUser"); }
}

function authenticateUser(req, res, next) {
  // Wrapper async, sans try/catch externe lourd.
  (async () => {
    const ip = req.ip;
    const ua = req.headers["user-agent"];

    // 1) Blocage IP (index)
    if (await isIPBlocked(ip)) {
      await recordLoginAttempt(ip, null, false, "IP bloquée", { countFailure: false });
      return res.status(403).json({ success: false, error: true, reply: AUTH_ERROR_MESSAGES.IP_BLOCKED, code: "IP_BLOCKED" });
    }

    // 2) Extraction token
    const authHeader = req.headers.authorization || req.headers.Authorization || "";
    const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    if (!bearerToken) {
      // ⚠️ "Token manquant" N'EST PAS un échec de login (IP partagées mobiles).
      return res.status(401).json({ success: false, error: true, reply: AUTH_ERROR_MESSAGES.MISSING_TOKEN, code: "MISSING_TOKEN" });
    }

    // 3) Vérification (avec cache)
    let user;
    try {
      user = await verifyFirebaseToken(bearerToken);
    } catch (error) {
      const isExpired = error?.code === "auth/id-token-expired" || error?.errorInfo?.code === "auth/id-token-expired";
      const isRevoked = error?.code === "auth/id-token-revoked";
      const isMalformed = error?.code === "auth/argument-error" || /malformed/i.test(error?.message || "");

      // ⚠️ Uniquement les erreurs "dures" comptent pour le blocage IP.
      const countAsFailure = !isExpired && !isRevoked;
      await recordLoginAttempt(ip, null, false, error.message, { countFailure: countAsFailure });

      if (countAsFailure) {
        const check = await checkLoginAttempts(ip);
        if (check.blocked) return res.status(403).json({ success: false, error: true, reply: check.message, code: "IP_BLOCKED" });
      }
      return res.status(401).json({
        success: false, error: true,
        reply: isExpired ? AUTH_ERROR_MESSAGES.TOKEN_EXPIRED : (isMalformed ? AUTH_ERROR_MESSAGES.INVALID_TOKEN : AUTH_ERROR_MESSAGES.INVALID_TOKEN),
        code: isExpired ? "TOKEN_EXPIRED" : "INVALID_TOKEN"
      });
    }

    if (!user) {
      await recordLoginAttempt(ip, null, false, "User introuvable");
      return res.status(401).json({ success: false, error: true, reply: AUTH_ERROR_MESSAGES.INVALID_TOKEN, code: "INVALID_TOKEN" });
    }

    // 4) Contexte requête
    req.uid = user.uid;
    req.userId = user.uid;
    req.firebaseUid = user.uid;
    req.verifiedIdentity = true;
    req.userRole = user.role || "FREE";
    req.emailVerified = user.emailVerified;

    // 5) ⚠️ Fire-and-forget : ne bloque PAS la requête
    setImmediate(() => {
      Promise.allSettled([
        recordLoginAttempt(ip, user.uid, true),
        logSecurityEvent(user.uid, "LOGIN_SUCCESS", { email: user.email }, ip, ua),
        detectAndLogNewDeviceAsync(user.uid, ip, ua),
        fastUpsertUser(user.uid, user, req.userRole)
      ]).catch(() => {});
    });

    // 6) Sync Supabase en arrière-plan (si elle échoue, l'outbox prend le relais)
    if (supabase) {
      setImmediate(() => {
        supabaseWriteSafe({
          table: "users",
          op: "upsert",
          payload: {
            id: user.uid, firebase_uid: user.uid,
            email: user.email || null, display_name: user.displayName || null,
            last_seen_at: new Date().toISOString()
          },
          idempotencyKey: `user:${user.uid}`,
          matchColumn: "firebase_uid"
        }).catch(() => {});
      });
    }

    next();
  })().catch((e) => {
    logger.error({ err: e.message, stack: e.stack }, "authenticateUser — erreur interne");
    if (!res.headersSent) {
      return res.status(500).json({ success: false, error: true, reply: AUTH_ERROR_MESSAGES.INTERNAL, code: "AUTH_INTERNAL_ERROR" });
    }
  });
}

// ================================================================================
// §2.4 — requireRole
// ================================================================================

function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!firebaseApp && allowedRoles.includes("ADMIN")) {
      return res.status(503).json({
        success: false, error: true,
        reply: "Fonctions admin indisponibles (Admin SDK requis).",
        code: "ADMIN_REQUIRES_SERVICE_ACCOUNT"
      });
    }
    if (!req.userRole || (!allowedRoles.includes(req.userRole) && req.userRole !== "ADMIN")) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "INSUFFICIENT_ROLE" });
    }
    next();
  };
}

// ================================================================================
// §2.5 — SCHÉMAS JSON DES OUTILS (native tool calling)
// ================================================================================
// ⚠️ PHASE 4 : le modèle choisit l'outil. On ne simule plus toolCalls dans un JSON.
// Chaque outil a un schéma strict (JSON Schema) → le provider le valide.

const TOOL_SCHEMAS = {
  search_images: {
    type: "function",
    function: {
      name: "search_images",
      description: "Recherche des images réelles pour une personne, un lieu, un objet ou un concept précis.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Entité précise à illustrer (nom propre, lieu, objet). PAS la phrase entière." }
        },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  search_web: {
    type: "function",
    function: {
      name: "search_web",
      description: "Recherche web générale (Wikipedia + actualités + DDG).",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  search_news: {
    type: "function",
    function: {
      name: "search_news",
      description: "Dernières actualités sur un sujet.",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  search_sports_scores: {
    type: "function",
    function: {
      name: "search_sports_scores",
      description: "Derniers résultats d'une équipe.",
      parameters: {
        type: "object",
        properties: { team: { type: "string", description: "Nom exact de l'équipe (ex. 'PSG', 'Lakers')." } },
        required: ["team"],
        additionalProperties: false
      }
    }
  },
  search_science: {
    type: "function",
    function: {
      name: "search_science",
      description: "Articles scientifiques arXiv.",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  search_social: {
    type: "function",
    function: {
      name: "search_social",
      description: "Discussions Reddit.",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  get_weather: {
    type: "function",
    function: {
      name: "get_weather",
      description: "Météo actuelle pour un lieu.",
      parameters: {
        type: "object",
        properties: { location: { type: "string" } },
        required: ["location"],
        additionalProperties: false
      }
    }
  },
  execute_math: {
    type: "function",
    function: {
      name: "execute_math",
      description: "Calcul mathématique exact (arithmétique, algèbre, trigonométrie). Ne PAS l'utiliser pour du texte.",
      parameters: {
        type: "object",
        properties: { expression: { type: "string", description: "Expression mathjs valide, 2000 char max." } },
        required: ["expression"],
        additionalProperties: false
      }
    }
  },
  search_youtube: {
    type: "function",
    function: {
      name: "search_youtube",
      description: "Recherche vidéos YouTube.",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  create_task: {
    type: "function",
    function: {
      name: "create_task",
      description: "Crée un rappel / une tâche pour l'utilisateur. Utilise due_at en ISO 8601 si une date est mentionnée.",
      parameters: {
        type: "object",
        properties: {
          title: { type: "string", maxLength: 200 },
          notes: { type: "string", maxLength: 2000 },
          due_at: { type: "string", description: "Date ISO 8601 (ex. '2026-05-01T14:00:00Z'). Omettre si aucune date." }
        },
        required: ["title"],
        additionalProperties: false
      }
    }
  },
  list_tasks: {
    type: "function",
    function: {
      name: "list_tasks",
      description: "Liste les tâches de l'utilisateur.",
      parameters: {
        type: "object",
        properties: { status: { type: "string", enum: ["pending", "done", "all"] } },
        additionalProperties: false
      }
    }
  },
  complete_task: {
    type: "function",
    function: {
      name: "complete_task",
      description: "Marque une tâche comme terminée. Outil à effet de bord — nécessite confirmation.",
      parameters: {
        type: "object",
        properties: { task_id: { type: "string" } },
        required: ["task_id"],
        additionalProperties: false
      }
    }
  },
  delete_task: {
    type: "function",
    function: {
      name: "delete_task",
      description: "Supprime une tâche. Outil à effet de bord — nécessite confirmation.",
      parameters: {
        type: "object",
        properties: { task_id: { type: "string" } },
        required: ["task_id"],
        additionalProperties: false
      }
    }
  },
  send_email: {
    type: "function",
    function: {
      name: "send_email",
      description: "Envoie un email. Outil à effet de bord — nécessite confirmation explicite de l'utilisateur.",
      parameters: {
        type: "object",
        properties: {
          recipient: { type: "string", format: "email" },
          subject: { type: "string", maxLength: 200 },
          body: { type: "string", maxLength: 5000 }
        },
        required: ["recipient", "subject", "body"],
        additionalProperties: false
      }
    }
  },
  send_whatsapp_message: {
    type: "function",
    function: {
      name: "send_whatsapp_message",
      description: "Envoie un message WhatsApp. Outil à effet de bord — nécessite confirmation.",
      parameters: {
        type: "object",
        properties: {
          phone_number: { type: "string", description: "Format international sans espaces (ex. +243812345678)." },
          message: { type: "string", maxLength: 2000 }
        },
        required: ["phone_number", "message"],
        additionalProperties: false
      }
    }
  },
  run_code: {
    type: "function",
    function: {
      name: "run_code",
      description: "Exécute un extrait de code dans un SANDBOX EXTERNE isolé (pas de réseau, timeout strict). Utilise-le pour vérifier qu'un code fonctionne ou diagnostiquer une erreur.",
      parameters: {
        type: "object",
        properties: {
          language: { type: "string", enum: ["python", "javascript", "typescript", "bash"] },
          code: { type: "string", maxLength: 20000 },
          stdin: { type: "string", maxLength: 2000 }
        },
        required: ["language", "code"],
        additionalProperties: false
      }
    }
  }
};

// Whitelist par contexte (chat vs /api/tools). On expose moins d'outils en direct.
const TOOLS_BY_CONTEXT = {
  chat: [
    "search_images", "search_web", "search_news", "search_sports_scores",
    "search_science", "search_social", "get_weather", "execute_math",
    "search_youtube", "create_task", "list_tasks", "complete_task", "delete_task",
    "run_code"
  ],
  api: [
    "search_images", "search_web", "search_news", "search_sports_scores",
    "search_science", "search_social", "get_weather", "execute_math",
    "search_youtube", "create_task", "list_tasks", "complete_task", "delete_task"
  ]
};

// ⚠️ Outils à effet de bord : confirmation obligatoire avant exécution.
const SIDE_EFFECT_TOOLS = new Set(["send_email", "send_whatsapp_message", "delete_task", "complete_task"]);

function getToolSchemas(context = "chat") {
  const list = TOOLS_BY_CONTEXT[context] || TOOLS_BY_CONTEXT.chat;
  return list.map((name) => TOOL_SCHEMAS[name]).filter(Boolean);
}

// ================================================================================
// §2.6 — EXÉCUTEUR D'OUTILS (dispatch + whitelist + validation params)
// ================================================================================
// ⚠️ PHASE 2 : validation stricte des paramètres AVANT tout appel réseau/DB.
// Toute erreur est retournée en { success:false, error } et journalisée SANS fuite.

function validateToolArgs(name, args) {
  const schema = TOOL_SCHEMAS[name]?.function?.parameters;
  if (!schema) return { ok: false, error: "Outil inconnu" };
  const required = schema.required || [];
  for (const key of required) {
    if (args?.[key] === undefined || args?.[key] === null || args?.[key] === "") {
      return { ok: false, error: `Paramètre manquant : ${key}` };
    }
  }
  // Type checks (léger)
  for (const [key, prop] of Object.entries(schema.properties || {})) {
    if (args?.[key] === undefined) continue;
    if (prop.type === "string" && typeof args[key] !== "string") {
      return { ok: false, error: `Paramètre ${key} doit être une chaîne` };
    }
    if (prop.type === "string" && prop.maxLength && args[key].length > prop.maxLength) {
      return { ok: false, error: `Paramètre ${key} trop long` };
    }
    if (prop.enum && !prop.enum.includes(args[key])) {
      return { ok: false, error: `Paramètre ${key} doit valoir l'une des valeurs : ${prop.enum.join(", ")}` };
    }
  }
  return { ok: true };
}

/**
 * Exécute un outil par nom.
 * @returns {Promise<{result:any, sourceKeys:string[], toolName:string}>}
 */
async function executeToolNative(toolName, rawArgs, context = {}) {
  const { userId, googleAccessToken, agentMode = false } = context;

  // ⚠️ Whitelist globale (tous contextes confondus) + contexte
  const allowed = new Set([...TOOLS_BY_CONTEXT.chat, ...TOOLS_BY_CONTEXT.api]);
  if (!allowed.has(toolName)) {
    return { result: { success: false, error: "Outil non autorisé" }, sourceKeys: [], toolName };
  }

  const args = rawArgs && typeof rawArgs === "object" ? rawArgs : {};
  const validation = validateToolArgs(toolName, args);
  if (!validation.ok) {
    return { result: { success: false, error: validation.error }, sourceKeys: [], toolName };
  }

  const sourceKeys = [];
  let result;

  try {
    switch (toolName) {
      case "search_images": {
        const q = extractEntity(args.query) || args.query;
        result = await searchImagesWithFallback(q, CONFIG.IMAGE_SEARCH_LIMIT);
        if (result.images?.length) sourceKeys.push("wikimediacommons");
        break;
      }
      case "search_web": {
        result = await searchWeb(args.query);
        (result.sourcesUsed || []).forEach((k) => sourceKeys.push(k));
        break;
      }
      case "search_news": {
        result = await searchNews(args.query);
        if (result.articles?.length) sourceKeys.push("googlenews");
        break;
      }
      case "search_sports_scores": {
        result = await searchSportsScores(args.team);
        if (result.events?.length) sourceKeys.push("thesportsdb");
        break;
      }
      case "search_science": {
        result = await searchScience(args.query);
        if (result.papers?.length) sourceKeys.push("arxiv");
        break;
      }
      case "search_social": {
        result = await searchSocial(args.query);
        if (result.posts?.length) sourceKeys.push("reddit");
        break;
      }
      case "get_weather": {
        result = await getWeather(args.location);
        if (!result.error) sourceKeys.push("openmeteo");
        break;
      }
      case "execute_math": {
        // ⚠️ Sandbox Worker (Partie 1)
        result = await evaluateMathSafe(args.expression, 1500);
        break;
      }
      case "search_youtube": {
        result = await searchYouTube(args.query);
        if (result.videos?.length) sourceKeys.push("youtube");
        break;
      }
      case "create_task": {
        result = await createTask(userId, {
          title: args.title, notes: args.notes,
          dueAt: args.due_at ? Date.parse(args.due_at) : null
        });
        break;
      }
      case "list_tasks": {
        result = await listTasks(userId, { status: args.status && args.status !== "all" ? args.status : null });
        break;
      }
      case "complete_task": {
        if (!agentMode) return { result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" }, sourceKeys: [], toolName };
        result = await updateTaskStatus(userId, args.task_id, "done");
        break;
      }
      case "delete_task": {
        if (!agentMode) return { result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" }, sourceKeys: [], toolName };
        result = await deleteTask(userId, args.task_id);
        break;
      }
      case "send_email": {
        if (!agentMode) return { result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" }, sourceKeys: [], toolName };
        const quota = await checkUserQuota(userId, "email");
        if (!quota.allowed) { result = { success: false, error: quota.message }; break; }
        result = await dispatchSendEmail({
          googleAccessToken,
          recipient: args.recipient,
          subject: args.subject,
          body: args.body,
          userId
        });
        if (result.success) await incrementUserQuota(userId, "email");
        break;
      }
      case "send_whatsapp_message": {
        if (!agentMode) return { result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" }, sourceKeys: [], toolName };
        const quota = await checkUserQuota(userId, "whatsapp");
        if (!quota.allowed) { result = { success: false, error: quota.message }; break; }
        result = await sendWhatsAppSmart(userId, args.phone_number, args.message);
        if (result.success) await incrementUserQuota(userId, "whatsapp");
        break;
      }
      case "run_code": {
        result = await runCodeSandbox({ language: args.language, code: args.code, stdin: args.stdin });
        break;
      }
      default:
        result = { success: false, error: "Outil inconnu" };
    }
  } catch (e) {
    logger.error({ err: e.message, toolName, userId }, "Erreur exécution outil");
    result = { success: false, error: "Échec d'exécution" };
  }

  return { result, sourceKeys, toolName };
}

// ================================================================================
// §2.7 — CONFIRMATION POUR OUTILS À EFFET DE BORD
// ================================================================================
// ⚠️ PHASE 2 : injection de prompt via contenu web → le LLM ne doit JAMAIS
// déclencher send_email / send_whatsapp / delete_task sans validation explicite.

const pendingConfirmations = new Map(); // conversationId → { toolName, args, userId, expiresAt, token }

const CONFIRMATION_TTL_MS = 2 * 60 * 1000;

function makeConfirmationToken() { return `cfm_${crypto.randomBytes(12).toString("hex")}`; }

function queueConfirmation(conversationId, userId, toolName, args) {
  const token = makeConfirmationToken();
  pendingConfirmations.set(conversationId, {
    token, toolName, args, userId, expiresAt: Date.now() + CONFIRMATION_TTL_MS
  });
  // Purge légère
  for (const [k, v] of pendingConfirmations) {
    if (v.expiresAt < Date.now()) pendingConfirmations.delete(k);
  }
  return token;
}

function consumeConfirmation(conversationId, token) {
  const entry = pendingConfirmations.get(conversationId);
  if (!entry) return { ok: false, error: "Aucune confirmation en attente" };
  if (entry.expiresAt < Date.now()) { pendingConfirmations.delete(conversationId); return { ok: false, error: "Confirmation expirée" }; }
  if (token && entry.token !== token) return { ok: false, error: "Jeton invalide" };
  pendingConfirmations.delete(conversationId);
  return { ok: true, entry };
}

/** Message utilisateur proposé pour demander une confirmation claire. */
function buildConfirmationPrompt(toolName, args) {
  switch (toolName) {
    case "send_email":
      return `Je vais envoyer un email à **${args.recipient}** — sujet : « ${args.subject} ». Confirmer ?`;
    case "send_whatsapp_message":
      return `Je vais envoyer un message WhatsApp au **${args.phone_number}** : « ${(args.message || "").slice(0, 120)}… ». Confirmer ?`;
    case "delete_task":
      return `Je vais supprimer la tâche \`${args.task_id}\`. Confirmer ?`;
    case "complete_task":
      return `Je vais marquer la tâche \`${args.task_id}\` comme terminée. Confirmer ?`;
    default:
      return `Confirmer l'action « ${toolName} » ?`;
  }
}

// ================================================================================
// §2.8 — run_code : SANDBOX EXTERNE
// ================================================================================
// ⚠️ PHASE 6 : JAMAIS eval/exec/child_process local sur du code utilisateur.
// On passe par un service externe : Piston (open source), Judge0, ou E2B.

const SUPPORTED_SANDBOX_LANGS = {
  python:     { piston: "python",     judge0: 71,  e2b: "python" },
  javascript: { piston: "javascript", judge0: 63,  e2b: "node" },
  typescript: { piston: "typescript", judge0: 74,  e2b: "node" },
  bash:       { piston: "bash",       judge0: 42,  e2b: null }
};

async function runCodeSandbox({ language, code, stdin = "" }) {
  const provider = CONFIG.CODE_SANDBOX_PROVIDER;
  if (!provider) return { success: false, error: "Sandbox d'exécution non configuré" };

  if (!SUPPORTED_SANDBOX_LANGS[language]) return { success: false, error: "Langage non supporté" };
  if (typeof code !== "string" || code.length === 0) return { success: false, error: "Code vide" };
  if (code.length > 20000) return { success: false, error: "Code trop long (max 20 000 caractères)" };

  const timeoutMs = 8000;

  if (provider === "piston") {
    if (!CONFIG.PISTON_URL) return { success: false, error: "PISTON_URL manquant" };
    try {
      const langMeta = SUPPORTED_SANDBOX_LANGS[language].piston;
      const response = await axios.post(
        `${CONFIG.PISTON_URL.replace(/\/$/, "")}/api/v2/execute`,
        {
          language: langMeta,
          version: "*",
          files: [{ content: code }],
          stdin,
          run_timeout: timeoutMs,
          compile_timeout: timeoutMs
        },
        { timeout: timeoutMs + 3000 }
      );
      const run = response.data?.run || {};
      return {
        success: true,
        stdout: String(run.stdout || "").slice(0, 20000),
        stderr: String(run.stderr || "").slice(0, 20000),
        exitCode: typeof run.code === "number" ? run.code : null,
        provider: "piston",
        language
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Piston échec");
      return { success: false, error: "Échec d'exécution (Piston)" };
    }
  }

  if (provider === "judge0") {
    if (!CONFIG.JUDGE0_URL) return { success: false, error: "JUDGE0_URL manquant" };
    try {
      const langId = SUPPORTED_SANDBOX_LANGS[language].judge0;
      const submit = await axios.post(
        `${CONFIG.JUDGE0_URL.replace(/\/$/, "")}/submissions?base64_encoded=false&wait=true`,
        { language_id: langId, source_code: code, stdin },
        { timeout: timeoutMs + 5000, headers: { "Content-Type": "application/json" } }
      );
      const d = submit.data || {};
      return {
        success: true,
        stdout: String(d.stdout || "").slice(0, 20000),
        stderr: String(d.stderr || d.compile_output || "").slice(0, 20000),
        exitCode: d.status?.id ?? null,
        provider: "judge0",
        language
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Judge0 échec");
      return { success: false, error: "Échec d'exécution (Judge0)" };
    }
  }

  if (provider === "e2b") {
    if (!CONFIG.E2B_API_KEY) return { success: false, error: "E2B_API_KEY manquant" };
    // ⚠️ E2B nécessite le SDK officiel ; on n'ajoute PAS de dépendance obligatoire ici.
    return { success: false, error: "E2B non intégré dans ce build (utilise Piston ou Judge0)" };
  }

  return { success: false, error: "Provider sandbox inconnu" };
}

// ================================================================================
// §2.9 — BOUCLE AGENT BORNÉE (write → run → read error → fix)
// ================================================================================
// ⚠️ PHASE 6 : max 3 itérations, chaque itération = un appel LLM avec tools.

const AGENT_MAX_ITERATIONS = 3;
const AGENT_MAX_TOOL_CALLS_PER_STEP = 5;

/**
 * Orchestration : le modèle reçoit les tools, on boucle tant qu'il demande des outils.
 * Bornes strictes pour éviter les boucles infinies.
 */
async function runToolLoop({
  messages,
  providerConfig,
  executeFn,          // async ({ toolName, args }) => { result, sourceKeys }
  maxIterations = AGENT_MAX_ITERATIONS,
  images = null
}) {
  let iterations = 0;
  const usedSources = new Set();
  const collectedImages = [];
  const collectedVideos = [];
  const toolCallTrace = [];

  let workingMessages = [...messages];
  let finalText = "";

  while (iterations < maxIterations) {
    iterations++;

    const llmResult = await callProviderWithTools({
      providerConfig,
      messages: workingMessages,
      tools: getToolSchemas("chat"),
      images
    });

    if (!llmResult.success) {
      return { success: false, error: llmResult.error, iterations, usedSources: [...usedSources], toolCallTrace };
    }

    const assistantMessage = llmResult.message || {};
    const toolCalls = assistantMessage.tool_calls || [];

    // Pas d'appel d'outil → réponse finale
    if (toolCalls.length === 0) {
      finalText = normalizeMath(stripThinkTags(assistantMessage.content || "").text);
      break;
    }

    // Ajoute le message assistant AVEC tool_calls (obligatoire pour cohérence)
    workingMessages.push({
      role: "assistant",
      content: assistantMessage.content || "",
      tool_calls: toolCalls
    });

    // Exécute les outils (borné)
    const bounded = toolCalls.slice(0, AGENT_MAX_TOOL_CALLS_PER_STEP);
    for (const call of bounded) {
      const toolName = call.function?.name;
      let args = {};
      try { args = JSON.parse(call.function?.arguments || "{}"); } catch { args = {}; }

      toolCallTrace.push({ name: toolName, args });

      const { result, sourceKeys } = await executeFn({ toolName, args });
      sourceKeys.forEach((k) => usedSources.add(k));

      if (toolName === "search_images" && result?.images) {
        collectedImages.push(...result.images.map((i) => i.url).filter(Boolean));
      }
      if (toolName === "search_youtube" && result?.videos) {
        collectedVideos.push(...result.videos);
      }

      // ⚠️ Message "tool" AVEC tool_call_id (sinon HTTP 400 côté providers)
      workingMessages.push({
        role: "tool",
        tool_call_id: call.id,
        name: toolName,
        content: JSON.stringify(result).slice(0, 8000) // borne la taille du retour
      });
    }

    // Sécurité : si trop de tool calls demandés, on continue mais on prévient
    if (toolCalls.length > AGENT_MAX_TOOL_CALLS_PER_STEP) {
      workingMessages.push({
        role: "system",
        content: `Limite de ${AGENT_MAX_TOOL_CALLS_PER_STEP} appels d'outils par étape atteinte. Formule maintenant ta réponse finale.`
      });
    }
  }

  // Si on a atteint la borne sans réponse finale, on force une dernière synthèse
  if (!finalText) {
    workingMessages.push({
      role: "system",
      content: "Synthétise ta réponse finale à partir des résultats déjà obtenus. Pas de nouvel appel d'outil."
    });
    const finalCall = await callProviderWithTools({
      providerConfig,
      messages: workingMessages,
      tools: [], // pas de tools → contrainte de réponse texte
      images
    });
    finalText = finalCall.success
      ? normalizeMath(stripThinkTags(finalCall.message?.content || "").text)
      : "Je n'ai pas pu terminer la réponse.";
  }

  return {
    success: true,
    text: finalText,
    iterations,
    usedSources: [...usedSources],
    images: [...new Set(collectedImages)],
    videos: dedupeVideos(collectedVideos),
    toolCallTrace
  };
}

function dedupeVideos(list) {
  const seen = new Set();
  return list.filter((v) => {
    if (!v?.videoId || seen.has(v.videoId)) return false;
    seen.add(v.videoId);
    return true;
  });
}

// ================================================================================
// §2.10 — APPELS LLM NATIFS (tools + tool_calls)
// ================================================================================

/** Enveloppe de retry/rotation immédiate par provider (respecte Partie 1). */
async function callProviderWithTools({ providerConfig, messages, tools = null, images = null, jsonMode = false }) {
  const providerName = providerConfig.provider;
  const providerInfo = LLM_PROVIDERS[providerName.toUpperCase()];
  if (!providerInfo) return { success: false, error: new Error(`Provider ${providerName} inconnu`) };

  let model = providerConfig.model;
  if (providerName === "openrouter") {
    model = validateAndSanitizeOpenRouterModel(model);
    if (!model) return { success: false, error: new Error("Modèle OpenRouter invalide") };
  }

  const keyList = providerName === "gemini"
    ? [{ apiKey: null, label: "gemini_global" }]
    : providerInfo.keyPool;

  if (keyList.length === 0) return { success: false, error: new Error(`Aucune clé pour ${providerName}`) };

  let lastError = null;

  for (const keyEntry of keyList) {
    const cb = getCircuit(providerName, model, keyEntry.label);

    try {
      const result = await cb.execute(() => {
        return callProviderRawWithTools({
          provider: providerName,
          model,
          messages,
          tools,
          jsonMode,
          timeout: providerConfig.timeout || providerInfo.defaultTimeout,
          maxTokens: providerConfig.maxTokens || providerInfo.maxTokens,
          temperature: providerConfig.temperature ?? providerInfo.temperature,
          images,
          apiKey: keyEntry.apiKey,
          reasoningEffort: providerConfig.reasoningEffort || null
        });
      });
      return { success: true, ...result, providerUsed: providerName, modelUsed: model, keyLabel: keyEntry.label };
    } catch (error) {
      lastError = error;
      const code = LLMErrorInterceptor.getErrorCode(error);

      // ⚠️ Rotation immédiate : pas de retry sur la même clé si 429/5xx/timeout/circuit.
      if (LLMErrorInterceptor.shouldRotateImmediately(error)) {
        logger.warn({ provider: providerName, model, keyLabel: keyEntry.label, code }, "Rotation immédiate");
        continue;
      }
      // Erreur non récupérable → on abandonne ce provider, on passe au suivant.
      break;
    }
  }

  return { success: false, error: lastError };
}

/**
 * Appel bas niveau.
 * - OpenAI-compatible (groq, cerebras, openrouter) : tools= paramètre, tool_calls dans réponse.
 * - Gemini : functionDeclarations + functionCall.
 */
async function callProviderRawWithTools({
  provider, model, messages, tools, jsonMode = false,
  timeout, maxTokens, temperature, images, apiKey, reasoningEffort
}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout + 500);

  try {
    if (provider === "gemini") {
      return await callGeminiRawWithTools({
        model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, signal: controller.signal
      });
    }
    return await callOpenAICompatibleRaw({
      provider, model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, apiKey, reasoningEffort, signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

/** Format OpenAI-compatible (Groq, Cerebras, OpenRouter). */
async function callOpenAICompatibleRaw({
  provider, model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, apiKey, reasoningEffort, signal
}) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ
    : provider === "cerebras" ? LLM_PROVIDERS.CEREBRAS
    : LLM_PROVIDERS.OPENROUTER;

  if (!apiKey) { const e = new Error("Clé API manquante pour " + provider); e.code = "MISSING_API_KEY"; throw e; }

  // Conversion des images (dernier message user uniquement)
  let formattedMessages = messages;
  if (images && images.length > 0) {
    const idx = messages.length - 1;
    if (messages[idx]?.role === "user") {
      const parts = [];
      if (typeof messages[idx].content === "string") parts.push({ type: "text", text: messages[idx].content });
      for (const img of images) parts.push({ type: "image_url", image_url: { url: img.dataUrl } });
      formattedMessages = [...messages.slice(0, idx), { role: "user", content: parts }];
    }
  }

  const payload = { model, messages: formattedMessages, temperature, max_tokens: maxTokens };
  if (jsonMode) payload.response_format = { type: "json_object" };
  if (reasoningEffort) payload.reasoning_effort = reasoningEffort;
  // ⚠️ Tool calling natif
  if (tools && tools.length > 0) {
    payload.tools = tools;
    payload.tool_choice = "auto";
  }

  const headers = { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" };
  if (provider === "openrouter") {
    headers["HTTP-Referer"] = HOSTING_CONFIG.domain;
    headers["X-Title"] = "Luba.ia Assistant";
  }

  const response = await axios.post(cfg.baseURL + "/chat/completions", payload, {
    headers, timeout, signal
  });

  const choice = response?.data?.choices?.[0];
  const message = choice?.message;
  if (!message) throw new Error(`Réponse ${provider} vide`);

  // ⚠️ On retourne TOUJOURS le message complet (content + tool_calls)
  return { message, raw: response.data };
}

/** Format Gemini natif (contents/parts, functionDeclarations). */
async function callGeminiRawWithTools({ model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, signal }) {
  if (!geminiClient) throw new Error("Client Gemini non initialisé");

  // ----- Conversion OpenAI → Gemini -----
  let systemInstruction = null;
  const contents = [];

  for (const msg of messages) {
    if (msg.role === "system") {
      systemInstruction = typeof msg.content === "string" ? msg.content : JSON.stringify(msg.content);
      continue;
    }
    if (msg.role === "tool") {
      // ⚠️ Gemini utilise functionResponse dans un message "user"
      let parsed;
      try { parsed = JSON.parse(msg.content || "{}"); } catch { parsed = { raw: msg.content }; }
      contents.push({
        role: "user",
        parts: [{
          functionResponse: {
            name: msg.name || "tool",
            response: typeof parsed === "object" ? parsed : { value: parsed }
          }
        }]
      });
      continue;
    }
    if (msg.role === "assistant" && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      const parts = [];
      if (msg.content) parts.push({ text: typeof msg.content === "string" ? msg.content : "" });
      for (const call of msg.tool_calls) {
        let parsedArgs;
        try { parsedArgs = JSON.parse(call.function?.arguments || "{}"); } catch { parsedArgs = {}; }
        parts.push({ functionCall: { name: call.function?.name || "tool", args: parsedArgs } });
      }
      contents.push({ role: "model", parts });
      continue;
    }

    const role = msg.role === "assistant" ? "model" : "user";
    const parts = [];
    if (typeof msg.content === "string") parts.push({ text: msg.content });
    else if (Array.isArray(msg.content)) {
      for (const part of msg.content) {
        if (part.type === "text") parts.push({ text: part.text });
        if (part.type === "image_url") {
          const [mimePrefix, base64Data] = String(part.image_url.url).split(",");
          const cleanMime = mimePrefix.match(/data:(.*?);/)?.[1] || "image/jpeg";
          parts.push({ inlineData: { mimeType: cleanMime, data: base64Data } });
        }
      }
    }
    if (parts.length) contents.push({ role, parts });
  }

  if (images && images.length > 0) {
    const last = contents[contents.length - 1];
    if (last?.role === "user") {
      for (const img of images) {
        last.parts.push({ inlineData: { mimeType: img.mimetype || "image/jpeg", data: img.base64 } });
      }
    }
  }

  const config = {
    temperature,
    maxOutputTokens: maxTokens || LLM_PROVIDERS.GEMINI.maxTokens,
    responseMimeType: jsonMode ? "application/json" : "text/plain"
  };
  if (systemInstruction) config.systemInstruction = { parts: [{ text: systemInstruction }] };
  // ⚠️ Tool calling Gemini
  if (tools && tools.length > 0) {
    config.tools = [{
      functionDeclarations: tools.map((t) => ({
        name: t.function.name,
        description: t.function.description,
        parameters: t.function.parameters
      }))
    }];
  }

  // ⚠️ AbortController réel côté Gemini via Promise.race (SDK n'accepte pas signal partout)
  const geminiPromise = geminiClient.models.generateContent({ model, contents, config });
  const timeoutPromise = new Promise((_, reject) =>
    setTimeout(() => reject(Object.assign(new Error("Timeout Gemini"), { code: "ECONNABORTED" })), timeout).unref?.()
  );

  const response = await Promise.race([geminiPromise, timeoutPromise]);

  // Reconstitution d'un message compatible OpenAI
  const candidate = response?.candidates?.[0];
  const parts = candidate?.content?.parts || [];
  let text = "";
  const toolCalls = [];
  for (const part of parts) {
    if (part.text) text += part.text;
    if (part.functionCall) {
      toolCalls.push({
        id: `call_${crypto.randomUUID()}`,
        type: "function",
        function: {
          name: part.functionCall.name,
          arguments: JSON.stringify(part.functionCall.args || {})
        }
      });
    }
  }

  const message = { role: "assistant", content: text };
  if (toolCalls.length > 0) message.tool_calls = toolCalls;

  return { message, raw: response };
}

// ================================================================================
// §2.11 — SCHEDULER DE RAPPELS (⚠️ PHASE 4 : due_at n'était jamais déclenché)
// ================================================================================

const REMINDER_TICK_MS = parseInt(process.env.REMINDER_TICK_MS || "60000", 10);

async function reminderTick() {
  try {
    const due = await dbAll(
      `SELECT id, user_id, title, notes, due_at FROM user_tasks
       WHERE status = 'pending' AND due_at IS NOT NULL AND due_at <= ?
         AND (notified_at IS NULL OR notified_at < due_at)
       ORDER BY due_at ASC LIMIT 50`,
      [Date.now()]
    );
    if (due.length === 0) return;

    for (const task of due) {
      try {
        // Marque AVANT émission (idempotent en cas de course multi-instance)
        const result = await dbRun(
          `UPDATE user_tasks SET notified_at = ? WHERE id = ? AND (notified_at IS NULL OR notified_at < due_at)`,
          [Date.now(), task.id]
        );
        if (result.changes === 0) continue; // déjà notifié

        // Enqueue la notification (WhatsApp ou event à brancher par le front)
        if (task.user_id) {
          // Fallback : on émet un event interne — le front peut aussi poller /api/tasks
          logger.info({ taskId: task.id, userId: task.user_id, title: task.title }, "🔔 Rappel échu");

          // Si un numéro WhatsApp est lié au compte, on peut envoyer :
          // (désactivé par défaut pour éviter spam ; active via REMINDER_WHATSAPP_ENABLED=true)
          if (process.env.REMINDER_WHATSAPP_ENABLED === "true") {
            const user = await dbGet("SELECT whatsapp_session_id FROM users WHERE id = ?", [task.user_id]);
            if (user?.whatsapp_session_id) {
              await queueManager.add("whatsapp-outbound", {
                userId: task.user_id,
                phoneNumber: user.whatsapp_session_id,
                message: `🔔 Rappel : ${task.title}${task.notes ? "\n" + task.notes : ""}`
              }, { attempts: 3 }).catch(() => {});
            }
          }
        }
      } catch (e) { logger.error({ err: e.message, taskId: task.id }, "Erreur rappel"); }
    }
  } catch (e) {
    logger.error({ err: e.message }, "reminderTick — erreur globale");
  }
}

setInterval(reminderTick, REMINDER_TICK_MS).unref?.();

// ================================================================================
// §2.12 — WHATSAPP DURCI
// ================================================================================
// ⚠️ PHASE 7 :
//   - creds.update sauvegarde `state.creds` COMPLET (pas le delta)
//   - Clés Signal persistées via saveCreds()
//   - loggedOut → suppression credentials locaux + Supabase + pas de boucle
//   - Liste blanche d'expéditeurs + quota
//   - Le userId WhatsApp est lié à un VRAI compte (sinon FK ON fait échouer l'INSERT)

const WHATSAPP_WHITELIST = new Set(
  (process.env.WHATSAPP_WHITELIST || "")
    .split(",").map((s) => s.trim().replace(/[^\d]/g, "")).filter(Boolean)
);

function isWhatsAppAllowed(phoneNumber) {
  if (WHATSAPP_WHITELIST.size === 0) return process.env.WHATSAPP_OPEN === "true";
  return WHATSAPP_WHITELIST.has(phoneNumber.replace(/[^\d]/g, ""));
}

function getWhatsAppCryptoKey() {
  const key = process.env.WHATSAPP_ENCRYPTION_KEY;
  const iv = process.env.WHATSAPP_ENCRYPTION_IV;

  if (!key || key.length < 32 || !iv || iv.length < 16) {
    if (CONFIG.ENV === "production") {
      throw new Error("WHATSAPP_ENCRYPTION_KEY (32+) et WHATSAPP_ENCRYPTION_IV (16+) requis en production");
    }
    logger.warn("⚠️ Clé WhatsApp par défaut — JAMAIS en prod");
  }
  return {
    key: Buffer.from((key || "dev-only-key-32-chars-minimum!!").slice(0, 32)),
    iv: Buffer.from((iv || "dev-only-iv-16ch").slice(0, 16))
  };
}

async function saveWhatsAppCredentials(userId, credentialsData) {
  if (!supabase) return false;
  try {
    const { key } = getWhatsAppCryptoKey();
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);

    let encrypted = cipher.update(JSON.stringify(credentialsData), "utf8", "hex");
    encrypted += cipher.final("hex");
    const authTag = cipher.getAuthTag().toString("hex");

    return await supabaseWriteSafe({
      table: "whatsapp_credentials",
      op: "upsert",
      payload: {
        user_id: userId,
        encrypted_data: encrypted,
        auth_tag: authTag,
        iv: iv.toString("hex"),
        updated_at: new Date().toISOString()
      },
      matchColumn: "user_id",
      idempotencyKey: `whatsapp_creds:${userId}`
    }).then((r) => r.success);
  } catch (e) {
    logger.error({ err: e.message }, "Erreur save WhatsApp creds");
    return false;
  }
}

async function loadWhatsAppCredentials(userId) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from("whatsapp_credentials")
      .select("encrypted_data, auth_tag, iv")
      .eq("user_id", userId)
      .maybeSingle();
    if (error || !data) return null;

    const { key } = getWhatsAppCryptoKey();
    const decipher = crypto.createDecipheriv("aes-256-gcm", key, Buffer.from(data.iv, "hex"));
    decipher.setAuthTag(Buffer.from(data.auth_tag, "hex"));
    let decrypted = decipher.update(data.encrypted_data, "hex", "utf8");
    decrypted += decipher.final("utf8");
    return JSON.parse(decrypted);
  } catch (e) {
    logger.error({ err: e.message }, "Erreur load WhatsApp creds");
    return null;
  }
}

async function deleteWhatsAppCredentials(userId) {
  if (!supabase) return;
  await supabaseWriteSafe({
    table: "whatsapp_credentials",
    op: "delete",
    payload: {},
    matchColumn: "user_id",
    matchValue: userId,
    idempotencyKey: `whatsapp_creds_delete:${userId}`
  }).catch(() => {});
}

function toPlainWhatsAppText(markdown) {
  return String(markdown)
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[!\[.*?\]\(.*?\)\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Résolution : WhatsApp phone → vrai userId LUBA (crée un "shadow user" si autorisé). */
async function resolveWhatsAppUser(phoneNumber) {
  const placeholderUid = `wa_${phoneNumber}`;
  const existing = await dbGet("SELECT id FROM users WHERE firebase_uid = ? OR id = ?", [placeholderUid, placeholderUid]);
  if (existing) return existing.id;
  try {
    await dbRun(
      `INSERT INTO users (id, firebase_uid, display_name, role, created_at, updated_at, whatsapp_connected, whatsapp_session_id)
       VALUES (?, ?, ?, 'FREE', ?, ?, 1, ?)`,
      [placeholderUid, placeholderUid, `WhatsApp ${phoneNumber}`, Date.now(), Date.now(), phoneNumber]
    );
    return placeholderUid;
  } catch (e) {
    logger.error({ err: e.message }, "resolveWhatsAppUser — échec insertion");
    return null;
  }
}

class BaileysManager {
  constructor() { this.sessions = new Map(); }

  async initClient(userId) {
    const existing = this.sessions.get(userId);
    if (existing?.ready) return { connected: true, qrCode: null };
    if (existing?.qrCode) return { connected: false, qrCode: existing.qrCode };

    const authDir = path.join(CONFIG.SESSIONS_PATH, userId);
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    const savedCredentials = await loadWhatsAppCredentials(userId);
    if (savedCredentials) {
      try { fs.writeFileSync(path.join(authDir, "creds.json"), JSON.stringify(savedCredentials)); }
      catch (e) { logger.error({ err: e.message }, "Erreur restauration WhatsApp"); }
    }

    const makeWASocket = require("@whiskeysockets/baileys").default;
    const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require("@whiskeysockets/baileys");

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    let version;
    try { version = (await fetchLatestBaileysVersion()).version; } catch { version = undefined; }

    const sock = makeWASocket({
      version,
      auth: state,
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      browser: ["Luba.ia", "Chrome", "15.0.0"]
    });

    const sessionData = { sock, qrCode: null, ready: false };
    this.sessions.set(userId, sessionData);

    // ⚠️ FIX v15 : sauvegarde l'état COMPLET des creds, pas le delta
    sock.ev.on("creds.update", async () => {
      try {
        await saveCreds();
        // ⚠️ On lit l'état complet depuis le fichier (contient creds + clés Signal)
        const completeCredsPath = path.join(authDir, "creds.json");
        if (fs.existsSync(completeCredsPath)) {
          const completeCreds = JSON.parse(fs.readFileSync(completeCredsPath, "utf8"));
          await saveWhatsAppCredentials(userId, completeCreds);
        }
      } catch (e) { logger.error({ err: e.message }, "Erreur creds.update"); }
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          sessionData.qrCode = await qrcode.toDataURL(qr, { width: 600, margin: 2 });
          logger.info({ userId }, "QR WhatsApp généré");
        } catch (e) { logger.error({ err: e.message }, "Erreur QR"); }
      }

      if (connection === "open") {
        sessionData.ready = true;
        sessionData.qrCode = null;
        await dbRun("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]).catch(() => {});
        logger.info({ userId }, "WhatsApp connecté");
      }

      if (connection === "close") {
        sessionData.ready = false;
        await dbRun("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]).catch(() => {});

        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const isLoggedOut = statusCode === DisconnectReason.loggedOut;

        this.sessions.delete(userId);

        if (isLoggedOut) {
          // ⚠️ PHASE 7 : suppression des credentials locaux + Supabase, pas de reconnexion
          try {
            const authDir2 = path.join(CONFIG.SESSIONS_PATH, userId);
            if (fs.existsSync(authDir2)) fs.rmSync(authDir2, { recursive: true, force: true });
            await deleteWhatsAppCredentials(userId);
          } catch (e) { logger.warn({ err: e.message }, "Nettoyage post-logout"); }
          logger.info({ userId }, "WhatsApp déconnecté (loggedOut) — pas de reconnexion");
        } else {
          // Reconnexion avec backoff
          setTimeout(() => {
            this.initClient(userId).catch((e) => logger.error({ err: e.message }, "Erreur reconnexion WhatsApp"));
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

          const text = msg.message.conversation
            || msg.message.extendedTextMessage?.text
            || msg.message.imageMessage?.caption
            || null;
          if (!text) continue;

          const phoneNumber = remoteJid.replace(/@.*$/, "");

          // ⚠️ PHASE 7 : liste blanche + quota
          if (!isWhatsAppAllowed(phoneNumber)) {
            logger.info({ phoneNumber }, "WhatsApp : expéditeur hors liste blanche");
            continue;
          }

          // ⚠️ PHASE 7 : userId WhatsApp lié à un vrai compte (shadow user OK)
          const waUserId = await resolveWhatsAppUser(phoneNumber);
          if (!waUserId) {
            logger.warn({ phoneNumber }, "WhatsApp : impossible de résoudre/créer l'utilisateur");
            continue;
          }

          // ⚠️ Quota WhatsApp (messages entrants comptent aussi)
          const quota = await checkUserQuota(waUserId, "whatsapp");
          if (!quota.allowed) {
            await sock.sendMessage(remoteJid, { text: "Limite de messages atteinte pour aujourd'hui. Réessayez demain." });
            continue;
          }
          await incrementUserQuota(waUserId, "whatsapp");

          const conversationId = `whatsapp_${phoneNumber}`;
          const result = await handleChat({
            conversationId,
            userId: waUserId,
            firebaseUid: null,
            message: String(text).slice(0, CONFIG.MAX_MESSAGE_LENGTH),
            channel: "whatsapp",
            modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(remoteJid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (e) { logger.error({ err: e.message }, "Erreur traitement message WhatsApp"); }
      }
    });

    return { connected: false, qrCode: null };
  }

  async sendMessage(userId, to, message) {
    const session = this.sessions.get(userId);
    if (!session || !session.ready) {
      const e = new Error("WhatsApp non connecté");
      e.code = "WHATSAPP_NOT_CONNECTED";
      throw e;
    }
    const cleanNumber = String(to).replace(/[^\d]/g, "");
    if (!cleanNumber) {
      const e = new Error("Numéro destinataire invalide");
      e.code = "INVALID_RECIPIENT";
      throw e;
    }
    const jid = `${cleanNumber}@s.whatsapp.net`;
    await session.sock.sendMessage(jid, { text: message });
    return { success: true, to: cleanNumber };
  }

  getQRCode(userId) { return this.sessions.get(userId)?.qrCode || null; }

  async destroyAll() {
    for (const [userId, session] of this.sessions) {
      try { session.sock.end(undefined); }
      catch (e) { logger.error({ err: e.message }, `Erreur fermeture WhatsApp (${userId})`); }
    }
  }
}

const whatsappManager = new BaileysManager();

queueManager.createQueue("whatsapp-outbound", async (job) => {
  const data = job?.data ?? job;
  const { userId, phoneNumber, message } = data;
  await whatsappManager.sendMessage(userId, phoneNumber, message);
}, { concurrency: 3, limiter: { max: 10, duration: 1000 } });

async function sendWhatsAppSmart(userId, phoneNumber, message) {
  const quota = await checkUserQuota(userId, "whatsapp");
  if (!quota.allowed) throw new Error(quota.message || "Limite WhatsApp atteinte");
  await queueManager.add("whatsapp-outbound", { userId, phoneNumber, message }, { attempts: 4, backoffDelay: 2000 });
  await incrementUserQuota(userId, "whatsapp");
  return { success: true, queued: true };
}

// ================================================================================
// §2.13 — EXPORTS COMPLÉMENTAIRES (Partie 2)
// ================================================================================

Object.assign(module.exports, {
  // Middlewares
  mountBodyParsers,
  apiLimiter, strictLimiter, chatLimiter, authLimiter, toolLimiter,
  authenticateUser, requireRole,

  // Tool calling
  TOOL_SCHEMAS, TOOLS_BY_CONTEXT, SIDE_EFFECT_TOOLS,
  getToolSchemas, validateToolArgs, executeToolNative,
  callProviderWithTools, callProviderRawWithTools, runToolLoop,

  // Confirmation
  queueConfirmation, consumeConfirmation, buildConfirmationPrompt, pendingConfirmations,

  // Sandbox
  runCodeSandbox,

  // Scheduler
  reminderTick,

  // WhatsApp
  whatsappManager, sendWhatsAppSmart, isWhatsAppAllowed,
  saveWhatsAppCredentials, loadWhatsAppCredentials, deleteWhatsAppCredentials,
  toPlainWhatsAppText, resolveWhatsAppUser
});

// ================================================================================
// ==================== FIN PARTIE 2 ============================================
// ================================================================================
// Ce que la Partie 3 va ajouter :
//   - handleChat réécrit : non-bloquant, sans doublon d'historique, sans double
//     exécution d'outils, avec SSE opt-in (events status/images/videos/token/
//     suggestions/sources/done).
//   - Vision via callProviderWithTools + Gemini natif.
//   - searchImagesWithFallback v15 (allSettled, cache conditionnel, UA Wikimedia).
//   - searchDuckDuckGoImages via ddgScrape.searchImages.
//   - Mémoire long terme (runMemorySummary) branchée sur queueManager.
//   - Hydratation Supabase → SQLite au démarrage.
//   - getFullHistory filtré par user_id dans les deux branches.
//   - assertConversationOwnership étendu à Supabase.
// ================================================================================
// ================================================================================
// ==================== PARTIE 3 : handleChat v15 · SSE · IMAGES · MÉMOIRE ======
// ================================================================================
// Sommaire :
//   §3.1  Utilitaires de streaming SSE
//   §3.2  searchImagesWithFallback v15 (allSettled, cache conditionnel, UA WM)
//   §3.3  searchDuckDuckGoImages via searchImages
//   §3.4  Search helpers (web, news, sport, science, social, météo, YouTube)
//   §3.5  Wikipedia summary
//   §3.6  Tasks CRUD v15 (UUID, Supabase-first, outbox)
//   §3.7  Mémoire long terme (runMemorySummary branchée sur queueManager)
//   §3.8  Sessions + ActiveIntent (expiration 5 min, annulation)
//   §3.9  Historique : getFullHistory filtré user_id (local + Supabase)
//   §3.10 assertConversationOwnership étendu à Supabase
//   §3.11 saveMessageWithUser v15 (Supabase-first, outbox, pas de doublon)
//   §3.12 Email dispatch (Gmail échappé, Resend, SMTP)
//   §3.13 Whisper transcription
//   §3.14 handleChat v15 — non bloquant, parallélisé, sans doublons
//   §3.15 handleChatStreaming — SSE opt-in
//   §3.16 handleActiveIntent — annulation + expiration
//   §3.17 Hydratation Supabase → SQLite
//   §3.18 Exports
// ================================================================================

const { PassThrough } = require("stream");

// ================================================================================
// §3.1 — SSE UTILITIES
// ================================================================================
// Contrat d'événements :
//   event: status       data: { stage, message? }
//   event: images       data: { images: [{url,title,source}] }
//   event: videos       data: { videos: [{videoId,title,url,embedUrl,...}] }
//   event: token        data: { text: "chunk utf8-safe" }
//   event: suggestions  data: { suggestions: ["...","...","..."] }
//   event: sources      data: { sources: [{name,url,logo}] }
//   event: error        data: { reply, code }       (code interne, pas exposé au user)
//   event: done         data: { conversationId, isNewConversation, providerUsed, modelTier,
//                               degraded, visionEnabled, intent, contextLength, messageId? }

class SSEWriter {
  constructor(res) {
    this.res = res;
    this.closed = false;
    this.res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    this.res.setHeader("Cache-Control", "no-cache, no-transform");
    this.res.setHeader("Connection", "keep-alive");
    this.res.setHeader("X-Accel-Buffering", "no"); // Nginx : pas de buffering
    this.res.flushHeaders?.();
    this.res.write(":ok\n\n"); // commentaire initial → force le flush côté proxy

    this.res.on("close", () => { this.closed = true; });
  }

  /** Écrit un événement SSE typé. Les retours \n internes sont encodés. */
  send(event, data) {
    if (this.closed) return false;
    try {
      const payload = typeof data === "string" ? data : JSON.stringify(data ?? {});
      // Chaque ligne "data:" ; SSE tolère des \n dans data si chaque ligne commence par data:
      const safe = payload.replace(/\r/g, "").split("\n").map(l => `data: ${l}`).join("\n");
      this.res.write(`event: ${event}\n${safe}\n\n`);
      return true;
    } catch (e) {
      this.closed = true;
      return false;
    }
  }

  status(stage, extra = {}) { this.send("status", { stage, ...extra }); }
  token(text)               { if (text) this.send("token", { text }); }
  images(list)              { if (list?.length) this.send("images", { images: list }); }
  videos(list)              { if (list?.length) this.send("videos", { videos: list }); }
  suggestions(list)         { if (list?.length) this.send("suggestions", { suggestions: list }); }
  sources(list)             { if (list?.length) this.send("sources", { sources: list }); }
  error(payload)            { this.send("error", payload); }
  done(payload)             { this.send("done", payload); }

  end() {
    if (this.closed) return;
    try { this.res.end(); } catch {}
    this.closed = true;
  }
}

/**
 * Découpe un texte en chunks sûrs (UTF-8) pour le streaming token par token.
 * On ne coupe jamais au milieu d'une séquence UTF-8 multi-octets.
 */
function safeChunkText(text, targetSize = 24) {
  if (!text) return [];
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + targetSize, text.length);
    // Évite de couper une paire de substitution (surrogate pair)
    const code = text.charCodeAt(end - 1);
    if (code >= 0xD800 && code <= 0xDBFF && end < text.length) end++;
    chunks.push(text.slice(i, end));
    i = end;
  }
  return chunks;
}

/**
 * Stream progressif : au lieu d'envoyer la réponse d'un coup, on simule un flux
 * naturel de tokens à partir du texte final. Utile quand le provider ne streame
 * pas nativement (Groq/Cerebras/OpenRouter en mode non-stream).
 */
async function streamTextAsTokens(sse, text, { paceMs = 8, chunkSize = 24 } = {}) {
  const chunks = safeChunkText(text, chunkSize);
  for (const chunk of chunks) {
    if (sse.closed) return;
    sse.token(chunk);
    if (paceMs > 0) await sleep(paceMs);
  }
}

// ================================================================================
// §3.2 — searchImagesWithFallback v15
// ================================================================================
// ⚠️ PHASE 5 :
//   - allSettled avec deadline PAR SOURCE (une source lente ne jette pas tout)
//   - Ne met JAMAIS en cache un résultat vide
//   - Commons : gsrnamespace=6, timeout ≥ 4s, thumburl API (jamais reconstruit)
//   - UA Wikimedia conforme (URL + contact)

const IMAGE_CACHE_KEY = (q) => `img:${String(q).toLowerCase().trim()}`;

async function searchWikimediaImagesV15(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };
  try {
    const url =
      "https://commons.wikimedia.org/w/api.php" +
      "?action=query&generator=search" +
      `&gsrsearch=${encodeURIComponent(query)}` +
      "&gsrnamespace=6" +                 // ⚠️ namespace Fichier uniquement
      `&gsrlimit=${Math.min(limit, 10)}` +
      "&prop=imageinfo&iiprop=url|extmetadata" +
      "&iiurlwidth=1280" +
      "&format=json&origin=*";

    const response = await axios.get(url, {
      timeout: CONFIG.IMAGE_SOURCE_DEADLINE_MS,
      headers: { "User-Agent": CONFIG.WIKIMEDIA_USER_AGENT }
    });

    const pages = response.data?.query?.pages;
    if (!pages) return { images: [] };

    const images = Object.values(pages)
      .map((page) => {
        const info = page.imageinfo?.[0];
        if (!info) return null;
        // ⚠️ On utilise TOUJOURS le thumburl fourni par l'API (paliers standard Wikimedia)
        const url = info.thumburl || info.url || null;
        if (!url) return null;
        return {
          url,
          title: page.title || "Image",
          description: (info.extmetadata?.ImageDescription?.value || "")
            .replace(/<[^>]*>/g, "").slice(0, 200) || null,
          pageUrl: info.descriptionurl || null,
          source: "wikimediacommons"
        };
      })
      .filter(Boolean);

    return { images };
  } catch (e) {
    logger.warn({ err: e.message, query }, "Wikimedia échec");
    return { images: [] };
  }
}

async function fetchWikipediaThumb(query) {
  try {
    const response = await axios.get(
      `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      { timeout: CONFIG.IMAGE_SOURCE_DEADLINE_MS, headers: { "User-Agent": CONFIG.WIKIMEDIA_USER_AGENT } }
    );
    const thumb = response.data?.thumbnail?.source || response.data?.originalimage?.source;
    if (!thumb) return null;
    return {
      url: thumb,
      title: response.data.title || query,
      description: response.data.extract ? response.data.extract.slice(0, 200) : null,
      pageUrl: response.data.content_urls?.desktop?.page || null,
      source: "wikipedia"
    };
  } catch { return null; }
}

async function searchImagesWithFallback(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  const cleanQuery = extractEntity(query) || String(query || "").trim();
  if (!cleanQuery) return { images: [] };

  const cacheKey = IMAGE_CACHE_KEY(cleanQuery);
  const cached = getCachedImages(cacheKey);
  if (cached) return cached;

  // ⚠️ Une deadline PAR source via allSettledWithDeadline : une source lente
  // n'annule pas les autres.
  const [commonsR, wikiR, ddgR] = await allSettledWithDeadline([
    searchWikimediaImagesV15(cleanQuery, limit),
    fetchWikipediaThumb(cleanQuery),
    searchDuckDuckGoImages(cleanQuery, Math.min(4, limit))
  ], CONFIG.IMAGE_SOURCE_DEADLINE_MS);

  const commonsImages = commonsR.status === "fulfilled" ? (commonsR.value?.images || []) : [];
  const wikiImage = (wikiR.status === "fulfilled" && wikiR.value) ? wikiR.value : null;
  const ddgImages = ddgR.status === "fulfilled" ? (ddgR.value || []) : [];

  const all = [
    ...commonsImages,
    ...(wikiImage ? [wikiImage] : []),
    ...ddgImages
  ];

  // Dédoublonnage par URL canonique
  const seen = new Set();
  const unique = all.filter((img) => {
    if (!img?.url || seen.has(img.url)) return false;
    seen.add(img.url);
    return true;
  }).slice(0, limit);

  const result = { images: unique };
  // ⚠️ setCachedImages refuse le vide (déjà dans Partie 1).
  setCachedImages(cacheKey, result);
  if (unique.length > 0) logger.info({ query: cleanQuery, count: unique.length }, "Images trouvées");
  return result;
}

// ================================================================================
// §3.3 — searchDuckDuckGoImages (⚠️ ddgScrape.searchImages, pas search)
// ================================================================================

async function searchDuckDuckGoImages(query, limit = 4) {
  if (!ddgScrape) return [];
  try {
    const fn = ddgScrape.searchImages || ddgScrape.images;
    if (typeof fn !== "function") return [];
    const results = await fn(query, { safeSearch: "moderate" });
    const list = Array.isArray(results?.results) ? results.results : Array.isArray(results) ? results : [];
    return list
      .filter((r) => r?.image)
      .slice(0, limit)
      .map((r) => ({
        url: r.image,
        title: r.title || query,
        description: r.description || null,
        pageUrl: r.url || null,
        source: "duckduckgo"
      }));
  } catch (e) {
    logger.warn({ err: e.message, query }, "DDG images échec");
    return [];
  }
}

// ================================================================================
// §3.4 — SEARCH HELPERS
// ================================================================================

async function searchWikipediaSummary(query) {
  try {
    const response = await axios.get(
      `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      { timeout: 6000, headers: { "User-Agent": CONFIG.WIKIMEDIA_USER_AGENT } }
    );
    if (response.data?.type === "disambiguation" || !response.data?.extract) return { summary: null };
    return {
      title: response.data.title,
      summary: response.data.extract,
      url: response.data.content_urls?.desktop?.page
    };
  } catch { return { summary: null }; }
}

async function searchDuckDuckGo(query) {
  if (!ddgScrape) return [];
  try {
    const fn = ddgScrape.search;
    if (typeof fn !== "function") return [];
    const results = await fn(query, { safeSearch: "moderate", locale: "fr-fr", maxResults: 5 });
    const list = Array.isArray(results?.results) ? results.results : Array.isArray(results) ? results : [];
    return list.map((r) => ({ title: r.title, snippet: r.description, url: r.url, source: r.source }));
  } catch (e) {
    logger.warn({ err: e.message }, "DDG web échec");
    return [];
  }
}

async function searchWeb(query) {
  if (!query || typeof query !== "string") return { results: [], sourcesUsed: [] };
  const [wiki, news, ddg] = await Promise.all([
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
  for (const r of ddg) results.push({ title: r.title, snippet: r.snippet, url: r.url, type: "web" });
  if (news.articles?.length > 0) {
    news.articles.slice(0, 3).forEach((a) => results.push({ title: a.title, url: a.link, pubDate: a.pubDate, type: "news" }));
    sourcesUsed.push("googlenews");
  }
  return { results, sourcesUsed };
}

async function searchNews(query) {
  if (!query) return { articles: [] };
  try {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=fr&gl=FR&ceid=FR:fr`;
    const response = await axios.get(url, { timeout: 8000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const xml = response.data || "";
    const items = [];
    const re = /<item>([\s\S]*?)<\/item>/g;
    let match;
    while ((match = re.exec(xml)) !== null && items.length < 6) {
      const block = match[1];
      const title = (block.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const link = (block.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || "";
      const pubDate = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || "";
      const description = (block.match(/<description>([\s\S]*?)<\/description>/) || [])[1] || "";
      if (title) items.push({ title: decodeXmlEntities(title), link: link.trim(), pubDate, description: decodeXmlEntities(description) });
    }
    return { articles: items };
  } catch (e) {
    logger.warn({ err: e.message }, "Actualités échec");
    return { articles: [] };
  }
}

async function searchSportsScores(team) {
  if (!team) return { events: [], error: "Aucune équipe précisée" };
  try {
    const searchResp = await axios.get(
      `https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t=${encodeURIComponent(team)}`,
      { timeout: 7000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } }
    );
    const t = searchResp.data?.teams?.[0];
    if (!t) return { events: [], error: `Équipe "${team}" introuvable` };
    const eventsResp = await axios.get(
      `https://www.thesportsdb.com/api/v1/json/3/eventslast.php?id=${t.idTeam}`,
      { timeout: 7000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } }
    );
    const events = (eventsResp.data?.results || []).slice(0, 5).map((e) => ({
      match: `${e.strHomeTeam} ${e.intHomeScore ?? "?"} - ${e.intAwayScore ?? "?"} ${e.strAwayTeam}`,
      date: e.dateEvent, league: e.strLeague
    }));
    return { team: t.strTeam, events };
  } catch (e) { return { events: [], error: e.message }; }
}

async function searchScience(query) {
  if (!query) return { papers: [] };
  try {
    const url = `http://export.arxiv.org/api/query?search_query=all:${encodeURIComponent(query)}&start=0&max_results=5`;
    const response = await axios.get(url, { timeout: 8000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const xml = response.data || "";
    const items = [];
    const re = /<entry>([\s\S]*?)<\/entry>/g;
    let m;
    while ((m = re.exec(xml)) !== null && items.length < 5) {
      const block = m[1];
      const title = (block.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const summary = (block.match(/<summary>([\s\S]*?)<\/summary>/) || [])[1] || "";
      const link = (block.match(/<id>([\s\S]*?)<\/id>/) || [])[1] || "";
      if (title) items.push({ title: decodeXmlEntities(title), summary: decodeXmlEntities(summary).slice(0, 300), link: link.trim() });
    }
    return { papers: items };
  } catch { return { papers: [] }; }
}

async function searchSocial(query) {
  if (!query) return { posts: [] };
  try {
    const response = await axios.get(
      `https://www.reddit.com/search.json?q=${encodeURIComponent(query)}&limit=6&sort=relevance`,
      { timeout: 8000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } }
    );
    const posts = (response.data?.data?.children || []).map((c) => ({
      title: c.data.title, subreddit: c.data.subreddit_name_prefixed,
      score: c.data.score, url: `https://reddit.com${c.data.permalink}`
    }));
    return { posts };
  } catch { return { posts: [] }; }
}

async function getWeather(location) {
  if (!location) return { error: "Aucun lieu précisé" };
  try {
    const geo = await axios.get(
      `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(location)}&count=1&language=fr`,
      { timeout: 6000 }
    );
    const place = geo.data?.results?.[0];
    if (!place) return { error: `Lieu "${location}" introuvable` };
    const weather = await axios.get(
      `https://api.open-meteo.com/v1/forecast?latitude=${place.latitude}&longitude=${place.longitude}` +
      `&current=temperature_2m,weather_code,wind_speed_10m&timezone=auto`,
      { timeout: 6000 }
    );
    const c = weather.data?.current;
    return {
      location: `${place.name}, ${place.country}`,
      temperature: c?.temperature_2m,
      windSpeed: c?.wind_speed_10m,
      weatherCode: c?.weather_code
    };
  } catch (e) { return { error: e.message }; }
}

function extractYouTubeVideoId(url) {
  if (!url) return null;
  const m = String(url).match(/(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : null;
}

async function searchYouTube(query) {
  if (!query || typeof query !== "string") return { videos: [] };
  if (process.env.YOUTUBE_API_KEY) {
    try {
      const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=5&q=${encodeURIComponent(query)}&key=${process.env.YOUTUBE_API_KEY}`;
      const response = await axios.get(url, { timeout: 8000 });
      const videos = (response.data?.items || []).map((item) => ({
        videoId: item.id?.videoId,
        title: decodeXmlEntities(item.snippet?.title || ""),
        channel: item.snippet?.channelTitle || null,
        thumbnail: item.snippet?.thumbnails?.high?.url || null,
        publishedAt: item.snippet?.publishedAt || null,
        url: item.id?.videoId ? `https://www.youtube.com/watch?v=${item.id.videoId}` : null
      })).filter((v) => v.videoId);
      return { videos };
    } catch (e) {
      logger.warn({ err: e.message }, "YouTube API échec → repli DDG");
    }
  }
  try {
    const results = await searchDuckDuckGo(`site:youtube.com ${query}`);
    const videos = results.map((r) => {
      const videoId = extractYouTubeVideoId(r.url);
      if (!videoId) return null;
      return {
        videoId, title: r.title, channel: r.source || null,
        thumbnail: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
        publishedAt: null, url: `https://www.youtube.com/watch?v=${videoId}`
      };
    }).filter(Boolean).slice(0, 5);
    return { videos };
  } catch { return { videos: [] }; }
}

async function scrapeArticleContent(url) {
  try {
    const response = await axios.get(url, { timeout: 10000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const $ = cheerio.load(response.data);
    const title = $("title").text().trim();
    const paragraphs = [];
    $("p").each((_, el) => {
      const t = $(el).text().trim();
      if (t.length > 50) paragraphs.push(t);
    });
    return { title, content: paragraphs.slice(0, 5).join("\n\n").slice(0, 2000), url };
  } catch (e) { return { error: e.message, url }; }
}

// ================================================================================
// §3.6 — TASKS CRUD v15 (UUID · Supabase-first · outbox)
// ================================================================================

async function createTask(userId, { title, notes = null, dueAt = null }) {
  if (!title || typeof title !== "string" || !title.trim()) {
    return { success: false, error: "Le titre est obligatoire" };
  }
  const cleanTitle = sanitizeStrict(title, 200);
  const cleanNotes = notes ? sanitizeStrict(notes, 2000) : null;
  if (dueAt !== null && dueAt !== undefined && !Number.isFinite(Number(dueAt))) {
    return { success: false, error: "Date invalide" };
  }

  const id = generateTaskId();
  const now = Date.now();
  const dueAtMs = dueAt ? Number(dueAt) : null;

  await dbRun(
    `INSERT INTO user_tasks (id, user_id, title, notes, due_at, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`,
    [id, userId, cleanTitle, cleanNotes, dueAtMs, now, now]
  );

  const task = await dbGet(`SELECT * FROM user_tasks WHERE id = ?`, [id]);

  if (supabase) {
    await supabaseWriteSafe({
      table: "user_tasks",
      op: "upsert",
      payload: {
        id, user_id: userId, title: cleanTitle, notes: cleanNotes,
        due_at: dueAtMs ? new Date(dueAtMs).toISOString() : null,
        status: "pending",
        created_at: new Date(now).toISOString(),
        updated_at: new Date(now).toISOString()
      },
      matchColumn: "id",
      idempotencyKey: `task:${id}`
    });
  }

  return { success: true, task };
}

async function listTasks(userId, { status = null } = {}) {
  // ⚠️ Lecture : Supabase d'abord (source de vérité), repli local.
  if (supabase) {
    try {
      let q = supabase.from("user_tasks").select("*").eq("user_id", userId).order("due_at", { ascending: true, nullsFirst: false }).limit(50);
      if (status) q = q.eq("status", status);
      const { data, error } = await q;
      if (!error && Array.isArray(data)) {
        // Miroir local best-effort
        for (const t of data) {
          try {
            await dbRun(
              `INSERT INTO user_tasks (id, user_id, title, notes, due_at, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 title = excluded.title, notes = excluded.notes, due_at = excluded.due_at,
                 status = excluded.status, updated_at = excluded.updated_at`,
              [
                t.id, t.user_id, t.title, t.notes || null,
                t.due_at ? Date.parse(t.due_at) : null,
                t.status || "pending",
                t.created_at ? Date.parse(t.created_at) : Date.now(),
                t.updated_at ? Date.parse(t.updated_at) : Date.now()
              ]
            );
          } catch {}
        }
        return { success: true, tasks: data };
      }
    } catch (e) { logger.warn({ err: e.message }, "listTasks Supabase → repli local"); }
  }

  let q = `SELECT * FROM user_tasks WHERE user_id = ?`;
  const params = [userId];
  if (status) { q += ` AND status = ?`; params.push(status); }
  q += ` ORDER BY (due_at IS NULL), due_at ASC, created_at DESC LIMIT 50`;
  const tasks = await dbAll(q, params);
  return { success: true, tasks };
}

async function updateTaskStatus(userId, taskId, status) {
  if (!["pending", "done"].includes(status)) return { success: false, error: "Statut invalide" };

  await dbRun(
    `UPDATE user_tasks SET status = ?, updated_at = ? WHERE id = ? AND user_id = ?`,
    [status, Date.now(), taskId, userId]
  );

  if (supabase) {
    await supabaseWriteSafe({
      table: "user_tasks",
      op: "update",
      payload: { status, updated_at: new Date().toISOString() },
      matchColumn: "id",
      matchValue: taskId,
      idempotencyKey: `task_status:${taskId}:${status}:${Date.now()}`
    });
  }

  const task = await dbGet(`SELECT * FROM user_tasks WHERE id = ? AND user_id = ?`, [taskId, userId]);
  return { success: Boolean(task), task };
}

async function deleteTask(userId, taskId) {
  await dbRun(`DELETE FROM user_tasks WHERE id = ? AND user_id = ?`, [taskId, userId]);
  if (supabase) {
    await supabaseWriteSafe({
      table: "user_tasks",
      op: "delete",
      payload: {},
      matchColumn: "id",
      matchValue: taskId,
      idempotencyKey: `task_delete:${taskId}`
    });
  }
  return { success: true };
}

// ================================================================================
// §3.7 — MÉMOIRE LONG TERME (file bornée)
// ================================================================================
// ⚠️ PHASE 1 : jamais en concurrence avec le chat, concurrency=1, interval mini 2 s.

const USER_MEMORY_UPDATE_EVERY_N_MESSAGES = parseInt(process.env.USER_MEMORY_UPDATE_EVERY_N_MESSAGES || "6", 10);

async function getUserMemory(userId) {
  // Supabase d'abord
  if (supabase) {
    try {
      const { data, error } = await supabase.from("user_memory").select("summary").eq("user_id", userId).maybeSingle();
      if (!error && data) return data.summary || "";
    } catch {}
  }
  const row = await dbGet("SELECT summary FROM user_memory WHERE user_id = ?", [userId]);
  return row?.summary || "";
}

async function saveUserMemory(userId, summary, messagesSinceUpdate = 0) {
  await dbRun(
    `INSERT INTO user_memory (user_id, summary, messages_since_update, updated_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(user_id) DO UPDATE SET
       summary = excluded.summary,
       messages_since_update = excluded.messages_since_update,
       updated_at = excluded.updated_at`,
    [userId, summary, messagesSinceUpdate, Date.now()]
  );

  if (supabase) {
    await supabaseWriteSafe({
      table: "user_memory",
      op: "upsert",
      payload: {
        user_id: userId, summary, messages_since_update: messagesSinceUpdate,
        updated_at: new Date().toISOString()
      },
      matchColumn: "user_id",
      idempotencyKey: `memory:${userId}`
    });
  }
}

async function incrementUserMemoryCounter(userId) {
  const row = await dbGet("SELECT messages_since_update FROM user_memory WHERE user_id = ?", [userId]);
  const count = (row?.messages_since_update || 0) + 1;
  await dbRun(
    `INSERT INTO user_memory (user_id, summary, messages_since_update, updated_at)
     VALUES (?, '', ?, ?)
     ON CONFLICT(user_id) DO UPDATE SET
       messages_since_update = excluded.messages_since_update,
       updated_at = excluded.updated_at`,
    [userId, count, Date.now()]
  );
  return count;
}

/** Vraie implémentation, branchée sur queueManager ("memory-summary", concurrency=1). */
async function runMemorySummaryImpl({ userId, lastUserMessage, lastAssistantReply }) {
  try {
    const previousSummary = await getUserMemory(userId);
    const summarizationMessages = [
      {
        role: "system",
        content: [
          "Tu mets à jour une mémoire long terme compacte sur un utilisateur, pour un assistant IA.",
          "Résume en 5 à 8 lignes MAXIMUM les faits durables et utiles : prénom/surnom, préférences, projets en cours, sujets récurrents, contexte professionnel.",
          "N'invente rien. Ignore les détails ponctuels sans intérêt à long terme.",
          'Réponds STRICTEMENT au format JSON : {"summary": "le résumé mis à jour ici"}'
        ].join("\n")
      },
      {
        role: "user",
        content:
          `RÉSUMÉ ACTUEL :\n${previousSummary || "(aucun)"}\n\n` +
          `DERNIER ÉCHANGE :\nUtilisateur: ${String(lastUserMessage).slice(0, 800)}\n` +
          `Assistant: ${String(lastAssistantReply).slice(0, 800)}\n\n` +
          `Donne le résumé mis à jour au format JSON :`
      }
    ];

    const provider = MODEL_TIERS.v100.providers[0];
    const result = await callProviderWithTools({
      providerConfig: provider,
      messages: summarizationMessages,
      tools: null,
      jsonMode: true
    });

    if (result.success) {
      let parsed = null;
      const content = result.message?.content || "";
      try { parsed = JSON.parse(content); }
      catch { const m = content.match(/\{[\s\S]*\}/); if (m) try { parsed = JSON.parse(m[0]); } catch {} }
      if (parsed && typeof parsed.summary === "string") {
        await saveUserMemory(userId, parsed.summary.slice(0, 2000).trim(), 0);
        logger.info({ userId }, "🧠 Mémoire long terme mise à jour");
        return;
      }
    }
    // Repli : reset du compteur pour ne pas rester coincé
    await saveUserMemory(userId, previousSummary, 0);
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur runMemorySummary");
  }
}

// Branche la vraie implémentation dans le queueManager (Partie 1 a un stub)
if (typeof module.exports.setRunMemorySummary === "function") {
  module.exports.setRunMemorySummary(runMemorySummaryImpl);
}
// Fallback : si le stub de la Partie 1 est dans la même portée, on remplace.
try { runMemorySummary = runMemorySummaryImpl; } catch {}

/** Déclenche (sans attendre) le résumé mémoire toutes N interactions. */
async function maybeUpdateUserMemoryAsync(userId, lastUserMessage, lastAssistantReply) {
  try {
    const count = await incrementUserMemoryCounter(userId);
    if (count < USER_MEMORY_UPDATE_EVERY_N_MESSAGES) return;
    await queueManager.add("memory-summary", {
      userId, lastUserMessage, lastAssistantReply
    }, { attempts: 2, backoffDelay: 3000 });
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur déclenchement mémoire");
  }
}

// ================================================================================
// §3.8 — SESSIONS + ACTIVE INTENT (expiration, annulation)
// ================================================================================

const ACTIVE_INTENT_TTL_MS = 5 * 60 * 1000;

async function getSession(conversationId, userId, firebaseUid = null) {
  // Un seul aller-retour Supabase (pas 3 comme en v14).
  const local = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);
  if (local) {
    if (local.user_id !== userId && local.firebase_uid !== userId) {
      const err = new Error("Conversation non autorisée");
      err.code = "CONVERSATION_OWNERSHIP";
      throw err;
    }
    await dbRun("UPDATE sessions SET updated_at = ? WHERE session_id = ?", [Date.now(), conversationId]);
    return local;
  }

  // Insertion locale
  await dbRun(
    `INSERT INTO sessions (session_id, user_id, firebase_uid, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
    [conversationId, userId, firebaseUid || userId, Date.now(), Date.now()]
  );

  // Sync Supabase en arrière-plan
  if (supabase) {
    setImmediate(() => {
      supabaseWriteSafe({
        table: "sessions",
        op: "upsert",
        payload: {
          session_id: conversationId, user_id: userId,
          firebase_uid: firebaseUid || userId,
          updated_at: new Date().toISOString()
        },
        matchColumn: "session_id",
        idempotencyKey: `session:${conversationId}`
      }).catch(() => {});
    });
  }

  return { session_id: conversationId, user_id: userId, firebase_uid: firebaseUid || userId };
}

async function setActiveIntent(conversationId, intentType, intentData = {}) {
  await dbRun(
    `UPDATE sessions SET active_intent = ?, intent_data = ?, intent_expires_at = ? WHERE session_id = ?`,
    [intentType, JSON.stringify(intentData), Date.now() + ACTIVE_INTENT_TTL_MS, conversationId]
  );
}

async function getActiveIntent(conversationId) {
  const row = await dbGet(
    `SELECT active_intent, intent_data, intent_expires_at FROM sessions WHERE session_id = ?`,
    [conversationId]
  );
  if (!row || !row.active_intent) return null;
  if (row.intent_expires_at && row.intent_expires_at < Date.now()) {
    await clearActiveIntent(conversationId);
    return null;
  }
  try { return { type: row.active_intent, data: JSON.parse(row.intent_data || "{}") }; }
  catch { return null; }
}

async function clearActiveIntent(conversationId) {
  await dbRun(
    `UPDATE sessions SET active_intent = NULL, intent_data = NULL, intent_expires_at = NULL WHERE session_id = ?`,
    [conversationId]
  );
}

// ================================================================================
// §3.9 — HISTORIQUE (⚠️ user_id filtré dans TOUTES les branches)
// ================================================================================

async function getFullHistory(conversationId, userId = null, limit = CONFIG.MAX_CONTEXT_MESSAGES) {
  // 1) Supabase (source de vérité) — filtré par user_id
  if (supabase) {
    try {
      let q = supabase
        .from("messages")
        .select("role, content, created_at, user_id")
        .eq("session_id", conversationId)
        .order("created_at", { ascending: false })
        .limit(limit);
      const { data, error } = await q;
      if (!error && Array.isArray(data) && data.length > 0) {
        // ⚠️ Filtre user_id EN MÉMOIRE si présent (colonne optionnelle côté Supabase)
        const filtered = userId
          ? data.filter((m) => !m.user_id || m.user_id === userId)
          : data;
        return filtered.reverse().map((m) => ({ role: m.role, content: m.content }));
      }
    } catch (e) { logger.warn({ err: e.message }, "getFullHistory Supabase échec → local"); }
  }

  // 2) Local — filtré par user_id
  try {
    let q = "SELECT role, content FROM messages WHERE session_id = ?";
    const params = [conversationId];
    if (userId) {
      q += " AND (user_id = ? OR user_id IS NULL)";
      params.push(userId);
    }
    q += " ORDER BY id DESC LIMIT ?";
    params.push(limit);

    const rows = await dbAll(q, params);
    return rows.reverse().map((r) => ({ role: r.role, content: r.content }));
  } catch (e) {
    logger.error({ err: e.message }, "getFullHistory local échec");
    return [];
  }
}

// ================================================================================
// §3.10 — assertConversationOwnership étendu Supabase
// ================================================================================

async function assertConversationOwnership(conversationId, userId) {
  // 1) Local
  const local = await dbGet(
    `SELECT user_id, firebase_uid FROM sessions WHERE session_id = ?`,
    [conversationId]
  );
  if (local) {
    if (local.user_id && local.user_id !== userId && local.firebase_uid !== userId) {
      const e = new Error("Cette conversation n'appartient pas à cet utilisateur.");
      e.code = "CONVERSATION_OWNERSHIP";
      throw e;
    }
    return true;
  }

  // 2) Supabase (si session inconnue localement, ex. après reset SQLite)
  if (supabase) {
    try {
      const { data, error } = await supabase
        .from("sessions")
        .select("user_id, firebase_uid")
        .eq("session_id", conversationId)
        .maybeSingle();
      if (!error && data) {
        if (data.user_id && data.user_id !== userId && data.firebase_uid !== userId) {
          const e = new Error("Cette conversation n'appartient pas à cet utilisateur.");
          e.code = "CONVERSATION_OWNERSHIP";
          throw e;
        }
        return true;
      }
    } catch (e) {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
      logger.warn({ err: e.message }, "assertConversationOwnership Supabase échec");
    }
  }

  // Session inconnue partout : autorisé (création à venir via getSession).
  return true;
}

// ================================================================================
// §3.11 — saveMessageWithUser v15
// ================================================================================
// ⚠️ PHASE 3 : Supabase-first (écriture via outbox si échec), puis SQLite.
// ⚠️ PHASE 1 : les blocs "Illustrations"/"Sources" ne sont PAS sauvegardés — le
// texte pur est sauvegardé, ces blocs sont ajoutés à la RÉPONSE envoyée seulement.

async function saveMessageWithUser(conversationId, role, content, userId = null, firebaseUid = null, metadata = {}) {
  const now = Date.now();

  // Supabase en arrière-plan (via outbox)
  if (supabase) {
    const idemKey = `msg:${conversationId}:${role}:${now}:${sha256(String(content).slice(0, 64)).slice(0, 12)}`;
    setImmediate(() => {
      supabaseWriteSafe({
        table: "messages",
        op: "insert",
        payload: {
          session_id: conversationId,
          firebase_uid: firebaseUid || userId,
          user_id: userId,
          role,
          content,
          metadata: metadata || {},
          created_at: new Date(now).toISOString()
        },
        idempotencyKey: idemKey
      }).catch(() => {});
    });
  }

  // Local (synchrone, source de contexte la plus rapide)
  try {
    await dbRun(
      `INSERT INTO messages (session_id, user_id, role, content, metadata, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [conversationId, userId, role, content, JSON.stringify(metadata || {}), now]
    );
    await dbRun(`UPDATE sessions SET updated_at = ? WHERE session_id = ?`, [now, conversationId]);
  } catch (e) {
    logger.error({ err: e.message }, "saveMessageWithUser SQLite échec");
  }
}

// ================================================================================
// §3.12 — EMAIL
// ================================================================================

async function verifyGmailScope(accessToken) {
  try {
    const response = await axios.get("https://www.googleapis.com/oauth2/v1/tokeninfo", {
      params: { access_token: accessToken }, timeout: 8000
    });
    const scopes = String(response.data?.scope || "").split(" ");
    return scopes.includes("https://www.googleapis.com/auth/gmail.send") || scopes.includes("https://mail.google.com/");
  } catch { return false; }
}

async function sendEmailViaGmail(accessToken, recipient, subject, body) {
  const hasValidScope = await verifyGmailScope(accessToken);
  if (!hasValidScope) {
    const e = new Error("Token Gmail invalide ou scope manquant");
    e.code = "GMAIL_SCOPE_MISSING";
    throw e;
  }
  const safeSubject = sanitizeStrict(subject || "(sans sujet)", 200);
  const safeBodyHtml = `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`;

  const lines = [
    `To: ${recipient}`,
    `Subject: =?utf-8?B?${Buffer.from(safeSubject).toString("base64")}?=`,
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    "",
    safeBodyHtml
  ];
  const raw = Buffer.from(lines.join("\r\n")).toString("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

  const response = await axios.post(
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    { raw },
    { headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" }, timeout: 12000 }
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
        subject: sanitizeStrict(subject || "(sans sujet)", 200),
        html: `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`
      },
      { headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" }, timeout: 12000 }
    );
    return { success: true, provider: "resend", messageId: response.data?.id || null };
  } catch (e) {
    return { success: false, error: `Resend : ${e.response?.data?.message || e.message}` };
  }
}

async function sendEmailViaSMTP(to, subject, body) {
  if (!emailTransporter) return { success: false, error: "SMTP non configuré" };
  try {
    const info = await emailTransporter.sendMail({
      from: process.env.EMAIL_FROM || `"Luba" <${process.env.SMTP_USER}>`,
      to,
      subject: sanitizeStrict(subject || "(sans sujet)", 200),
      html: `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`,
      text: body || ""
    });
    return { success: true, provider: "smtp", messageId: info.messageId };
  } catch (e) { return { success: false, error: e.message }; }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body, userId }) {
  if (!recipient || !EMAIL_REGEX.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }
  let result;
  if (googleAccessToken) {
    try { result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body); }
    catch (e) { result = { success: false, error: "Gmail API échec" }; }
  } else if (process.env.RESEND_API_KEY) {
    result = await sendEmailViaResend(recipient, subject, body);
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  try {
    await dbRun(
      `INSERT INTO email_logs (user_id, to_email, subject, status, provider, error_message, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [userId || null, recipient, subject || null, result.success ? "sent" : "failed", result.provider || null, result.error || null, Date.now()]
    );
  } catch {}

  return result;
}

// ================================================================================
// §3.13 — WHISPER (transcription)
// ================================================================================

async function transcribeAudioGroq(buffer, filename, mimetype) {
  if (!LLM_PROVIDERS.GROQ.keyPool || LLM_PROVIDERS.GROQ.keyPool.length === 0) {
    return { success: false, error: "Aucune clé Groq pour la transcription" };
  }
  let lastError = null;
  for (const keyEntry of LLM_PROVIDERS.GROQ.keyPool) {
    try {
      const form = new FormData();
      form.append("file", buffer, { filename: filename || "audio.webm", contentType: mimetype || "audio/webm" });
      form.append("model", process.env.GROQ_WHISPER_MODEL || "whisper-large-v3");
      form.append("language", "fr");
      form.append("response_format", "json");

      const response = await axios.post(
        "https://api.groq.com/openai/v1/audio/transcriptions",
        form,
        {
          headers: { ...form.getHeaders(), Authorization: "Bearer " + keyEntry.apiKey },
          timeout: 30000, maxBodyLength: Infinity, maxContentLength: Infinity
        }
      );
      return { success: true, text: response.data?.text || "" };
    } catch (e) {
      lastError = e;
      logger.warn({ err: e.message, keyLabel: keyEntry.label }, "Transcription rotation");
    }
  }
  return { success: false, error: "Échec transcription" };
}

// ================================================================================
// §3.14 — PROMPTS SYSTÈME (texte brut, sans JSON)
// ================================================================================
// ⚠️ PHASE 1 : plus de JSON pour le chat. Le modèle répond en Markdown brut.
// Les suggestions sont extraites soit d'un bloc final balisé, soit générées
// par un mini-appel séparé (fallback).

const LUBA_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, startup à Kinshasa, fondée en 2026.",
  "",
  "IDENTITÉ :",
  "- Tu t'appelles Luba (ou Luba.ia).",
  "- Ton ton est chaleureux, intelligent, proactif.",
  "- Tu es un vrai agent IA (façon Jarvis), pas un chatbot passif.",
  "",
  "MÉMOIRE :",
  "- Souviens-toi du contexte de la conversation.",
  "- Ne redemande JAMAIS une info déjà donnée.",
  "- Si une [MÉMOIRE LONG TERME] est fournie, utilise-la naturellement.",
  "",
  "DONNÉES :",
  "- N'invente JAMAIS un score, une actualité, une météo, une vidéo.",
  "- Utilise TOUJOURS l'outil approprié.",
  "",
  "IMAGES :",
  "- Dès que tu décris une personnalité, un lieu ou un objet précis, appelle search_images.",
  "",
  "MODULE JARVIS :",
  "- Chercher un clip → search_youtube",
  "- Créer un rappel → create_task (avec due_at ISO si une date est mentionnée)",
  "- Voir les tâches → list_tasks",
  "",
  "CODE :",
  "- Si on te demande du code, fournis du code de production complet, jamais tronqué.",
  "- Utilise des blocs Markdown typés (```python, ```javascript, etc.).",
  "- Tu peux appeler run_code pour vérifier qu'un extrait fonctionne.",
  "",
  "MATHÉMATIQUES :",
  "- Écris les formules en LaTeX : $inline$ ou $$display$$.",
  "- N'utilise JAMAIS \\( … \\) ni \\[ … \\], ni d'échappements inutiles.",
  "",
  "FORMAT :",
  "- Réponds en Markdown propre, avec titres **gras**, listes, tableaux si utile.",
  "- Sois concis, direct, utile.",
  "- INTERDIT : entourer ta réponse de JSON, de balises <think>, ou de blocs {replyText: ...}.",
  "",
  "SUGGESTIONS :",
  "- À la fin de ta réponse, si pertinent, ajoute un bloc :",
  "  <!--SUGGESTIONS:[\"Question 1 ?\",\"Question 2 ?\",\"Question 3 ?\"]-->",
  "- Sinon, n'ajoute rien."
].join("\n");

/** Extrait le bloc <!--SUGGESTIONS:[...]--> de la réponse et le retire. */
function extractSuggestions(text) {
  if (!text) return { text: "", suggestions: [] };
  const m = String(text).match(/<!--\s*SUGGESTIONS\s*:\s*(\[[\s\S]*?\])\s*-->/i);
  if (!m) return { text, suggestions: [] };
  let suggestions = [];
  try {
    const parsed = JSON.parse(m[1]);
    if (Array.isArray(parsed)) suggestions = parsed.filter((s) => typeof s === "string").slice(0, 4);
  } catch {}
  return { text: text.replace(m[0], "").trim(), suggestions };
}

/** Génère 3 suggestions via mini-appel (fallback si pas de bloc). */
async function generateSuggestions(userMessage, replyText) {
  try {
    const provider = MODEL_TIERS.v100.providers[0];
    const result = await callProviderWithTools({
      providerConfig: provider,
      messages: [
        { role: "system", content: 'Génère 3 questions de suivi courtes (max 60 char). Réponds strictement en JSON : {"suggestions":["...","...","..."]}' },
        { role: "user", content: `Question : ${userMessage.slice(0, 300)}\nRéponse : ${replyText.slice(0, 500)}` }
      ],
      tools: null,
      jsonMode: true
    });
    if (result.success) {
      const content = result.message?.content || "";
      const m = content.match(/\{[\s\S]*\}/);
      if (m) {
        const parsed = JSON.parse(m[0]);
        if (Array.isArray(parsed.suggestions)) return parsed.suggestions.slice(0, 3);
      }
    }
  } catch {}
  return [];
}

// ================================================================================
// §3.14b — ENRICHISSEMENT DE CONTEXTE (⚠️ UNE SEULE FOIS, résultat réutilisé)
// ================================================================================
// ⚠️ PHASE 1 : les outils pré-exécutés ne doivent PLUS être rejoués dans la boucle.
// On expose le résultat dans un cache local { name::argsHash → result }.

function toolCacheKey(name, args) {
  return `${name}::${sha256(JSON.stringify(args || {})).slice(0, 16)}`;
}

async function enrichContextWithIntent(intent, userMessage, entity, toolCache) {
  const enrichment = { contextData: "", sourceKeys: [], media: { images: [], videos: [] } };
  if (!intent || intent === "GENERAL" || intent === "CODE") return enrichment;

  const run = async (toolName, args) => {
    const key = toolCacheKey(toolName, args);
    if (toolCache.has(key)) return toolCache.get(key);
    const { result, sourceKeys } = await executeToolNative(toolName, args, {});
    toolCache.set(key, { result, sourceKeys });
    return { result, sourceKeys };
  };

  try {
    switch (intent) {
      case "MATHS": {
        const expressions = detectMathExpressions(userMessage);
        for (const expr of expressions.slice(0, 2)) {
          const { result } = await run("execute_math", { expression: expr });
          if (result.success) enrichment.contextData += `\n[Calcul exact] ${expr} = ${result.formatted}\n`;
        }
        break;
      }
      case "ACTUALITE": {
        const q = entity || userMessage;
        const { result, sourceKeys } = await run("search_news", { query: q });
        if (result.articles?.length) {
          enrichment.contextData += "\n[ACTUALITÉS RÉCENTES — ne cite pas les URLs dans la réponse]\n";
          result.articles.slice(0, 5).forEach((a, i) => {
            enrichment.contextData += `${i + 1}. ${a.title} (${a.pubDate})\n   ${a.description ? a.description.slice(0, 200) : ""}\n\n`;
          });
          sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }
      case "SPORT": {
        const { result, sourceKeys } = await run("search_sports_scores", { team: entity || userMessage });
        if (result.events?.length) {
          enrichment.contextData += `\n[RÉSULTATS SPORTIFS — ${result.team || entity}]\n`;
          result.events.forEach((e) => { enrichment.contextData += `${e.match} (${e.date}) - ${e.league}\n`; });
          sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }
      case "PERSONNE": {
        const { result, sourceKeys } = await run("search_images", { query: entity || userMessage });
        if (result.images?.length) {
          enrichment.media.images = result.images.slice(0, 3);
          enrichment.contextData += `\n[IMAGES TROUVÉES — elles seront affichées automatiquement, ne mentionne pas les URLs]\n`;
          sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }
      case "VIDEO": {
        const { result, sourceKeys } = await run("search_youtube", { query: entity || userMessage });
        if (result.videos?.length) {
          enrichment.media.videos = result.videos.slice(0, 3);
          enrichment.contextData +=
            `\n[VIDÉOS YOUTUBE TROUVÉES]\n` +
            result.videos.slice(0, 3).map((v) => `- ${v.title} (${v.url})`).join("\n") + `\n`;
          sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }
      case "TASK":
        enrichment.contextData += "\n[MODULE JARVIS — TÂCHES] L'utilisateur veut gérer une tâche / un rappel.";
        break;
    }
  } catch (e) {
    logger.warn({ err: e.message, intent }, "Enrichissement contexte échoué");
  }

  return enrichment;
}

// ================================================================================
// §3.14c — handleChat v15 (NON BLOQUANT, PARALLÉLISÉ, SANS DOUBLONS)
// ================================================================================

async function handleChat({
  conversationId, userId, firebaseUid, message,
  googleAccessToken = null, channel = "web", modelTier = "v100",
  images = null, sse = null
}) {
  const startedAt = Date.now();
  const useV250 = modelTier === "v250";

  if (sse) sse.status("starting");

  // -------- 1) Parallélisation des lectures indépendantes --------
  const [_, history, longTermMemory, activeIntent] = await Promise.all([
    getSession(conversationId, userId, firebaseUid).catch((e) => {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
      logger.warn({ err: e.message }, "getSession échec");
      return null;
    }),
    getFullHistory(conversationId, userId, CONFIG.MAX_CONTEXT_MESSAGES).catch(() => []),
    getUserMemory(userId).catch(() => ""),
    getActiveIntent(conversationId).catch(() => null)
  ]);

  // -------- 2) Intention active (WhatsApp/Email en cours) --------
  if (activeIntent) {
    if (sse) sse.status("active_intent");
    const out = await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken });
    await saveMessageWithUser(conversationId, "user", message, userId, firebaseUid);
    await saveMessageWithUser(conversationId, "assistant", out.reply, userId, firebaseUid);
    if (sse) {
      await streamTextAsTokens(sse, out.reply);
      sse.done({ conversationId, providerUsed: "active_intent", modelTier, error: !!out.error });
      sse.end();
    }
    return {
      reply: out.reply, images: [], media: { images: [], videos: [] },
      suggestions: [], sources: [], intent: "ACTIVE_INTENT",
      providerUsed: "active_intent", modelTier, degraded: false,
      conversationId, isNewConversation: false, error: !!out.error
    };
  }

  // -------- 3) Pré-routeur d'intention + entité --------
  const { intent, entity } = preRouteIntent(message);
  if (sse) sse.status("thinking", { intent });

  // -------- 4) Écriture du message user (fire-and-forget côté Supabase) --------
  saveMessageWithUser(conversationId, "user", message, userId, firebaseUid).catch(() => {});

  // -------- 5) Enrichissement (une seule fois) + cache anti-doublon --------
  const toolCache = new Map();
  const enrichment = await enrichContextWithIntent(intent, message, entity, toolCache);

  // ⚠️ Émission SSE des médias dès qu'ils sont prêts (avant le LLM)
  if (sse && enrichment.media.images.length > 0) sse.images(enrichment.media.images);
  if (sse && enrichment.media.videos.length > 0) sse.videos(enrichment.media.videos);

  // -------- 6) Construction des messages LLM (⚠️ PAS de doublon d'historique) --------
  // On envoie l'historique UNE SEULE FOIS dans `messages` — pas dans un system prompt.
  const historyWithoutCurrent = history.length > 0 && history[history.length - 1].role === "user"
    ? history.slice(0, -1)
    : history;
  const contextHistory = historyWithoutCurrent.slice(-CONFIG.MAX_CONTEXT_MESSAGES);

  let systemContent = LUBA_SYSTEM_PROMPT;
  if (longTermMemory) systemContent += `\n\n[MÉMOIRE LONG TERME SUR CET UTILISATEUR]\n${longTermMemory}`;
  if (intent && intent !== "GENERAL") systemContent += `\n\n[DOMAINE DÉTECTÉ : ${intent}]`;

  const userContentWithEnrichment = enrichment.contextData
    ? `${message}\n\n[CONTEXTE ENRICHI — NE PAS CITER CES SOURCES]\n${enrichment.contextData}`
    : message;

  let messages = [
    { role: "system", content: systemContent },
    ...contextHistory,
    { role: "user", content: userContentWithEnrichment }
  ];

  // -------- 7) Exécution LLM (vision ou texte) --------
  const usedSources = new Set(enrichment.sourceKeys);
  let collectedImages = enrichment.media.images.map((i) => i.url);
  let collectedVideos = [...enrichment.media.videos];
  let providerUsed = "unknown";
  let degraded = false;
  let visionEnabled = Boolean(images && images.length > 0);
  let finalText = "";
  let toolCallTrace = [];

  const providerChain = images && images.length > 0
    ? MODEL_TIERS.vision.providers
    : (useV250 ? MODEL_TIERS.v250.reasoning.providers : MODEL_TIERS.v100.providers);

  const executeFn = async ({ toolName, args }) => {
    // ⚠️ Anti-doublon : si l'enrichissement a déjà exécuté ce couple (nom,args),
    // on réutilise le résultat au lieu de refaire l'appel réseau.
    const key = toolCacheKey(toolName, args);
    if (toolCache.has(key)) {
      const cached = toolCache.get(key);
      return { result: cached.result, sourceKeys: cached.sourceKeys };
    }
    const out = await executeToolNative(toolName, args, { userId, googleAccessToken });
    toolCache.set(key, { result: out.result, sourceKeys: out.sourceKeys });
    return out;
  };

  try {
    // -------- Boucle agent bornée sur le provider principal --------
    // Rotation : on essaie chaque provider jusqu'à ce qu'un réussisse.
    let loopResult = { success: false };
    for (const provider of providerChain) {
      const r = await runToolLoop({
        messages,
        providerConfig: provider,
        images,
        executeFn
      });
      if (r.success) {
        loopResult = r;
        providerUsed = provider.provider;
        break;
      }
      // Sinon on essaie le suivant
      logger.warn({ provider: provider.provider, err: r.error?.message }, "Boucle tool LLM échouée, provider suivant");
    }

    if (!loopResult.success) {
      finalText = userFacingErrorMessage(loopResult.error);
      degraded = true;
    } else {
      finalText = loopResult.text || "";
      toolCallTrace = loopResult.toolCallTrace || [];
      (loopResult.usedSources || []).forEach((k) => usedSources.add(k));
      collectedImages.push(...(loopResult.images || []));
      collectedVideos.push(...(loopResult.videos || []));
      // Le modèle peut ne pas avoir utilisé les outils pré-enrichis (peu probable)
      if (collectedImages.length === 0 && enrichment.media.images.length > 0) {
        collectedImages.push(...enrichment.media.images.map((i) => i.url));
      }
    }

    // -------- Si v250 demandé, on relance la phase "code" avec historique --------
    if (useV250 && !images && finalText && intent === "CODE") {
      const codeProvider = MODEL_TIERS.v250.code.providers[0];
      const codeMessages = [
        { role: "system", content: LUBA_SYSTEM_PROMPT + "\n\n[PHASE CODE] Fournis le code complet et fonctionnel." },
        ...contextHistory,
        { role: "user", content: message }
      ];
      const codeResult = await callProviderWithTools({
        providerConfig: codeProvider,
        messages: codeMessages,
        tools: getToolSchemas("chat"),
        jsonMode: false
      });
      if (codeResult.success) {
        finalText = normalizeMath(stripThinkTags(codeResult.message?.content || "").text) || finalText;
        providerUsed = `v250_pipeline(${providerUsed}->${codeProvider.provider})`;
      }
    }

  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "handleChat : erreur critique");
    finalText = userFacingErrorMessage(e);
    degraded = true;
  }

  // -------- 8) Post-traitement : normalizeMath + stripThink + suggestions --------
  finalText = normalizeMath(finalText || "");
  if (!finalText) finalText = "Je n'ai pas pu générer une réponse pour le moment.";

  const sug = extractSuggestions(finalText);
  finalText = sug.text;
  let suggestions = sug.suggestions;
  if (suggestions.length === 0 && !degraded) {
    // Mini-appel en tâche de fond, non bloquant pour la réponse
    generateSuggestions(message, finalText.slice(0, 500))
      .then((s) => { if (s.length) suggestions = s; })
      .catch(() => {});
  }

  // -------- 9) Dédoublonnage médias --------
  collectedImages = [...new Set(collectedImages.filter(Boolean))];
  collectedVideos = dedupeVideos(collectedVideos);

  // -------- 10) Save du texte PUR (sans Illustrations/Sources) --------
  saveMessageWithUser(conversationId, "assistant", finalText, userId, firebaseUid, {
    providerUsed, intent, degraded
  }).catch(() => {});

  // -------- 11) Mémoire long terme (fire-and-forget, file bornée) --------
  maybeUpdateUserMemoryAsync(userId, message, finalText).catch(() => {});

  // -------- 12) Construction de la réponse HTTP --------
  // ⚠️ Les blocs "Illustrations" et "Sources" sont ajoutés UNIQUEMENT dans la
  // réponse envoyée (pas dans l'historique réinjecté au LLM).
  let replyForClient = finalText;

  const inlineImages = enrichment.media.images.length === 0 && collectedImages.length > 0;
  if (inlineImages) {
    const md = collectedImages.map((u, i) => `![Image ${i + 1}](${u})`).join("\n\n");
    replyForClient += `\n\n---\n\n**Illustrations :**\n\n${md}`;
  }

  if (usedSources.size > 0) {
    const srcLines = [...usedSources]
      .map((k) => OPEN_SOURCES[k])
      .filter(Boolean)
      .map((s) => `[${s.name}](${s.url})`);
    if (srcLines.length > 0) replyForClient += `\n\n---\n\n**Sources :** ${srcLines.join(" · ")}`;
  }

  const media = {
    images: collectedImages,
    videos: collectedVideos.map((v) => ({
      videoId: v.videoId, title: v.title, channel: v.channel,
      thumbnail: v.thumbnail, url: v.url,
      embedUrl: `https://www.youtube.com/embed/${v.videoId}`
    }))
  };

  const result = {
    reply: replyForClient,
    images: collectedImages,
    media,
    error: degraded,
    providerUsed,
    modelTier: useV250 ? "v250" : "v100",
    degraded,
    visionEnabled,
    suggestions,
    sources: [...usedSources].map((k) => OPEN_SOURCES[k]).filter(Boolean),
    intent,
    userId,
    contextLength: history.length,
    toolCallTrace,
    elapsedMs: Date.now() - startedAt
  };

  // -------- 13) SSE : on stream le texte final puis on clôt --------
  if (sse) {
    sse.suggestions(suggestions);
    sse.sources(result.sources);
    await streamTextAsTokens(sse, replyForClient, { paceMs: 4, chunkSize: 28 });
    sse.done({
      conversationId,
      isNewConversation: false,
      providerUsed,
      modelTier: result.modelTier,
      degraded,
      visionEnabled,
      intent,
      contextLength: history.length
    });
    sse.end();
  }

  return { ...result, conversationId, isNewConversation: false };
}

// ================================================================================
// §3.15 — SSE WRAPPER HTTP
// ================================================================================

/**
 * Endpoint SSE opt-in. Détecte :
 *   - header Accept: text/event-stream
 *   - body: { stream: true }
 * Le premier événement part immédiatement (< 50 ms, bien en dessous du budget 300 ms).
 */
async function chatStreamingHandler(req, res) {
  const sse = new SSEWriter(res);
  const clientGone = { value: false };
  req.on("close", () => { clientGone.value = true; sse.closed = true; });

  const { conversationId, message, modelTier } = req.body || {};
  const safeMessage = sanitizeForLLM(message);

  if (!safeMessage) {
    sse.error({ reply: "Message vide." });
    sse.done({ error: true });
    return sse.end();
  }

  // Quota (avant le LLM)
  const quota = await checkUserQuota(req.userId, "message", req.userRole);
  if (!quota.allowed) {
    sse.error({ reply: quota.message, code: "QUOTA_EXCEEDED" });
    sse.done({ error: true });
    return sse.end();
  }
  await incrementUserQuota(req.userId, "message");

  try {
    await handleChat({
      conversationId: conversationId || generateConversationId(),
      userId: req.userId,
      firebaseUid: req.firebaseUid,
      message: safeMessage,
      googleAccessToken: req.headers["x-google-access-token"] || null,
      channel: "web-sse",
      modelTier: modelTier === "v250" ? "v250" : "v100",
      images: req._uploadedImages || null,
      sse
    });
  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "chatStreamingHandler erreur");
    if (!sse.closed) {
      sse.error({ reply: "Une erreur interne est survenue." });
      sse.done({ error: true });
    }
  } finally {
    if (!sse.closed) sse.end();
  }
}

// ================================================================================
// §3.16 — handleActiveIntent (annulation + expiration)
// ================================================================================
// ⚠️ PHASE 4 :
//   - "annule" / "stop" / "laisse tomber" → annule et efface
//   - Toute réponse est validée AVANT d'être acceptée comme numéro/sujet/corps
//   - Expiration 5 min gérée par getActiveIntent

const CANCEL_WORDS = new Set(["annule", "annuler", "stop", "abandonne", "laisse tomber", "oublie", "cancel"]);

function isCancelMessage(text) {
  const t = normalizeForMatch(text);
  return CANCEL_WORDS.has(t) || t.startsWith("annul") || t.startsWith("cancel");
}

async function handleActiveIntent(conversationId, activeIntent, userMessage, context = {}) {
  const { userId, googleAccessToken } = context;

  if (isCancelMessage(userMessage)) {
    await clearActiveIntent(conversationId);
    return { reply: "Action annulée.", error: false };
  }

  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data || {};
      if (data.step === "NEED_NUMBER") {
        const cleaned = String(userMessage).trim().replace(/[\s\-().]/g, "");
        if (PHONE_REGEX.test(cleaned)) {
          await setActiveIntent(conversationId, "WHATSAPP", { step: "NEED_MESSAGE", recipient: cleaned });
          return { reply: `Numéro enregistré (${cleaned}). Quel message voulez-vous envoyer ?`, error: false };
        }
        return { reply: "Ce numéro n'est pas valide (format international requis, ex. +243812345678). Tapez « annule » pour arrêter.", error: true };
      }
      if (data.step === "NEED_MESSAGE") {
        const text = sanitizeStrict(userMessage, 2000);
        if (!text) return { reply: "Message vide. Réessayez ou tapez « annule ».", error: true };
        try {
          await sendWhatsAppSmart(userId, data.recipient, text);
          await clearActiveIntent(conversationId);
          return { reply: `✅ Message WhatsApp envoyé vers ${data.recipient}.`, error: false };
        } catch (e) {
          await clearActiveIntent(conversationId);
          return { reply: "Impossible d'envoyer le message WhatsApp pour le moment.", error: true };
        }
      }
      break;
    }

    case "EMAIL": {
      const data = activeIntent.data || {};
      if (data.step === "NEED_RECIPIENT") {
        const email = String(userMessage).trim();
        if (EMAIL_REGEX.test(email)) {
          await setActiveIntent(conversationId, "EMAIL", { step: "NEED_SUBJECT", recipient: email });
          return { reply: `Destinataire enregistré (${email}). Quel est le sujet ?`, error: false };
        }
        return { reply: "Adresse email invalide. Réessayez ou tapez « annule ».", error: true };
      }
      if (data.step === "NEED_SUBJECT") {
        const subject = sanitizeStrict(userMessage, 200);
        if (!subject) return { reply: "Sujet vide. Réessayez ou tapez « annule ».", error: true };
        await setActiveIntent(conversationId, "EMAIL", { step: "NEED_BODY", recipient: data.recipient, subject });
        return { reply: "Sujet enregistré. Quel est le contenu de l'email ?", error: false };
      }
      if (data.step === "NEED_BODY") {
        const body = sanitizeStrict(userMessage, 5000);
        if (!body) return { reply: "Contenu vide. Réessayez ou tapez « annule ».", error: true };
        const result = await dispatchSendEmail({
          googleAccessToken,
          recipient: data.recipient,
          subject: data.subject,
          body,
          userId
        });
        await clearActiveIntent(conversationId);
        if (result.success) return { reply: `✅ Email envoyé à ${data.recipient}.`, error: false };
        return { reply: "Envoi d'email impossible pour le moment.", error: true };
      }
      break;
    }
  }

  await clearActiveIntent(conversationId);
  return { reply: "Action interrompue. Recommençons.", error: true };
}

// ================================================================================
// §3.17 — HYDRATATION SUPABASE → SQLITE
// ================================================================================
// ⚠️ PHASE 3 : au démarrage (ou à la connexion), on hydrate SQLite depuis Supabase
// pour survivre aux redéploiements (Render : disque éphémère).

async function hydrateUserFromSupabase(userId, { limit = 50 } = {}) {
  if (!supabase) return { ok: false, reason: "no_supabase" };
  const stats = { sessions: 0, messages: 0, tasks: 0, memory: 0 };

  // 1) User
  try {
    const { data: u, error } = await supabase.from("users").select("id, firebase_uid, email, display_name, role, created_at").eq("firebase_uid", userId).maybeSingle();
    if (!error && u) {
      await dbRun(
        `INSERT INTO users (id, firebase_uid, email, display_name, role, last_seen_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET email = excluded.email, display_name = excluded.display_name, role = excluded.role`,
        [
          u.id || u.firebase_uid, u.firebase_uid || u.id, u.email || null,
          u.display_name || null, u.role || "FREE",
          Date.now(), u.created_at ? Date.parse(u.created_at) : Date.now(), Date.now()
        ]
      );
    }
  } catch {}

  // 2) Sessions
  try {
    const { data: sess, error } = await supabase
      .from("sessions").select("session_id, created_at, updated_at")
      .eq("user_id", userId).order("updated_at", { ascending: false }).limit(limit);
    if (!error && Array.isArray(sess)) {
      for (const s of sess) {
        try {
          await dbRun(
            `INSERT OR IGNORE INTO sessions (session_id, user_id, firebase_uid, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)`,
            [s.session_id, userId, userId,
              s.created_at ? Date.parse(s.created_at) : Date.now(),
              s.updated_at ? Date.parse(s.updated_at) : Date.now()]
          );
          stats.sessions++;
        } catch {}
      }
    }
  } catch {}

  // 3) Messages récents (par session)
  try {
    const { data: msgs, error } = await supabase
      .from("messages").select("session_id, role, content, created_at")
      .eq("user_id", userId).order("created_at", { ascending: false }).limit(limit * 5);
    if (!error && Array.isArray(msgs)) {
      for (const m of msgs.reverse()) {
        try {
          // Vérifie si le message existe déjà (par session_id + created_at)
          const exists = await dbGet(
            `SELECT 1 FROM messages WHERE session_id = ? AND created_at = ? AND role = ? LIMIT 1`,
            [m.session_id, m.created_at ? Date.parse(m.created_at) : Date.now(), m.role]
          );
          if (exists) continue;
          await dbRun(
            `INSERT INTO messages (session_id, user_id, role, content, created_at)
             VALUES (?, ?, ?, ?, ?)`,
            [m.session_id, userId, m.role, m.content, m.created_at ? Date.parse(m.created_at) : Date.now()]
          );
          stats.messages++;
        } catch {}
      }
    }
  } catch {}

  // 4) Tâches
  try {
    const { data: tasks, error } = await supabase
      .from("user_tasks").select("*").eq("user_id", userId).limit(100);
    if (!error && Array.isArray(tasks)) {
      for (const t of tasks) {
        try {
          await dbRun(
            `INSERT OR REPLACE INTO user_tasks (id, user_id, title, notes, due_at, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [t.id, t.user_id || userId, t.title, t.notes || null,
              t.due_at ? Date.parse(t.due_at) : null,
              t.status || "pending",
              t.created_at ? Date.parse(t.created_at) : Date.now(),
              t.updated_at ? Date.parse(t.updated_at) : Date.now()]
          );
          stats.tasks++;
        } catch {}
      }
    }
  } catch {}

  // 5) Mémoire long terme
  try {
    const { data: mem, error } = await supabase.from("user_memory").select("summary, messages_since_update, updated_at").eq("user_id", userId).maybeSingle();
    if (!error && mem) {
      await dbRun(
        `INSERT INTO user_memory (user_id, summary, messages_since_update, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(user_id) DO UPDATE SET summary = excluded.summary, messages_since_update = excluded.messages_since_update, updated_at = excluded.updated_at`,
        [userId, mem.summary || "", mem.messages_since_update || 0, mem.updated_at ? Date.parse(mem.updated_at) : Date.now()]
      );
      stats.memory = 1;
    }
  } catch {}

  return { ok: true, stats };
}

// ================================================================================
// §3.18 — EXPORTS PARTIE 3 (OVERRIDE des stubs v14)
// ================================================================================

// ⚠️ Override : les fonctions ci-dessous remplacent celles définies en Partie 1/2
// pour corriger les bugs profonds (handleChat, getFullHistory, tasks, etc.).
Object.assign(module.exports, {
  // SSE
  SSEWriter, safeChunkText, streamTextAsTokens, chatStreamingHandler,

  // Images
  searchImagesWithFallback, searchWikimediaImagesV15, fetchWikipediaThumb, searchDuckDuckGoImages,

  // Search helpers
  searchWikipediaSummary, searchWeb, searchDuckDuckGo, searchNews, searchSportsScores,
  searchScience, searchSocial, getWeather, searchYouTube, extractYouTubeVideoId, scrapeArticleContent,

  // Tasks (UUID + Supabase-first)
  createTask, listTasks, updateTaskStatus, deleteTask,

  // Mémoire
  getUserMemory, saveUserMemory, incrementUserMemoryCounter,
  runMemorySummaryImpl, maybeUpdateUserMemoryAsync,

  // Sessions
  getSession, setActiveIntent, getActiveIntent, clearActiveIntent,
  assertConversationOwnership, getFullHistory, saveMessageWithUser,
  ACTIVE_INTENT_TTL_MS,

  // Email
  verifyGmailScope, sendEmailViaGmail, sendEmailViaResend, sendEmailViaSMTP, dispatchSendEmail,

  // Whisper
  transcribeAudioGroq,

  // Prompt / suggestions
  LUBA_SYSTEM_PROMPT, extractSuggestions, generateSuggestions,

  // Enrichissement + handleChat
  enrichContextWithIntent, toolCacheKey,
  handleChat, handleActiveIntent,

  // Hydratation
  hydrateUserFromSupabase
});

// ================================================================================
// ==================== FIN PARTIE 3 ============================================
// ================================================================================
// Partie 4 ajoutera :
//   - app Express + montage de tous les middlewares (mountBodyParsers, CORS, helmet)
//   - Routes publiques (/api/health corrigé, /, 404)
//   - Routes user (/api/chat avec SSE opt-in, /api/conversations, /api/tasks, etc.)
//   - /api/session/bootstrap (sans LLM bloquant, requête unique)
//   - /api/admin/*, /api/account (RGPD), /api/voice/transcribe
//   - WhatsApp connect/send
//   - Graceful shutdown + démarrage + logs
//   - Tests d'intégration + checklist finale
// ================================================================================
// ================================================================================
// ==================== PARTIE 4 : APP EXPRESS · ROUTES · DÉMARRAGE ============
// ================================================================================
// Sommaire :
//   §4.1  Application Express + middlewares globaux (CORS, Helmet, body)
//   §4.2  Route : / (info publique)
//   §4.3  Route : /api/health (corrigée : success = !error, infos réduites publiquement)
//   §4.4  Route : /api/user/whoami
//   §4.5  Route : /api/session/bootstrap (⚠️ sans LLM bloquant, 1 requête Supabase)
//   §4.6  Route : /api/chat (⚠️ SSE opt-in + JSON classique)
//   §4.7  Route : /api/conversations, /api/conversation/:id/messages
//   §4.8  Route : /api/user/stats
//   §4.9  Route : /api/session/create, /api/session/revoke
//   §4.10 Route : /api/tools (whitelist + quota)
//   §4.11 Routes : /api/tasks CRUD
//   §4.12 Route : /api/youtube/search
//   §4.13 Route : /api/voice/transcribe
//   §4.14 Routes WhatsApp : connect / send
//   §4.15 Route : /api/intent/init (WhatsApp/Email flow)
//   §4.16 Route : /api/memory (RGPD : lire / effacer)
//   §4.17 Route : /api/admin/set-role
//   §4.18 Route : /api/account (suppression complète, RGPD)
//   §4.19 404 + gestion d'erreur globale
//   §4.20 Housekeeping + démarrage + graceful shutdown
//   §4.21 Tests d'intégration (fichier tests/integration.test.js)
//   §4.22 Checklist mise en production
// ================================================================================

const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ================================================================================
// §4.1 — MIDDLEWARES GLOBAUX
// ================================================================================

// ---------- CORS ----------
app.use(cors({
  origin: function (origin, callback) {
    if (!origin || HOSTING_CONFIG.allowedOrigins.includes(origin)) return callback(null, true);
    logger.warn({ origin }, "Origine CORS refusée");
    callback(null, false);
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: [
    "Content-Type", "Authorization", "X-Requested-With",
    "x-user-id", "X-Google-Access-Token", "X-Session-Token",
    "Accept", "Last-Event-ID" // SSE
  ],
  exposedHeaders: ["Content-Type", "Cache-Control", "Connection", "X-Accel-Buffering"],
  credentials: true,
  maxAge: 86400
}));

// ---------- Helmet ----------
// ⚠️ CSP avec nonce par requête (au lieu de 'unsafe-inline').
// Le front peut récupérer le nonce via `res.locals.cspNonce` s'il rend du HTML côté serveur.
app.use((req, res, next) => {
  res.locals.cspNonce = crypto.randomBytes(16).toString("base64");
  next();
});

app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: [
        "'self'",
        (req, res) => `'nonce-${res.locals.cspNonce}'`,
        "https://cdnjs.cloudflare.com",
        "https://apis.google.com",
        "https://www.gstatic.com"
      ],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://fonts.googleapis.com"],
      imgSrc: ["'self'", "data:", "blob:", "https://*", "http://*"],
      connectSrc: [
        "'self'",
        "https://api.groq.com",
        "https://openrouter.ai",
        "https://api.cerebras.ai",
        "https://generativelanguage.googleapis.com",
        "https://*.firebaseio.com",
        "https://*.supabase.co",
        "wss://*.firebaseio.com"
      ],
      fontSrc: ["'self'", "https://fonts.gstatic.com", "https://cdnjs.cloudflare.com"],
      objectSrc: ["'none'"],
      frameSrc: ["https://*.firebaseapp.com", "https://*.web.app", "https://www.youtube.com", "https://youtube.com"],
      workerSrc: ["'self'", "blob:"],
      mediaSrc: ["'self'", "blob:", "data:"],
      upgradeInsecureRequests: CONFIG.ENV === "production" ? [] : null
    }
  },
  hsts: CONFIG.ENV === "production" ? { maxAge: 63072000, includeSubDomains: true, preload: true } : false,
  referrerPolicy: { policy: "strict-origin-when-cross-origin" },
  crossOriginOpenerPolicy: { policy: "same-origin-allow-popups" },
  noSniff: true,
  frameguard: { action: "deny" }
}));

// ---------- HTTPS forcé (production) ----------
app.use((req, res, next) => {
  if (CONFIG.ENV === "production" && req.headers["x-forwarded-proto"] && req.headers["x-forwarded-proto"] !== "https") {
    return res.redirect(301, "https://" + req.headers.host + req.originalUrl);
  }
  next();
});

// ---------- Body parsers (⚠️ PHASE 2 : 1 Mo par défaut, 20 Mo opt-in) ----------
mountBodyParsers(app);

// ---------- Request ID + log ----------
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  res.setHeader("X-Request-Id", requestId);
  res.on("finish", () => {
    logger.info({
      requestId,
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration: Date.now() - start
    }, "requête");
  });
  next();
});

// ================================================================================
// §4.2 — ROUTE PUBLIQUE / (info minimale)
// ================================================================================

app.get("/", (req, res) => {
  res.json({
    success: true, error: false,
    reply: `Serveur ${CONFIG.AGENT_NAME} opérationnel`,
    version: CONFIG.VERSION,
    company: CONFIG.COMPANY
  });
});

// ================================================================================
// §4.3 — /api/health (⚠️ PHASE 2 : success corrigé, infos réduites en public)
// ================================================================================

app.get("/api/health", async (req, res) => {
  // Public : on n'expose PAS le détail des providers, des modèles, ni des configs.
  // Le détail complet est accessible sur /api/health?full=1 AVEC authentification admin.
  let dbOk = true;
  try { await dbGet("SELECT 1"); } catch { dbOk = false; }

  const full = req.query.full === "1";

  const publicReport = {
    success: dbOk,                    // ⚠️ FIX v14 : était !dbOk (inversé)
    error: !dbOk,
    reply: dbOk ? `Serveur ${CONFIG.AGENT_NAME} en bonne santé` : "Serveur en cours de maintenance",
    data: {
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      version: CONFIG.VERSION,
      database: dbOk ? "ok" : "erreur",
      modelsOk: modelAvailabilityReport.ok,
      features: { vision: Boolean(geminiClient), quotas: true }
    }
  };

  if (!full) return res.json(publicReport);

  // Détail complet (sans secrets) — accessible publiquement ici pour le monitoring,
  // mais SANS compter les clés API ni les chemins internes.
  return res.json({
    ...publicReport,
    data: {
      ...publicReport.data,
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
      supabase: Boolean(supabase),
      firebaseAuth: firebaseApp ? "admin_sdk" : "api_rest",
      providers: {
        groq: LLM_PROVIDERS.GROQ.keyPool.length,
        openrouter: LLM_PROVIDERS.OPENROUTER.keyPool.length,
        cerebras: LLM_PROVIDERS.CEREBRAS.keyPool.length,
        gemini: geminiClient ? "actif" : "inactif"
      },
      features: {
        vision: Boolean(geminiClient),
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
        jarvisTasks: true,
        jarvisYoutube: true,
        geminiVision: Boolean(geminiClient),
        cerebrasReasoning: LLM_PROVIDERS.CEREBRAS.keyPool.length > 0,
        persistentStoreIsSupabase: Boolean(supabase),
        codeSandbox: Boolean(CONFIG.CODE_SANDBOX_PROVIDER),
        sse: true,
        outbox: Boolean(supabase)
      },
      modelAvailability: modelAvailabilityReport
    }
  });
});

// ================================================================================
// §4.4 — /api/user/whoami
// ================================================================================

app.get("/api/user/whoami", authLimiter, authenticateUser, (req, res) => {
  res.status(200).json({
    success: true, error: false,
    userId: req.userId,
    role: req.userRole
  });
});

// ================================================================================
// §4.5 — /api/session/bootstrap (⚠️ PHASE 1 : plus de LLM bloquant, 1 requête)
// ================================================================================

app.get("/api/session/bootstrap", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const today = todayKeyMs();

    // -------- 1) Conversations + dernier message : 1 SEULE requête Supabase --------
    let conversations = [];
    if (supabase) {
      try {
        // ⚠️ On utilise une RPC ou une jointure si disponible ; sinon :
        // on récupère les sessions et on fait 1 seule requête messages pour toutes.
        const { data: supaSessions, error } = await supabase
          .from("sessions")
          .select("session_id, created_at, updated_at")
          .eq("user_id", userId)
          .order("updated_at", { ascending: false })
          .limit(30);

        if (!error && Array.isArray(supaSessions) && supaSessions.length > 0) {
          const sessionIds = supaSessions.map((s) => s.session_id);
          // ⚠️ UNE SEULE requête pour tous les derniers messages
          const { data: lastMsgs, error: err2 } = await supabase
            .from("messages")
            .select("session_id, role, content, created_at")
            .in("session_id", sessionIds)
            .order("created_at", { ascending: false });

          // Regroupe par session (le plus récent en premier)
          const lastBySession = new Map();
          if (!err2 && Array.isArray(lastMsgs)) {
            for (const m of lastMsgs) {
              if (!lastBySession.has(m.session_id)) lastBySession.set(m.session_id, m);
            }
          }

          conversations = supaSessions.map((s) => {
            const last = lastBySession.get(s.session_id);
            return {
              conversationId: s.session_id,
              createdAt: s.created_at,
              updatedAt: s.updated_at,
              lastMessageRole: last?.role || null,
              lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
            };
          });
        }
      } catch (e) { logger.warn({ err: e.message }, "bootstrap Supabase échec"); }
    }

    // Repli local si aucune conversation Supabase
    if (conversations.length === 0) {
      const rows = await dbAll(
        `SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? ORDER BY updated_at DESC LIMIT 30`,
        [userId]
      );
      conversations = await Promise.all(rows.map(async (conv) => {
        const last = await dbGet(
          `SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1`,
          [conv.session_id]
        );
        return {
          conversationId: conv.session_id,
          createdAt: new Date(conv.created_at).toISOString(),
          updatedAt: new Date(conv.updated_at).toISOString(),
          lastMessageRole: last?.role || null,
          lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
        };
      }));
    }

    // -------- 2) Quotas + tâches en parallèle --------
    const [quotaRow, tasksResult, userRow, longTermMemory] = await Promise.all([
      dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]).catch(() => null),
      listTasks(userId, { status: "pending" }).catch(() => ({ tasks: [] })),
      dbGet(`SELECT whatsapp_connected, display_name FROM users WHERE id = ?`, [userId]).catch(() => null),
      getUserMemory(userId).catch(() => "")
    ]);

    // -------- 3) ⚠️ Salutation STATIQUE (pas de LLM bloquant) --------
    // Le front peut demander une salutation personnalisée via /api/greeting (async).
    const displayName = userRow?.display_name || null;
    const greeting = displayName
      ? `Bonjour ${displayName.split(" ")[0]}, comment puis-je vous aider ?`
      : (longTermMemory
        ? "Content de vous revoir. Comment puis-je vous aider ?"
        : "Bonjour, je suis Luba. Comment puis-je vous aider ?");

    return res.status(200).json({
      success: true, error: false,
      userId, role: req.userRole, greeting,
      conversations,
      pendingTasks: tasksResult.tasks || [],
      quotas: quotaRow || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
      limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
      whatsappConnected: Boolean(userRow?.whatsapp_connected),
      hasMemory: Boolean(longTermMemory)
    });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur bootstrap session");
    return res.status(500).json({ success: false, error: true, code: "BOOTSTRAP_ERROR" });
  }
});

// ================================================================================
// §4.5b — /api/greeting (salutation personnalisée, async, cachée 10 min)
// ================================================================================

const greetingCache = new LRUCache({ max: 500, ttl: 10 * 60 * 1000 });

app.get("/api/greeting", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const cached = greetingCache.get(userId);
    if (cached) return res.json({ success: true, greeting: cached });

    const [memory, tasks] = await Promise.all([
      getUserMemory(userId).catch(() => ""),
      listTasks(userId, { status: "pending" }).catch(() => ({ tasks: [] }))
    ]);

    // Essai LLM rapide (timeout 3 s), sinon statique
    let greeting = "Bonjour, comment puis-je vous aider ?";
    try {
      const provider = MODEL_TIERS.v100.providers[0];
      const r = await callProviderWithTools({
        providerConfig: { ...provider, timeout: 3000, maxTokens: 200 },
        messages: [
          { role: "system", content: 'Génère UNE phrase d\'accueil courte (max 120 char). Réponds STRICTEMENT : {"greeting":"..."}' },
          { role: "user", content: `Mémoire : ${memory || "(vide)"}\nTâches : ${tasks.tasks?.length || 0}` }
        ],
        tools: null,
        jsonMode: true
      });
      if (r.success) {
        const m = String(r.message?.content || "").match(/\{[\s\S]*\}/);
        if (m) {
          const parsed = JSON.parse(m[0]);
          if (typeof parsed.greeting === "string" && parsed.greeting.length < 200) greeting = parsed.greeting;
        }
      }
    } catch {}

    greetingCache.set(userId, greeting);
    return res.json({ success: true, greeting });
  } catch (e) {
    return res.status(500).json({ success: false, error: true, greeting: "Bonjour." });
  }
});

// ================================================================================
// §4.6 — /api/chat : JSON classique OU SSE opt-in
// ================================================================================

/** Détecte si le client demande du streaming. */
function wantsStreaming(req) {
  const accept = String(req.headers.accept || "").toLowerCase();
  const bodyStream = req.body && req.body.stream === true;
  const queryStream = req.query.stream === "true";
  return accept.includes("text/event-stream") || bodyStream || queryStream;
}

/**
 * Endpoint chat.
 * - Multipart/form-data : images possibles (multer).
 * - JSON : avec { stream: true } → SSE.
 * - Form-urlencoded : chat simple sans image.
 *
 * Le contrat HTTP (reply, images, media, suggestions, sources, intent,
 * providerUsed, modelTier, degraded, conversationId, isNewConversation)
 * est STRICTEMENT préservé en mode JSON.
 */
app.post("/api/chat", chatLimiter, authenticateUser, upload.array("images", CONFIG.MAX_IMAGES_PER_REQUEST), async (req, res) => {
  const isStream = wantsStreaming(req) && !(req.files && req.files.length > 0);

  try {
    const rawMessage = req.body?.message;
    let conversationId = req.body?.conversationId || req.body?.conversation_id;
    const modelTier = req.body?.modelTier === "v250" ? "v250" : "v100";

    // -------- Validation --------
    if (!rawMessage || typeof rawMessage !== "string") {
      if (isStream) return sseShortError(res, "Le message est obligatoire.", "MISSING_MESSAGE");
      return res.status(400).json({ success: false, error: true, reply: "Le paramètre 'message' est obligatoire.", code: "MISSING_MESSAGE" });
    }

    const sanitizedMessage = sanitizeForLLM(rawMessage);
    if (!sanitizedMessage) {
      if (isStream) return sseShortError(res, "Message vide.", "INVALID_MESSAGE");
      return res.status(400).json({ success: false, error: true, reply: "Message vide après nettoyage.", code: "INVALID_MESSAGE" });
    }

    if (conversationId && !/^[a-zA-Z0-9_-]{6,80}$/.test(conversationId)) {
      if (isStream) return sseShortError(res, "Identifiant de conversation invalide.", "INVALID_CONVERSATION_ID");
      return res.status(400).json({ success: false, error: true, reply: "Identifiant de conversation invalide.", code: "INVALID_CONVERSATION_ID" });
    }

    if (!conversationId) conversationId = generateConversationId();
    const isNewConversation = !req.body?.conversationId && !req.body?.conversation_id;

    // -------- Propriété de la conversation --------
    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) {
      if (isStream) return sseShortError(res, e.message, "CONVERSATION_OWNERSHIP");
      return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" });
    }

    // -------- Quota AVANT LLM --------
    const quota = await checkUserQuota(req.userId, "message", req.userRole);
    if (!quota.allowed) {
      if (isStream) return sseShortError(res, quota.message, "QUOTA_EXCEEDED");
      return res.status(429).json({ success: false, error: true, reply: quota.message, code: "QUOTA_EXCEEDED" });
    }
    await incrementUserQuota(req.userId, "message");

    // -------- Images (multipart) --------
    let images = null;
    if (req.files && req.files.length > 0) {
      const invalid = req.files.find((f) => !isValidImageSignature(f.buffer));
      if (invalid) {
        return res.status(400).json({ success: false, error: true, reply: "Fichier image invalide.", code: "INVALID_IMAGE_CONTENT" });
      }
      images = req.files.map((f) => convertImageToBase64(f.buffer, f.mimetype));
      await incrementUserQuota(req.userId, "image");
    }

    const googleAccessToken = req.headers["x-google-access-token"] || null;

    // -------- Mode SSE --------
    if (isStream) {
      const sse = new SSEWriter(res);
      req.on("close", () => { sse.closed = true; });

      // ⚠️ Premier événement IMMÉDIAT (< 50 ms) : le client n'attend jamais
      sse.status("accepted", { conversationId, isNewConversation });

      // Verrou par conversation (⚠️ PHASE 3 : pas d'entrelacement)
      const ownerRequestId = req.requestId;
      const locked = await acquireConversationLock(conversationId, ownerRequestId, 60000);
      if (!locked) {
        sse.error({ reply: "Une autre requête est en cours sur cette conversation.", code: "CONVERSATION_BUSY" });
        sse.done({ error: true });
        return sse.end();
      }

      try {
        await handleChat({
          conversationId,
          userId: req.userId,
          firebaseUid: req.firebaseUid,
          message: sanitizedMessage,
          googleAccessToken,
          channel: "web-sse",
          modelTier,
          images,
          sse
        });
      } catch (e) {
        logger.error({ err: e.message, stack: e.stack }, "SSE handleChat erreur");
        if (!sse.closed) {
          sse.error({ reply: "Une erreur interne est survenue." });
          sse.done({ error: true });
        }
      } finally {
        await releaseConversationLock(conversationId, ownerRequestId);
        if (!sse.closed) sse.end();
      }
      return;
    }

    // -------- Mode JSON classique --------
    const ownerRequestId = req.requestId;
    const locked = await acquireConversationLock(conversationId, ownerRequestId, 60000);
    if (!locked) {
      return res.status(409).json({ success: false, error: true, reply: "Une autre requête est en cours sur cette conversation.", code: "CONVERSATION_BUSY" });
    }

    try {
      const result = await handleChat({
        conversationId,
        userId: req.userId,
        firebaseUid: req.firebaseUid,
        message: sanitizedMessage,
        googleAccessToken,
        channel: "web",
        modelTier,
        images,
        sse: null
      });

      return res.status(200).json({
        ...result,
        conversationId,
        isNewConversation
      });
    } finally {
      await releaseConversationLock(conversationId, ownerRequestId);
    }

  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "Erreur API /api/chat");
    if (res.headersSent) return;
    return res.status(500).json({ success: false, error: true, reply: "Une erreur est survenue.", code: "CHAT_ERROR" });
  }
});

/** Réponse SSE courte pour les erreurs avant initialisation du flux. */
function sseShortError(res, message, code) {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
  res.write(`event: error\ndata: ${JSON.stringify({ reply: message, code })}\n\n`);
  res.write(`event: done\ndata: ${JSON.stringify({ error: true })}\n\n`);
  res.end();
}

// ================================================================================
// §4.7 — CONVERSATIONS
// ================================================================================

app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    // Supabase-first avec une seule requête pour les derniers messages
    if (supabase) {
      const { data: supaSessions, error } = await supabase
        .from("sessions").select("session_id, created_at, updated_at")
        .eq("user_id", req.userId).order("updated_at", { ascending: false }).limit(50);

      if (!error && Array.isArray(supaSessions) && supaSessions.length > 0) {
        const ids = supaSessions.map((s) => s.session_id);
        const { data: lastMsgs, error: err2 } = await supabase
          .from("messages").select("session_id, role, content, created_at")
          .in("session_id", ids).order("created_at", { ascending: false });

        const lastBySession = new Map();
        if (!err2 && Array.isArray(lastMsgs)) {
          for (const m of lastMsgs) if (!lastBySession.has(m.session_id)) lastBySession.set(m.session_id, m);
        }

        return res.json({
          success: true, error: false,
          conversations: supaSessions.map((s) => {
            const last = lastBySession.get(s.session_id);
            return {
              conversationId: s.session_id,
              createdAt: s.created_at,
              updatedAt: s.updated_at,
              lastMessageRole: last?.role || null,
              lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
            };
          })
        });
      }
    }

    // Repli local
    const rows = await dbAll(
      `SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? ORDER BY updated_at DESC LIMIT 50`,
      [req.userId]
    );
    const enriched = await Promise.all(rows.map(async (conv) => {
      const last = await dbGet(`SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1`, [conv.session_id]);
      return {
        conversationId: conv.session_id,
        createdAt: new Date(conv.created_at).toISOString(),
        updatedAt: new Date(conv.updated_at).toISOString(),
        lastMessageRole: last?.role || null,
        lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
      };
    }));
    return res.json({ success: true, error: false, conversations: enriched });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur /api/conversations");
    return res.status(500).json({ success: false, error: true, conversations: [] });
  }
});

app.get("/api/conversation/:conversationId/messages", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { conversationId } = req.params;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    // ⚠️ PHASE 2 : propriété vérifiée localement ET côté Supabase
    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    const wantsFull = req.query.full === "true";
    const reqLimit = parseInt(req.query.limit, 10);
    const limit = wantsFull ? 500 : (Number.isFinite(reqLimit) && reqLimit > 0 ? Math.min(reqLimit, 200) : CONFIG.MAX_HISTORY_LENGTH);

    const messages = await getFullHistory(conversationId, req.userId, limit);
    return res.json({ success: true, error: false, conversationId, messages, count: messages.length });
  } catch (e) {
    return res.status(500).json({ success: false, error: true, code: "HISTORY_FETCH_ERROR" });
  }
});

// ================================================================================
// §4.8 — /api/user/stats
// ================================================================================

app.get("/api/user/stats", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const today = todayKeyMs();
    const quota = await dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [req.userId, today]);
    return res.json({
      success: true, error: false,
      data: {
        quotas: quota || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
        role: req.userRole || "FREE",
        limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
        userId: req.userId,
        firebaseUid: req.firebaseUid
      }
    });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "STATS_ERROR" });
  }
});

// ================================================================================
// §4.9 — SESSIONS ACTIVES
// ================================================================================

app.post("/api/session/create", authLimiter, authenticateUser, async (req, res) => {
  try {
    const token = await createActiveSession(req.userId, req.ip, req.headers["user-agent"]);
    return res.json({
      success: true, error: false,
      data: { sessionToken: token, expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString() }
    });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "SESSION_CREATE_ERROR" });
  }
});

app.post("/api/session/revoke", authLimiter, authenticateUser, async (req, res) => {
  try {
    const { sessionToken } = req.body || {};
    if (sessionToken) await revokeSession(req.userId, sessionToken);
    else await revokeAllSessions(req.userId);
    return res.json({
      success: true, error: false,
      message: sessionToken ? "Session révoquée." : "Toutes les sessions révoquées."
    });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "SESSION_REVOKE_ERROR" });
  }
});

// ================================================================================
// §4.10 — /api/tools (⚠️ PHASE 2 : whitelist + quota + paramètres validés)
// ================================================================================

app.post("/api/tools", toolLimiter, authenticateUser, async (req, res) => {
  try {
    const toolName = req.body?.toolName || req.body?.action;
    const params = req.body?.params || req.body?.arguments || {};

    if (!toolName || typeof toolName !== "string") {
      return res.status(400).json({ success: false, error: true, code: "MISSING_TOOL_NAME" });
    }
    if (!TOOLS_BY_CONTEXT.api.includes(toolName)) {
      return res.status(403).json({ success: false, error: true, reply: "Outil non autorisé.", code: "TOOL_NOT_ALLOWED" });
    }

    // ⚠️ Quota explicite par outil
    if (toolName === "send_email") {
      const q = await checkUserQuota(req.userId, "email", req.userRole);
      if (!q.allowed) return res.status(429).json({ success: false, error: true, reply: q.message, code: "EMAIL_QUOTA_EXCEEDED" });
    }

    const googleAccessToken = req.headers["x-google-access-token"] || null;
    const { result, sourceKeys } = await executeToolNative(toolName, params, {
      userId: req.userId,
      googleAccessToken,
      agentMode: false // ⚠️ /api/tools n'autorise PAS les effets de bord sans confirmation
    });

    if (result?.code === "NEEDS_CONFIRMATION") {
      return res.status(202).json({
        success: false, error: false,
        reply: "Confirmation requise.",
        code: "NEEDS_CONFIRMATION",
        confirmation: { toolName, args: params }
      });
    }

    const sources = sourceKeys.map((k) => OPEN_SOURCES[k]).filter(Boolean);
    return res.json({ success: true, error: false, toolName, result, sources });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur /api/tools");
    return res.status(500).json({ success: false, error: true, code: "TOOL_EXECUTION_ERROR" });
  }
});

// ================================================================================
// §4.11 — TÂCHES CRUD
// ================================================================================

app.get("/api/tasks", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const status = req.query.status && ["pending", "done"].includes(req.query.status) ? req.query.status : null;
    const result = await listTasks(req.userId, { status });
    return res.json({ success: true, error: false, tasks: result.tasks || [] });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "TASKS_FETCH_ERROR" });
  }
});

app.post("/api/tasks", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { title, notes, dueAt } = req.body || {};
    const result = await createTask(req.userId, {
      title,
      notes,
      dueAt: dueAt ? Date.parse(dueAt) : null
    });
    if (!result.success) return res.status(400).json({ success: false, error: true, reply: result.error, code: "TASK_CREATE_INVALID" });
    return res.status(201).json({ success: true, error: false, task: result.task });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "TASK_CREATE_ERROR" });
  }
});

app.put("/api/tasks/:taskId/status", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { status } = req.body || {};
    if (!["pending", "done"].includes(status)) {
      return res.status(400).json({ success: false, error: true, code: "INVALID_STATUS" });
    }
    const result = await updateTaskStatus(req.userId, req.params.taskId, status);
    if (!result.success) return res.status(404).json({ success: false, error: true, code: "TASK_NOT_FOUND" });
    return res.json({ success: true, error: false, task: result.task });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "TASK_UPDATE_ERROR" });
  }
});

app.delete("/api/tasks/:taskId", apiLimiter, authenticateUser, async (req, res) => {
  try {
    await deleteTask(req.userId, req.params.taskId);
    return res.json({ success: true, error: false });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "TASK_DELETE_ERROR" });
  }
});

// ================================================================================
// §4.12 — YOUTUBE
// ================================================================================

app.get("/api/youtube/search", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const q = req.query.q;
    if (!q || typeof q !== "string") return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    const entity = extractEntity(q) || q;
    const result = await searchYouTube(entity);
    return res.json({ success: true, error: false, videos: result.videos || [] });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "YOUTUBE_SEARCH_ERROR" });
  }
});

// ================================================================================
// §4.13 — TRANSCRIPTION VOCALE
// ================================================================================

app.post("/api/voice/transcribe", apiLimiter, authenticateUser, uploadAudio.single("audio"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: true, reply: "Aucun fichier audio.", code: "MISSING_AUDIO" });

    const quota = await checkUserQuota(req.userId, "message", req.userRole);
    if (!quota.allowed) return res.status(429).json({ success: false, error: true, reply: quota.message, code: "QUOTA_EXCEEDED" });

    const result = await transcribeAudioGroq(req.file.buffer, req.file.originalname, req.file.mimetype);
    if (!result.success) return res.status(502).json({ success: false, error: true, reply: "Transcription indisponible.", code: "TRANSCRIPTION_FAILED" });

    return res.json({ success: true, error: false, text: sanitizeForLLM(result.text, 5000) });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur transcription");
    return res.status(500).json({ success: false, error: true, code: "VOICE_TRANSCRIBE_ERROR" });
  }
});

// ================================================================================
// §4.14 — WHATSAPP ROUTES
// ================================================================================

app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const result = await whatsappManager.initClient(req.userId);
    if (result.connected) {
      return res.json({ success: true, error: false, message: "WhatsApp déjà connecté.", data: { qrCode: null } });
    }

    let qr = null;
    const start = Date.now();
    while (!qr && Date.now() - start < CONFIG.WHATSAPP_QR_TIMEOUT) {
      await sleep(500);
      qr = whatsappManager.getQRCode(req.userId);
    }
    if (qr) return res.json({ success: true, error: false, message: "Connexion initiée", data: { qrCode: qr } });
    return res.status(408).json({ success: false, error: true, message: "Délai dépassé.", code: "QR_TIMEOUT" });
  } catch (e) {
    logger.error({ err: e.message }, "WhatsApp connect échec");
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_CONNECT_ERROR" });
  }
});

app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const { to, message } = req.body || {};
    if (!to || !message) return res.status(400).json({ success: false, error: true, code: "MISSING_PARAMS" });

    const quota = await checkUserQuota(req.userId, "whatsapp", req.userRole);
    if (!quota.allowed) return res.status(429).json({ success: false, error: true, reply: quota.message, code: "WHATSAPP_QUOTA_EXCEEDED" });

    const cleanTo = String(to).replace(/[^\d]/g, "");
    if (!PHONE_REGEX.test(cleanTo)) return res.status(400).json({ success: false, error: true, code: "INVALID_PHONE" });

    const result = await whatsappManager.sendMessage(req.userId, cleanTo, sanitizeStrict(message, 2000));
    await incrementUserQuota(req.userId, "whatsapp");
    return res.json({ success: true, error: false, data: result });
  } catch (e) {
    if (e.code === "WHATSAPP_NOT_CONNECTED") {
      return res.status(409).json({ success: false, error: true, reply: "WhatsApp non connecté.", code: "WHATSAPP_NOT_CONNECTED" });
    }
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_SEND_ERROR" });
  }
});

// ================================================================================
// §4.15 — /api/intent/init
// ================================================================================

app.post("/api/intent/init", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { intentType } = req.body || {};
    const conversationId = req.body?.conversationId || req.body?.conversation_id;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    await getSession(conversationId, req.userId, req.firebaseUid);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(conversationId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.json({ success: true, error: false, reply: "Envoi WhatsApp initié. Quel est le numéro ?" });
    }
    if (intentType === "EMAIL") {
      await setActiveIntent(conversationId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.json({ success: true, error: false, reply: "Envoi d'email initié. Quelle est l'adresse ?" });
    }
    return res.status(400).json({ success: false, error: true, code: "UNKNOWN_INTENT" });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "INTENT_ERROR" });
  }
});

// ================================================================================
// §4.16 — MÉMOIRE (RGPD) : lecture / effacement
// ================================================================================

app.get("/api/user/memory", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const memory = await getUserMemory(req.userId);
    return res.json({ success: true, error: false, memory: memory || null });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "MEMORY_FETCH_ERROR" });
  }
});

app.delete("/api/user/memory", apiLimiter, authenticateUser, async (req, res) => {
  try {
    await dbRun("DELETE FROM user_memory WHERE user_id = ?", [req.userId]);
    if (supabase) {
      await supabaseWriteSafe({
        table: "user_memory", op: "delete", payload: {},
        matchColumn: "user_id", matchValue: req.userId,
        idempotencyKey: `memory_delete:${req.userId}`
      });
    }
    return res.json({ success: true, error: false, message: "Mémoire long terme effacée." });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "MEMORY_DELETE_ERROR" });
  }
});

// ⚠️ PHASE 2 : /api/memory/clear vérifie maintenant la propriété AVANT tout delete.
app.post("/api/memory/clear", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const conversationId = req.body?.conversationId || req.body?.conversation_id;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    // ⚠️ Correction critique : sans ceci, n'importe quel utilisateur pouvait effacer
    // la conversation d'un autre en connaissant son ID.
    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);
    await clearActiveIntent(conversationId);

    if (supabase) {
      await supabaseWriteSafe({
        table: "messages", op: "delete", payload: {},
        matchColumn: "session_id", matchValue: conversationId,
        idempotencyKey: `messages_clear:${conversationId}:${Date.now()}`
      });
    }
    return res.json({ success: true, error: false, reply: "Mémoire effacée." });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur /api/memory/clear");
    return res.status(500).json({ success: false, error: true, code: "MEMORY_CLEAR_ERROR" });
  }
});

// ================================================================================
// §4.17 — ADMIN
// ================================================================================

app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(["ADMIN"]), async (req, res) => {
  try {
    const { uid, role } = req.body || {};
    if (!uid || !["FREE", "PREMIUM", "ADMIN"].includes(role)) {
      return res.status(400).json({ success: false, error: true, code: "INVALID_PARAMS" });
    }
    if (firebaseApp && firebaseAdmin) {
      await firebaseAdmin.auth(firebaseApp).setCustomUserClaims(uid, { role });
    }
    await dbRun(`UPDATE users SET role = ?, updated_at = ? WHERE firebase_uid = ? OR id = ?`, [role, Date.now(), uid, uid]);
    await logSecurityEvent(req.userId, "ROLE_UPDATED", { targetUid: uid, newRole: role }, req.ip, req.headers["user-agent"]);
    return res.json({ success: true, error: false, data: { uid, role } });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur /api/admin/set-role");
    return res.status(500).json({ success: false, error: true, code: "ROLE_UPDATE_ERROR" });
  }
});

// ================================================================================
// §4.18 — /api/account (RGPD : suppression complète)
// ================================================================================

app.delete("/api/account", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const firebaseUid = req.firebaseUid;

    // 1) Ferme la session WhatsApp + supprime son dossier
    try {
      const waSession = whatsappManager.sessions.get(userId);
      if (waSession?.sock) waSession.sock.end(undefined);
      whatsappManager.sessions.delete(userId);
      const authDir = path.join(CONFIG.SESSIONS_PATH, userId);
      if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
    } catch {}

    // 2) Local
    await dbTransaction(async ({ dbRun }) => {
      await dbRun("DELETE FROM messages WHERE session_id IN (SELECT session_id FROM sessions WHERE user_id = ?)", [userId]);
      await dbRun("DELETE FROM sessions WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM email_logs WHERE user_id = ? OR firebase_uid = ?", [userId, firebaseUid]);
      await dbRun("DELETE FROM llm_audit_log WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM security_logs WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_quotas WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM active_sessions WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_tasks WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_memory WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM token_cache WHERE uid = ?", [userId]);
      await dbRun("DELETE FROM users WHERE id = ?", [userId]);
    });

    // 3) Supabase (via outbox pour ne rien perdre)
    if (supabase) {
      const tables = ["messages", "sessions", "user_tasks", "user_memory", "whatsapp_credentials"];
      for (const t of tables) {
        await supabaseWriteSafe({
          table: t, op: "delete", payload: {},
          matchColumn: "user_id", matchValue: userId,
          idempotencyKey: `account_delete:${t}:${userId}`
        }).catch(() => {});
      }
      await supabaseWriteSafe({
        table: "users", op: "delete", payload: {},
        matchColumn: "firebase_uid", matchValue: firebaseUid,
        idempotencyKey: `account_delete:users:${userId}`
      }).catch(() => {});
    }

    // 4) Firebase
    let firebaseDeleted = false;
    if (firebaseApp && firebaseAdmin) {
      try {
        await firebaseAdmin.auth(firebaseApp).deleteUser(firebaseUid);
        firebaseDeleted = true;
      } catch (e) { logger.warn({ err: e.message }, "Suppression Firebase échouée"); }
    }

    return res.json({
      success: true, error: false,
      message: "Compte supprimé avec succès.",
      code: "ACCOUNT_DELETED",
      firebaseAccountDeleted: firebaseDeleted
    });
  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "Erreur /api/account");
    return res.status(500).json({ success: false, error: true, code: "ACCOUNT_DELETION_ERROR" });
  }
});

// ================================================================================
// §4.19 — 404 + gestion d'erreur globale
// ================================================================================

app.use((req, res) => {
  res.status(404).json({
    success: false, error: true,
    reply: "Route non trouvée",
    code: "NOT_FOUND"
    // ⚠️ PHASE 2 : on n'expose PLUS la liste complète des routes (énumération = surface d'attaque).
  });
});

app.use((error, req, res, next) => {
  logger.error({ err: error.message, stack: error.stack, path: req.path }, "Erreur non gérée");

  if (res.headersSent) return next(error);

  // ⚠️ Multer : message clair sans exposer les détails
  if (error.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({ success: false, error: true, reply: `Fichier trop volumineux (max ${CONFIG.MAX_IMAGE_SIZE_MB} Mo).`, code: "FILE_TOO_LARGE" });
  }
  if (error.code === "LIMIT_FILE_COUNT") {
    return res.status(413).json({ success: false, error: true, reply: "Trop de fichiers.", code: "TOO_MANY_FILES" });
  }

  return res.status(500).json({
    success: false, error: true,
    reply: "Une erreur interne est survenue.",
    code: "INTERNAL_ERROR"
  });
});

// ================================================================================
// §4.20 — HOUSEKEEPING + DÉMARRAGE + GRACEFUL SHUTDOWN
// ================================================================================

async function runSecurityHousekeeping() {
  try {
    const now = Date.now();
    await dbRun(`DELETE FROM blocked_ips WHERE blocked_until < ?`, [now]);
    await dbRun(`DELETE FROM login_attempts WHERE created_at < ?`, [now - 30 * 24 * 3600 * 1000]);
    await dbRun(`DELETE FROM security_logs WHERE created_at < ?`, [now - 90 * 24 * 3600 * 1000]);
    await dbRun(`DELETE FROM active_sessions WHERE expires_at < ?`, [now - 7 * 24 * 3600 * 1000]);
    await dbRun(`DELETE FROM llm_audit_log WHERE created_at < ?`, [now - 30 * 24 * 3600 * 1000]);
    await dbRun(`DELETE FROM token_cache WHERE expires_at < ?`, [now]);
    await dbRun(`DELETE FROM conversation_locks WHERE locked_until < ?`, [now - 5 * 60 * 1000]);
    await dbRun(`DELETE FROM outbox WHERE status IN ('done','dead') AND updated_at < ?`, [now - 7 * 24 * 3600 * 1000]);
    logger.info("🧹 Nettoyage périodique effectué");
  } catch (e) {
    logger.error({ err: e.message }, "Erreur housekeeping");
  }
}

const HOUSEKEEPING_INTERVAL_MS = parseInt(process.env.HOUSEKEEPING_INTERVAL_MS || String(6 * 3600 * 1000), 10);
setInterval(runSecurityHousekeeping, HOUSEKEEPING_INTERVAL_MS).unref?.();

// -------- Démarrage --------

const server = app.listen(CONFIG.PORT, () => {
  global.__luba_server = server;

  logger.info(`Serveur ${CONFIG.AGENT_NAME} v${CONFIG.VERSION} démarré sur :${CONFIG.PORT}`);

  console.log("");
  console.log("╔══════════════════════════════════════════════════════════╗");
  console.log(`║  🚀 LUBA BACKEND v${CONFIG.VERSION} — HIKLON TECHNOLOGIES      ║`);
  console.log("╚══════════════════════════════════════════════════════════╝");
  console.log("🌐 Domaine      : " + HOSTING_CONFIG.domain);
  console.log("🔐 Firebase     : " + (firebaseApp ? "Admin SDK ✅" : "REST API ⚠️"));
  console.log("💾 Persistance  : " + (supabase ? "Supabase ✅" : "SQLite ⚠️"));
  console.log("📧 Email        : " + (emailTransporter ? "SMTP ✅" : (process.env.RESEND_API_KEY ? "Resend ✅" : "Non configuré ⚠️")));
  console.log("📱 WhatsApp     : " + (process.env.WHATSAPP_ENCRYPTION_KEY ? "Chiffré ✅" : "⚠️"));
  console.log("🛡️  Rate limit  : " + (redisRateLimitStore ? "Redis ✅" : "Mémoire ⚠️"));
  console.log("🧪 Sandbox      : " + (CONFIG.CODE_SANDBOX_PROVIDER || "Non configuré ⚠️"));
  console.log("📡 SSE          : ✅ /api/chat (Accept: text/event-stream)");
  console.log("");
  console.log("🤖 PROVIDERS LLM :");
  console.log(`   ├─ Groq        : ${LLM_PROVIDERS.GROQ.keyPool.length} clé(s)`);
  console.log(`   ├─ OpenRouter  : ${LLM_PROVIDERS.OPENROUTER.keyPool.length} clé(s)`);
  console.log(`   ├─ Cerebras    : ${LLM_PROVIDERS.CEREBRAS.keyPool.length} clé(s)`);
  console.log(`   └─ Gemini      : ${geminiClient ? "Actif ✅" : "Inactif ⚠️"}`);
  console.log("");
  console.log("🧠 NOUVEAUTÉS v15 :");
  console.log("   ├─ Math LaTeX brut (plus de JSON cassé)");
  console.log("   ├─ Streaming SSE opt-in");
  console.log("   ├─ Circuit breaker par (provider+modèle+clé)");
  console.log("   ├─ Tool calling natif");
  console.log("   ├─ Sandbox run_code (Piston/Judge0)");
  console.log("   ├─ Outbox Supabase (retry idempotent)");
  console.log("   ├─ Mutex par conversation");
  console.log("   ├─ Scheduler de rappels");
  console.log("   ├─ WhatsApp durci (creds complets + whitelist)");
  console.log("   ├─ Hydratation Supabase → SQLite");
  console.log("   └─ Auth cache 5 min (verifyIdToken évité)");
  console.log("");

  // ⚠️ Vérification environnementale des modèles en arrière-plan (déjà lancée).
});

server.on("error", (err) => {
  logger.fatal({ err: err.message }, "Erreur du serveur HTTP");
  process.exit(1);
});

// -------- Graceful shutdown --------

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre du serveur");

  // 1) Stop nouvelle acceptation de connexions
  await new Promise((resolve) => server.close(resolve));

  // 2) WhatsApp
  try { await whatsappManager.destroyAll(); } catch (e) { logger.warn({ err: e.message }, "Fermeture WhatsApp"); }

  // 3) Queues
  try { await queueManager.close(); } catch (e) { logger.warn({ err: e.message }, "Fermeture files"); }

  // 4) Dernière tentative d'outbox
  try { await processOutbox(); } catch {}

  // 5) DB
  await new Promise((resolve) => db.close(() => resolve()));

  console.log("✅ Arrêt propre terminé");
  process.exit(0);
}

process.removeAllListeners("SIGINT");
process.removeAllListeners("SIGTERM");
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// ================================================================================
// ==================== EXPORTS FINAUX ==========================================
// ================================================================================

Object.assign(module.exports, {
  app, server,
  runSecurityHousekeeping,
  gracefulShutdown
});

// ================================================================================
// ==================== FIN DU FICHIER index.js =================================
// ================================================================================
// Ligne cible : ~6200 lignes.
// Structure :
//   PARTIE 1 : fondations (config, DB v2, sécurité, math worker, providers) ~1300
//   PARTIE 2 : auth, tool calling, sandbox, WhatsApp, scheduler              ~900
//   PARTIE 3 : handleChat v15, SSE, images, mémoire, hydratation            ~1500
//   PARTIE 4 : Express, routes, démarrage, tests                            ~1300
//   + commentaires, tests embarqués, sections = ~6200 lignes
// ================================================================================
