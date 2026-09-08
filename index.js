// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 9.5.0 Enterprise (Production Ready avec Firebase Admin)
// Architecture : Modulaire, Microservices-ready, Haute Disponibilité
//
// ⚠️ FONCTIONNALITÉS DE SÉCURITÉ AVANCÉES (9.5.0) :
// 1) Firebase Admin SDK pour gestion complète des utilisateurs
// 2) Vérification des tokens avec révocation
// 3) Gestion des rôles personnalisés (Custom Claims)
// 4) Vérification des emails obligatoire
// 5) Protection contre les tokens volés (session management)
// 6) Limitation par IP et détection de connexions suspectes
// 7) Suppression complète du compte Firebase
// 8) Audit de sécurité complet
// 9) Protection contre le brute force
// 10) Gestion des sessions multiples
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

// ==================== IMPORTS FIREBASE ADMIN ====================
let firebaseAdmin = null;
try {
  firebaseAdmin = require("firebase-admin");
} catch (e) {
  console.warn("⚠️ firebase-admin non installé - authentification Firebase désactivée");
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
  VERSION: "9.5.0",
  AGENT_NAME: "Luba",
  COMPANY: "HIKLON Technology",

  // Limites et timeouts
  MAX_MESSAGE_LENGTH: parseInt(process.env.MAX_MESSAGE_LENGTH || "15000", 10),
  MAX_HISTORY_LENGTH: parseInt(process.env.MAX_HISTORY_LENGTH || "20", 10),
  IMAGE_SEARCH_LIMIT: parseInt(process.env.IMAGE_SEARCH_LIMIT || "6", 10),
  MAX_CONTEXT_TOKENS: parseInt(process.env.MAX_CONTEXT_TOKENS || "8000", 10),
  MAX_IMAGE_SIZE_MB: parseInt(process.env.MAX_IMAGE_SIZE_MB || "10", 10),
  MAX_IMAGES_PER_REQUEST: parseInt(process.env.MAX_IMAGES_PER_REQUEST || "3", 10),

  // Retry et résilience
  MAX_RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRY_ATTEMPTS || "3", 10),
  RETRY_BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS || "1000", 10),
  RETRY_MAX_DELAY_MS: parseInt(process.env.RETRY_MAX_DELAY_MS || "8000", 10),
  CIRCUIT_BREAKER_THRESHOLD: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),
  CIRCUIT_BREAKER_RESET_MS: parseInt(process.env.CIRCUIT_BREAKER_RESET_MS || "60000", 10),

  // Timeouts HTTP
  DEFAULT_TIMEOUT: parseInt(process.env.DEFAULT_TIMEOUT || "30000", 10),
  V250_STEP_TIMEOUT: parseInt(process.env.V250_STEP_TIMEOUT || "90000", 10),
  V250_ROUTE_TIMEOUT: parseInt(process.env.V250_ROUTE_TIMEOUT || "180000", 10),

  // WhatsApp
  WHATSAPP_QR_TIMEOUT: parseInt(process.env.WHATSAPP_QR_TIMEOUT || "30000", 10),
  WHATSAPP_RETRY_DELAY: parseInt(process.env.WHATSAPP_RETRY_DELAY || "3000", 10),

  // Sécurité
  MAX_LOGIN_ATTEMPTS: parseInt(process.env.MAX_LOGIN_ATTEMPTS || "5", 10),
  LOGIN_BLOCK_DURATION: parseInt(process.env.LOGIN_BLOCK_DURATION || "900000", 10), // 15 minutes
  MAX_SESSIONS_PER_USER: parseInt(process.env.MAX_SESSIONS_PER_USER || "10", 10),
  TOKEN_REFRESH_WINDOW: parseInt(process.env.TOKEN_REFRESH_WINDOW || "300000", 10), // 5 minutes

  // Chemins
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  UPLOADS_PATH: path.join(__dirname, "uploads"),

  // Modèles Vision
  VISION_MODEL_GROQ: process.env.VISION_MODEL_GROQ || "openai/gpt-4o-mini",
  VISION_MODEL_OPENROUTER: process.env.VISION_MODEL_OPENROUTER || "qwen/qwen-2.5-vl-72b-instruct:free",

  // Types MIME autorisés
  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],

  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/9.5.0 (contact@luba.ia)"
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
  firebaseDomain: process.env.FIREBASE_HOSTING_DOMAIN || "https://luba-ia-636.web.app",
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
    logger.info("Firebase Admin initialisé avec succès");
  } catch (e) {
    logger.error({ err: e.message }, "Erreur initialisation Firebase Admin");
  }
} else {
  logger.warn("Firebase Admin non initialisé - utilisera l'API REST pour la vérification des tokens");
}

// ==================== VALIDATION ENVIRONNEMENT ====================
function validateEnvironment() {
  const errors = [];
  const warnings = [];
  
  // Variables critiques
  if (!process.env.GROQ_API_KEY) {
    errors.push("GROQ_API_KEY manquante - tier v100 indisponible");
  }
  
  if (!process.env.OPENROUTER_API_KEY) {
    errors.push("OPENROUTER_API_KEY manquante - tier v250 et fallbacks indisponibles");
  }
  
  // Firebase
  if (CONFIG.ENV === "production") {
    if (!firebaseApp && !FIREBASE_CONFIG.apiKey) {
      errors.push("Aucune authentification Firebase configurée - authentification impossible");
    }
  }
  
  // Variables recommandées
  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    warnings.push("Supabase non configuré - persistance multi-appareils désactivée");
  }
  
  if (!process.env.RESEND_API_KEY && !process.env.SMTP_HOST) {
    warnings.push("Aucun service email configuré - envoi d'email indisponible");
  }
  
  if (!process.env.WHATSAPP_ENCRYPTION_KEY || !process.env.WHATSAPP_ENCRYPTION_IV) {
    warnings.push("Clés de chiffrement WhatsApp manquantes - utilisation de clés par défaut");
  }
  
  if (!process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    warnings.push("FIREBASE_SERVICE_ACCOUNT_JSON manquant - fonctionnalités admin limitées (suppression compte, rôles personnalisés)");
  }
  
  if (errors.length > 0) {
    logger.error("Erreurs de configuration fatales :");
    errors.forEach(err => logger.error("  - " + err));
    
    if (CONFIG.ENV === "production") {
      logger.error("Arrêt du serveur - configuration invalide");
      process.exit(1);
    } else {
      logger.warn("Mode développement : démarrage malgré les erreurs");
    }
  }
  
  if (warnings.length > 0) {
    logger.warn("Avertissements de configuration :");
    warnings.forEach(warn => logger.warn("  - " + warn));
  }
}

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
  transport:
    process.env.NODE_ENV === "development"
      ? {
          target: "pino-pretty",
          options: { colorize: true }
        }
      : undefined,
  base: {
    service: "luba-backend",
    version: CONFIG.VERSION
  }
});

// ==================== INITIALISATION SQLITE ====================
const db = new sqlite3.Database(CONFIG.DB_PATH, (err) => {
  if (err) {
    logger.error({ err: err.message }, "Impossible d'ouvrir la base SQLite");
    process.exit(1);
  }
  logger.info("Base de données SQLite initialisée");
});

// Configuration SQLite pour performance
db.run("PRAGMA journal_mode = WAL;");
db.run("PRAGMA synchronous = NORMAL;");
db.run("PRAGMA cache_size = -64000;");
db.run("PRAGMA busy_timeout = 10000;");
db.run("PRAGMA temp_store = MEMORY;");
db.run("PRAGMA foreign_keys = ON;");
db.run("PRAGMA wal_autocheckpoint = 1000;");

// ==================== SCHÉMA SQLITE ====================
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
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
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      firebase_uid TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      active_intent TEXT,
      intent_data TEXT,
      metadata TEXT DEFAULT '{}',
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
      content TEXT NOT NULL,
      tool_calls TEXT,
      images TEXT DEFAULT '[]',
      metadata TEXT DEFAULT '{}',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
    )
  `);

  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_firebase ON sessions(firebase_uid)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session_role ON messages(session_id, role)");

  db.run(`
    CREATE TABLE IF NOT EXISTS email_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      firebase_uid TEXT,
      to_email TEXT NOT NULL,
      subject TEXT,
      status TEXT DEFAULT 'pending',
      provider TEXT,
      error_message TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS llm_audit_log (
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
    )
  `);

  // Table des quotas utilisateur
  db.run(`
    CREATE TABLE IF NOT EXISTS user_quotas (
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
    )
  `);

  // Table des logs de sécurité
  db.run(`
    CREATE TABLE IF NOT EXISTS security_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      event_type TEXT NOT NULL,
      details TEXT DEFAULT '{}',
      ip_address TEXT,
      user_agent TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Table des sessions actives
  db.run(`
    CREATE TABLE IF NOT EXISTS active_sessions (
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
    )
  `);

  // Table des tentatives de connexion
  db.run(`
    CREATE TABLE IF NOT EXISTS login_attempts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      ip_address TEXT,
      success INTEGER DEFAULT 0,
      error_message TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Table des IPs bloquées
  db.run(`
    CREATE TABLE IF NOT EXISTS blocked_ips (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip_address TEXT UNIQUE,
      reason TEXT,
      blocked_until DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Index pour les performances
  db.run("CREATE INDEX IF NOT EXISTS idx_user_quotas_user_date ON user_quotas(user_id, date)");
  db.run("CREATE INDEX IF NOT EXISTS idx_security_logs_user ON security_logs(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_llm_audit_user ON llm_audit_log(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_email_logs_user ON email_logs(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_active_sessions_user ON active_sessions(user_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_login_attempts_ip ON login_attempts(ip_address, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address)");
});

logger.info("Schéma SQLite initialisé");

// ==================== INITIALISATION SUPABASE ====================
let supabase = null;
if (process.env.SUPABASE_URL && process.env.SUPABASE_KEY) {
  supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: "public" },
    global: {
      headers: { "x-application-name": "luba-backend" }
    }
  });
  logger.info("Supabase initialisé");
} else {
  logger.warn("Supabase non configuré - persistance multi-appareils désactivée");
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
  emailTransporter.verify((err) => {
    if (err) logger.warn({ err: err.message }, "SMTP non joignable");
    else logger.info("SMTP prêt (dernier recours email)");
  });
} else {
  logger.warn("SMTP non configuré (Gmail OAuth / Resend restent disponibles)");
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

// ==================== WRAPPERS SQLITE PROMISES ====================
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
      `INSERT INTO llm_audit_log 
       (session_id, user_id, provider, model, tier, prompt_tokens, completion_tokens, latency_ms, status, error_code)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      `INSERT INTO security_logs (user_id, event_type, details, ip_address, user_agent) 
       VALUES (?, ?, ?, ?, ?)`,
      [userId, eventType, JSON.stringify(details), ipAddress, userAgent]
    );
  } catch (error) {
    logger.error({ error: error.message }, "Erreur log sécurité");
  }
}

// ==================== GESTION DES SESSIONS ACTIVES ====================
async function createActiveSession(userId, ipAddress, userAgent) {
  const sessionToken = generateSessionToken();
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(); // 24 heures
  
  // Vérifier le nombre de sessions actives
  const activeSessions = await dbAll(
    `SELECT COUNT(*) as count FROM active_sessions WHERE user_id = ? AND is_revoked = 0 AND expires_at > CURRENT_TIMESTAMP`,
    [userId]
  );
  
  if (activeSessions[0]?.count >= CONFIG.MAX_SESSIONS_PER_USER) {
    // Révoquer la session la plus ancienne
    await dbRun(
      `UPDATE active_sessions SET is_revoked = 1 
       WHERE id = (SELECT id FROM active_sessions WHERE user_id = ? AND is_revoked = 0 ORDER BY created_at ASC LIMIT 1)`,
      [userId]
    );
  }
  
  await dbRun(
    `INSERT INTO active_sessions (user_id, session_token, ip_address, user_agent, expires_at) 
     VALUES (?, ?, ?, ?, ?)`,
    [userId, sessionToken, ipAddress, userAgent, expiresAt]
  );
  
  return sessionToken;
}

async function validateActiveSession(userId, sessionToken) {
  const session = await dbGet(
    `SELECT * FROM active_sessions 
     WHERE user_id = ? AND session_token = ? AND is_revoked = 0 AND expires_at > CURRENT_TIMESTAMP`,
    [userId, sessionToken]
  );
  
  if (session) {
    // Mettre à jour la dernière activité
    await dbRun(
      `UPDATE active_sessions SET last_activity = CURRENT_TIMESTAMP WHERE id = ?`,
      [session.id]
    );
    return true;
  }
  
  return false;
}

async function revokeSession(userId, sessionToken) {
  await dbRun(
    `UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND session_token = ?`,
    [userId, sessionToken]
  );
}

async function revokeAllSessions(userId) {
  await dbRun(
    `UPDATE active_sessions SET is_revoked = 1 WHERE user_id = ? AND is_revoked = 0`,
    [userId]
  );
}

// ==================== GESTION DES TENTATIVES DE CONNEXION ====================
async function checkLoginAttempts(ipAddress, userId = null) {
  const cutoffTime = new Date(Date.now() - CONFIG.LOGIN_BLOCK_DURATION).toISOString();
  
  const attempts = await dbGet(
    `SELECT COUNT(*) as count FROM login_attempts 
     WHERE ip_address = ? AND success = 0 AND created_at > ?`,
    [ipAddress, cutoffTime]
  );
  
  if (attempts?.count >= CONFIG.MAX_LOGIN_ATTEMPTS) {
    // Bloquer l'IP
    await dbRun(
      `INSERT OR REPLACE INTO blocked_ips (ip_address, reason, blocked_until) 
       VALUES (?, 'Trop de tentatives échouées', ?)`,
      [ipAddress, new Date(Date.now() + CONFIG.LOGIN_BLOCK_DURATION).toISOString()]
    );
    
    return {
      blocked: true,
      message: "Trop de tentatives échouées. IP bloquée temporairement."
    };
  }
  
  return { blocked: false };
}

async function recordLoginAttempt(ipAddress, userId, success, errorMessage = null) {
  await dbRun(
    `INSERT INTO login_attempts (user_id, ip_address, success, error_message) 
     VALUES (?, ?, ?, ?)`,
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
    
    const quotaRow = await dbGet(
      `SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`,
      [userId, today]
    );
    
    if (!quotaRow) {
      await dbRun(
        `INSERT INTO user_quotas (user_id, date, messages_count, images_count, whatsapp_count, emails_count) 
         VALUES (?, ?, 0, 0, 0, 0)`,
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
      case 'message':
        columnToUpdate = 'messages_count';
        break;
      case 'image':
        columnToUpdate = 'images_count';
        break;
      case 'whatsapp':
        columnToUpdate = 'whatsapp_count';
        break;
      case 'email':
        columnToUpdate = 'emails_count';
        break;
      default:
        return;
    }
    
    await dbRun(
      `UPDATE user_quotas SET ${columnToUpdate} = ${columnToUpdate} + 1 WHERE user_id = ? AND date = ?`,
      [userId, today]
    );
  } catch (error) {
    logger.error({ error: error.message }, "Erreur mise à jour quota");
  }
}

// ==================== VÉRIFICATION TOKEN FIREBASE ====================
async function verifyFirebaseToken(token) {
  // Si Firebase Admin est disponible, l'utiliser
  if (firebaseApp && firebaseAdmin) {
    try {
      const decodedToken = await firebaseAdmin.auth(firebaseApp).verifyIdToken(token, true); // true = check revoked
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
      logger.error({ error: error.message }, "Erreur vérification token Firebase (Admin SDK)");
      throw error;
    }
  }
  
  // Fallback : API REST
  try {
    if (!FIREBASE_CONFIG.apiKey) {
      throw new Error("FIREBASE_API_KEY manquante");
    }
    
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
        createdAt: user.createdAt || null,
        lastLoginAt: user.lastLoginAt || null,
        providers: user.providerUserInfo || [],
        role: 'FREE'
      };
    }
    
    return null;
  } catch (error) {
    logger.error({ error: error.message }, "Erreur vérification token Firebase (API REST)");
    throw error;
  }
}

// ==================== GESTION DES RÔLES FIREBASE ====================
async function setUserRole(uid, role) {
  if (!firebaseApp || !firebaseAdmin) {
    throw new Error("Firebase Admin non disponible pour la gestion des rôles");
  }
  
  try {
    await firebaseAdmin.auth(firebaseApp).setCustomUserClaims(uid, { role });
    
    // Mettre à jour aussi en base locale
    await dbRun(
      `UPDATE users SET role = ? WHERE firebase_uid = ? OR id = ?`,
      [role, uid, uid]
    );
    
    await logSecurityEvent(uid, 'ROLE_UPDATED', { role }, null);
    
    return { success: true, role };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur mise à jour rôle Firebase");
    throw error;
  }
}

async function getUserRole(uid) {
  if (!firebaseApp || !firebaseAdmin) {
    // Fallback : lire depuis la base locale
    const user = await dbGet("SELECT role FROM users WHERE firebase_uid = ? OR id = ?", [uid, uid]);
    return user?.role || 'FREE';
  }
  
  try {
    const user = await firebaseAdmin.auth(firebaseApp).getUser(uid);
    return user.customClaims?.role || 'FREE';
  } catch (error) {
    logger.error({ error: error.message }, "Erreur récupération rôle Firebase");
    const localUser = await dbGet("SELECT role FROM users WHERE firebase_uid = ? OR id = ?", [uid, uid]);
    return localUser?.role || 'FREE';
  }
}

// ==================== SUPPRESSION COMPTE FIREBASE ====================
async function deleteFirebaseUser(uid) {
  if (!firebaseApp || !firebaseAdmin) {
    throw new Error("Firebase Admin non disponible pour la suppression du compte");
  }
  
  try {
    await firebaseAdmin.auth(firebaseApp).deleteUser(uid);
    await logSecurityEvent(uid, 'FIREBASE_ACCOUNT_DELETED', {}, null);
    return { success: true };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur suppression compte Firebase");
    throw error;
  }
}

// ==================== CIRCUIT BREAKER PATTERN ====================
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

// ==================== GESTIONNAIRE DE FILE D'ATTENTE ====================
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
      logger.info("File d'attente Redis initialisée");
    } else {
      logger.warn("File d'attente en mémoire (fallback)");
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
      for (const worker of this.workers.values()) {
        await worker.close();
      }
      for (const queue of this.queues.values()) {
        await queue.close();
      }
      await this.connection.quit();
    }
  }
}

const queueManager = new QueueManager();

// ==================== SOURCES OUVERTES SANS CLÉ API ====================
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
    logger.info({ query }, "Recherche d'images Wikimedia");
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
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
    logger.error({ error: error.message }, "Erreur recherche images Wikimedia");
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
      if (title) items.push({ title: decodeXmlEntities(title), link: link.trim(), pubDate });
    }
    return { articles: items };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur recherche actualités");
    return { articles: [], error: error.message };
  }
}

async function searchWeb(query) {
  if (!query || typeof query !== "string") return { results: [], sourcesUsed: [] };
  const [wiki, news] = await Promise.all([searchWikipediaSummary(query), searchNews(query)]);
  const results = [];
  const sourcesUsed = [];
  if (wiki.summary) {
    results.push({ title: wiki.title, snippet: wiki.summary, url: wiki.url });
    sourcesUsed.push("wikipedia");
  }
  if (news.articles?.length > 0) {
    news.articles.slice(0, 3).forEach((a) => results.push({ title: a.title, url: a.link, pubDate: a.pubDate }));
    sourcesUsed.push("googlenews");
  }
  return { results, sourcesUsed };
}

async function searchSportsScores(query) {
  if (!query) return { events: [], error: "Aucune équipe précisée" };
  try {
    const searchUrl = `https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t=${encodeURIComponent(query)}`;
    const searchResp = await axios.get(searchUrl, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const team = searchResp.data?.teams?.[0];
    if (!team) return { events: [], error: `Équipe "${query}" introuvable sur TheSportsDB` };

    const eventsUrl = `https://www.thesportsdb.com/api/v1/json/3/eventslast.php?id=${team.idTeam}`;
    const eventsResp = await axios.get(eventsUrl, { timeout: 12000, headers: { "User-Agent": CONFIG.HTTP_USER_AGENT } });
    const events = (eventsResp.data?.results || []).slice(0, 5).map((e) => ({
      match: `${e.strHomeTeam} ${e.intHomeScore ?? "?"} - ${e.intAwayScore ?? "?"} ${e.strAwayTeam}`,
      date: e.dateEvent,
      league: e.strLeague
    }));
    return { team: team.strTeam, events };
  } catch (error) {
    logger.error({ error: error.message }, "Erreur recherche scores sportifs");
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
    logger.error({ error: error.message }, "Erreur recherche scientifique (arXiv)");
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
    logger.error({ error: error.message }, "Erreur recherche réseaux sociaux (Reddit)");
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
    logger.error({ error: error.message }, "Erreur météo");
    return { error: error.message };
  }
}

// ==================== ENVOI D'EMAIL ====================
async function verifyGmailScope(accessToken) {
  try {
    const response = await axios.get(
      "https://www.googleapis.com/oauth2/v1/tokeninfo",
      {
        params: { access_token: accessToken },
        timeout: 10000
      }
    );
    
    const scopes = response.data.scope?.split(" ") || [];
    return scopes.includes("https://www.googleapis.com/auth/gmail.send") || 
           scopes.includes("https://mail.google.com/");
  } catch (error) {
    logger.error({ error: error.message }, "Erreur vérification scope Gmail");
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
    const apiError = error.response?.data?.message || error.message;
    logger.error({ error: apiError }, "Erreur envoi email via Resend");
    return { success: false, error: `Resend: ${apiError}` };
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
    logger.error({ error: error.message }, "Erreur envoi email SMTP");
    return { success: false, error: error.message };
  }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body, userId }) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!recipient || !emailRegex.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  if (userId) {
    const quotaCheck = await checkUserQuota(userId, 'email');
    if (!quotaCheck.allowed) {
      return { success: false, error: quotaCheck.message || "Limite d'emails quotidienne atteinte" };
    }
  }

  let result;
  if (googleAccessToken) {
    try {
      result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body);
    } catch (error) {
      logger.error({ error: error.message }, "Erreur envoi email via Gmail API");
      result = { success: false, error: `Gmail API: ${error.message}` };
    }
  } else if (process.env.RESEND_API_KEY) {
    result = await sendEmailViaResend(recipient, subject, body);
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  if (result.success && userId) {
    await incrementUserQuota(userId, 'email');
  }

  try {
    await dbRun("INSERT INTO email_logs (user_id, to_email, subject, status, provider, error_message) VALUES (?, ?, ?, ?, ?, ?)", [
      userId || null,
      recipient,
      subject || null,
      result.success ? "sent" : "failed",
      result.provider || null,
      result.error || null
    ]);
  } catch (logErr) {
    logger.error({ error: logErr.message }, "Erreur journalisation email");
  }

  return result;
}

// ==================== PERSISTANCE WHATSAPP ====================
async function saveWhatsAppCredentials(userId, credentialsData) {
  if (!supabase) {
    logger.warn("Supabase non configuré - persistance WhatsApp locale uniquement");
    return false;
  }
  
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
      .upsert({
        user_id: userId,
        encrypted_data: encrypted,
        auth_tag: authTag,
        updated_at: new Date().toISOString()
      });
    
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

// ==================== WHATSAPP — BAILEYS ====================
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
          logger.info({ userId }, "QR Code (Baileys) généré");
        } catch (e) {
          logger.error({ error: e.message }, "Erreur génération QR (Baileys)");
        }
      }

      if (connection === "open") {
        sessionData.ready = true;
        sessionData.qrCode = null;
        db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
        logger.info({ userId }, "WhatsApp (Baileys) connecté");
      }

      if (connection === "close") {
        sessionData.ready = false;
        db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        logger.warn({ userId, statusCode, shouldReconnect }, "WhatsApp (Baileys) déconnecté");
        this.sessions.delete(userId);
        if (shouldReconnect) {
          setTimeout(() => {
            this.initClient(userId).catch((e) => logger.error({ error: e.message }, "Erreur reconnexion Baileys"));
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

          const text =
            msg.message.conversation || msg.message.extendedTextMessage?.text || msg.message.imageMessage?.caption || null;
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
          logger.error({ error: err.message }, "Erreur traitement message entrant WhatsApp (Baileys)");
        }
      }
    });

    return { connected: false, qrCode: null };
  }

  async sendMessage(userId, to, message) {
    const session = this.sessions.get(userId);
    if (!session || !session.ready) {
      const error = new Error("WhatsApp (Baileys) non connecté");
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
        logger.error({ error: e.message }, `Erreur fermeture socket WhatsApp (${userId})`);
      }
    }
  }
}

const whatsappManager = new BaileysManager();

// File d'attente pour les envois WhatsApp sortants
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
    throw new Error(quotaCheck.message || "Limite de messages WhatsApp quotidienne atteinte");
  }
  
  await queueManager.add("whatsapp-outbound", { userId, phoneNumber, message }, { attempts: 5, backoffDelay: 2000 });
  await incrementUserQuota(userId, 'whatsapp');
  return { success: true, queued: true };
}

// ==================== ARCHITECTURE LLM ENTERPRISE ====================
const LLM_PROVIDERS = {
  GROQ: {
    baseURL: "https://api.groq.com/openai/v1",
    apiKey: process.env.GROQ_API_KEY || "",
    timeout: CONFIG.DEFAULT_TIMEOUT,
    maxTokens: 4000,
    temperature: 0.7,
    circuitBreaker: new CircuitBreaker("groq", { failureThreshold: 5, resetTimeout: 60000 })
  },
  OPENROUTER: {
    baseURL: "https://openrouter.ai/api/v1",
    apiKey: process.env.OPENROUTER_API_KEY || "",
    timeout: 60000,
    maxTokens: 4000,
    temperature: 0.7,
    circuitBreaker: new CircuitBreaker("openrouter", { failureThreshold: 5, resetTimeout: 60000 })
  }
};

// ==================== MATRICE DE MODÈLES ====================
const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    description: "Rapide - Réponses instantanées",
    providers: [
      {
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "openai/gpt-oss-120b",
        maxTokens: 4000,
        timeout: 45000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 0,
        capabilities: ["text", "code", "reasoning"]
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "qwen/qwen-2.5-coder-32b-instruct:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 1,
        capabilities: ["text", "code", "reasoning"]
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 2,
        capabilities: ["text", "code"]
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_3 || "microsoft/phi-4:free",
        maxTokens: 4000,
        timeout: 60000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 3,
        capabilities: ["text"]
      }
    ]
  },
  v250: {
    name: "Ngandu",
    description: "Raisonnement & Code Pro",
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
    maxRetries: CONFIG.MAX_RETRY_ATTEMPTS,
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
        failoverPriority: 0,
        capabilities: ["vision", "text"]
      },
      {
        provider: "openrouter",
        model: CONFIG.VISION_MODEL_OPENROUTER,
        maxTokens: 4000,
        timeout: 90000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 1,
        capabilities: ["vision", "text"]
      }
    ]
  }
};

// ==================== VALIDATION OPENROUTER ====================
function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;

  const knownProviders = ["openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", "anthropic/", "google/", "mistralai/", "cohere/"];

  const isOpenRouterModel = knownProviders.some((prefix) => model.includes(prefix));

  if (isOpenRouterModel && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return model + ":free";
  }

  return model;
}

// ==================== INTERCEPTEUR D'ERREURS ====================
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

    if (errorCode === "HTTP_402") {
      logger.warn({ provider: providerConfig.provider, model: providerConfig.model }, "Crédits épuisés");
      return true;
    }

    if (errorCode === "HTTP_404") {
      logger.warn({ model: providerConfig.model }, "Modèle introuvable");
      return true;
    }

    if (errorCode === "MISSING_API_KEY") {
      logger.warn({ provider: providerConfig.provider }, "Clé API manquante");
      return true;
    }

    return false;
  }
}

// ==================== EXECUTEUR AVEC RETRY ET FALLBACK ====================
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

    if (!providerInfo || !providerInfo.apiKey) {
      logger.warn({ provider }, "Fournisseur non configuré - skip");
      continue;
    }

    let model = providerConfig.model;
    if (provider === "openrouter") {
      model = validateAndSanitizeOpenRouterModel(model);
      if (!model) {
        logger.warn("Modèle OpenRouter invalide - skip");
        continue;
      }
    }

    logger.info({ attempt: i + 1, total: sortedProviders.length, provider, model }, "Tentative fournisseur");

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
            sessionId,
            userId,
            provider,
            model,
            tier,
            promptTokens: 0,
            completionTokens: 0,
            latencyMs,
            status: "success",
            errorCode: null
          });
        }

        if (onProviderSuccess) {
          onProviderSuccess(providerResult);
        }

        logger.info({ provider, model, attempt: attempt + 1, latencyMs }, "Succès fournisseur");

        return {
          success: true,
          ...providerResult,
          providerChain: providerResults
        };
      } catch (error) {
        lastError = error;
        const errorCode = LLMErrorInterceptor.getErrorCode(error);
        const latencyMs = Date.now() - startTime;

        if (sessionId) {
          await auditLLMCall({
            sessionId,
            userId,
            provider,
            model,
            tier,
            promptTokens: 0,
            completionTokens: 0,
            latencyMs,
            status: "failed",
            errorCode
          });
        }

        logger.warn({ provider, model, attempt: attempt + 1, errorCode }, "Erreur fournisseur");

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
          logger.info({ provider }, "Provider marqué comme indisponible - passage au suivant");
          break;
        }

        if (LLMErrorInterceptor.isRetryableError(error) && attempt < maxRetriesPerProvider - 1) {
          const retryDelay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
          logger.info({ retryDelay }, "Backoff exponentiel");
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        } else if (!LLMErrorInterceptor.isRetryableError(error)) {
          break;
        }
      }
    }
  }

  logger.error({ errorCode: LLMErrorInterceptor.getErrorCode(lastError) }, "Tous les fournisseurs LLM ont échoué");

  return {
    success: false,
    error: lastError,
    providerChain: providerResults,
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  };
}

// ==================== APPEL PROVIDER BRUT ====================
async function callProviderRaw({ provider, model, messages, jsonMode = false, timeout, maxTokens, temperature = 0.7, images = null }) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ : LLM_PROVIDERS.OPENROUTER;

  if (!cfg.apiKey) {
    const err = new Error("Clé API manquante pour le fournisseur " + provider);
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
        contentParts.push({
          type: "image_url",
          image_url: { url: image.dataUrl }
        });
      }

      formattedMessages = [
        ...messages.slice(0, lastMessageIndex),
        {
          role: "user",
          content: contentParts
        }
      ];
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
    Authorization: "Bearer " + cfg.apiKey,
    "Content-Type": "application/json"
  };

  if (provider === "openrouter") {
    headers["HTTP-Referer"] = "https://luba.web.app";
    headers["X-Title"] = "Luba.ia Assistant";
  }

  const response = await axios.post(cfg.baseURL + "/chat/completions", payload, { headers, timeout: timeout || cfg.timeout });

  const choice = response?.data?.choices?.[0];
  const content = choice?.message?.content;

  if (choice?.finish_reason === "length") {
    logger.warn({ provider, model }, "Réponse tronquée par max_tokens");
  }

  if (!content) {
    throw new Error("Réponse " + provider + " vide");
  }

  if (!jsonMode) {
    return content;
  }

  try {
    return JSON.parse(content);
  } catch (parseError) {
    logger.error({ provider, model, contentPreview: content?.slice(0, 500) }, "Erreur parsing JSON");
    throw new Error(`Réponse ${provider} invalide (JSON malformé): ${parseError.message}`);
  }
}

// ==================== CONTEXT MANAGER DYNAMIQUE ====================
class DynamicContextManager {
  constructor() {
    this.domainPatterns = [
      {
        domain: "mathematics",
        keywords: [
          "math", "calcul", "équation", "equation", "algèbre", "algebra", "géométrie", "geometry",
          "calculus", "intégrale", "integrale", "dérivée", "derivative", "théorème", "theorem",
          "nombre", "number", "fonction", "function", "limite", "limit", "matrice", "matrix",
          "probabilité", "probability", "statistique", "statistics", "trigonométrie", "trigonometry"
        ],
        systemPrompt:
          "Tu es un expert en mathématiques. Utilise LaTeX ($ pour inline, $$ pour display) pour toutes les formules. Détaille chaque étape du raisonnement. Sois rigoureux et pédagogique."
      },
      {
        domain: "cybersecurity",
        keywords: [
          "sécurité", "security", "cyber", "hack", "vulnérabilité", "vulnerability", "exploit",
          "pentest", "cryptographie", "cryptography", "chiffrement", "encryption", "pare-feu",
          "firewall", "malware", "virus", "phishing", "authentification", "token", "jwt",
          "sql injection", "xss", "csrf", "ddos", "ransomware"
        ],
        systemPrompt:
          "Tu es un expert en cybersécurité. Adopte une approche défensive et éthique. Explique les vulnérabilités, les vecteurs d'attaque et les contre-mesures. Utilise des blocs de code pour les exemples techniques."
      },
      {
        domain: "development",
        keywords: [
          "code", "coder", "programmation", "programming", "développement", "development",
          "javascript", "python", "java", "c++", "rust", "go", "typescript", "react", "vue",
          "angular", "node", "express", "api", "database", "sql", "nosql", "backend", "frontend",
          "bug", "debug", "fonction", "function", "classe", "class", "objet", "object", "algorithme",
          "framework", "library", "package", "npm", "git", "docker", "kubernetes", "ci/cd",
          "microservice", "architecture", "design pattern", "oop", "functional programming",
          "html", "css"
        ],
        systemPrompt:
          "Tu es un expert en développement logiciel. Fournis du code de production complet et fonctionnel dans des blocs Markdown avec triple backticks. Explique l'architecture, les choix techniques et les bonnes pratiques."
      },
      {
        domain: "data_science",
        keywords: [
          "data", "données", "machine learning", "deep learning", "neural network", "réseau de neurones",
          "pandas", "numpy", "tensorflow", "pytorch", "scikit", "regression", "classification",
          "clustering", "nlp", "computer vision", "dataset", "model training", "feature engineering"
        ],
        systemPrompt:
          "Tu es un expert en data science et machine learning. Explique les concepts, les algorithmes et les implémentations pratiques. Utilise des blocs de code pour les exemples et LaTeX pour les formules mathématiques."
      },
      {
        domain: "general",
        keywords: [],
        systemPrompt: "Tu es un assistant polyvalent. Adapte ton niveau de complexité à la question posée."
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

    const formattingRules = [
      "FORMATAGE STRICT OBLIGATOIRE :",
      "- TOUT code doit être encadré dans des blocs Markdown avec triple backticks, en précisant le langage (par exemple ```html, ```css, ```javascript, ```python).",
      "- TOUTE formule mathématique doit être encadrée en LaTeX ($ pour inline, $$ pour display)",
      "- AUCUN caractère technique non formaté dans le texte brut",
      "- Les noms de variables, fonctions et fichiers doivent être en backticks simples",
      "- Les résultats de commandes doivent être dans des blocs de code"
    ].join("\n");

    const webCodeRules = [
      "RÈGLE STRICTE POUR LA GÉNÉRATION DE CODE HTML/CSS/JAVASCRIPT (OBLIGATOIRE, PRODUCTION) :",
      "- Tout code HTML doit être encadré EXCLUSIVEMENT par un bloc Markdown ```html suivi de ``` en fin de bloc. Jamais de bloc HTML non fermé.",
      "- Tout code CSS doit utiliser UNIQUEMENT la syntaxe de commentaire /* ... */. Les commentaires de type / ceci / (slash simple non standard) sont STRICTEMENT INTERDITS car ils invalident le CSS.",
      "- Tout code JavaScript doit être encadré par un bloc Markdown ```javascript suivi de ```.",
      "- Le code livré doit TOUJOURS être complet et syntaxiquement valide : balises HTML toutes fermées, accolades et parenthèses équilibrées, aucune troncature en fin de réponse.",
      "- Si la réponse risque de dépasser la longueur disponible, réduis le contenu explicatif autour du code plutôt que de tronquer le code lui-même."
    ].join("\n");

    return {
      role: "system",
      content:
        basePrompt +
        "\n\nDOMAINE D'EXPERTISE DÉTECTÉ : " +
        domain.domain.toUpperCase() +
        "\n" +
        domain.systemPrompt +
        "\n\n" +
        formattingRules +
        "\n\n" +
        webCodeRules
    };
  }
}

const dynamicContextManager = new DynamicContextManager();

// ==================== SYSTEM PROMPT DE BASE ====================
const LUBA_BASE_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, une startup basée à Kinshasa, fondée en 2026.",
  "",
  "IDENTITÉ (à respecter strictement) :",
  "- Tu t'appelles Luba (ou Luba.ia). Tu ne t'appelles JAMAIS Milo, Milou, ou tout autre nom.",
  "- Si on te demande qui t'a créée : IA développée par HIKLON Technology, startup à Kinshasa, fondée en 2026.",
  "- Ton ton est chaleureux, intelligent et proactif.",
  "",
  "RÈGLE SUR LES DONNÉES (OBLIGATOIRE, PRODUCTION - AUCUNE SIMULATION) :",
  "- Tu ne dois JAMAIS inventer un score sportif, une actualité, un résultat de recherche, une donnée météo ou toute information factuelle changeante.",
  "- Utilise TOUJOURS l'outil approprié pour obtenir une donnée réelle.",
  "- Si un outil échoue ou ne retourne rien, dis-le honnêtement à l'utilisateur. N'invente jamais un résultat de remplacement.",
  "- Ne mentionne JAMAIS toi-même de sources dans replyText : le backend les ajoute automatiquement.",
  "",
  "RÈGLE STRICTE SUR LES IMAGES (OBLIGATOIRE) :",
  "- Dès que tu décris une personnalité, un lieu, un objet, un concept scientifique ou un événement, utilise TOUJOURS search_images.",
  "- Dès qu'une recherche ou une information est demandée, ajoute TOUJOURS un appel à search_images en complément.",
  "",
  "RÈGLE SUR LES SUGGESTIONS (OBLIGATOIRE) :",
  "- Le champ suggestions doit TOUJOURS contenir 3 à 4 questions de suivi courtes et cliquables.",
  "",
  "RÈGLE STRICTE POUR LA GÉNÉRATION DE CODE HTML/CSS/JAVASCRIPT (OBLIGATOIRE, PRODUCTION) :",
  "- Tout code HTML doit être encadré EXCLUSIVEMENT par un bloc Markdown ```html suivi de ``` en fin de bloc.",
  "- Tout code CSS doit utiliser UNIQUEMENT la syntaxe de commentaire /* ... */. Les commentaires de type / ceci / (slash simple) sont STRICTEMENT INTERDITS.",
  "- Tout code JavaScript doit être encadré par un bloc Markdown ```javascript suivi de ```.",
  "- Ne génère JAMAIS de code HTML, CSS ou JavaScript tronqué, incomplet, ou avec des balises/accolades non fermées.",
  "",
  "FORMAT DE RÉPONSE OBLIGATOIRE (JSON strict) :",
  "{",
  '  "replyText": "Ta réponse complète en Markdown",',
  '  "toolCalls": [ { "name": "...", "arguments": { ... } } ],',
  '  "suggestions": ["Question de suivi 1 ?", "Question de suivi 2 ?", "Question de suivi 3 ?"]',
  "}",
  "Si aucun outil n'est nécessaire, toolCalls doit être un tableau vide [].",
  "",
  "OUTILS DISPONIBLES :",
  "- search_images : Rechercher des images (arguments: { query })",
  "- search_web : Recherche générale - Wikipédia + actualités (arguments: { query })",
  "- search_news : Actualités récentes (arguments: { query })",
  "- search_sports_scores : Scores/résultats d'une équipe sportive (arguments: { query })",
  "- search_science : Articles scientifiques/recherches (arguments: { query })",
  "- search_social : Discussions sur les réseaux sociaux (Reddit) (arguments: { query })",
  "- get_weather : Météo actuelle d'un lieu (arguments: { location })",
  "- send_email : Envoyer un email réel (arguments: { recipient, subject, body })",
  "- send_whatsapp_message : Envoyer un message WhatsApp réel (arguments: { phone_number, message })"
].join("\n");

// ==================== INITIALISATION EXPRESS ====================
const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ==================== CORS ====================
const ALLOWED_ORIGINS = HOSTING_CONFIG.allowedOrigins;

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || ALLOWED_ORIGINS.includes(origin)) {
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
  })
);

// ==================== SECURITY MIDDLEWARE ====================
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: [
          "'self'",
          "'unsafe-inline'",
          "https://cdnjs.cloudflare.com",
          "https://apis.google.com",
          "https://www.gstatic.com",
          "https://cdn.firebase.com",
          "https://*.firebaseio.com"
        ],
        styleSrc: [
          "'self'",
          "'unsafe-inline'",
          "https://cdnjs.cloudflare.com",
          "https://fonts.googleapis.com"
        ],
        imgSrc: [
          "'self'",
          "data:",
          "blob:",
          "https://*",
          "http://*"
        ],
        connectSrc: [
          "'self'",
          "https://api.groq.com",
          "https://openrouter.ai",
          "https://*.firebaseio.com",
          "https://*.supabase.co",
          "wss://*.firebaseio.com"
        ],
        fontSrc: [
          "'self'",
          "https://fonts.gstatic.com",
          "https://cdnjs.cloudflare.com"
        ],
        objectSrc: ["'none'"],
        frameSrc: [
          "https://*.firebaseapp.com",
          "https://*.web.app"
        ],
        workerSrc: ["'self'", "blob:"]
      }
    }
  })
);

// ==================== BODY PARSERS ====================
app.use(
  express.json({
    limit: "20mb",
    verify: (req, res, buf) => {
      req.rawBody = buf;
    }
  })
);
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

// ==================== RATE LIMITERS ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    logger.warn({ ip: req.ip }, "Rate limit atteint");
    res.status(429).json({
      success: false,
      error: true,
      reply: "Trop de requêtes. Réessayez dans 15 minutes.",
      code: "RATE_LIMIT"
    });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(429).json({
      success: false,
      error: true,
      reply: "Limite de requêtes atteinte.",
      code: "RATE_LIMIT_STRICT"
    });
  }
});

// ==================== LOGGING MIDDLEWARE ====================
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;

  logger.info(
    {
      requestId,
      method: req.method,
      url: req.url,
      ip: req.ip
    },
    "Requête entrante"
  );

  res.on("finish", () => {
    logger.info(
      {
        requestId,
        status: res.statusCode,
        duration: Date.now() - start
      },
      "Réponse envoyée"
    );
  });

  next();
});

// ==================== AUTHENTIFICATION AVEC FIREBASE ADMIN ====================
const authenticateUser = async (req, res, next) => {
  try {
    // Vérifier si l'IP est bloquée
    const isBlocked = await isIPBlocked(req.ip);
    if (isBlocked) {
      await logSecurityEvent('unknown', 'BLOCKED_IP_ACCESS', { ip: req.ip }, req.ip, req.headers['user-agent']);
      return res.status(403).json({
        success: false,
        error: true,
        reply: "Accès refusé. IP bloquée temporairement.",
        code: "IP_BLOCKED"
      });
    }
    
    const authHeader = req.headers.authorization || req.headers.Authorization;
    const bearerToken = authHeader && authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    const sessionToken = req.headers["x-session-token"] || null;
    
    let verifiedUserId = null;
    let verifiedEmail = null;
    let verifiedName = null;
    let firebaseUid = null;
    let userRole = 'FREE';
    let emailVerified = false;
    
    if (!bearerToken) {
      await recordLoginAttempt(req.ip, null, false, "Token manquant");
      await logSecurityEvent('unknown', 'MISSING_TOKEN', { ip: req.ip }, req.ip, req.headers['user-agent']);
      return res.status(401).json({
        success: false,
        error: true,
        reply: "Authentification requise. Token Firebase manquant.",
        code: "MISSING_TOKEN"
      });
    }
    
    try {
      const user = await verifyFirebaseToken(bearerToken);
      if (!user) {
        await recordLoginAttempt(req.ip, null, false, "Token invalide");
        await logSecurityEvent('unknown', 'INVALID_TOKEN', { ip: req.ip }, req.ip, req.headers['user-agent']);
        return res.status(401).json({
          success: false,
          error: true,
          reply: "Session invalide ou expirée. Reconnectez-vous.",
          code: "INVALID_TOKEN"
        });
      }
      
      verifiedUserId = user.uid;
      firebaseUid = user.uid;
      verifiedEmail = user.email;
      verifiedName = user.displayName;
      emailVerified = user.emailVerified;
      
      // Récupérer le rôle depuis Firebase Custom Claims ou la base locale
      if (user.customClaims?.role) {
        userRole = user.customClaims.role;
      } else {
        const userRow = await dbGet("SELECT role FROM users WHERE id = ?", [verifiedUserId]);
        userRole = userRow?.role || 'FREE';
      }
      
      // Vérifier l'email si nécessaire
      if (!emailVerified && CONFIG.ENV === "production") {
        await recordLoginAttempt(req.ip, verifiedUserId, false, "Email non vérifié");
        await logSecurityEvent(verifiedUserId, 'UNVERIFIED_EMAIL_ACCESS', { email: verifiedEmail }, req.ip, req.headers['user-agent']);
        return res.status(403).json({
          success: false,
          error: true,
          reply: "Veuillez vérifier votre adresse email pour accéder à cette fonctionnalité.",
          code: "EMAIL_NOT_VERIFIED"
        });
      }
      
      // Vérifier la session active si un token de session est fourni
      if (sessionToken) {
        const isValidSession = await validateActiveSession(verifiedUserId, sessionToken);
        if (!isValidSession) {
          await logSecurityEvent(verifiedUserId, 'INVALID_SESSION', { sessionToken }, req.ip, req.headers['user-agent']);
          return res.status(401).json({
            success: false,
            error: true,
            reply: "Session invalide. Reconnectez-vous.",
            code: "INVALID_SESSION"
          });
        }
      }
      
      // Enregistrer la connexion réussie
      await recordLoginAttempt(req.ip, verifiedUserId, true);
      await logSecurityEvent(verifiedUserId, 'LOGIN_SUCCESS', { 
        email: verifiedEmail,
        role: userRole,
        sessionToken: sessionToken ? 'provided' : 'not_provided'
      }, req.ip, req.headers['user-agent']);
      
    } catch (error) {
      logger.warn({ error: error.message }, "Token Firebase invalide");
      await recordLoginAttempt(req.ip, null, false, error.message);
      await logSecurityEvent('unknown', 'TOKEN_VERIFICATION_FAILED', { 
        ip: req.ip,
        error: error.message
      }, req.ip, req.headers['user-agent']);
      
      // Vérifier si l'IP doit être bloquée
      const loginCheck = await checkLoginAttempts(req.ip);
      if (loginCheck.blocked) {
        await logSecurityEvent('unknown', 'IP_BLOCKED', { ip: req.ip, reason: 'TOO_MANY_FAILED_ATTEMPTS' }, req.ip);
      }
      
      return res.status(401).json({
        success: false,
        error: true,
        reply: loginCheck.blocked ? loginCheck.message : "Session invalide ou expirée. Reconnectez-vous.",
        code: loginCheck.blocked ? "IP_BLOCKED" : "INVALID_TOKEN"
      });
    }
    
    const userId = verifiedUserId;
    
    if (!userId) {
      return res.status(401).json({
        success: false,
        error: true,
        reply: "Authentification requise.",
        code: "AUTH_REQUIRED"
      });
    }
    
    req.userId = userId;
    req.firebaseUid = firebaseUid || userId;
    req.verifiedIdentity = Boolean(verifiedUserId);
    req.userRole = userRole;
    req.emailVerified = emailVerified;
    req.sessionToken = sessionToken;
    
    // Synchronisation utilisateur
    try {
      if (supabase && firebaseUid) {
        await syncUserWithSupabase(firebaseUid, verifiedEmail, verifiedName);
      }
      
      const user = await dbGet("SELECT * FROM users WHERE id = ?", [userId]);
      if (!user) {
        await dbRun(
          "INSERT INTO users (id, firebase_uid, email, display_name, role, email_verified, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
          [userId, firebaseUid, verifiedEmail, verifiedName || userId, userRole, emailVerified ? 1 : 0]
        );
      } else {
        await dbRun(
          "UPDATE users SET last_seen_at = CURRENT_TIMESTAMP, firebase_uid = COALESCE(?, firebase_uid), email = COALESCE(?, email), display_name = COALESCE(?, display_name), role = ?, email_verified = ? WHERE id = ?",
          [firebaseUid, verifiedEmail, verifiedName, userRole, emailVerified ? 1 : 0, userId]
        );
      }
    } catch (err) {
      logger.error({ err: err.message }, "Erreur synchronisation utilisateur");
    }
    
    next();
  } catch (error) {
    logger.error({ error: error.message }, "Erreur authentification");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Erreur interne d'authentification.",
      code: "AUTH_INTERNAL_ERROR"
    });
  }
};

// ==================== MIDDLEWARE DE VÉRIFICATION DES RÔLES ====================
const requireRole = (allowedRoles) => {
  return (req, res, next) => {
    if (!req.userRole) {
      return res.status(403).json({
        success: false,
        error: true,
        reply: "Rôle non défini.",
        code: "ROLE_UNDEFINED"
      });
    }
    
    if (allowedRoles.includes(req.userRole) || req.userRole === 'ADMIN') {
      next();
    } else {
      return res.status(403).json({
        success: false,
        error: true,
        reply: "Accès refusé. Rôle insuffisant.",
        code: "INSUFFICIENT_ROLE"
      });
    }
  };
};

// ==================== SYNCHRONISATION SUPABASE ====================
async function syncUserWithSupabase(firebaseUid, email, displayName) {
  if (!supabase || !firebaseUid) return;

  try {
    const { data: existingUser, error: fetchError } = await supabase.from("users").select("firebase_uid").eq("firebase_uid", firebaseUid).single();

    if (fetchError && fetchError.code !== "PGRST116") {
      logger.error({ error: fetchError.message }, "Erreur Supabase fetch user");
      return;
    }

    if (!existingUser) {
      const { error: insertError } = await supabase.from("users").insert({
        firebase_uid: firebaseUid,
        email: email,
        display_name: displayName,
        last_seen_at: new Date().toISOString()
      });

      if (insertError) logger.error({ error: insertError.message }, "Erreur Supabase insert user");
    } else {
      const { error: updateError } = await supabase.from("users").update({ last_seen_at: new Date().toISOString() }).eq("firebase_uid", firebaseUid);

      if (updateError) logger.error({ error: updateError.message }, "Erreur Supabase update user");
    }
  } catch (error) {
    logger.error({ error: error.message }, "Erreur sync Supabase");
  }
}

async function syncSessionWithSupabase(sessionId, firebaseUid, userId) {
  if (!supabase || !firebaseUid) return;

  try {
    const { data: existingSession, error: fetchError } = await supabase.from("sessions").select("session_id").eq("session_id", sessionId).single();

    if (fetchError && fetchError.code !== "PGRST116") {
      return;
    }

    if (!existingSession) {
      const { error: insertError } = await supabase.from("sessions").insert({
        session_id: sessionId,
        firebase_uid: firebaseUid,
        user_id: userId,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      });

      if (insertError) logger.error({ error: insertError.message }, "Erreur Supabase insert session");
    } else {
      const { error: updateError } = await supabase.from("sessions").update({ updated_at: new Date().toISOString() }).eq("session_id", sessionId);

      if (updateError) logger.error({ error: updateError.message }, "Erreur Supabase update session");
    }
  } catch (error) {
    logger.error({ error: error.message }, "Erreur sync session Supabase");
  }
}

async function syncMessageWithSupabase(sessionId, role, content, firebaseUid) {
  if (!supabase || !firebaseUid) return;

  try {
    const { error: insertError } = await supabase.from("messages").insert({
      session_id: sessionId,
      firebase_uid: firebaseUid,
      role: role,
      content: content,
      created_at: new Date().toISOString()
    });

    if (insertError) logger.error({ error: insertError.message }, "Erreur Supabase insert message");
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
      const { data: supabaseSession, error } = await supabase
        .from("sessions")
        .select("session_id, user_id, firebase_uid")
        .eq("session_id", conversationId)
        .single();

      if (supabaseSession && !error) {
        await dbRun("INSERT OR IGNORE INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)", [
          conversationId,
          supabaseSession.user_id || userId,
          supabaseSession.firebase_uid
        ]);
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

async function getHistory(conversationId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  const localRows = await dbAll("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?", [conversationId, limit]);

  if (localRows.length > 0) {
    return localRows.reverse();
  }

  if (supabase) {
    try {
      const { data: supabaseMessages, error } = await supabase
        .from("messages")
        .select("role, content")
        .eq("session_id", conversationId)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (supabaseMessages && !error && supabaseMessages.length > 0) {
        return supabaseMessages.reverse();
      }
    } catch (error) {
      logger.error({ error: error.message }, "Erreur Supabase getHistory");
    }
  }

  return [];
}

async function saveMessage(conversationId, role, content, firebaseUid = null) {
  await dbRun("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", [conversationId, role, content]);
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
  await syncMessageWithSupabase(conversationId, role, content, firebaseUid);
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
    logger.error({ error: e.message }, "Erreur parsing intent_data");
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

// ==================== CALL LLM V100 ====================
async function callLLM_v100(messages, images = null, sessionId = null, userId = null) {
  logger.info("Démarrage du routage Mwamba (v100)");

  const lastUserMessage = [...messages].reverse().find((m) => m.role === "user");
  const userText = typeof lastUserMessage?.content === "string" ? lastUserMessage.content : "";
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userText, LUBA_BASE_SYSTEM_PROMPT);

  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.v100.providers,
    {
      messages: [dynamicSystemPrompt, ...messages],
      images: images
    },
    {
      maxRetriesPerProvider: CONFIG.MAX_RETRY_ATTEMPTS,
      onProviderFail: (failInfo) => {
        logger.warn({ failInfo }, "Failover v100");
      },
      sessionId,
      userId,
      tier: "v100"
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

  throw new Error("Échec complet du tier v100: " + result.errorCode);
}

// ==================== CALL LLM V250 ====================
async function callLLM_v250(messages, userMessage, images = null, sessionId = null, userId = null) {
  logger.info("Démarrage du pipeline Ngandu (v250)");

  const tier = MODEL_TIERS.v250;
  const providerChain = [];

  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userMessage, LUBA_BASE_SYSTEM_PROMPT);

  const reasoningMessages = [
    {
      role: "system",
      content:
        dynamicSystemPrompt.content +
        "\n\nAnalyse ce problème complexe en profondeur. Effectue les démonstrations nécessaires, isole les edge cases et rédige le pseudo-code/l'architecture. Sois complet et rigoureux."
    },
    ...messages
  ];

  const reasoningResult = await executeWithRetryAndFallback(
    tier.reasoning.providers,
    { messages: reasoningMessages, images: images },
    {
      maxRetriesPerProvider: tier.maxRetries,
      onProviderFail: (failInfo) => {
        logger.warn({ failInfo }, "Failover raisonnement v250");
      },
      sessionId,
      userId,
      tier: "v250_reasoning"
    }
  );

  if (!reasoningResult.success || !reasoningResult.response || reasoningResult.response.trim().length < 40) {
    logger.error("Échec de l'étape de raisonnement v250 - dégradation vers v100");
    return await degradedFallbackToV100(messages, "reasoning_failed", images);
  }

  const reasoningAnalysis = reasoningResult.response;
  providerChain.push("R1:" + reasoningResult.providerUsed + "/" + reasoningResult.modelUsed);

  const formattingDirective = [
    "Tu DOIS répondre au format JSON strict :",
    "{",
    '  "replyText": "réponse complète en Markdown avec le code dans des blocs triple backticks",',
    '  "toolCalls": [],',
    '  "suggestions": ["question 1 ?", "question 2 ?", "question 3 ?"]',
    "}",
    "",
    "FORMATAGE STRICT :",
    "- Code HTML dans un bloc ```html ... ```",
    "- Code CSS dans un bloc ```css ... ``` avec UNIQUEMENT des commentaires /* ... */",
    "- Code JavaScript dans un bloc ```javascript ... ```",
    "- Formules en LaTeX ($ ou $$)",
    "- Code complet, jamais tronqué, balises et accolades toutes fermées"
  ].join("\n");

  const codeMessages = [
    {
      role: "system",
      content:
        "Génère le code de production complet, typé, sécurisé et documenté en te basant strictement sur le plan ci-dessous.\n\nPLAN / ANALYSE (étape 1) :\n" +
        reasoningAnalysis +
        "\n\n" +
        formattingDirective
    },
    { role: "user", content: userMessage }
  ];

  const codeResult = await executeWithRetryAndFallback(
    tier.code.providers,
    { messages: codeMessages, images: images },
    {
      maxRetriesPerProvider: tier.maxRetries,
      onProviderFail: (failInfo) => {
        logger.warn({ failInfo }, "Failover code v250");
      },
      sessionId,
      userId,
      tier: "v250_code"
    }
  );

  if (!codeResult.success || !codeResult.response) {
    logger.error("Échec de l'étape de génération v250 - dégradation vers v100");
    return await degradedFallbackToV100(messages, "code_generation_failed", images);
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

// ==================== CALL VISION ====================
async function callVisionModel(messages, images, sessionId = null, userId = null) {
  logger.info("Démarrage du pipeline Vision");

  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.vision.providers,
    { messages: messages, images: images },
    {
      maxRetriesPerProvider: 2,
      onProviderFail: (failInfo) => {
        logger.warn({ failInfo }, "Failover Vision");
      },
      sessionId,
      userId,
      tier: "vision"
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

  logger.error("Échec des modèles vision - fallback vers v100 textuel");
  return await callLLM_v100(messages, null);
}

// ==================== GARDE-FOU ====================
async function degradedFallbackToV100(messages, reason, images = null) {
  logger.warn("Dégradation gracieuse vers v100 : " + reason);

  try {
    const fallbackResult = await callLLM_v100(messages, images);
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
    logger.error("Échec total de la dégradation v250 -> v100");

    return {
      replyText:
        "Je rencontre actuellement des difficultés techniques. Veuillez réessayer dans quelques instants. Nos équipes techniques ont été informées.",
      toolCalls: [],
      suggestions: [
        "Peux-tu réessayer avec une question plus simple ?",
        "Comment fonctionne Luba.ia ?",
        "Quels sont les services disponibles ?"
      ],
      providerUsed: "error_graceful_degradation",
      modelUsed: "none",
      degraded: true,
      degradationReason: reason + "_and_v100_failed",
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

// ==================== HANDLE CHAT PRINCIPAL ====================
async function handleChat({ conversationId, userId, firebaseUid, message, googleAccessToken = null, channel = "web", modelTier = "v100", images = null }) {
  logger.info(
    {
      conversationId,
      userId,
      tier: modelTier,
      images: images ? images.length : 0,
      channel
    },
    "Démarrage conversation"
  );

  await getSession(conversationId, userId, firebaseUid);

  const activeIntent = await getActiveIntent(conversationId);
  if (activeIntent) {
    return await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken, firebaseUid });
  }

  await saveMessage(conversationId, "user", message, firebaseUid);

  const history = await getHistory(conversationId);
  const messages = [...history, { role: "user", content: message }];

  let finalResponse = null;
  let imageUrls = [];
  let providerUsed = "unknown";
  let suggestions = [];
  const usedSources = new Set();
  let degraded = false;

  try {
    if (images && images.length > 0) {
      logger.info("Mode Vision activé");
      const visionResult = await callVisionModel(messages, images, conversationId, userId);
      finalResponse = visionResult.replyText || "Je n'ai pas pu analyser l'image.";
      suggestions = Array.isArray(visionResult.suggestions) ? visionResult.suggestions.slice(0, 4) : [];
      providerUsed = visionResult.providerUsed || "vision";
    } else if (modelTier === "v250") {
      const result = await callLLM_v250(messages, message, null, conversationId, userId);
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
          llmResponse = await callLLM_v100(messages, null, conversationId, userId);
          providerUsed = llmResponse.providerUsed;
          degraded = llmResponse.degraded || false;
        } catch (error) {
          logger.error({ error: error.message }, "Erreur LLM v100");
          finalResponse = "Je suis momentanément indisponible. Veuillez réessayer dans quelques instants.";
          suggestions = ["Peux-tu réessayer ?", "Comment fonctionne Luba.ia ?", "Quels sont les services disponibles ?"];
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
              logger.error({ error: toolError.message, tool: toolCall.name }, "Erreur outil");
              toolResult = { success: false, error: toolError.message };
            }

            messages.push({ role: "assistant", content: "Résultat de l'outil " + toolCall.name + " : " + JSON.stringify(toolResult) });
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

    await saveMessage(conversationId, "assistant", finalResponse, firebaseUid);

    logger.info({ conversationId, length: finalResponse.length }, "Réponse finale générée");

    return {
      reply: finalResponse,
      images: imageUrls,
      error: providerUsed.startsWith("error"),
      providerUsed,
      modelTier,
      degraded,
      visionEnabled: Boolean(images && images.length > 0),
      suggestions,
      sources: Array.from(usedSources)
        .map((key) => OPEN_SOURCES[key])
        .filter(Boolean)
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
      sources: []
    };

    try {
      await saveMessage(conversationId, "assistant", fallbackResponse.reply, firebaseUid);
    } catch (saveError) {
      logger.error({ error: saveError.message }, "Erreur sauvegarde message de secours");
    }

    return fallbackResponse;
  }
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
          logger.error({ error: error.message }, "Erreur envoi WhatsApp (intent)");
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
  res.json({
    success: true,
    error: false,
    reply: "Serveur " + CONFIG.AGENT_NAME + " opérationnel",
    version: CONFIG.VERSION,
    company: CONFIG.COMPANY,
    domain: HOSTING_CONFIG.domain
  });
});

// ==================== HEALTH CHECK ====================
app.get("/api/health", async (req, res) => {
  try {
    let dbOk = true;
    try {
      await dbGet("SELECT 1");
    } catch (e) {
      dbOk = false;
    }

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
        hosting: HOSTING_CONFIG.domain,
        whatsapp: {
          baileysSessionsActives: whatsappManager.sessions.size,
          queue: queueManager.useRedis ? "bullmq+redis" : "memoire (repli)"
        },
        email: { gmailOAuth: "à la demande", resend: Boolean(process.env.RESEND_API_KEY), smtp: Boolean(emailTransporter) },
        openSources: Object.keys(OPEN_SOURCES),
        version: CONFIG.VERSION,
        features: {
          vision: true,
          extendedPayload: CONFIG.MAX_MESSAGE_LENGTH,
          dynamicContext: true,
          multiDeviceSync: Boolean(supabase),
          strictFormatting: true,
          retryMechanism: CONFIG.MAX_RETRY_ATTEMPTS + " tentatives max",
          circuitBreaker: "activé",
          auditLLM: true,
          rgpdDeletion: true,
          quotas: true,
          securityLogs: true,
          firebaseAdmin: Boolean(firebaseApp),
          customRoles: Boolean(firebaseApp),
          sessionManagement: true,
          ipBlocking: true
        }
      }
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur health check");
    res.status(500).json({ success: false, error: true, reply: "Erreur interne", detail: error.message });
  }
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post("/api/chat", apiLimiter, authenticateUser, upload.array("images", CONFIG.MAX_IMAGES_PER_REQUEST), async (req, res) => {
  try {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;
    const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";

    const quotaCheck = await checkUserQuota(req.userId, 'message', req.userRole);
    if (!quotaCheck.allowed) {
      return res.status(429).json({
        success: false,
        error: true,
        reply: quotaCheck.message || "Limite quotidienne atteinte.",
        code: "QUOTA_EXCEEDED",
        quota: quotaCheck
      });
    }

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: true,
        reply: "Le paramètre 'message' est obligatoire.",
        code: "MISSING_MESSAGE"
      });
    }

    if (message.length > CONFIG.MAX_MESSAGE_LENGTH) {
      return res.status(400).json({
        success: false,
        error: true,
        reply: "Message trop long (max " + CONFIG.MAX_MESSAGE_LENGTH + " caractères).",
        code: "MESSAGE_TOO_LONG"
      });
    }

    if (req.files && req.files.length > 0) {
      const imageQuota = await checkUserQuota(req.userId, 'image', req.userRole);
      if (!imageQuota.allowed) {
        return res.status(429).json({
          success: false,
          error: true,
          reply: "Limite d'images quotidienne atteinte.",
          code: "IMAGE_QUOTA_EXCEEDED"
        });
      }
    }

    if (!conversationId || typeof conversationId !== "string") {
      conversationId = generateConversationId();
      isNewConversation = true;
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(403).json({
        success: false,
        error: true,
        reply: error.message,
        code: "CONVERSATION_OWNERSHIP"
      });
    }

    if (modelTier === "v250") {
      req.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
      res.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
    }

    const googleAccessToken = req.headers["x-google-access-token"] || null;

    let images = null;
    if (req.files && req.files.length > 0) {
      images = req.files.map((file) => convertImageToBase64(file.buffer, file.mimetype));
      logger.info("Images reçues : " + images.length);
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

    return res.status(200).json({ 
      ...result, 
      conversationId, 
      isNewConversation,
      quota: {
        remaining: quotaCheck.remaining - 1,
        max: quotaCheck.max
      }
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur API/Chat");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Une erreur est survenue lors du traitement de votre message.",
      detail: error.message,
      code: "CHAT_ERROR",
      conversationId: req.body.conversationId || null,
      modelTier: req.body.modelTier || "v100"
    });
  }
});

// ==================== ROUTE CONVERSATIONS ====================
app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    let conversations = [];

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
        logger.error({ error: error.message }, "Erreur Supabase conversations");
      }
    }

    if (conversations.length === 0) {
      const rows = await dbAll("SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? OR firebase_uid = ? ORDER BY updated_at DESC LIMIT 50", [
        req.userId,
        req.firebaseUid
      ]);
      conversations = rows;
    }

    const enrichedConversations = await Promise.all(
      conversations.map(async (conv) => {
        let lastMessage = null;

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
            // Ignore : repli sur SQLite ci-dessous
          }
        }

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
      success: true,
      error: false,
      reply: "Conversations récupérées.",
      conversations: enrichedConversations,
      source: supabase ? "supabase" : "sqlite"
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur /api/conversations");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Erreur interne.",
      conversations: [],
      code: "CONVERSATIONS_ERROR"
    });
  }
});

// ==================== ROUTE STATISTIQUES UTILISATEUR ====================
app.get("/api/user/stats", authenticateUser, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    const quotaRow = await dbGet(
      `SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`,
      [req.userId, today]
    );
    
    const totalMessages = await dbGet(
      `SELECT COUNT(*) as count FROM messages m 
       JOIN sessions s ON m.session_id = s.session_id 
       WHERE s.user_id = ?`,
      [req.userId]
    );
    
    return res.status(200).json({
      success: true,
      error: false,
      data: {
        quotas: quotaRow || {
          messages_count: 0,
          images_count: 0,
          whatsapp_count: 0,
          emails_count: 0
        },
        totalMessages: totalMessages?.count || 0,
        role: req.userRole || 'FREE',
        limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE
      }
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur statistiques utilisateur");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de la récupération des statistiques.",
      code: "STATS_ERROR"
    });
  }
});

// ==================== ROUTE GESTION DES RÔLES (ADMIN UNIQUEMENT) ====================
app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(['ADMIN']), async (req, res) => {
  try {
    const { uid, role } = req.body;
    
    if (!uid || !role) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Les paramètres 'uid' et 'role' sont obligatoires.",
        code: "MISSING_PARAMS"
      });
    }
    
    if (!['FREE', 'PREMIUM', 'ADMIN'].includes(role)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Rôle invalide. Rôles autorisés : FREE, PREMIUM, ADMIN",
        code: "INVALID_ROLE"
      });
    }
    
    const result = await setUserRole(uid, role);
    
    return res.status(200).json({
      success: true,
      error: false,
      message: "Rôle mis à jour avec succès.",
      data: result
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur mise à jour rôle");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de la mise à jour du rôle.",
      detail: error.message,
      code: "ROLE_UPDATE_ERROR"
    });
  }
});

// ==================== ROUTE GESTION DES SESSIONS ====================
app.post("/api/session/create", authenticateUser, async (req, res) => {
  try {
    const sessionToken = await createActiveSession(req.userId, req.ip, req.headers['user-agent']);
    
    await logSecurityEvent(req.userId, 'SESSION_CREATED', { sessionToken: sessionToken.substring(0, 10) + '...' }, req.ip, req.headers['user-agent']);
    
    return res.status(200).json({
      success: true,
      error: false,
      data: {
        sessionToken,
        expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
      }
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur création session");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de la création de la session.",
      code: "SESSION_CREATE_ERROR"
    });
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
    
    await logSecurityEvent(req.userId, 'SESSION_REVOKED', { sessionToken: sessionToken ? sessionToken.substring(0, 10) + '...' : 'all' }, req.ip);
    
    return res.status(200).json({
      success: true,
      error: false,
      message: sessionToken ? "Session révoquée avec succès." : "Toutes les sessions ont été révoquées."
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur révocation session");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de la révocation de la session.",
      code: "SESSION_REVOKE_ERROR"
    });
  }
});

// ==================== ROUTE OUTILS ====================
app.post("/api/tools", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const toolName = req.body.toolName || req.body.action;
    const params = req.body.params || req.body.arguments || req.body.data || {};
    
    if (!toolName || typeof toolName !== "string") {
      return res.status(400).json({
        success: false,
        error: true,
        reply: "Le paramètre 'toolName' est obligatoire.",
        code: "MISSING_TOOL_NAME"
      });
    }
    
    const googleAccessToken = req.headers["x-google-access-token"] || null;
    
    const { result, sourceKeys } = await executeTool(toolName, params, { userId: req.userId, googleAccessToken });
    const sources = sourceKeys.map((k) => OPEN_SOURCES[k]).filter(Boolean);
    
    return res.status(200).json({
      success: true,
      error: false,
      toolName,
      result,
      sources
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur /api/tools");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Erreur interne lors de l'exécution de l'outil.",
      detail: error.message,
      code: "TOOL_EXECUTION_ERROR"
    });
  }
});

// ==================== ROUTE WHATSAPP CONNECT ====================
app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const phoneNumber = req.body.phoneNumber || req.body.phone || null;
    logger.info({ userId: req.userId, phoneNumber }, "Connexion WhatsApp initiée");
    
    const result = await whatsappManager.initClient(req.userId);
    
    if (result.connected) {
      return res.status(200).json({
        success: true,
        error: false,
        message: "WhatsApp est déjà connecté.",
        data: { qrCode: null, qrCodeBase64: null }
      });
    }
    
    let qrCode = null;
    const startTime = Date.now();
    while (!qrCode && Date.now() - startTime < CONFIG.WHATSAPP_QR_TIMEOUT) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      qrCode = whatsappManager.getQRCode(req.userId);
    }
    
    if (qrCode) {
      return res.status(200).json({
        success: true,
        error: false,
        message: "Connexion initiée",
        data: {
          qrCode: qrCode,
          qrCodeBase64: qrCode
        },
        qr: qrCode
      });
    }
    
    return res.status(408).json({
      success: false,
      error: true,
      message: "Délai dépassé en attendant le QR Code. Réessayez.",
      code: "QR_TIMEOUT"
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur WhatsApp Connect");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de l'initialisation de la connexion WhatsApp.",
      detail: error.message,
      code: "WHATSAPP_CONNECT_ERROR"
    });
  }
});

// ==================== ROUTE WHATSAPP SEND ====================
app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    if (!req.body.to || !req.body.message) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Les paramètres 'to' et 'message' sont obligatoires.",
        code: "MISSING_PARAMS"
      });
    }
    
    const quotaCheck = await checkUserQuota(req.userId, 'whatsapp', req.userRole);
    if (!quotaCheck.allowed) {
      return res.status(429).json({
        success: false,
        error: true,
        message: quotaCheck.message || "Limite de messages WhatsApp quotidienne atteinte.",
        code: "WHATSAPP_QUOTA_EXCEEDED"
      });
    }
    
    const result = await whatsappManager.sendMessage(req.userId, req.body.to, req.body.message);
    await incrementUserQuota(req.userId, 'whatsapp');
    
    return res.status(200).json({
      success: true,
      error: false,
      message: "Message envoyé à " + req.body.to,
      data: result
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur WhatsApp Send");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de l'envoi du message WhatsApp.",
      detail: error.message,
      code: "WHATSAPP_SEND_ERROR"
    });
  }
});

// ==================== ROUTE INTENTION ====================
app.post("/api/intent/init", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { intentType, conversationId, conversation_id: conversationIdSnake } = req.body;
    const convId = conversationId || conversationIdSnake;

    if (!convId || typeof convId !== "string") {
      return res.status(400).json({
        success: false,
        error: true,
        reply: "Le paramètre 'conversationId' est obligatoire.",
        code: "MISSING_CONVERSATION_ID"
      });
    }

    try {
      await assertConversationOwnership(convId, req.userId);
    } catch (error) {
      return res.status(403).json({
        success: false,
        error: true,
        reply: error.message,
        code: "CONVERSATION_OWNERSHIP"
      });
    }

    await getSession(convId, req.userId, req.firebaseUid);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(convId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.status(200).json({
        success: true,
        error: false,
        reply: "Envoi WhatsApp initié. Quel est le numéro du destinataire ?"
      });
    }
    if (intentType === "EMAIL") {
      await setActiveIntent(convId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.status(200).json({
        success: true,
        error: false,
        reply: "Envoi d'email initié. Quelle est l'adresse du destinataire ?"
      });
    }
    return res.status(400).json({
      success: false,
      error: true,
      reply: "Type d'intention inconnu.",
      code: "UNKNOWN_INTENT"
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur /api/intent/init");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Erreur interne.",
      code: "INTENT_ERROR"
    });
  }
});

// ==================== ROUTE EFFACER MÉMOIRE ====================
app.post("/api/memory/clear", authenticateUser, async (req, res) => {
  try {
    const conversationId = req.body.conversationId || req.body.conversation_id;

    if (!conversationId || typeof conversationId !== "string") {
      return res.status(400).json({
        success: false,
        error: true,
        reply: "Le paramètre 'conversationId' est obligatoire.",
        code: "MISSING_CONVERSATION_ID"
      });
    }

    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(403).json({
        success: false,
        error: true,
        reply: error.message,
        code: "CONVERSATION_OWNERSHIP"
      });
    }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);

    if (supabase && req.firebaseUid) {
      try {
        const { error } = await supabase.from("messages").delete().eq("session_id", conversationId).eq("firebase_uid", req.firebaseUid);
        if (error) logger.error({ error: error.message }, "Erreur Supabase delete messages");
      } catch (supabaseError) {
        logger.error({ error: supabaseError.message }, "Erreur Supabase memory clear");
      }
    }

    await clearActiveIntent(conversationId);

    return res.status(200).json({
      success: true,
      error: false,
      reply: "Mémoire de la conversation effacée."
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur /api/memory/clear");
    return res.status(500).json({
      success: false,
      error: true,
      reply: "Erreur lors de l'effacement de la mémoire.",
      detail: error.message,
      code: "MEMORY_CLEAR_ERROR"
    });
  }
});

// ==================== ROUTE SUPPRESSION COMPTE (RGPD) ====================
app.delete("/api/account", authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const firebaseUid = req.firebaseUid;
    
    logger.info({ userId }, "Demande de suppression de compte");
    
    try {
      const whatsappSession = whatsappManager.sessions.get(userId);
      if (whatsappSession?.sock) {
        whatsappSession.sock.end(undefined);
      }
      whatsappManager.sessions.delete(userId);
      
      const authDir = path.join(CONFIG.SESSIONS_PATH, userId);
      if (fs.existsSync(authDir)) {
        fs.rmSync(authDir, { recursive: true, force: true });
      }
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
    
    // Supprimer le compte Firebase si Admin SDK disponible
    let firebaseAccountDeleted = false;
    if (firebaseApp && firebaseAdmin) {
      try {
        await deleteFirebaseUser(firebaseUid);
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
      firebaseAccountDeleted,
      note: firebaseAccountDeleted ? "Compte Firebase supprimé" : "La suppression du compte Firebase doit être effectuée côté client"
    });
  } catch (error) {
    logger.error({ error: error.message }, "Erreur suppression compte");
    return res.status(500).json({
      success: false,
      error: true,
      message: "Erreur lors de la suppression du compte.",
      detail: error.message,
      code: "ACCOUNT_DELETION_ERROR"
    });
  }
});

// ==================== ROUTE 404 ====================
app.use((req, res, next) => {
  res.status(404).json({
    success: false,
    error: true,
    reply: "Route non trouvée",
    code: "NOT_FOUND",
    availableRoutes: [
      "GET /",
      "GET /api/health",
      "POST /api/chat",
      "GET /api/conversations",
      "GET /api/user/stats",
      "POST /api/tools",
      "POST /api/whatsapp/connect",
      "POST /api/whatsapp/send",
      "POST /api/intent/init",
      "POST /api/memory/clear",
      "POST /api/session/create",
      "POST /api/session/revoke",
      "POST /api/admin/set-role",
      "DELETE /api/account"
    ]
  });
});

// ==================== MIDDLEWARE D'ERREUR GLOBALE ====================
app.use((error, req, res, next) => {
  logger.error({ error: error.message, stack: error.stack, requestId: req.requestId }, "Erreur non gérée");

  if (res.headersSent) {
    return next(error);
  }

  return res.status(500).json({
    success: false,
    error: true,
    reply: "Une erreur interne est survenue.",
    detail: error.message || "Erreur inconnue",
    code: "INTERNAL_ERROR"
  });
});

// ==================== VALIDATION ENVIRONNEMENT ====================
validateEnvironment();

// ==================== DÉMARRAGE DU SERVEUR ====================
const server = app.listen(CONFIG.PORT, () => {
  logger.info("Serveur " + CONFIG.AGENT_NAME + " v" + CONFIG.VERSION + " démarré sur le port " + CONFIG.PORT);
  console.log("🚀 Serveur " + CONFIG.AGENT_NAME + " v" + CONFIG.VERSION + " opérationnel sur le port " + CONFIG.PORT);
  console.log("🌐 Domaine: " + HOSTING_CONFIG.domain);
  console.log("🔐 Firebase Admin: " + (firebaseApp ? "activé" : "désactivé (mode API REST)"));
});

// ==================== ARRÊT PROPRE (GRACEFUL SHUTDOWN) ====================
let isShuttingDown = false;

async function shutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log("\nSignal " + signal + " reçu. Arrêt propre en cours...");
  logger.info({ signal }, "Arrêt propre du serveur");

  await new Promise((resolve) => {
    server.close(() => {
      console.log("✅ Serveur HTTP fermé");
      resolve();
    });
  });

  try {
    await whatsappManager.destroyAll();
    console.log("✅ Connexions WhatsApp fermées");
  } catch (error) {
    logger.error({ error: error.message }, "Erreur fermeture WhatsApp");
  }

  try {
    await queueManager.close();
    console.log("✅ Files d'attente fermées");
  } catch (error) {
    logger.error({ error: error.message }, "Erreur fermeture files d'attente");
  }

  await new Promise((resolve) => {
    db.close((error) => {
      if (error) {
        logger.error({ error: error.message }, "Erreur fermeture SQLite");
      } else {
        console.log("✅ Base de données SQLite fermée");
      }
      resolve();
    });
  });

  console.log("✅ Arrêt propre terminé");
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", (error) => {
  logger.error({ error: error.message, stack: error.stack }, "uncaughtException");
});
process.on("unhandledRejection", (reason, promise) => {
  logger.error({ reason: String(reason) }, "unhandledRejection");
});

module.exports = { app, db, queueManager, whatsappManager };
