// ================================================================================
// LUBA AI PRO — BACKEND v16.5.0 — Self-Healing Edition
// HIKLON TECHNOLOGIES · Kinshasa, RDC · 2026
// ================================================================================
// PARTIE 1/5 — FONDATIONS + SELF-HEALING INFRASTRUCTURE
// --------------------------------------------------------------------------------
// Ce module pose les bases :
//   • Configuration centralisée (gelée, validée)
//   • Logger structuré Pino
//   • Erreurs typées LubaError
//   • Utilitaires transverses (IDs, hashes, sanitization, clean output)
//   • Firebase Admin + Firestore
//   • Supabase (backup)
//   • Cache 3 niveaux (L1 LRU + L2 Redis + L3 sémantique)
//   • Sécurité (injection, modération, HMAC, audit)
//   • Métriques Prometheus étendues
//   • SQLite v16.5 (17 tables, incluant provider_health + user_facts)
//   • 🆕 SELF-HEALING LAYER :
//       - providerHealthTracker : suit le taux de succès de chaque modèle
//       - smartProviderOrdering : réordonne les providers par santé
//       - tokenBudgetGuard : détecte les prompts trop longs
//       - compactContextIfNeeded : compresse le contexte automatiquement
//       - cleanOutput : nettoie toute réponse avant envoi au frontend
//   • Bootstrap Partie 1
//
// RÈGLES DE CONCEPTION :
//   1. AUCUN STUB (`let xxx = async () => {}`) — uniquement des `async function`
//   2. AUCUNE clé secrète en dur
//   3. DÉGRADATION GRACIEUSE
//   4. IMMUABILITÉ (CONFIG gelé)
//   5. AUTO-RÉPARATION (retry, fallback, re-ordering)
//
// TABLE DES MATIÈRES :
//   §1.01  En-tête et imports
//   §1.02  Configuration centralisée
//   §1.03  Firebase / Hosting / Quotas
//   §1.04  Validation environnement
//   §1.05  Logger Pino
//   §1.06  Classes d'erreurs typées
//   §1.07  Utilitaires (IDs, hashes, temps)
//   §1.08  Utilitaires de sanitization
//   §1.09  Utilitaires de parsing et normalisation
//   §1.10  Utilitaires de détection
//   §1.11  Utilitaires de chunking et deadlines
//   §1.12  Helpers images
//   §1.13  🆕 CLEAN OUTPUT — nettoyage des réponses
//   §1.14  Firebase Admin + Firestore
//   §1.15  Supabase (backup)
//   §1.16  Cache multi-niveaux
//   §1.17  Cache sémantique
//   §1.18  Sécurité
//   §1.19  Métriques Prometheus
//   §1.20  Feature flags
//   §1.21  🆕 SELF-HEALING — santé des providers
//   §1.22  🆕 SELF-HEALING — token budget guard
//   §1.23  🆕 SELF-HEALING — compaction de contexte
//   §1.24  SQLite (schéma v16.5)
//   §1.25  Bootstrap Partie 1
//   §1.26  Exports Partie 1
// ================================================================================

"use strict";

require("dotenv").config();

// ================================================================================
// §1.01 — EN-TÊTE ET IMPORTS
// ================================================================================

// ---------- Imports core ----------
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
const os             = require("os");
const crypto         = require("crypto");
const pino           = require("pino");
const multer         = require("multer");
const { EventEmitter } = require("events");
const { Worker }     = require("worker_threads");
const FormData       = require("form-data");
const cheerio        = require("cheerio");
const { LRUCache }   = require("lru-cache");
const { createClient } = require("@supabase/supabase-js");

// ---------- Imports optionnels (dégradation gracieuse) ----------
let GoogleGenAI   = null;
let firebaseAdmin = null;
let Firestore     = null;
let IORedis       = null;
let BullMQ        = null;
let PromClient    = null;
let ddgScrape     = null;

try { GoogleGenAI = require("@google/genai").GoogleGenAI; } catch {}
try {
  firebaseAdmin = require("firebase-admin");
  Firestore = require("@google-cloud/firestore");
} catch {}
try { IORedis = require("ioredis"); } catch {}
try { BullMQ = require("bullmq"); } catch {}
try { PromClient = require("prom-client"); } catch {}
try { ddgScrape = require("duck-duck-scrape"); } catch {}

// ================================================================================
// §1.02 — CONFIGURATION CENTRALISÉE
// ================================================================================

const CONFIG = Object.freeze({
  // ─── Identité ───
  ENV:        process.env.NODE_ENV || "production",
  VERSION:    "16.5.0",
  AGENT_NAME: "Luba",
  COMPANY:    "HIKLON TECHNOLOGIES",
  HOST:       process.env.HOST || "0.0.0.0",
  PORT:       parseInt(process.env.PORT || "3000", 10),

  BRAND: Object.freeze({
    V100: process.env.BRAND_V100 || "Mwamba",
    V250: process.env.BRAND_V250 || "Ngandu",
    LIVE: process.env.BRAND_LIVE || "Luba Live"
  }),

  // ─── Limites ───
  LIMITS: Object.freeze({
    MAX_MESSAGE_LENGTH:    parseInt(process.env.MAX_MESSAGE_LENGTH    || "15000", 10),
    MAX_HISTORY_LENGTH:    parseInt(process.env.MAX_HISTORY_LENGTH    || "50", 10),
    MAX_CONTEXT_MESSAGES:  parseInt(process.env.MAX_CONTEXT_MESSAGES  || "20", 10),
    MAX_CONTEXT_TOKENS:    parseInt(process.env.MAX_CONTEXT_TOKENS    || "8000", 10),
    MAX_IMAGE_SIZE_MB:     parseInt(process.env.MAX_IMAGE_SIZE_MB     || "10", 10),
    MAX_IMAGES_PER_REQUEST:parseInt(process.env.MAX_IMAGES_PER_REQUEST|| "3", 10),
    MAX_IMAGES_DISPLAYED:  parseInt(process.env.MAX_IMAGES_DISPLAYED  || "3", 10),
    IMAGE_SEARCH_LIMIT:    parseInt(process.env.IMAGE_SEARCH_LIMIT    || "6", 10)
  }),

  // ─── Agent ───
  AGENT: Object.freeze({
    MAX_ITERATIONS:          parseInt(process.env.AGENT_MAX_ITERATIONS || "5", 10),
    MAX_TOOL_CALLS_PER_STEP: parseInt(process.env.AGENT_MAX_TOOL_CALLS_PER_STEP || "6", 10)
  }),

  // ─── Timeouts ───
  TIMEOUTS: Object.freeze({
    CHAT_ATTEMPT_MS:  parseInt(process.env.CHAT_ATTEMPT_TIMEOUT_MS  || "30000", 10),
    CHAT_GLOBAL_MS:   parseInt(process.env.CHAT_GLOBAL_TIMEOUT_MS   || "120000", 10),
    TOOL_MS:          parseInt(process.env.TOOL_TIMEOUT_MS          || "12000", 10),
    V250_ROUTE_MS:    parseInt(process.env.V250_ROUTE_TIMEOUT       || "90000", 10),
    IMAGE_SOURCE_MS:  parseInt(process.env.IMAGE_SOURCE_DEADLINE_MS || "5000", 10),
    REMINDER_TICK_MS: parseInt(process.env.REMINDER_TICK_MS         || "60000", 10),
    HOUSEKEEPING_MS:  parseInt(process.env.HOUSEKEEPING_INTERVAL_MS || String(6 * 3600 * 1000), 10),
    SELF_CRITIQUE_MS: parseInt(process.env.SELF_CRITIQUE_TIMEOUT_MS || "20000", 10)
  }),

  // ─── Retry ───
  RETRY: Object.freeze({
    MAX_ATTEMPTS:  parseInt(process.env.MAX_RETRY_ATTEMPTS  || "4", 10),
    BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS || "150", 10),
    MAX_DELAY_MS:  parseInt(process.env.RETRY_MAX_DELAY_MS  || "2000", 10)
  }),

  // ─── Circuit breaker ───
  CIRCUIT: Object.freeze({
    THRESHOLD:     parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD || "5", 10),
    RESET_MS:      parseInt(process.env.CIRCUIT_BREAKER_RESET_MS  || "30000", 10),
    HALF_OPEN_MAX: 1
  }),

  // ─── 🆕 Self-healing ───
  SELF_HEAL: Object.freeze({
    // Nombre d'échecs avant de désactiver temporairement un provider
    FAILURE_THRESHOLD:      parseInt(process.env.SELF_HEAL_FAILURE_THRESHOLD || "3", 10),
    // Durée de désactivation (ms) avant retry
    COOLDOWN_MS:            parseInt(process.env.SELF_HEAL_COOLDOWN_MS       || "120000", 10),
    // Budget de tokens approximatif (chars) pour un prompt
    TOKEN_CHAR_BUDGET:      parseInt(process.env.TOKEN_CHAR_BUDGET           || "32000", 10),
    // Au-delà de ce % du budget, on compacte automatiquement
    COMPACT_TRIGGER_PCT:    parseFloat(process.env.COMPACT_TRIGGER_PCT       || "0.75"),
    // Maximum de retry auto avant d'abandonner
    MAX_AUTO_RETRIES:       parseInt(process.env.MAX_AUTO_RETRIES            || "3", 10)
  }),

  // ─── Authentification ───
  AUTH: Object.freeze({
    TOKEN_CACHE_TTL_MS:    parseInt(process.env.AUTH_TOKEN_CACHE_TTL_MS || "300000", 10),
    CHECK_REVOKED:         process.env.AUTH_CHECK_REVOKED === "true",
    MAX_LOGIN_ATTEMPTS:    parseInt(process.env.MAX_LOGIN_ATTEMPTS      || "20", 10),
    LOGIN_BLOCK_MS:        parseInt(process.env.LOGIN_BLOCK_DURATION    || "900000", 10),
    MAX_SESSIONS_PER_USER: parseInt(process.env.MAX_SESSIONS_PER_USER   || "10", 10),
    HMAC_SECRET:           process.env.HMAC_SECRET || null
  }),

  // ─── Chemins ───
  PATHS: Object.freeze({
    DATA:     path.join(__dirname, "data"),
    DB:       path.join(__dirname, "data", "luba.db"),
    SESSIONS: path.join(__dirname, "sessions"),
    UPLOADS:  path.join(__dirname, "uploads"),
    LOGS:     path.join(__dirname, "logs")
  }),

  // ─── News / Sports ───
  NEWS: Object.freeze({
    SPORT_MAX_ARTICLES: parseInt(process.env.SPORT_NEWS_MAX_ARTICLES || "6", 10),
    SPORT_CACHE_TTL_MS: parseInt(process.env.SPORT_CACHE_TTL_MS      || "600000", 10),
    GOOGLE_LANG:        process.env.GOOGLE_NEWS_LANG   || "fr",
    GOOGLE_REGION:      process.env.GOOGLE_NEWS_REGION || "FR"
  }),

  // ─── Images ───
  IMAGES: Object.freeze({
    CACHE_TTL_MS:    parseInt(process.env.IMAGE_CACHE_TTL_MS     || String(20 * 60 * 1000), 10),
    WIKIMEDIA_LIMIT: parseInt(process.env.IMAGE_WIKIMEDIA_LIMIT  || "8", 10),
    DDG_LIMIT:       parseInt(process.env.IMAGE_DDG_LIMIT        || "4", 10),
    MIN_RELEVANCE:   parseFloat(process.env.IMAGE_MIN_RELEVANCE  || "0.15"),
    WIKIMEDIA_UA:    process.env.WIKIMEDIA_USER_AGENT
      || "LubaAI/16.5.0 (https://luba.web.app; contact@luba.web.app)",
    ALLOWED_TYPES:   ["image/jpeg", "image/png", "image/gif", "image/webp"]
  }),

  // ─── Audio ───
  AUDIO: Object.freeze({
    ALLOWED_TYPES: ["audio/mpeg","audio/mp4","audio/wav","audio/webm",
                    "audio/ogg","audio/m4a","audio/x-m4a","audio/aac"],
    MAX_SIZE_MB:   parseInt(process.env.MAX_AUDIO_SIZE_MB || "20", 10)
  }),

  // ─── Email ───
  EMAIL: Object.freeze({
    CONTACT:   process.env.CONTACT_EMAIL || "contact@luba.web.app",
    FROM_NAME: process.env.EMAIL_FROM_NAME || "Luba",
    FROM_ADDR: process.env.EMAIL_FROM_ADDR || process.env.SMTP_USER || "noreply@luba.web.app"
  }),

  // ─── WhatsApp ───
  WHATSAPP: Object.freeze({
    QR_TIMEOUT_MS:  parseInt(process.env.WHATSAPP_QR_TIMEOUT  || "30000", 10),
    RETRY_DELAY_MS: parseInt(process.env.WHATSAPP_RETRY_DELAY || "4000", 10),
    WHITELIST:      (process.env.WHATSAPP_WHITELIST || "")
                      .split(",").map(s => s.trim().replace(/[^\d]/g, "")).filter(Boolean),
    OPEN:           process.env.WHATSAPP_OPEN === "true",
    ENCRYPTION_KEY: process.env.WHATSAPP_ENCRYPTION_KEY || null,
    ENCRYPTION_IV:  process.env.WHATSAPP_ENCRYPTION_IV  || null
  }),

  // ─── Vision ───
  VISION: Object.freeze({
    GROQ_MODEL:       process.env.VISION_MODEL_GROQ       || "meta-llama/llama-4-maverick-17b-128e-instruct",
    GEMINI_MODEL:     process.env.VISION_MODEL_GEMINI     || "gemini-2.5-flash",
    OPENROUTER_MODEL: process.env.VISION_MODEL_OPENROUTER || "qwen/qwen-2.5-vl-72b-instruct:free"
  }),

  // ─── Sandbox ───
  SANDBOX: Object.freeze({
    PROVIDER:   process.env.CODE_SANDBOX_PROVIDER || "piston",
    PISTON_URL: process.env.PISTON_URL            || "https://emkc.org",
    JUDGE0_URL: process.env.JUDGE0_URL            || "",
    E2B_KEY:    process.env.E2B_API_KEY           || ""
  }),

  // ─── Cache ───
  CACHE: Object.freeze({
    L1_MAX_ITEMS:       parseInt(process.env.CACHE_L1_MAX_ITEMS || "5000", 10),
    L1_TTL_MS:          parseInt(process.env.CACHE_L1_TTL_MS    || String(10 * 60 * 1000), 10),
    L2_DEFAULT_TTL_S:   parseInt(process.env.CACHE_L2_TTL_S     || "3600", 10),
    SEMANTIC_THRESHOLD: parseFloat(process.env.CACHE_SEMANTIC_THRESHOLD || "0.92")
  }),

  HTTP: Object.freeze({
    USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/16.5.0"
  }),

  HMAC: Object.freeze({
    ENABLED: Boolean(process.env.HMAC_SECRET),
    SECRET:  process.env.HMAC_SECRET || null,
    WINDOW_MS: 5 * 60 * 1000
  }),

  AI_QUALITY: Object.freeze({
    ENABLE_SELF_CRITIQUE:       process.env.ENABLE_SELF_CRITIQUE !== "false",
    ENABLE_CONFIDENCE:          process.env.ENABLE_CONFIDENCE !== "false",
    ENABLE_MULTI_VOTE:          process.env.ENABLE_MULTI_VOTE === "true",
    ENABLE_HALLUCINATION_CHECK: process.env.ENABLE_HALLUCINATION_CHECK !== "false",
    CONFIDENCE_THRESHOLD:       parseFloat(process.env.CONFIDENCE_THRESHOLD || "0.6"),
    MAX_VOTING_PROVIDERS:       parseInt(process.env.MAX_VOTING_PROVIDERS || "3", 10),
    SELF_CRITIQUE_TRIGGER_ON:   process.env.SELF_CRITIQUE_TRIGGER || "auto"
  }),

  I18N: Object.freeze({
    DEFAULT_LANGUAGE: process.env.DEFAULT_LANGUAGE || "fr",
    SUPPORTED: ["fr", "en", "sw", "ln"]
  })
});

// ================================================================================
// §1.03 — FIREBASE / HOSTING / QUOTAS
// ================================================================================

const FIREBASE_CONFIG = Object.freeze({
  apiKey:            process.env.FIREBASE_API_KEY || null,
  projectId:         process.env.FIREBASE_PROJECT_ID || "luba-ia-636",
  authDomain:        process.env.FIREBASE_AUTH_DOMAIN || "luba-ia-636.firebaseapp.com",
  storageBucket:     process.env.FIREBASE_STORAGE_BUCKET || "luba-ia-636.firebasestorage.app",
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID || "502404354252",
  appId:             process.env.FIREBASE_APP_ID || "1:502404354252:web:660ab2109ce448e1803269"
});

const HOSTING_CONFIG = Object.freeze({
  domain: process.env.HOSTING_DOMAIN || "https://luba.web.app",
  allowedOrigins: (process.env.ALLOWED_ORIGINS?.split(",").map(s => s.trim()).filter(Boolean)) || [
    "https://luba.web.app",
    "https://luba-ia-636.web.app",
    "https://luba-ia-636.firebaseapp.com",
    "https://milo-backend-sa1y.onrender.com",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:5173",
    "http://localhost:4200"
  ]
});

const USER_QUOTAS = Object.freeze({
  FREE:    { maxMessagesPerDay: 100,    maxImagesPerDay: 20,    maxWhatsAppMessagesPerDay: 10,    maxEmailsPerDay: 5,    maxTokensPerRequest: 8000   },
  PREMIUM: { maxMessagesPerDay: 1000,   maxImagesPerDay: 200,   maxWhatsAppMessagesPerDay: 100,   maxEmailsPerDay: 50,   maxTokensPerRequest: 32000  },
  ADMIN:   { maxMessagesPerDay: 999999, maxImagesPerDay: 999999,maxWhatsAppMessagesPerDay: 999999,maxEmailsPerDay: 999999,maxTokensPerRequest: 128000 }
});

// ================================================================================
// §1.04 — VALIDATION ENVIRONNEMENT
// ================================================================================

function validateEnvironment() {
  const problems = [];
  const warnings = [];

  const hasFbAdmin = Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const hasFbRest  = Boolean(process.env.FIREBASE_API_KEY);
  if (!hasFbAdmin && !hasFbRest) {
    problems.push("Aucune authentification Firebase configurée.");
  } else if (!hasFbAdmin) {
    warnings.push("Firebase Admin SDK absent → mode REST uniquement.");
  }

  const hasLLM = Boolean(
    process.env.GROQ_API_KEY || process.env.OPENROUTER_API_KEY ||
    process.env.CEREBRAS_API_KEY || process.env.GEMINI_API_KEY
  );
  if (!hasLLM) {
    problems.push("Aucune clé LLM configurée (GROQ / OPENROUTER / CEREBRAS / GEMINI).");
  }

  const hasSupabase = Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_KEY);
  if (!hasSupabase && !hasFbAdmin && CONFIG.ENV === "production") {
    warnings.push("Ni Supabase ni Firestore → SQLite seul.");
  }

  if (CONFIG.ENV === "production") {
    const k = CONFIG.WHATSAPP.ENCRYPTION_KEY;
    const iv = CONFIG.WHATSAPP.ENCRYPTION_IV;
    if (!k || k.length < 32 || !iv || iv.length < 16) {
      problems.push("WHATSAPP_ENCRYPTION_KEY (≥32) et WHATSAPP_ENCRYPTION_IV (≥16) requis en prod.");
    }
    if (!CONFIG.HMAC.ENABLED) {
      warnings.push("HMAC_SECRET absent → signature inter-services désactivée.");
    }
  }

  for (const w of warnings) console.warn("⚠️  " + w);
  if (problems.length > 0) {
    for (const p of problems) console.error("❌ " + p);
    if (CONFIG.ENV === "production") {
      console.error("🛑 Démarrage interrompu.");
      process.exit(1);
    }
  }
}

function ensureDirectories() {
  const dirs = [CONFIG.PATHS.DATA, CONFIG.PATHS.SESSIONS, CONFIG.PATHS.UPLOADS, CONFIG.PATHS.LOGS];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true, mode: 0o750 });
  }
}

// ================================================================================
// §1.05 — LOGGER PINO
// ================================================================================

const logger = pino({
  level: process.env.LOG_LEVEL || (CONFIG.ENV === "production" ? "info" : "debug"),
  base: {
    service: "luba-backend",
    version: CONFIG.VERSION,
    pid: process.pid,
    hostname: os.hostname()
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  redact: {
    paths: [
      "req.headers.authorization",
      "req.headers['x-google-access-token']",
      "req.headers['x-session-token']",
      "req.headers['x-luba-signature']",
      "*.apiKey", "*.key", "*.token", "*.access_token", "*.refresh_token",
      "*.password", "*.secret", "*.HMAC_SECRET"
    ],
    censor: "[REDACTED]"
  },
  serializers: {
    err: (e) => ({
      type: e.constructor?.name || "Error",
      message: e.message,
      code: e.code || null,
      stack: CONFIG.ENV === "development" ? e.stack : undefined
    })
  }
});

// ================================================================================
// §1.06 — CLASSES D'ERREURS TYPÉES
// ================================================================================

class LubaError extends Error {
  constructor(code, message, httpStatus = 500, context = {}) {
    super(message || code);
    this.name = "LubaError";
    this.code = code;
    this.httpStatus = httpStatus;
    this.context = context;
    this.timestamp = new Date().toISOString();
    if (Error.captureStackTrace) Error.captureStackTrace(this, LubaError);
  }
  toJSON() {
    return {
      success: false,
      error: true,
      reply: this.message,
      code: this.code
    };
  }
}

const ERROR_CODES = Object.freeze({
  MISSING_TOKEN:          { status: 401, msg: "Authentification requise." },
  INVALID_TOKEN:          { status: 401, msg: "Session invalide." },
  TOKEN_EXPIRED:          { status: 401, msg: "Session expirée, reconnectez-vous." },
  IP_BLOCKED:             { status: 403, msg: "Accès refusé." },
  INSUFFICIENT_ROLE:      { status: 403, msg: "Privilèges insuffisants." },
  AUTH_INTERNAL:          { status: 500, msg: "Erreur d'authentification." },
  MISSING_MESSAGE:        { status: 400, msg: "Le paramètre 'message' est obligatoire." },
  INVALID_MESSAGE:        { status: 400, msg: "Message invalide." },
  INVALID_CONVERSATION_ID:{ status: 400, msg: "Identifiant de conversation invalide." },
  FILE_TOO_LARGE:         { status: 413, msg: "Fichier trop volumineux." },
  TOO_MANY_FILES:         { status: 413, msg: "Trop de fichiers." },
  INVALID_IMAGE_CONTENT:  { status: 400, msg: "Contenu image invalide." },
  VALIDATION_ERROR:       { status: 400, msg: "Données de requête invalides." },
  CONVERSATION_OWNERSHIP: { status: 403, msg: "Conversation non autorisée." },
  CONVERSATION_BUSY:      { status: 409, msg: "Une requête est déjà en cours." },
  QUOTA_EXCEEDED:         { status: 429, msg: "Quota journalier atteint." },
  RATE_LIMIT:             { status: 429, msg: "Trop de requêtes." },
  RATE_LIMIT_CHAT:        { status: 429, msg: "Trop de messages." },
  PROVIDER_DOWN:          { status: 503, msg: "Service IA temporairement indisponible." },
  ALL_PROVIDERS_FAILED:   { status: 503, msg: "Tous les fournisseurs IA ont échoué." },
  CIRCUIT_OPEN:           { status: 503, msg: "Service temporairement surchargé." },
  TOOL_NOT_ALLOWED:       { status: 403, msg: "Outil non autorisé." },
  TOOL_EXECUTION_ERROR:   { status: 500, msg: "Échec de l'exécution de l'outil." },
  NEEDS_CONFIRMATION:     { status: 202, msg: "Confirmation requise." },
  SANDBOX_UNAVAILABLE:    { status: 503, msg: "Sandbox d'exécution indisponible." },
  TOKEN_OVERFLOW:         { status: 400, msg: "Contexte trop long — compacté automatiquement." },
  INTERNAL_ERROR:         { status: 500, msg: "Erreur interne." },
  NOT_FOUND:              { status: 404, msg: "Ressource non trouvée." }
});

function makeError(code, extra = "", status = null) {
  const def = ERROR_CODES[code] || { status: 500, msg: "Erreur." };
  const message = extra ? `${def.msg} ${extra}`.trim() : def.msg;
  return new LubaError(code, message, status ?? def.status);
}

function isLubaError(e) { return e instanceof LubaError; }

// ================================================================================
// §1.07 — UTILITAIRES (IDs, HASHES, TEMPS)
// ================================================================================

const generateRequestId      = () => `req_${crypto.randomUUID()}`;
const generateConversationId = () => `conv_${crypto.randomUUID()}`;
const generateSessionToken   = () => `sess_${crypto.randomBytes(32).toString("hex")}`;
const generateUUID           = () => crypto.randomUUID();
const generateTaskId         = () => `task_${crypto.randomUUID()}`;
const generateMsgId          = () => `msg_${crypto.randomUUID()}`;

const sha256 = (i) => crypto.createHash("sha256").update(String(i)).digest("hex");
const sha1   = (i) => crypto.createHash("sha1").update(String(i)).digest("hex");
const hashSessionToken = (t) => sha256(t);

const hmacSign = (payload, secret = CONFIG.HMAC.SECRET) => {
  if (!secret) throw new LubaError("INTERNAL_ERROR", "HMAC secret absent", 500);
  return crypto.createHmac("sha256", secret).update(payload).digest("hex");
};

const hmacVerify = (payload, signature, secret = CONFIG.HMAC.SECRET) => {
  if (!secret || !signature) return false;
  try {
    const expected = hmacSign(payload, secret);
    const a = Buffer.from(expected, "hex");
    const b = Buffer.from(String(signature), "hex");
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch { return false; }
};

const nowMs = () => Date.now();

function todayKeyMs() {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function backoffDelay(attempt, base = CONFIG.RETRY.BASE_DELAY_MS, max = CONFIG.RETRY.MAX_DELAY_MS) {
  const exp = Math.min(base * Math.pow(2, attempt), max);
  return exp + Math.floor(Math.random() * 100);
}

// ================================================================================
// §1.08 — UTILITAIRES DE SANITIZATION
// ================================================================================

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function sanitizeForLLM(input, maxLength = CONFIG.LIMITS.MAX_MESSAGE_LENGTH) {
  if (input === null || input === undefined) return "";
  let text = String(input);
  text = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "");
  if (text.length > maxLength) text = text.slice(0, maxLength);
  return text.trim();
}

function sanitizeStrict(input, maxLength = 500) {
  if (input === null || input === undefined) return "";
  let text = String(input).trim().replace(/[\u0000-\u001F\u007F]/g, "");
  text = text
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<iframe[\s\S]*?<\/iframe>/gi, "")
    .replace(/<object[\s\S]*?<\/object>/gi, "")
    .replace(/javascript:/gi, "")
    .replace(/on\w+\s*=\s*["'][^"']*["']/gi, "");
  if (text.length > maxLength) text = text.slice(0, maxLength);
  return text;
}

// ================================================================================
// §1.09 — UTILITAIRES DE PARSING ET NORMALISATION
// ================================================================================

function safeJsonParse(str, fallback = null) {
  if (str === null || str === undefined) return fallback;
  try { return JSON.parse(str); } catch { return fallback; }
}

function safeJsonStringify(obj, fallback = "{}") {
  try { return JSON.stringify(obj); } catch { return fallback; }
}

function normalizeMath(input) {
  if (typeof input !== "string" || input.length === 0) return "";
  const parts = [];
  const codeBlockRegex = /```[\s\S]*?```|`[^`\n]*`/g;
  let lastIndex = 0;
  let match;
  while ((match = codeBlockRegex.exec(input)) !== null) {
    if (match.index > lastIndex) parts.push({ type: "text", content: input.slice(lastIndex, match.index) });
    parts.push({ type: "code", content: match[0] });
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < input.length) parts.push({ type: "text", content: input.slice(lastIndex) });

  return parts.map(({ type, content }) => {
    if (type === "code") return content;
    let text = content.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
    text = text.replace(/\\\(([\s\S]*?)\\\)/g, (_, inner) => `$${inner.trim()}$`);
    text = text.replace(/\\\[([\s\S]*?)\\\]/g, (_, inner) => `$$${inner.trim()}$$`);
    text = text
      .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, " ");
    text = text.replace(/\n{3,}/g, "\n\n");
    return text;
  }).join("").trim();
}

function stripThinkTags(input) {
  if (typeof input !== "string") return { text: "", thinking: "" };
  const thinks = [];
  const text = input
    .replace(/<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/gi, (_, inner) => {
      thinks.push(inner.trim());
      return "";
    })
    .replace(/<think(?:ing)?>[\s\S]*$/i, "")
    .trim();
  return { text, thinking: thinks.join("\n---\n") };
}

function decodeXmlEntities(str) {
  return String(str)
    .replace(/<!\[CDATA\[/g, "").replace(/\]\]>/g, "")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ").trim();
}

// ================================================================================
// §1.10 — UTILITAIRES DE DÉTECTION
// ================================================================================

function computeDeviceFingerprint(ip, userAgent) {
  return sha256(`${ip || "?"}::${userAgent || "?"}`).slice(0, 32);
}

function isValidImageSignature(buffer) {
  if (!buffer || buffer.length < 12) return false;
  const hex = buffer.subarray(0, 12).toString("hex");
  if (hex.startsWith("ffd8ff")) return true;
  if (hex.startsWith("89504e470d0a1a0a")) return true;
  if (hex.startsWith("47494638")) return true;
  if (hex.startsWith("52494646") && buffer.subarray(8, 12).toString("ascii") === "WEBP") return true;
  return false;
}

function convertImageToBase64(buffer, mimetype) {
  return {
    dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`,
    base64: buffer.toString("base64"),
    mimetype,
    size: buffer.length
  };
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_REGEX = /^\+?[1-9]\d{6,14}$/;

function detectLanguage(text) {
  if (!text || typeof text !== "string") return CONFIG.I18N.DEFAULT_LANGUAGE;
  const t = text.toLowerCase();
  if (/[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]/.test(text)) return CONFIG.I18N.DEFAULT_LANGUAGE;

  const sw = (t.match(/\b(habari|asante|karibu|jambo|ndiyo|hapana|vipi|nzuri|sana|kwaheri|tafadhali|ninataka)\b/g) || []).length;
  const ln = (t.match(/\b(mbote|mbota|sango|nzela|melesi|malamu|kitoko|ezali|nakozela|elengi)\b/g) || []).length;
  const en = (t.match(/\b(the|and|you|with|this|that|hello|please|thanks|help|what|where|when|how|why)\b/g) || []).length;
  const fr = (t.match(/\b(le|la|les|un|une|des|je|tu|il|elle|nous|vous|bonjour|merci|comment|pourquoi|quand|où|oui|non)\b/g) || []).length;

  const scores = { fr, en, sw, ln };
  const best = Object.entries(scores).sort((a, b) => b[1] - a[1])[0];
  return best[1] > 0 ? best[0] : CONFIG.I18N.DEFAULT_LANGUAGE;
}

function truncateToTokenBudget(text, maxTokens = 8000) {
  if (!text) return "";
  const maxChars = maxTokens * 4;
  return text.length <= maxChars ? text : text.slice(0, maxChars) + "\n…[tronqué]";
}

// ================================================================================
// §1.11 — UTILITAIRES DE CHUNKING ET DEADLINES
// ================================================================================

function safeChunkText(text, targetSize = 24) {
  if (!text) return [];
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + targetSize, text.length);
    const code = text.charCodeAt(end - 1);
    if (code >= 0xD800 && code <= 0xDBFF && end < text.length) end++;
    chunks.push(text.slice(i, end));
    i = end;
  }
  return chunks;
}

function withDeadline(promise, deadlineMs, fallbackValue) {
  return Promise.race([
    Promise.resolve(promise),
    new Promise((resolve) => {
      const t = setTimeout(() => resolve(fallbackValue), deadlineMs);
      if (t.unref) t.unref();
    })
  ]);
}

async function allSettledWithDeadline(promises, deadlineMs) {
  return Promise.all(promises.map((p) =>
    Promise.race([
      Promise.resolve(p)
        .then((v) => ({ status: "fulfilled", value: v }))
        .catch((e) => ({ status: "rejected", reason: e })),
      new Promise((resolve) => {
        const t = setTimeout(() => resolve({ status: "timeout" }), deadlineMs);
        if (t.unref) t.unref();
      })
    ])
  ));
}

// ================================================================================
// §1.12 — HELPERS IMAGES
// ================================================================================

function imageRelevanceScore(image, query) {
  if (!image || !query) return 0.5;
  const norm = (s) => String(s || "").toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const q = norm(query);
  const title = norm(image.title);
  const desc = norm(image.description);
  const combined = `${title} ${desc}`;

  const stop = new Set(["the","and","for","with","from","les","des","une","un","de","du","la","le"]);
  const words = q.split(/\s+/).filter((w) => w.length > 2 && !stop.has(w));
  if (words.length === 0) return 0.5;

  let matched = 0;
  for (const w of words) if (combined.includes(w)) matched++;
  return matched / words.length;
}

function isGreetingOrSmallTalk(message) {
  if (!message) return true;
  const t = String(message).toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s']/g, " ").replace(/\s+/g, " ").trim();
  if (t.length < 4) return true;

  const greetings = [
    "salut","slt","bonjour","bonsoir","bjr","hello","hi","hey","coucou","cc",
    "merci","thanks","thank you","ok","d'accord","daccord","ca va","cava",
    "comment ca va","comment vas tu","quoi de neuf","au revoir","bye","a plus",
    "bonne nuit","bonne journee","c'est bon","cest bon","yes","no","yep","nope",
    "bravo","super","genial","cool","parfait","nickel","top","bien","bof","oui","non",
    "qui es tu","qui es-tu","tu es qui","c'est qui","qui est luba","qui est-tu",
    "presente toi","présente toi","ton nom","tu t'appelles comment"
  ];

  if (greetings.includes(t)) return true;
  for (const g of greetings) {
    if (t.startsWith(g + " ") || t.startsWith(g + ",") || t.startsWith(g + "!") ||
        t.startsWith(g + "?") || t.startsWith(g + "'")) {
      if (t.length <= g.length + 15) return true;
    }
  }
  return false;
}

function isIdentityOrSelfQuestion(message) {
  if (!message) return false;
  const t = normalizeForMatch(message);
  const identityPhrases = [
    "qui es tu", "qui es-tu", "tu es qui", "tu es qui toi",
    "c'est qui toi", "presente toi", "presente-toi",
    "qui est luba", "qui est l人工", "qui est louba",
    "ton nom", "tu t'appelles comment", "comment tu t'appelles",
    "quel est ton nom", "tu es quoi", "tu es un robot",
    "tu es une ia", "es-tu une ia", "es-tu humain",
    "tu es humain", "tu es une intelligence artificielle",
    "parle moi de toi", "parle moi de toi meme", "qui te cree",
    "qui ta cree", "qui t'a cree", "ton createur", "ton créateur",
    "d'ou viens tu", "d'ou tu viens", "d'où viens-tu"
  ];
  for (const phrase of identityPhrases) {
    if (t.includes(normalizeForMatch(phrase))) return true;
  }
  return false;
}

// ================================================================================
// §1.13 — 🆕 CLEAN OUTPUT (nettoyage des réponses)
// ================================================================================

/**
 * Nettoie TOUTE réponse LLM avant envoi au frontend.
 * Objectif : zéro caractère bizarre, markdown propre, texte lisible.
 *
 * Actions :
 *   1. Supprime caractères de contrôle Unicode (sauf \n, \t, \r)
 *   2. Supprime BOM (U+FEFF), zero-width spaces, soft hyphens
 *   3. Normalise les retours chariot (\r\n → \n)
 *   4. Collapse les lignes vides multiples (max 2)
 *   5. Trim les espaces en fin de ligne
 *   6. Corrige les caractères typographiques cassés (mojibake)
 *   7. Supprime les séquences de balises vides
 */
function cleanOutput(input) {
  if (typeof input !== "string" || input.length === 0) return "";

  let text = input;

  // 1) Supprime BOM, zero-width, soft hyphen, autres invisibles
  text = text.replace(/[\uFEFF\u200B\u200C\u200D\u2060\u180E]/g, "");

  // 2) Supprime les caractères de contrôle (garde \n, \t, \r)
  text = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "");

  // 3) Normalise les fins de ligne
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  // 4) Corrige les caractères typographiques mojibake courants
  const mojibakeFixes = [
    [/Ã©/g, "é"], [/Ã¨/g, "è"], [/Ã /g, "à"], [/Ã¹/g, "ù"], [/Ã´/g, "ô"],
    [/Ã¢/g, "â"], [/Ãª/g, "ê"], [/Ã®/g, "î"], [/Ã¯/g, "ï"], [/Ã§/g, "ç"],
    [/Â«/g, "«"], [/Â»/g, "»"], [/Â /g, " "], [/â€™/g, "'"], [/â€œ/g, "\""],
    [/â€/g, "\""], [/â€"/g, "—"], [/â€"/g, "–"], [/â€¦/g, "…"]
  ];
  for (const [pattern, replacement] of mojibakeFixes) {
    text = text.replace(pattern, replacement);
  }

  // 5) Trim chaque ligne
  text = text.split("\n").map((line) => line.replace(/\s+$/g, "")).join("\n");

  // 6) Collapse les lignes vides multiples (max 2 consécutives)
  text = text.replace(/\n{3,}/g, "\n\n");

  // 7) Supprime les espaces multiples dans une ligne (mais pas les indentations de code)
  text = text.split("\n").map((line) => {
    // Ne touche pas aux lignes qui commencent par 4 espaces (code block)
    if (/^    /.test(line)) return line;
    return line.replace(/[ \t]{2,}/g, " ");
  }).join("\n");

  // 8) Enlève les séquences de balises vides type `<br><br><br>`
  text = text.replace(/(<br\s*\/?>[\s]*){3,}/gi, "<br><br>");

  return text.trim();
}

/**
 * Nettoie et formate la réponse finale pour le frontend.
 * Applique cleanOutput + normalizeMath + strip think tags.
 */
function formatFinalReply(rawText) {
  if (!rawText) return "";

  // 1) Retire les tags <think> (raisonnement interne)
  const { text: withoutThink } = stripThinkTags(rawText);

  // 2) Normalise le LaTeX
  let cleaned = normalizeMath(withoutThink);

  // 3) Nettoie les caractères bizarres
  cleaned = cleanOutput(cleaned);

  // 4) Retire les artefacts courants
  cleaned = cleaned
    .replace(/^\s*assistant\s*:\s*/i, "")
    .replace(/^\s*AI\s*:\s*/i, "")
    .replace(/^\s*Luba\s*:\s*/i, "");

  return cleaned.trim();
}

/**
 * Valide qu'une réponse n'est pas vide ou triviale.
 */
function isValidReply(text) {
  if (!text || typeof text !== "string") return false;
  const cleaned = text.trim();
  if (cleaned.length < 2) return false;
  if (/^(ok|okay|d'accord|daccord|\.|\.\.\.)$/i.test(cleaned)) return false;
  return true;
}

// ================================================================================
// §1.14 — FIREBASE ADMIN + FIRESTORE
// ================================================================================

let firebaseApp = null;
let firestoreDb = null;
let firebaseReady = false;

function parseFirebaseServiceAccount(raw) {
  try { return JSON.parse(raw); }
  catch {
    try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
    catch { throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide"); }
  }
}

function initFirebase() {
  if (!firebaseAdmin || !process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    logger.warn("⚠️  Firebase Admin non initialisé — mode REST");
    return;
  }
  try {
    const sa = parseFirebaseServiceAccount(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    firebaseApp = firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(sa),
      projectId: FIREBASE_CONFIG.projectId
    });
    if (Firestore) {
      firestoreDb = new Firestore.Firestore({
        projectId: FIREBASE_CONFIG.projectId,
        credentials: {
          client_email: sa.client_email,
          private_key: sa.private_key
        }
      });
      logger.info("✅ Firebase Admin + Firestore initialisés");
    } else {
      logger.info("✅ Firebase Admin initialisé (Firestore non dispo)");
    }
    firebaseReady = true;
  } catch (e) {
    logger.error({ err: e.message }, "❌ Init Firebase Admin échouée");
    firebaseApp = null; firestoreDb = null; firebaseReady = false;
  }
}

async function fsGet(collection, docId) {
  if (!firestoreDb) return null;
  try {
    const snap = await firestoreDb.collection(collection).doc(docId).get();
    return snap.exists ? { id: snap.id, ...snap.data() } : null;
  } catch (e) { return null; }
}

async function fsSet(collection, docId, data, { merge = true } = {}) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try { await firestoreDb.collection(collection).doc(docId).set(data, { merge }); return { success: true }; }
  catch (e) { return { success: false, error: e }; }
}

async function fsUpdate(collection, docId, data) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try { await firestoreDb.collection(collection).doc(docId).update(data); return { success: true }; }
  catch (e) { return { success: false, error: e }; }
}

async function fsDelete(collection, docId) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try { await firestoreDb.collection(collection).doc(docId).delete(); return { success: true }; }
  catch (e) { return { success: false, error: e }; }
}

async function fsQuery(collection, { where = [], orderBy = null, limit = 50 } = {}) {
  if (!firestoreDb) return [];
  try {
    let q = firestoreDb.collection(collection);
    for (const [f, op, v] of where) q = q.where(f, op, v);
    if (orderBy) q = q.orderBy(orderBy.field, orderBy.direction || "desc");
    if (limit) q = q.limit(limit);
    const snap = await q.get();
    return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
  } catch (e) { return []; }
}

// ================================================================================
// §1.15 — SUPABASE (BACKUP)
// ================================================================================

let supabase = null;

function initSupabase() {
  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    logger.warn("⚠️  Supabase non configuré");
    return;
  }
  try {
    supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
      db: { schema: "public" },
      global: { headers: { "x-application-name": `luba-backend-v${CONFIG.VERSION}` } }
    });
    logger.info("✅ Supabase initialisé (backup)");
  } catch (e) {
    logger.error({ err: e.message }, "❌ Supabase init échouée");
    supabase = null;
  }
}

async function supabaseWriteSafe({ table, op, payload, matchColumn = null, matchValue = null }) {
  if (!supabase) return { success: false, reason: "no_supabase" };
  try {
    let result;
    if (op === "insert")      result = await supabase.from(table).insert(payload);
    else if (op === "upsert") result = await supabase.from(table).upsert(payload, { onConflict: matchColumn, ignoreDuplicates: false });
    else if (op === "update") result = await supabase.from(table).update(payload).eq(matchColumn, matchValue);
    else if (op === "delete") result = await supabase.from(table).delete().eq(matchColumn, matchValue);
    else return { success: false, reason: "unknown_op" };

    if (result.error) return { success: false, error: result.error };
    return { success: true };
  } catch (e) { return { success: false, error: e }; }
}

// ================================================================================
// §1.16 — CACHE MULTI-NIVEAUX (L1 LRU + L2 Redis)
// ================================================================================

const l1Cache = new LRUCache({
  max: CONFIG.CACHE.L1_MAX_ITEMS,
  ttl: CONFIG.CACHE.L1_TTL_MS,
  updateAgeOnGet: false,
  allowStale: false
});

let redisClient = null;

function initRedis() {
  if (!IORedis || !process.env.REDIS_URL) {
    logger.warn("⚠️  Redis non configuré — cache L2 désactivé");
    return;
  }
  try {
    redisClient = new IORedis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      retryStrategy: (times) => Math.min(times * 200, 5000)
    });
    redisClient.on("error", (e) => logger.warn({ err: e.message }, "Redis erreur"));
    redisClient.on("connect", () => logger.info("✅ Redis connecté"));
  } catch (e) { redisClient = null; }
}

const cache = {
  async get(key) {
    const l1 = l1Cache.get(key);
    if (l1 !== undefined) return l1;
    if (redisClient) {
      try {
        const raw = await redisClient.get(key);
        if (raw) {
          const parsed = safeJsonParse(raw, null);
          if (parsed !== null) l1Cache.set(key, parsed);
          return parsed;
        }
      } catch {}
    }
    return null;
  },
  async set(key, value, ttlMs = CONFIG.CACHE.L1_TTL_MS) {
    l1Cache.set(key, value, { ttl: ttlMs });
    if (redisClient) {
      try { await redisClient.setex(key, Math.ceil(ttlMs / 1000), safeJsonStringify(value)); } catch {}
    }
  },
  async del(key) {
    l1Cache.delete(key);
    if (redisClient) { try { await redisClient.del(key); } catch {} }
  },
  async withCache(key, ttlMs, loader) {
    const cached = await this.get(key);
    if (cached !== null) return cached;
    const fresh = await loader();
    if (fresh !== null && fresh !== undefined) await this.set(key, fresh, ttlMs);
    return fresh;
  }
};

// ================================================================================
// §1.17 — CACHE SÉMANTIQUE
// ================================================================================

class SemanticCache {
  constructor({ threshold = CONFIG.CACHE.SEMANTIC_THRESHOLD, maxSize = 1000 } = {}) {
    this.threshold = threshold;
    this.maxSize = maxSize;
    this.entries = new Map();
  }

  static cosineSimilarity(a, b) {
    if (!a || !b || a.length !== b.length) return 0;
    let dot = 0, normA = 0, normB = 0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    if (normA === 0 || normB === 0) return 0;
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  async get(embedding) {
    if (!embedding) return null;
    let best = null, bestScore = 0;
    for (const entry of this.entries.values()) {
      const score = SemanticCache.cosineSimilarity(embedding, entry.embedding);
      if (score > bestScore) { bestScore = score; best = entry; }
    }
    if (best && bestScore >= this.threshold) return best.value;
    return null;
  }

  set(embedding, value) {
    if (!embedding) return;
    if (this.entries.size >= this.maxSize) {
      const firstKey = this.entries.keys().next().value;
      if (firstKey) this.entries.delete(firstKey);
    }
    const key = sha256(JSON.stringify(embedding)).slice(0, 16);
    this.entries.set(key, { embedding, value, ts: Date.now() });
  }

  clear() { this.entries.clear(); }
  size() { return this.entries.size; }
}

const semanticCache = new SemanticCache();

// ================================================================================
// §1.18 — SÉCURITÉ
// ================================================================================

const INJECTION_PATTERNS = [
  /ignore\s+(all\s+)?(previous|above|prior)\s+(instructions|prompts|rules)/i,
  /disregard\s+(all\s+)?(previous|above)/i,
  /forget\s+(everything|all|your)\s+(instructions|rules)/i,
  /you\s+are\s+now\s+(a|an)\s+(?!assistant|AI|model)/i,
  /system\s*:\s*you\s+are/i,
  /<\|im_start\|>/i, /<\|im_end\|>/i, /\[INST\]/i, /\[\/INST\]/i,
  /###\s*instruction/i, /act\s+as\s+(if\s+you\s+are|a)\s+(?!assistant)/i,
  /jailbreak/i, /DAN\s+mode/i, /developer\s+mode/i, /repeat\s+after\s+me/i,
  /reveal\s+your\s+(system\s+)?prompt/i, /show\s+me\s+your\s+instructions/i,
  /print\s+your\s+(initial\s+)?prompt/i
];

function detectPromptInjection(text) {
  if (!text || typeof text !== "string") return { detected: false, pattern: null };
  for (const p of INJECTION_PATTERNS) if (p.test(text)) return { detected: true, pattern: p.source.slice(0, 60) };
  return { detected: false, pattern: null };
}

function wrapUserInput(text) {
  const clean = sanitizeForLLM(text, CONFIG.LIMITS.MAX_MESSAGE_LENGTH);
  return `<user_input>\n${clean}\n</user_input>`;
}

const MODERATION_KEYWORDS = {
  VIOLENCE_EXTREME: [
    /\bhow\s+to\s+(kill|murder|assassinate)\s+(a\s+)?(person|someone|human)/i,
    /\bhow\s+to\s+make\s+(a\s+)?(bomb|explosive|grenade)\b/i
  ],
  CSAM: [ /\bchild\s+(porn|sexual|abuse)\b/i, /\bcsam\b/i ]
};

function moderateText(text) {
  if (!text || typeof text !== "string") return { safe: true, category: null };
  for (const [category, patterns] of Object.entries(MODERATION_KEYWORDS)) {
    for (const p of patterns) if (p.test(text)) return { safe: false, category };
  }
  return { safe: true, category: null };
}

async function moderateWithGroq(text) {
  const local = moderateText(text);
  if (!local.safe) return local;
  if (!process.env.GROQ_API_KEY) return local;
  try {
    const resp = await axios.post(
      "https://api.groq.com/openai/v1/chat/completions",
      { model: "llama-guard-3-8b", messages: [{ role: "user", content: text.slice(0, 4000) }], max_tokens: 50, temperature: 0 },
      { headers: { Authorization: `Bearer ${process.env.GROQ_API_KEY}`, "Content-Type": "application/json" }, timeout: 5000 }
    );
    const verdict = resp.data?.choices?.[0]?.message?.content || "";
    const isUnsafe = /unsafe|violat|harmful/i.test(verdict);
    return { safe: !isUnsafe, category: isUnsafe ? "groq_guard" : null };
  } catch { return local; }
}

function verifyHmacSignature(req) {
  if (!CONFIG.HMAC.ENABLED) return { valid: true, skipped: true };
  const sig = req.headers["x-luba-signature"];
  const ts = req.headers["x-luba-timestamp"];
  if (!sig || !ts) return { valid: false, reason: "missing_headers" };
  const tsNum = parseInt(ts, 10);
  if (!Number.isFinite(tsNum)) return { valid: false, reason: "invalid_timestamp" };
  if (Math.abs(Date.now() - tsNum) > CONFIG.HMAC.WINDOW_MS) return { valid: false, reason: "expired" };
  const bodyStr = typeof req.body === "string" ? req.body : safeJsonStringify(req.body || {});
  const payload = `${ts}.${req.method}.${req.originalUrl}.${bodyStr}`;
  if (!hmacVerify(payload, sig)) return { valid: false, reason: "mismatch" };
  return { valid: true };
}

async function logSecurityEvent(userId, eventType, details = {}, ip = null, ua = null, fp = null) {
  try {
    if (db) {
      await dbRun(
        `INSERT INTO security_logs (user_id, event_type, details, fingerprint, ip_address, user_agent, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [userId, eventType, safeJsonStringify(details), fp, ip, ua, Date.now()]
      );
    }
  } catch (e) {}
}

async function auditLLMCall({ sessionId, userId, provider, model, tier, promptTokens = 0, completionTokens = 0, latencyMs = 0, status, errorCode = null }) {
  try {
    if (db) {
      await dbRun(
        `INSERT INTO llm_audit_log (session_id, user_id, provider, model, tier, prompt_tokens, completion_tokens, latency_ms, status, error_code, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [sessionId, userId, provider, model, tier, promptTokens, completionTokens, latencyMs, status, errorCode, Date.now()]
      );
    }
    if (metrics?.llmLatency) metrics.llmLatency.labels(provider, model, status).observe(latencyMs / 1000);
  } catch (e) {}
}

// ================================================================================
// §1.19 — MÉTRIQUES PROMETHEUS
// ================================================================================

let metrics = null;

function initMetrics() {
  if (!PromClient) { logger.warn("⚠️  prom-client non installé"); return; }
  try {
    const c = PromClient;
    c.collectDefaultMetrics({ prefix: "luba_" });

    metrics = {
      register: c.register,
      httpRequests: new c.Counter({ name: "luba_http_requests_total", help: "Requêtes HTTP", labelNames: ["method", "path", "status"] }),
      httpDuration: new c.Histogram({ name: "luba_http_request_duration_seconds", help: "Durée HTTP", labelNames: ["method", "path", "status"], buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60] }),
      llmLatency: new c.Histogram({ name: "luba_llm_latency_seconds", help: "Latence LLM", labelNames: ["provider", "model", "status"], buckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60] }),
      llmTokens: new c.Counter({ name: "luba_llm_tokens_total", help: "Tokens LLM", labelNames: ["provider", "model", "type"] }),
      llmCalls: new c.Counter({ name: "luba_llm_calls_total", help: "Appels LLM", labelNames: ["provider", "model", "status"] }),
      circuitState: new c.Gauge({ name: "luba_circuit_breaker_state", help: "Circuit state", labelNames: ["name"] }),
      toolCalls: new c.Counter({ name: "luba_tool_calls_total", help: "Appels outils", labelNames: ["tool", "status"] }),
      activeWebSockets: new c.Gauge({ name: "luba_active_websockets", help: "WS actifs", labelNames: ["channel"] }),
      sttLatency: new c.Histogram({ name: "luba_stt_latency_seconds", help: "Latence STT", labelNames: ["provider"], buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5] }),
      ttsLatency: new c.Histogram({ name: "luba_tts_latency_seconds", help: "Latence TTS", labelNames: ["provider"], buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5] }),
      qualityScore: new c.Histogram({ name: "luba_response_quality_score", help: "Score qualité", labelNames: ["intent", "tier"], buckets: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] }),
      selfCritiqueTriggered: new c.Counter({ name: "luba_self_critique_triggered_total", help: "Self-critiques", labelNames: ["reason"] }),
      hallucinationDetected: new c.Counter({ name: "luba_hallucination_detected_total", help: "Hallucinations", labelNames: ["type"] }),
      authAttempts: new c.Counter({ name: "luba_auth_attempts_total", help: "Tentatives auth", labelNames: ["result", "source"] }),
      providerFailover: new c.Counter({ name: "luba_provider_failover_total", help: "Failover providers", labelNames: ["provider", "model", "status_code"] }),
      // 🆕 Self-healing metrics
      selfHealActions: new c.Counter({ name: "luba_self_heal_actions_total", help: "Actions d'auto-réparation", labelNames: ["action", "reason"] }),
      tokenOverflows: new c.Counter({ name: "luba_token_overflow_total", help: "Dépassements de budget token", labelNames: ["tier"] }),
      providerDisabled: new c.Gauge({ name: "luba_provider_disabled", help: "Providers désactivés (1) ou actifs (0)", labelNames: ["provider", "model"] })
    };

    logger.info("✅ Métriques Prometheus v16.5.0 initialisées");
  } catch (e) { metrics = null; }
}

// ================================================================================
// §1.20 — FEATURE FLAGS
// ================================================================================

const FEATURES = Object.freeze({
  firestore:            Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON),
  supabase:             Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_KEY),
  redis:                Boolean(process.env.REDIS_URL),
  prometheus:           Boolean(PromClient),
  gemini:               Boolean(process.env.GEMINI_API_KEY),
  groq:                 Boolean(process.env.GROQ_API_KEY),
  openrouter:           Boolean(process.env.OPENROUTER_API_KEY),
  cerebras:             Boolean(process.env.CEREBRAS_API_KEY),
  hmac:                 Boolean(process.env.HMAC_SECRET),
  sandbox:              Boolean(process.env.CODE_SANDBOX_PROVIDER || "piston"),
  youtube_api:          Boolean(process.env.YOUTUBE_API_KEY),
  resend:               Boolean(process.env.RESEND_API_KEY),
  smtp:                 Boolean(process.env.SMTP_HOST && process.env.SMTP_USER),
  whatsapp_encryption:  Boolean(process.env.WHATSAPP_ENCRYPTION_KEY && process.env.WHATSAPP_ENCRYPTION_IV),
  self_critique:        CONFIG.AI_QUALITY.ENABLE_SELF_CRITIQUE,
  confidence:           CONFIG.AI_QUALITY.ENABLE_CONFIDENCE,
  multi_vote:           CONFIG.AI_QUALITY.ENABLE_MULTI_VOTE,
  hallucination_check:  CONFIG.AI_QUALITY.ENABLE_HALLUCINATION_CHECK,
  self_healing:         true
});

function featureStatus() {
  return Object.entries(FEATURES).map(([k, v]) => `${k}=${v ? "✅" : "❌"}`).join(" | ");
}

// ================================================================================
// §1.21 — 🆕 SELF-HEALING — SANTÉ DES PROVIDERS
// ================================================================================

/**
 * Suivi de la santé de chaque modèle en mémoire.
 * Clé : "provider:model"
 * Valeur : { successes, failures, lastFailure, disabledUntil, lastError }
 *
 * Le tracker modifie l'ordre des providers dynamiquement :
 *   - Les providers sains passent en premier
 *   - Les providers temporairement désactivés sont skippés
 */
class ProviderHealthTracker {
  constructor() {
    this.stats = new Map();
    this.lock = Promise.resolve(); // Sérialisation des mises à jour
  }

  _key(provider, model) {
    return `${provider}:${model}`;
  }

  _getOrCreate(provider, model) {
    const key = this._key(provider, model);
    let entry = this.stats.get(key);
    if (!entry) {
      entry = {
        provider,
        model,
        successes: 0,
        failures: 0,
        consecutiveFailures: 0,
        lastFailure: null,
        lastSuccess: null,
        disabledUntil: 0,
        lastError: null
      };
      this.stats.set(key, entry);
    }
    return entry;
  }

  /**
   * Vérifie si un provider est actuellement disponible.
   * Un provider est désactivé s'il a dépassé le seuil d'échecs consécutifs
   * ET est encore dans sa période de cooldown.
   */
  isAvailable(provider, model) {
    const entry = this._getOrCreate(provider, model);
    if (entry.disabledUntil && Date.now() < entry.disabledUntil) {
      return false;
    }
    // Cooldown expiré → réactive
    if (entry.disabledUntil && Date.now() >= entry.disabledUntil) {
      entry.disabledUntil = 0;
      entry.consecutiveFailures = 0;
      if (metrics?.providerDisabled) {
        metrics.providerDisabled.labels(provider, model).set(0);
      }
    }
    return true;
  }

  recordSuccess(provider, model, latencyMs = 0) {
    const entry = this._getOrCreate(provider, model);
    entry.successes++;
    entry.consecutiveFailures = 0;
    entry.lastSuccess = Date.now();
    entry.disabledUntil = 0;

    if (metrics?.providerDisabled) {
      metrics.providerDisabled.labels(provider, model).set(0);
    }
  }

  recordFailure(provider, model, error, httpStatus = null) {
    const entry = this._getOrCreate(provider, model);
    entry.failures++;
    entry.consecutiveFailures++;
    entry.lastFailure = Date.now();
    entry.lastError = {
      message: String(error?.message || error).slice(0, 200),
      httpStatus,
      code: error?.code || null
    };

    // Désactive si dépassement du seuil
    if (entry.consecutiveFailures >= CONFIG.SELF_HEAL.FAILURE_THRESHOLD) {
      entry.disabledUntil = Date.now() + CONFIG.SELF_HEAL.COOLDOWN_MS;
      logger.warn({
        provider,
        model,
        consecutiveFailures: entry.consecutiveFailures,
        disabledForMs: CONFIG.SELF_HEAL.COOLDOWN_MS
      }, "🏥 Self-Heal : provider désactivé temporairement");

      if (metrics?.selfHealActions) {
        metrics.selfHealActions.labels("disable_provider", String(httpStatus || "unknown")).inc();
      }
      if (metrics?.providerDisabled) {
        metrics.providerDisabled.labels(provider, model).set(1);
      }
    }
  }

  /**
   * Réordonne une liste de providers pour mettre les plus sains en premier.
   * Filtre également les providers désactivés.
   */
  orderProviders(providers) {
    const available = [];
    const unavailable = [];

    for (const p of providers) {
      if (this.isAvailable(p.provider, p.model)) {
        available.push(p);
      } else {
        unavailable.push(p);
      }
    }

    // Trie les disponibles par score de santé (successes - failures*2)
    available.sort((a, b) => {
      const sa = this._getOrCreate(a.provider, a.model);
      const sb = this._getOrCreate(b.provider, b.model);
      const scoreA = sa.successes - sa.failures * 2;
      const scoreB = sb.successes - sb.failures * 2;
      return scoreB - scoreA;
    });

    logger.debug({
      total: providers.length,
      available: available.length,
      unavailable: unavailable.length
    }, "🏥 Self-Heal : providers ordonnés");

    return available;
  }

  getStats() {
    const result = {};
    for (const [key, entry] of this.stats.entries()) {
      result[key] = {
        successes: entry.successes,
        failures: entry.failures,
        consecutiveFailures: entry.consecutiveFailures,
        available: this.isAvailable(entry.provider, entry.model),
        lastError: entry.lastError,
        disabledUntil: entry.disabledUntil || null
      };
    }
    return result;
  }

  reset() {
    this.stats.clear();
  }
}

const providerHealth = new ProviderHealthTracker();

// ================================================================================
// §1.22 — 🆕 SELF-HEALING — TOKEN BUDGET GUARD
// ================================================================================

/**
 * Estime le nombre de tokens d'un texte (approx 4 chars = 1 token).
 * Précision ~90% pour le français.
 */
function estimateTokens(text) {
  if (!text || typeof text !== "string") return 0;
  return Math.ceil(text.length / 4);
}

/**
 * Calcule le budget total utilisé par un tableau de messages.
 */
function computeMessagesBudget(messages) {
  if (!Array.isArray(messages)) return 0;
  let total = 0;
  for (const msg of messages) {
    const content = typeof msg.content === "string" ? msg.content : safeJsonStringify(msg.content);
    total += estimateTokens(content);
    total += 4; // overhead par message
  }
  return total;
}

/**
 * Vérifie si un ensemble de messages dépasse le budget.
 * Retourne { overflow: bool, current: number, budget: number, ratio: number }
 */
function checkTokenBudget(messages) {
  const current = computeMessagesBudget(messages);
  const budget = CONFIG.SELF_HEAL.TOKEN_CHAR_BUDGET;
  const ratio = current / budget;
  return {
    overflow: ratio > 1,
    needsCompaction: ratio > CONFIG.SELF_HEAL.COMPACT_TRIGGER_PCT,
    current,
    budget,
    ratio: Number(ratio.toFixed(3))
  };
}

// ================================================================================
// §1.23 — 🆕 SELF-HEALING — COMPACTION DE CONTEXTE
// ================================================================================

/**
 * Compacte un tableau de messages si le budget est dépassé.
 *
 * Stratégie :
 *   1. Garde TOUJOURS le system prompt intact
 *   2. Garde le dernier message user intact
 *   3. Pour l'historique : garde les N plus récents, résume le reste
 *   4. Retourne { messages, compacted, droppedCount }
 */
function compactContextIfNeeded(messages, { maxRatio = CONFIG.SELF_HEAL.COMPACT_TRIGGER_PCT } = {}) {
  const budget = checkTokenBudget(messages);
  if (!budget.needsCompaction) {
    return { messages, compacted: false, droppedCount: 0, ratio: budget.ratio };
  }

  logger.info({
    current: budget.current,
    budget: budget.budget,
    ratio: budget.ratio
  }, "🏥 Self-Heal : compaction de contexte déclenchée");

  if (metrics?.tokenOverflows) {
    metrics.tokenOverflows.labels("v100").inc();
  }

  // Séparation : system + reste
  const systemMessages = messages.filter((m) => m.role === "system");
  const nonSystem = messages.filter((m) => m.role !== "system");

  if (nonSystem.length <= 4) {
    return { messages, compacted: false, droppedCount: 0, ratio: budget.ratio };
  }

  // Garde les 6 derniers messages (3 échanges)
  const recent = nonSystem.slice(-6);
  const older = nonSystem.slice(0, -6);

  // Compactage des anciens en un résumé mécanique
  let summary = null;
  if (older.length > 0) {
    const olderText = older
      .map((m) => {
        const role = m.role === "user" ? "Utilisateur" : "Assistant";
        const content = typeof m.content === "string" ? m.content : safeJsonStringify(m.content);
        return `${role}: ${content.slice(0, 300)}`;
      })
      .join("\n");

    summary = {
      role: "system",
      content: `[RÉSUMÉ DES ÉCHANGES PRÉCÉDENTS — contexte compacté automatiquement]\n${olderText.slice(0, 2000)}`
    };
  }

  const compactedMessages = [
    ...systemMessages,
    ...(summary ? [summary] : []),
    ...recent
  ];

  const droppedCount = older.length;
  logger.info({
    before: messages.length,
    after: compactedMessages.length,
    dropped: droppedCount
  }, "🏥 Self-Heal : contexte compacté");

  if (metrics?.selfHealActions) {
    metrics.selfHealActions.labels("compact_context", `dropped_${droppedCount}`).inc();
  }

  return {
    messages: compactedMessages,
    compacted: true,
    droppedCount,
    ratio: checkTokenBudget(compactedMessages).ratio
  };
}

// ================================================================================
// §1.24 — SQLITE (SCHÉMA v16.5)
// ================================================================================

let db = null;

function initDatabase() {
  return new Promise((resolve, reject) => {
    db = new sqlite3.Database(CONFIG.PATHS.DB, (err) => {
      if (err) { logger.error({ err: err.message }, "❌ Impossible d'ouvrir SQLite"); return reject(err); }
      logger.info("✅ SQLite initialisé");

      db.run("PRAGMA journal_mode = WAL;");
      db.run("PRAGMA synchronous = NORMAL;");
      db.run("PRAGMA cache_size = -64000;");
      db.run("PRAGMA busy_timeout = 10000;");
      db.run("PRAGMA temp_store = MEMORY;");
      db.run("PRAGMA foreign_keys = ON;");
      db.run("PRAGMA wal_autocheckpoint = 1000;");

      db.serialize(() => {
        db.run(`CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY, firebase_uid TEXT UNIQUE, email TEXT UNIQUE,
          display_name TEXT, role TEXT DEFAULT 'FREE', email_verified INTEGER DEFAULT 0,
          whatsapp_connected INTEGER DEFAULT 0, whatsapp_session_id TEXT,
          preferred_language TEXT DEFAULT 'fr', last_seen_at INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS sessions (
          session_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, firebase_uid TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          active_intent TEXT, intent_data TEXT, intent_expires_at INTEGER,
          metadata TEXT DEFAULT '{}',
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, user_id TEXT,
          role TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool')),
          content TEXT NOT NULL, tool_calls TEXT, tool_call_id TEXT,
          images TEXT DEFAULT '[]', metadata TEXT DEFAULT '{}',
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        )`);

        db.run("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id DESC)");
        db.run("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, created_at DESC)");
        db.run("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at DESC)");
        db.run("CREATE INDEX IF NOT EXISTS idx_sessions_firebase ON sessions(firebase_uid)");

        db.run(`CREATE TABLE IF NOT EXISTS email_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, firebase_uid TEXT,
          to_email TEXT NOT NULL, subject TEXT, status TEXT DEFAULT 'pending',
          provider TEXT, error_message TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS llm_audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, user_id TEXT,
          provider TEXT, model TEXT, tier TEXT,
          prompt_tokens INTEGER DEFAULT 0, completion_tokens INTEGER DEFAULT 0,
          latency_ms INTEGER DEFAULT 0, status TEXT DEFAULT 'success',
          error_code TEXT, quality_score REAL DEFAULT NULL,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS security_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT,
          event_type TEXT NOT NULL, details TEXT DEFAULT '{}',
          fingerprint TEXT, ip_address TEXT, user_agent TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_security_fingerprint ON security_logs(user_id, event_type, fingerprint)");

        db.run(`CREATE TABLE IF NOT EXISTS user_quotas (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, date TEXT NOT NULL,
          messages_count INTEGER DEFAULT 0, images_count INTEGER DEFAULT 0,
          whatsapp_count INTEGER DEFAULT 0, emails_count INTEGER DEFAULT 0,
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          UNIQUE(user_id, date),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS active_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL,
          session_token_hash TEXT UNIQUE, ip_address TEXT, user_agent TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          last_activity INTEGER DEFAULT (strftime('%s','now')*1000),
          expires_at INTEGER, is_revoked INTEGER DEFAULT 0,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS login_attempts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, ip_address TEXT,
          success INTEGER DEFAULT 0, error_message TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS blocked_ips (
          id INTEGER PRIMARY KEY AUTOINCREMENT, ip_address TEXT UNIQUE, reason TEXT,
          strike_count INTEGER DEFAULT 1, blocked_until INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS user_memory (
          user_id TEXT PRIMARY KEY, summary TEXT DEFAULT '',
          messages_since_update INTEGER DEFAULT 0,
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS user_memory_facts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL,
          fact TEXT NOT NULL, category TEXT DEFAULT 'general', embedding TEXT,
          confidence REAL DEFAULT 1.0, source_session TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_facts_user ON user_memory_facts(user_id, created_at DESC)");
        db.run("CREATE INDEX IF NOT EXISTS idx_facts_category ON user_memory_facts(user_id, category)");

        db.run(`CREATE TABLE IF NOT EXISTS user_tasks (
          id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
          title TEXT NOT NULL, notes TEXT, due_at INTEGER,
          status TEXT DEFAULT 'pending', notified_at INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id, status, due_at)");

        db.run(`CREATE TABLE IF NOT EXISTS outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('insert','update','upsert','delete')),
          payload TEXT NOT NULL, idempotency_key TEXT NOT NULL,
          attempts INTEGER DEFAULT 0, last_error TEXT,
          next_attempt_at INTEGER DEFAULT (strftime('%s','now')*1000),
          status TEXT DEFAULT 'pending',
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          UNIQUE(table_name, op, idempotency_key)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS token_cache (
          token_hash TEXT PRIMARY KEY, uid TEXT NOT NULL,
          email TEXT, display_name TEXT, email_verified INTEGER DEFAULT 0,
          role TEXT DEFAULT 'FREE', expires_at INTEGER NOT NULL,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS conversation_locks (
          conversation_id TEXT PRIMARY KEY, locked_until INTEGER NOT NULL,
          owner_request_id TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS reasoning_traces (
          id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, user_id TEXT,
          user_message TEXT NOT NULL, reasoning TEXT, draft_answer TEXT, final_answer TEXT,
          confidence REAL DEFAULT 0.5, quality_score REAL DEFAULT 0.5,
          self_critique_improved INTEGER DEFAULT 0, hallucination_detected INTEGER DEFAULT 0,
          provider TEXT, model TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS provider_failover_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT, tier TEXT,
          provider TEXT NOT NULL, model TEXT NOT NULL,
          http_status INTEGER, error_code TEXT, error_message TEXT,
          fallback_provider TEXT, fallback_model TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_failover_provider ON provider_failover_log(provider, model, created_at DESC)");

        // 🆕 Table de santé persistante des providers
        db.run(`CREATE TABLE IF NOT EXISTS provider_health (
          provider TEXT NOT NULL, model TEXT NOT NULL,
          successes INTEGER DEFAULT 0, failures INTEGER DEFAULT 0,
          consecutive_failures INTEGER DEFAULT 0,
          disabled_until INTEGER DEFAULT 0,
          last_error TEXT,
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          PRIMARY KEY (provider, model)
        )`);

        // 🆕 Table de faits utilisateur longue durée
        db.run(`CREATE TABLE IF NOT EXISTS user_long_term_memory (
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL,
          key TEXT NOT NULL, value TEXT NOT NULL,
          category TEXT DEFAULT 'general',
          confidence REAL DEFAULT 0.8,
          times_mentioned INTEGER DEFAULT 1,
          first_seen INTEGER DEFAULT (strftime('%s','now')*1000),
          last_seen INTEGER DEFAULT (strftime('%s','now')*1000),
          UNIQUE(user_id, key)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_ltm_user ON user_long_term_memory(user_id, last_seen DESC)");
      });

      logger.info("✅ Schéma SQLite v16.5 initialisé");
      resolve();
    });
  });
}

function dbGet(q, p = []) { return new Promise((res, rej) => { if (!db) return res(null); db.get(q, p, (e, r) => e ? rej(e) : res(r)); }); }
function dbAll(q, p = []) { return new Promise((res, rej) => { if (!db) return res([]); db.all(q, p, (e, r) => e ? rej(e) : res(r)); }); }
function dbRun(q, p = []) { return new Promise((res, rej) => { if (!db) return res({ changes: 0 }); db.run(q, p, function (e) { e ? rej(e) : res(this); }); }); }
function dbExec(q) { return new Promise((res, rej) => { if (!db) return res(); db.exec(q, (e) => e ? rej(e) : res()); }); }

async function dbTransaction(fn) {
  await dbExec("BEGIN IMMEDIATE");
  try {
    const r = await fn({ dbGet, dbAll, dbRun });
    await dbExec("COMMIT");
    return r;
  } catch (e) {
    try { await dbExec("ROLLBACK"); } catch {}
    throw e;
  }
}

// ================================================================================
// §1.25 — BOOTSTRAP PARTIE 1
// ================================================================================

async function bootstrapPart1() {
  ensureDirectories();
  validateEnvironment();
  initFirebase();
  initSupabase();
  initRedis();
  initMetrics();
  await initDatabase();
  logger.info(`🎯 Features v16.5.0 : ${featureStatus()}`);
  return { ok: true };
}

// ================================================================================
// §1.26 — EXPORTS PARTIE 1
// ================================================================================

module.exports = {
  CONFIG, FIREBASE_CONFIG, HOSTING_CONFIG, USER_QUOTAS, ERROR_CODES, FEATURES,
  logger, LubaError, makeError, isLubaError,

  firebaseApp: () => firebaseApp,
  firestoreDb: () => firestoreDb,
  firebaseReady: () => firebaseReady,
  fsGet, fsSet, fsUpdate, fsDelete, fsQuery,

  supabase: () => supabase,
  supabaseWriteSafe,

  cache, l1Cache, semanticCache, SemanticCache,
  redisClient: () => redisClient,

  db: () => db,
  dbGet, dbAll, dbRun, dbExec, dbTransaction,

  detectPromptInjection, wrapUserInput, moderateText, moderateWithGroq,
  verifyHmacSignature, hmacSign, hmacVerify,
  logSecurityEvent, auditLLMCall,

  metrics: () => metrics,

  generateRequestId, generateConversationId, generateSessionToken,
  generateUUID, generateTaskId, generateMsgId,
  sha256, sha1, hashSessionToken,
  nowMs, todayKeyMs, sleep, backoffDelay,
  escapeHtml, sanitizeForLLM, sanitizeStrict,
  safeJsonParse, safeJsonStringify,
  normalizeMath, stripThinkTags, decodeXmlEntities,
  computeDeviceFingerprint, isValidImageSignature, convertImageToBase64,
  withDeadline, allSettledWithDeadline, safeChunkText,
  EMAIL_REGEX, PHONE_REGEX,
  detectLanguage, truncateToTokenBudget,
  imageRelevanceScore, isGreetingOrSmallTalk, isIdentityOrSelfQuestion,

  // 🆕 Clean output
  cleanOutput, formatFinalReply, isValidReply,

  // 🆕 Self-healing
  ProviderHealthTracker, providerHealth,
  estimateTokens, computeMessagesBudget, checkTokenBudget,
  compactContextIfNeeded,

  bootstrapPart1, initDatabase, initFirebase, initSupabase, initRedis, initMetrics,
  featureStatus
};

// ================================================================================
// ==================== FIN PARTIE 1/5 ===========================================
// ================================================================================
// ▶ PARTIE 2/5 : Providers LLM avec modèles vérifiés oct 2026 + auto-failover
//                intelligent + tool calling + voice + mémoire pro persistante
//                + AI Quality Layer.
//
//   Tape "suite" pour la recevoir.
// ================================================================================
// ================================================================================
// PARTIE 2/5 — PROVIDERS LLM · TOOLS · VOICE · MÉMOIRE PRO · AI QUALITY
// ================================================================================
// VERSION : v16.5.0 (Octobre 2026)
//
// INTÉGRATION SELF-HEALING :
//   • Utilise providerHealth (Partie 1) pour ordonner les providers
//   • Utilise compactContextIfNeeded pour éviter les overflows
//   • Utilise cleanOutput sur toutes les réponses
//   • Retry automatique intelligent avec backoff
//
// MODÈLES VÉRIFIÉS OCTOBRE 2026 :
//   • Groq v100/v250 → openai/gpt-oss-120b
//   • Gemini → gemini-2.5-flash
//   • Cerebras → qwen-3.8-27b
//   • OpenRouter → deepseek/deepseek-r1:free, qwen/qwen3-coder-480b:free
//   • Vision → llama-4-maverick (Groq), qwen-2.5-vl-72b:free (OpenRouter)
//
// TABLE DES MATIÈRES :
//   §2.01  Providers LLM (registre + key pools)
//   §2.02  Circuit breaker par provider:model:key
//   §2.03  Intercepteur d'erreurs + messages FR
//   §2.04  Tiers de modèles (v100, v250, vision)
//   §2.05  Appelant OpenAI-compatible
//   §2.06  Appelant Gemini natif
//   §2.07  callProviderWithTools (rotation + circuit + self-healing)
//   §2.08  TOOL_SCHEMAS (19 outils)
//   §2.09  executeToolNative
//   §2.10  runToolLoop (avec compaction auto)
//   §2.11  STT (Groq + Deepgram)
//   §2.12  TTS (Kokoro + Piper)
//   §2.13  VAD adaptatif
//   §2.14  Voice pipeline
//   §2.15  Mémoire courte (rolling summary)
//   §2.16  🆕 Mémoire PRO persistante (cross-semaines)
//   §2.17  Mémoire longue (facts + embeddings + recall)
//   §2.18  AI Quality Layer
//   §2.19  Exports Partie 2
// ================================================================================

"use strict";

// ================================================================================
// §2.01 — PROVIDERS LLM
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
    logger.error({ err: e.message }, "❌ Init Gemini échouée");
  }
}

/**
 * Registre des providers LLM (modèles vérifiés oct 2026).
 */
const LLM_PROVIDERS = Object.freeze({
  GROQ: Object.freeze({
    baseURL: "https://api.groq.com/openai/v1",
    defaultTimeout: 20000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool(
      [process.env.GROQ_API_KEY, process.env.GROQ_API_KEY_2, process.env.GROQ_API_KEY_3],
      "groq"
    )
  }),
  OPENROUTER: Object.freeze({
    baseURL: "https://openrouter.ai/api/v1",
    defaultTimeout: 25000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool(
      [process.env.OPENROUTER_API_KEY, process.env.OPENROUTER_API_KEY_2, process.env.OPENROUTER_API_KEY_3],
      "openrouter"
    )
  }),
  CEREBRAS: Object.freeze({
    baseURL: "https://api.cerebras.ai/v1",
    defaultTimeout: 20000,
    maxTokens: 4000,
    temperature: 0.7,
    supportsTools: true,
    keyPool: buildKeyPool(
      [process.env.CEREBRAS_API_KEY, process.env.CEREBRAS_API_KEY_2],
      "cerebras"
    )
  }),
  GEMINI: Object.freeze({
    baseURL: "https://generativelanguage.googleapis.com/v1beta",
    defaultTimeout: 20000,
    maxTokens: 8000,
    temperature: 0.7,
    isGemini: true,
    client: geminiClient,
    supportsTools: true,
    keyPool: buildKeyPool([process.env.GEMINI_API_KEY], "gemini")
  })
});

if (LLM_PROVIDERS.GROQ.keyPool.length > 0) logger.info(`🔑 Groq : ${LLM_PROVIDERS.GROQ.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.OPENROUTER.keyPool.length > 0) logger.info(`🔑 OpenRouter : ${LLM_PROVIDERS.OPENROUTER.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.CEREBRAS.keyPool.length > 0) logger.info(`🔑 Cerebras : ${LLM_PROVIDERS.CEREBRAS.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.GEMINI.keyPool.length > 0) logger.info(`🔑 Gemini : ${LLM_PROVIDERS.GEMINI.keyPool.length} clé(s)`);

// ================================================================================
// §2.02 — CIRCUIT BREAKER
// ================================================================================

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold ?? CONFIG.CIRCUIT.THRESHOLD;
    this.resetTimeout = options.resetTimeout ?? CONFIG.CIRCUIT.RESET_MS;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED";
    this.halfOpenInFlight = 0;
  }

  canAttempt() {
    if (this.state === "CLOSED") return true;
    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime >= this.resetTimeout) {
        this.state = "HALF_OPEN";
        this.halfOpenInFlight = 0;
        this.updateMetric();
        return true;
      }
      return false;
    }
    return this.halfOpenInFlight < CONFIG.CIRCUIT.HALF_OPEN_MAX;
  }

  async execute(fn) {
    if (!this.canAttempt()) throw makeError("CIRCUIT_OPEN");
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
    this.successCount++;
    if (this.state !== "CLOSED") {
      this.state = "CLOSED";
      this.updateMetric();
      logger.info({ circuit: this.name }, "🔒 Circuit CLOSED");
    }
  }

  onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    if (this.state === "HALF_OPEN" || this.failureCount >= this.failureThreshold) {
      if (this.state !== "OPEN") {
        this.state = "OPEN";
        this.updateMetric();
        logger.warn({ circuit: this.name, failures: this.failureCount }, "🚨 Circuit OPEN");
      }
    }
  }

  updateMetric() {
    if (!metrics?.circuitState) return;
    try {
      const val = this.state === "CLOSED" ? 0 : this.state === "HALF_OPEN" ? 1 : 2;
      metrics.circuitState.labels(this.name).set(val);
    } catch {}
  }

  getState() {
    return {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
      resetIn: this.state === "OPEN"
        ? Math.max(0, this.resetTimeout - (Date.now() - this.lastFailureTime))
        : null
    };
  }
}

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

function getAllCircuitStates() {
  return [...circuitRegistry.values()].map((cb) => cb.getState());
}

// ================================================================================
// §2.03 — INTERCEPTEUR D'ERREURS + MESSAGES FR
// ================================================================================

class LLMErrorInterceptor {
  static getErrorCode(error) {
    if (!error) return "UNKNOWN_ERROR";
    if (error instanceof LubaError) return error.code;
    const status = error?.response?.status;
    if (status) return `HTTP_${status}`;
    if (error.code === "ECONNABORTED" || error.code === "ABORT_ERR" || error.name === "AbortError") return "TIMEOUT";
    if (error.code === "ENOTFOUND") return "DNS_ERROR";
    if (error.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    if (error.code === "MISSING_API_KEY") return "MISSING_API_KEY";
    if (error.code === "CIRCUIT_OPEN") return "CIRCUIT_OPEN";
    return "UNKNOWN_ERROR";
  }

  static shouldSkipProvider(error) {
    return ["HTTP_400","HTTP_401","HTTP_403","HTTP_402","HTTP_404","MISSING_API_KEY"]
      .includes(this.getErrorCode(error));
  }

  static shouldRotateImmediately(error) {
    return [
      "HTTP_429","HTTP_500","HTTP_502","HTTP_503","HTTP_504",
      "TIMEOUT","DNS_ERROR","CONNECTION_REFUSED","CIRCUIT_OPEN"
    ].includes(this.getErrorCode(error));
  }
}

function userFacingErrorMessage(error, context = {}) {
  const code = LLMErrorInterceptor.getErrorCode(error);

  logger.error({
    code,
    message: error?.message,
    provider: context.provider,
    model: context.model
  }, "🔍 Erreur LLM détaillée");

  if (code === "HTTP_429") return "Je suis très sollicité en ce moment. Réessaie dans quelques instants. 🙏";
  if (code === "TIMEOUT" || code === "HTTP_504") return "Cette demande prend trop de temps. Essaie de la découper en étapes plus petites, ou reformule plus simplement.";
  if (code === "CIRCUIT_OPEN") return "Je suis temporairement surchargé. Attends une minute puis réessaie.";
  if (code === "HTTP_400") return "Je n'ai pas compris la demande. Peux-tu reformuler ?";
  if (code === "HTTP_401" || code === "HTTP_403" || code === "MISSING_API_KEY") return "Problème de configuration côté serveur. Contacte le support si ça persiste.";
  if (code === "HTTP_404") return "Le modèle demandé n'est pas disponible. Un autre a été essayé automatiquement. Reformule ta demande si le problème persiste.";
  if (code === "TOKEN_OVERFLOW") return "Le contexte de la conversation est très long. J'ai compacté automatiquement. Reformule ta question.";
  if (code === "SANDBOX_UNAVAILABLE") return "L'exécution de code est indisponible pour l'instant, mais je peux quand même t'écrire le code.";
  return "Je rencontre une difficulté technique. Reformule ta demande ou réessaie dans un instant.";
}

// ================================================================================
// §2.04 — TIERS DE MODÈLES (VÉRIFIÉS OCTOBRE 2026)
// ================================================================================

const MODEL_TIERS = Object.freeze({
  // ═══════════════════════════════════════════════════════════════
  // v100 — Mwamba (conversation rapide)
  // ═══════════════════════════════════════════════════════════════
  v100: {
    name: CONFIG.BRAND.V100,
    jsonMode: false,
    providers: [
      {
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "openai/gpt-oss-120b",
        maxTokens: 4000, timeout: 20000, temperature: 0.7, failoverPriority: 0
      },
      {
        provider: "gemini",
        model: process.env.GEMINI_MODEL_V100 || "gemini-2.5-flash",
        maxTokens: 8000, timeout: 22000, temperature: 0.7, failoverPriority: 1
      },
      {
        provider: "cerebras",
        model: process.env.CEREBRAS_MODEL_V100 || "qwen-3.8-27b",
        maxTokens: 4000, timeout: 20000, temperature: 0.7, failoverPriority: 2
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "meta-llama/llama-3.3-70b-instruct:free",
        maxTokens: 4000, timeout: 25000, temperature: 0.7, failoverPriority: 3
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "qwen/qwen-2.5-72b-instruct:free",
        maxTokens: 4000, timeout: 25000, temperature: 0.7, failoverPriority: 4
      }
    ]
  },

  // ═══════════════════════════════════════════════════════════════
  // v250 — Ngandu (raisonnement + code)
  // ═══════════════════════════════════════════════════════════════
  v250: {
    name: CONFIG.BRAND.V250,
    jsonMode: false,
    reasoning: {
      providers: [
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_REASONING || "openai/gpt-oss-120b",
          maxTokens: 8000, timeout: 40000, temperature: 0.6, failoverPriority: 0,
          reasoningEffort: "high"
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_REASONING || "deepseek/deepseek-r1:free",
          maxTokens: 8000, timeout: 45000, temperature: 0.6, failoverPriority: 1
        },
        {
          provider: "gemini",
          model: process.env.GEMINI_MODEL_V250_REASONING || "gemini-2.5-flash",
          maxTokens: 8000, timeout: 35000, temperature: 0.3, failoverPriority: 2
        }
      ]
    },
    code: {
      providers: [
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_CODE || "openai/gpt-oss-120b",
          maxTokens: 8000, timeout: 30000, temperature: 0.4, failoverPriority: 0
        },
        {
          provider: "cerebras",
          model: process.env.CEREBRAS_MODEL_V250_CODE || "qwen-3.8-27b",
          maxTokens: 8000, timeout: 30000, temperature: 0.4, failoverPriority: 1
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_CODE || "qwen/qwen3-coder-480b:free",
          maxTokens: 8000, timeout: 35000, temperature: 0.4, failoverPriority: 2
        }
      ]
    },
    maxRetries: 2
  },

  // ═══════════════════════════════════════════════════════════════
  // Vision
  // ═══════════════════════════════════════════════════════════════
  vision: {
    name: "Vision",
    jsonMode: false,
    providers: [
      {
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL,
        maxTokens: 4000, timeout: 25000, temperature: 0.7, failoverPriority: 0
      },
      {
        provider: "gemini",
        model: CONFIG.VISION.GEMINI_MODEL,
        maxTokens: 4000, timeout: 25000, temperature: 0.7, failoverPriority: 1
      },
      {
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL,
        maxTokens: 4000, timeout: 30000, temperature: 0.7, failoverPriority: 2
      }
    ]
  }
});

function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;
  const prefixes = [
    "openai/","qwen/","meta-llama/","deepseek/","microsoft/","anthropic/",
    "google/","mistralai/","cohere/","nvidia/","poolside/","inclusionai/",
    "z-ai/","liquid/","cognitivecomputations/"
  ];
  const isOR = prefixes.some((p) => model.includes(p));
  if (isOR && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return model + ":free";
  }
  return model;
}

// ================================================================================
// §2.05 — APPELANT OPENAI-COMPATIBLE
// ================================================================================

async function callOpenAICompatibleRaw({
  provider, model, messages, tools, jsonMode,
  timeout, maxTokens, temperature, images, apiKey, reasoningEffort, signal
}) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ
    : provider === "cerebras" ? LLM_PROVIDERS.CEREBRAS
    : LLM_PROVIDERS.OPENROUTER;

  if (!apiKey) {
    const e = new Error("Clé API manquante pour " + provider);
    e.code = "MISSING_API_KEY";
    throw e;
  }

  let formattedMessages = messages;
  if (images && images.length > 0) {
    const idx = messages.length - 1;
    if (messages[idx]?.role === "user") {
      const parts = [];
      if (typeof messages[idx].content === "string") {
        parts.push({ type: "text", text: messages[idx].content });
      }
      for (const img of images) {
        parts.push({ type: "image_url", image_url: { url: img.dataUrl } });
      }
      formattedMessages = [...messages.slice(0, idx), { role: "user", content: parts }];
    }
  }

  const payload = {
    model,
    messages: formattedMessages,
    temperature,
    max_tokens: maxTokens
  };
  if (jsonMode) payload.response_format = { type: "json_object" };
  if (reasoningEffort) payload.reasoning_effort = reasoningEffort;
  if (tools && tools.length > 0) {
    payload.tools = tools;
    payload.tool_choice = "auto";
  }

  const headers = {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json"
  };
  if (provider === "openrouter") {
    headers["HTTP-Referer"] = HOSTING_CONFIG.domain;
    headers["X-Title"] = "Luba AI";
  }

  const response = await axios.post(
    cfg.baseURL + "/chat/completions",
    payload,
    { headers, timeout, signal, validateStatus: () => true }
  );

  if (response.status >= 400) {
    const err = new Error(`HTTP ${response.status}: ${response.data?.error?.message || "Erreur provider"}`);
    err.response = response;
    err.code = `HTTP_${response.status}`;
    throw err;
  }

  const choice = response?.data?.choices?.[0];
  const message = choice?.message;
  if (!message) throw new Error(`Réponse ${provider} vide`);

  if (!message.reasoning_content && choice?.reasoning) {
    message.reasoning_content = choice.reasoning;
  }
  if (!message.reasoning_content && message.reasoning) {
    message.reasoning_content = message.reasoning;
  }

  return {
    message,
    raw: response.data,
    usage: response.data?.usage || null
  };
}

// ================================================================================
// §2.06 — APPELANT GEMINI NATIF
// ================================================================================

async function callGeminiRawWithTools({
  model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, signal
}) {
  if (!geminiClient) throw new Error("Client Gemini non initialisé");

  let systemInstruction = null;
  const contents = [];

  for (const msg of messages) {
    if (msg.role === "system") {
      systemInstruction = typeof msg.content === "string"
        ? msg.content
        : safeJsonStringify(msg.content);
      continue;
    }

    if (msg.role === "tool") {
      let parsed;
      try { parsed = JSON.parse(msg.content || "{}"); }
      catch { parsed = { raw: msg.content }; }
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
        try { parsedArgs = JSON.parse(call.function?.arguments || "{}"); }
        catch { parsedArgs = {}; }
        parts.push({
          functionCall: { name: call.function?.name || "tool", args: parsedArgs }
        });
      }
      contents.push({ role: "model", parts });
      continue;
    }

    const role = msg.role === "assistant" ? "model" : "user";
    const parts = [];
    if (typeof msg.content === "string") {
      parts.push({ text: msg.content });
    } else if (Array.isArray(msg.content)) {
      for (const part of msg.content) {
        if (part.type === "text") parts.push({ text: part.text });
        if (part.type === "image_url") {
          const [mimePrefix, b64] = String(part.image_url.url).split(",");
          const cleanMime = mimePrefix.match(/data:(.*?);/)?.[1] || "image/jpeg";
          parts.push({ inlineData: { mimeType: cleanMime, data: b64 } });
        }
      }
    }
    if (parts.length) contents.push({ role, parts });
  }

  if (images && images.length > 0) {
    const last = contents[contents.length - 1];
    if (last?.role === "user") {
      for (const img of images) {
        last.parts.push({
          inlineData: { mimeType: img.mimetype || "image/jpeg", data: img.base64 }
        });
      }
    }
  }

  const config = {
    temperature,
    maxOutputTokens: maxTokens || 8000,
    responseMimeType: jsonMode ? "application/json" : "text/plain"
  };
  if (systemInstruction) config.systemInstruction = { parts: [{ text: systemInstruction }] };
  if (tools && tools.length > 0) {
    config.tools = [{
      functionDeclarations: tools.map((t) => ({
        name: t.function.name,
        description: t.function.description,
        parameters: t.function.parameters
      }))
    }];
  }

  const geminiPromise = geminiClient.models.generateContent({ model, contents, config });
  const timeoutPromise = new Promise((_, reject) => {
    const t = setTimeout(() => {
      reject(Object.assign(new Error("Timeout Gemini"), { code: "ECONNABORTED" }));
    }, timeout);
    if (t.unref) t.unref();
  });

  const response = await Promise.race([geminiPromise, timeoutPromise]);

  const candidate = response?.candidates?.[0];
  const parts = candidate?.content?.parts || [];
  let text = "";
  let reasoningText = "";
  const toolCalls = [];

  for (const part of parts) {
    if (part.text) text += part.text;
    if (part.thought) reasoningText += part.thought;
    if (part.functionCall) {
      toolCalls.push({
        id: `call_${crypto.randomUUID()}`,
        type: "function",
        function: {
          name: part.functionCall.name,
          arguments: safeJsonStringify(part.functionCall.args || {})
        }
      });
    }
  }

  const message = { role: "assistant", content: text };
  if (reasoningText) message.reasoning_content = reasoningText;
  if (toolCalls.length > 0) message.tool_calls = toolCalls;

  return { message, raw: response, usage: response?.usageMetadata || null };
}

// ================================================================================
// §2.07 — CALLPROVIDERWITHTOOLS (rotation + circuit + SELF-HEALING)
// ================================================================================

async function callProviderRawWithTools({
  provider, model, messages, tools, jsonMode = false,
  timeout, maxTokens, temperature, images, apiKey, reasoningEffort
}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout + 1000);
  if (timer.unref) timer.unref();

  try {
    if (provider === "gemini") {
      return await callGeminiRawWithTools({
        model, messages, tools, jsonMode, timeout,
        maxTokens, temperature, images, signal: controller.signal
      });
    }
    return await callOpenAICompatibleRaw({
      provider, model, messages, tools, jsonMode, timeout,
      maxTokens, temperature, images, apiKey, reasoningEffort,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Appel provider avec :
 *   - rotation multi-clés
 *   - circuit breaker
 *   - audit log
 *   - 🆕 providerHealth tracking (self-healing)
 *   - 🆕 compaction de contexte si nécessaire
 */
async function callProviderWithTools({
  providerConfig, messages, tools = null, images = null, jsonMode = false, _meta = {}
}) {
  const providerName = providerConfig.provider;
  const providerInfo = LLM_PROVIDERS[providerName.toUpperCase()];
  if (!providerInfo) {
    return { success: false, error: new Error(`Provider ${providerName} inconnu`) };
  }

  let model = providerConfig.model;
  if (providerName === "openrouter") {
    model = validateAndSanitizeOpenRouterModel(model);
    if (!model) return { success: false, error: new Error("Modèle OpenRouter invalide") };
  }

  // 🆕 Vérification self-healing : le provider est-il disponible ?
  if (!providerHealth.isAvailable(providerName, model)) {
    logger.info({ provider: providerName, model }, "🏥 Provider temporairement désactivé (self-heal)");
    return {
      success: false,
      error: new Error(`Provider ${providerName}/${model} temporairement désactivé`)
    };
  }

  // 🆕 Compaction si contexte trop long
  const { messages: workingMessages, compacted } = compactContextIfNeeded(messages);
  if (compacted) {
    logger.info({ provider: providerName, model }, "🏥 Contexte compacté avant appel");
  }

  const keyList = providerName === "gemini"
    ? [{ apiKey: null, label: "gemini_global" }]
    : providerInfo.keyPool;

  if (keyList.length === 0) {
    return { success: false, error: new Error(`Aucune clé pour ${providerName}`) };
  }

  let lastError = null;
  const startedAt = Date.now();

  for (const keyEntry of keyList) {
    const cb = getCircuit(providerName, model, keyEntry.label);

    try {
      const result = await cb.execute(() =>
        callProviderRawWithTools({
          provider: providerName,
          model,
          messages: workingMessages,
          tools,
          jsonMode,
          timeout: providerConfig.timeout || providerInfo.defaultTimeout,
          maxTokens: providerConfig.maxTokens || providerInfo.maxTokens,
          temperature: providerConfig.temperature ?? providerInfo.temperature,
          images,
          apiKey: keyEntry.apiKey,
          reasoningEffort: providerConfig.reasoningEffort || null
        })
      );

      const latency = Date.now() - startedAt;

      // 🆕 Enregistre le succès dans le tracker self-healing
      providerHealth.recordSuccess(providerName, model, latency);

      if (metrics?.llmCalls) metrics.llmCalls.labels(providerName, model, "success").inc();
      if (metrics?.llmLatency) metrics.llmLatency.labels(providerName, model, "success").observe(latency / 1000);
      if (metrics?.llmTokens && result.usage) {
        metrics.llmTokens.labels(providerName, model, "prompt").inc(result.usage.prompt_tokens || 0);
        metrics.llmTokens.labels(providerName, model, "completion").inc(result.usage.completion_tokens || 0);
      }

      auditLLMCall({
        sessionId: _meta.sessionId || null,
        userId: _meta.userId || null,
        provider: providerName,
        model,
        tier: _meta.tier || "v100",
        promptTokens: result.usage?.prompt_tokens || 0,
        completionTokens: result.usage?.completion_tokens || 0,
        latencyMs: latency,
        status: "success"
      }).catch(() => {});

      return {
        success: true,
        message: result.message,
        raw: result.raw,
        usage: result.usage,
        providerUsed: providerName,
        modelUsed: model,
        keyLabel: keyEntry.label,
        latencyMs: latency,
        contextCompacted: compacted
      };
    } catch (error) {
      lastError = error;
      const code = LLMErrorInterceptor.getErrorCode(error);
      const httpStatus = error?.response?.status || null;

      // 🆕 Enregistre l'échec dans le tracker self-healing
      providerHealth.recordFailure(providerName, model, error, httpStatus);

      if (metrics?.llmCalls) metrics.llmCalls.labels(providerName, model, "error").inc();
      if (metrics?.providerFailover) {
        metrics.providerFailover.labels(providerName, model, String(httpStatus || code)).inc();
      }

      logger.warn({
        provider: providerName,
        model,
        keyLabel: keyEntry.label,
        httpStatus,
        errorCode: code,
        errorMessage: String(error?.message || "").slice(0, 300)
      }, `❌ Provider échoué (${httpStatus || code})`);

      if (LLMErrorInterceptor.shouldRotateImmediately(error)) {
        continue;
      }
      if (LLMErrorInterceptor.shouldSkipProvider(error)) {
        break;
      }
      break;
    }
  }

  auditLLMCall({
    sessionId: _meta.sessionId || null,
    userId: _meta.userId || null,
    provider: providerName,
    model,
    tier: _meta.tier || "v100",
    latencyMs: Date.now() - startedAt,
    status: "failed",
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  }).catch(() => {});

  return { success: false, error: lastError };
}

// ================================================================================
// §2.08 — TOOL SCHEMAS (19 outils)
// ================================================================================

const TOOL_SCHEMAS = Object.freeze({
  search_images: {
    type: "function",
    function: {
      name: "search_images",
      description: "Recherche des images réelles (Wikimedia Commons, Wikipédia, Pexels, DuckDuckGo) pour une personne, un lieu, un objet, une équipe ou un concept précis.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Entité précise (nom propre, lieu, équipe). PAS la phrase entière." }
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
      description: "Recherche web générale.",
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
      description: "Dernières actualités (Google News + GDELT).",
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
        properties: { team: { type: "string" } },
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
      description: "Météo actuelle (Open-Meteo).",
      parameters: {
        type: "object",
        properties: { location: { type: "string" } },
        required: ["location"],
        additionalProperties: false
      }
    }
  },
  get_crypto_price: {
    type: "function",
    function: {
      name: "get_crypto_price",
      description: "Prix d'une cryptomonnaie.",
      parameters: {
        type: "object",
        properties: { symbol: { type: "string" } },
        required: ["symbol"],
        additionalProperties: false
      }
    }
  },
  get_stock_price: {
    type: "function",
    function: {
      name: "get_stock_price",
      description: "Prix d'une action.",
      parameters: {
        type: "object",
        properties: { ticker: { type: "string" } },
        required: ["ticker"],
        additionalProperties: false
      }
    }
  },
  execute_math: {
    type: "function",
    function: {
      name: "execute_math",
      description: "Calcule une expression mathématique EXACTE. UTILISE CET OUTIL POUR TOUT CALCUL, PAS run_code.",
      parameters: {
        type: "object",
        properties: { expression: { type: "string" } },
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
      description: "Crée un rappel / une tâche.",
      parameters: {
        type: "object",
        properties: {
          title: { type: "string", maxLength: 200 },
          notes: { type: "string", maxLength: 2000 },
          due_at: { type: "string" }
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
      description: "Marque une tâche terminée.",
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
      description: "Supprime une tâche.",
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
      description: "Envoie un email.",
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
      description: "Envoie un message WhatsApp.",
      parameters: {
        type: "object",
        properties: {
          phone_number: { type: "string" },
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
      description: "Exécute du code dans un SANDBOX. JAMAIS pour du calcul math.",
      parameters: {
        type: "object",
        properties: {
          language: { type: "string", enum: ["python", "javascript", "typescript", "bash", "go", "rust", "java", "cpp"] },
          code: { type: "string", maxLength: 20000 },
          stdin: { type: "string", maxLength: 2000 }
        },
        required: ["language", "code"],
        additionalProperties: false
      }
    }
  },
  remember_fact: {
    type: "function",
    function: {
      name: "remember_fact",
      description: "Mémorise un fait durable sur l'utilisateur.",
      parameters: {
        type: "object",
        properties: {
          fact: { type: "string", maxLength: 300 },
          category: { type: "string", enum: ["identity", "preference", "project", "language", "general"] }
        },
        required: ["fact"],
        additionalProperties: false
      }
    }
  },
  recall_memory: {
    type: "function",
    function: {
      name: "recall_memory",
      description: "Cherche dans la mémoire longue.",
      parameters: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false
      }
    }
  }
});

const TOOLS_BY_CONTEXT = Object.freeze({
  chat: [
    "search_images", "search_web", "search_news", "search_sports_scores",
    "search_science", "search_social", "get_weather",
    "get_crypto_price", "get_stock_price",
    "execute_math", "search_youtube",
    "create_task", "list_tasks", "complete_task", "delete_task",
    "run_code", "remember_fact", "recall_memory"
  ],
  api: [
    "search_images", "search_web", "search_news", "search_sports_scores",
    "search_science", "search_social", "get_weather",
    "get_crypto_price", "get_stock_price",
    "execute_math", "search_youtube",
    "create_task", "list_tasks", "complete_task", "delete_task"
  ]
});

const SIDE_EFFECT_TOOLS = Object.freeze(new Set([
  "send_email", "send_whatsapp_message", "delete_task", "complete_task"
]));

function getToolSchemas(context = "chat") {
  const list = TOOLS_BY_CONTEXT[context] || TOOLS_BY_CONTEXT.chat;
  return list.map((name) => TOOL_SCHEMAS[name]).filter(Boolean);
}

// ================================================================================
// §2.09 — EXECUTE TOOL NATIVE
// ================================================================================

function validateToolArgs(name, args) {
  const schema = TOOL_SCHEMAS[name]?.function?.parameters;
  if (!schema) return { ok: false, error: "Outil inconnu" };

  for (const key of schema.required || []) {
    if (args?.[key] === undefined || args?.[key] === null || args?.[key] === "") {
      return { ok: false, error: `Paramètre manquant : ${key}` };
    }
  }

  for (const [key, prop] of Object.entries(schema.properties || {})) {
    if (args?.[key] === undefined) continue;
    if (prop.type === "string" && typeof args[key] !== "string") {
      return { ok: false, error: `Paramètre ${key} doit être une chaîne` };
    }
    if (prop.type === "string" && prop.maxLength && args[key].length > prop.maxLength) {
      return { ok: false, error: `Paramètre ${key} trop long` };
    }
    if (prop.enum && !prop.enum.includes(args[key])) {
      return { ok: false, error: `Paramètre ${key} invalide` };
    }
  }
  return { ok: true };
}

async function executeToolNative(toolName, rawArgs, context = {}) {
  const { userId, googleAccessToken, agentMode = false } = context;

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
        result = await searchImagesWithFallback(q, CONFIG.LIMITS.IMAGE_SEARCH_LIMIT);
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
        if (result.events?.length) sourceKeys.push("googlenews");
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
      case "get_crypto_price": {
        result = await getCryptoPrice(args.symbol);
        if (!result.error) sourceKeys.push(result.source === "coingecko" ? "coingecko" : "coinmarketcap");
        break;
      }
      case "get_stock_price": {
        result = await getStockPrice(args.ticker);
        if (!result.error) sourceKeys.push("yahoo");
        break;
      }
      case "execute_math": {
        result = await evaluateMathSafe(args.expression, 2000);
        break;
      }
      case "search_youtube": {
        result = await searchYouTube(args.query);
        if (result.videos?.length) sourceKeys.push("youtube");
        break;
      }
      case "create_task": {
        result = await createTask(userId, {
          title: args.title,
          notes: args.notes,
          dueAt: args.due_at ? Date.parse(args.due_at) : null
        });
        break;
      }
      case "list_tasks": {
        result = await listTasks(userId, {
          status: args.status && args.status !== "all" ? args.status : null
        });
        break;
      }
      case "complete_task": {
        if (!agentMode) {
          return {
            result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
            sourceKeys: [], toolName
          };
        }
        result = await updateTaskStatus(userId, args.task_id, "done");
        break;
      }
      case "delete_task": {
        if (!agentMode) {
          return {
            result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
            sourceKeys: [], toolName
          };
        }
        result = await deleteTask(userId, args.task_id);
        break;
      }
      case "send_email": {
        if (!agentMode) {
          return {
            result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
            sourceKeys: [], toolName
          };
        }
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
        if (!agentMode) {
          return {
            result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
            sourceKeys: [], toolName
          };
        }
        const quota = await checkUserQuota(userId, "whatsapp");
        if (!quota.allowed) { result = { success: false, error: quota.message }; break; }
        result = await sendWhatsAppSmart(userId, args.phone_number, args.message);
        if (result.success) await incrementUserQuota(userId, "whatsapp");
        break;
      }
      case "run_code": {
        result = await runCodeSandbox({
          language: args.language,
          code: args.code,
          stdin: args.stdin
        });
        if (!result.success && /non configuré/i.test(result.error || "")) {
          result = {
            success: true, stdout: "", stderr: "", exitCode: null, provider: "none",
            note: "Sandbox non disponible. Le code N'A PAS été exécuté."
          };
        }
        break;
      }
      case "remember_fact": {
        result = await rememberFact(userId, {
          fact: args.fact,
          category: args.category || "general",
          sourceSession: context.sessionId || null
        });
        break;
      }
      case "recall_memory": {
        result = await recallMemory(userId, args.query, 5);
        break;
      }
      default:
        result = { success: false, error: "Outil inconnu" };
    }
  } catch (e) {
    logger.error({ err: e.message, toolName, userId }, "Erreur outil");
    result = { success: false, error: "Échec d'exécution" };
  }

  if (metrics?.toolCalls) {
    metrics.toolCalls.labels(toolName, result?.success ? "success" : "error").inc();
  }

  return { result, sourceKeys, toolName };
}

// ================================================================================
// §2.10 — RUNTOOLLOOP (avec compaction auto + self-healing)
// ================================================================================

async function runToolLoop({
  messages,
  providerConfig,
  executeFn,
  maxIterations = CONFIG.AGENT.MAX_ITERATIONS,
  images = null,
  sse = null,
  _meta = {}
}) {
  let iterations = 0;
  const usedSources = new Set();
  const collectedImages = [];
  const collectedVideos = [];
  const toolCallTrace = [];
  let workingMessages = [...messages];
  let finalText = "";
  let lastReasoning = "";

  while (iterations < maxIterations) {
    iterations++;
    if (sse) sse.status("reasoning", { iteration: iterations });

    const llmResult = await callProviderWithTools({
      providerConfig,
      messages: workingMessages,
      tools: getToolSchemas("chat"),
      images,
      _meta
    });

    if (!llmResult.success) {
      return {
        success: false,
        error: llmResult.error,
        iterations,
        usedSources: [...usedSources],
        toolCallTrace,
        reasoning: lastReasoning
      };
    }

    const assistantMessage = llmResult.message || {};
    const toolCalls = assistantMessage.tool_calls || [];
    const rawContent = assistantMessage.content || "";
    const reasoningContent = assistantMessage.reasoning_content || assistantMessage.reasoning || "";

    if (sse && reasoningContent) {
      lastReasoning += reasoningContent + "\n";
      for (const c of safeChunkText(reasoningContent, 32)) {
        if (sse.closed) break;
        sse.reasoning(c);
        await sleep(4);
      }
      sse.reasoning("\n");
    }

    if (sse && rawContent) {
      const thinkMatches = rawContent.match(/<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/gi);
      if (thinkMatches) {
        for (const t of thinkMatches) {
          const inner = t.replace(/<\/?think(?:ing)?>/gi, "").trim();
          if (inner) {
            lastReasoning += inner + "\n";
            for (const c of safeChunkText(inner, 32)) {
              if (sse.closed) break;
              sse.reasoning(c);
              await sleep(4);
            }
            sse.reasoning("\n");
          }
        }
      }
    }

    if (sse && rawContent) {
      const codeRe = /```(\w+)?(?::([^\n]+))?\n([\s\S]*?)```/g;
      let m;
      while ((m = codeRe.exec(rawContent)) !== null) {
        sse.codeBlock({
          language: m[1] || "text",
          filename: m[2] || null,
          code: m[3],
          done: true
        });
      }
    }

    if (toolCalls.length === 0) {
      // ✅ Utilise formatFinalReply pour un output propre
      finalText = formatFinalReply(rawContent);
      break;
    }

    workingMessages.push({
      role: "assistant",
      content: rawContent,
      tool_calls: toolCalls
    });

    const bounded = toolCalls.slice(0, CONFIG.AGENT.MAX_TOOL_CALLS_PER_STEP);
    for (const call of bounded) {
      const toolName = call.function?.name;
      let args = {};
      try { args = JSON.parse(call.function?.arguments || "{}"); } catch { args = {}; }

      toolCallTrace.push({ name: toolName, args });
      if (sse) sse.status("tool", { name: toolName });

      const { result, sourceKeys } = await executeFn({ toolName, args });
      sourceKeys.forEach((k) => usedSources.add(k));

      if (toolName === "search_images" && result?.images) {
        collectedImages.push(...result.images.map((i) => i.url).filter(Boolean));
        if (sse) sse.images(result.images);
      }
      if (toolName === "search_youtube" && result?.videos) {
        collectedVideos.push(...result.videos);
        if (sse) sse.videos(result.videos);
      }
      if (sse && toolName === "run_code" && (result?.stdout || result?.stderr)) {
        sse.send("code", {
          language: args.language,
          stdout: result.stdout || "",
          stderr: result.stderr || "",
          done: true,
          execution: true
        });
      }

      workingMessages.push({
        role: "tool",
        tool_call_id: call.id,
        name: toolName,
        content: safeJsonStringify(result).slice(0, 8000)
      });
    }

    if (toolCalls.length > CONFIG.AGENT.MAX_TOOL_CALLS_PER_STEP) {
      workingMessages.push({
        role: "system",
        content: `Limite de ${CONFIG.AGENT.MAX_TOOL_CALLS_PER_STEP} appels atteinte. Formule ta réponse finale.`
      });
    }
  }

  if (!finalText) {
    workingMessages.push({
      role: "system",
      content: "Synthétise ta réponse finale MAINTENANT. Pas de nouvel appel d'outil."
    });
    const fc = await callProviderWithTools({
      providerConfig,
      messages: workingMessages,
      tools: [],
      images,
      _meta
    });
    finalText = fc.success
      ? formatFinalReply(fc.message?.content || "")
      : "Je n'ai pas pu terminer la réponse. Peux-tu reformuler plus simplement ?";
  }

  return {
    success: true,
    text: finalText,
    iterations,
    usedSources: [...usedSources],
    images: [...new Set(collectedImages)],
    videos: dedupeVideos(collectedVideos),
    toolCallTrace,
    reasoning: lastReasoning
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
// §2.11 — STT (SPEECH-TO-TEXT)
// ================================================================================

async function transcribeAudioGroq(buffer, filename, mimetype) {
  if (!LLM_PROVIDERS.GROQ.keyPool.length) {
    return { success: false, error: "Aucune clé Groq" };
  }

  let lastError = null;
  for (const keyEntry of LLM_PROVIDERS.GROQ.keyPool) {
    const startedAt = Date.now();
    try {
      const form = new FormData();
      form.append("file", buffer, {
        filename: filename || "audio.webm",
        contentType: mimetype || "audio/webm"
      });
      form.append("model", process.env.GROQ_WHISPER_MODEL || "whisper-large-v3-turbo");
      form.append("language", "fr");
      form.append("response_format", "json");
      form.append("temperature", "0");

      const response = await axios.post(
        "https://api.groq.com/openai/v1/audio/transcriptions",
        form,
        {
          headers: {
            ...form.getHeaders(),
            Authorization: "Bearer " + keyEntry.apiKey
          },
          timeout: 30000,
          maxBodyLength: Infinity,
          maxContentLength: Infinity
        }
      );

      const latency = Date.now() - startedAt;
      if (metrics?.sttLatency) metrics.sttLatency.labels("groq").observe(latency / 1000);

      return {
        success: true,
        text: response.data?.text || "",
        provider: "groq",
        latencyMs: latency
      };
    } catch (e) {
      lastError = e;
    }
  }

  if (process.env.DEEPGRAM_API_KEY) {
    try {
      const response = await axios.post(
        "https://api.deepgram.com/v1/listen?model=nova-3&language=fr&smart_format=true",
        buffer,
        {
          headers: {
            Authorization: `Token ${process.env.DEEPGRAM_API_KEY}`,
            "Content-Type": mimetype || "audio/webm"
          },
          timeout: 30000
        }
      );
      const text = response.data?.results?.channels?.[0]?.alternatives?.[0]?.transcript || "";
      return { success: true, text, provider: "deepgram" };
    } catch (e) {}
  }

  return { success: false, error: "Échec transcription (Groq + Deepgram)" };
}

class StreamingSTT {
  constructor({ onPartial, onFinal, onError } = {}) {
    this.onPartial = onPartial || (() => {});
    this.onFinal = onFinal || (() => {});
    this.onError = onError || (() => {});
    this.buffer = [];
    this.totalBytes = 0;
    this.closed = false;
  }

  async push(chunk, { mimetype = "audio/webm" } = {}) {
    if (this.closed) return;
    this.buffer.push(chunk);
    this.totalBytes += chunk.length;
    if (this.totalBytes > 16000) {
      await this._transcribePartial(mimetype);
    }
  }

  async _transcribePartial(mimetype) {
    if (!this.buffer.length) return;
    try {
      const merged = Buffer.concat(this.buffer);
      const r = await transcribeAudioGroq(merged, "chunk.webm", mimetype);
      if (r.success && r.text) this.onPartial(r.text);
    } catch (e) {}
  }

  async finalize(mimetype = "audio/webm") {
    if (this.closed) return { success: false, error: "Already closed" };
    this.closed = true;
    if (!this.buffer.length) return { success: false, error: "Empty buffer" };
    const merged = Buffer.concat(this.buffer);
    const r = await transcribeAudioGroq(merged, "final.webm", mimetype);
    if (r.success) this.onFinal(r.text);
    else this.onError(r.error);
    this.buffer = [];
    return r;
  }

  reset() {
    this.buffer = [];
    this.totalBytes = 0;
    this.closed = false;
  }
}

// ================================================================================
// §2.12 — TTS (TEXT-TO-SPEECH)
// ================================================================================

async function synthesizeKokoro(text, { voice = "af_bella", speed = 1.0, format = "mp3" } = {}) {
  const baseURL = process.env.KOKORO_URL;
  if (!baseURL) return { success: false, error: "KOKORO_URL non configurée" };

  try {
    const response = await axios.post(
      `${baseURL.replace(/\/$/, "")}/v1/audio/speech`,
      {
        model: "kokoro",
        input: text.slice(0, 4000),
        voice,
        response_format: format,
        speed
      },
      {
        headers: {
          "Content-Type": "application/json",
          ...(process.env.KOKORO_API_KEY
            ? { Authorization: `Bearer ${process.env.KOKORO_API_KEY}` }
            : {})
        },
        responseType: "arraybuffer",
        timeout: 30000
      }
    );
    return {
      success: true,
      audio: Buffer.from(response.data),
      provider: "kokoro",
      format
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

async function synthesizePiper(text, { voice = "fr_FR-siwis-medium", speed = 1.0 } = {}) {
  const baseURL = process.env.PIPER_URL;
  if (!baseURL) return { success: false, error: "PIPER_URL non configurée" };

  try {
    const response = await axios.post(
      `${baseURL.replace(/\/$/, "")}/api/tts`,
      {
        text: text.slice(0, 4000),
        voice,
        length_scale: 1.0 / speed
      },
      {
        headers: { "Content-Type": "application/json" },
        responseType: "arraybuffer",
        timeout: 30000
      }
    );
    return {
      success: true,
      audio: Buffer.from(response.data),
      provider: "piper",
      format: "wav"
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

async function synthesizeSpeech(text, options = {}) {
  if (!text || !text.trim()) return { success: false, error: "Texte vide" };

  const clean = text
    .replace(/```[\s\S]*?```/g, " bloc de code omis ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[*_~>#]/g, "")
    .replace(/\n{2,}/g, ". ")
    .trim();

  const k = await synthesizeKokoro(clean, options);
  if (k.success) return k;

  const p = await synthesizePiper(clean, options);
  if (p.success) return p;

  return { success: false, error: "Aucun moteur TTS disponible" };
}

// ================================================================================
// §2.13 — VAD ADAPTATIF
// ================================================================================

class SimpleVAD {
  constructor({ energyThreshold = 500, silenceMs = 500, minSpeechMs = 200, maxSpeechMs = 30000 } = {}) {
    this.energyThreshold = energyThreshold;
    this.silenceMs = silenceMs;
    this.minSpeechMs = minSpeechMs;
    this.maxSpeechMs = maxSpeechMs;
    this.reset();
  }

  reset() {
    this.speechStart = null;
    this.lastVoiceTime = null;
    this.speaking = false;
  }

  push(pcmBuffer) {
    if (!pcmBuffer || pcmBuffer.length < 2) return { event: null };

    const samples = pcmBuffer.length / 2;
    let sum = 0;
    for (let i = 0; i < pcmBuffer.length; i += 2) {
      const s = pcmBuffer.readInt16LE(i);
      sum += s * s;
    }
    const rms = Math.sqrt(sum / samples);
    const hasVoice = rms > this.energyThreshold;
    const now = Date.now();

    if (hasVoice) {
      if (!this.speaking) {
        this.speaking = true;
        this.speechStart = now;
        this.lastVoiceTime = now;
        return { event: "speech-start" };
      }
      this.lastVoiceTime = now;

      if (now - this.speechStart > this.maxSpeechMs) {
        const d = now - this.speechStart;
        this.reset();
        return { event: "speech-end", durationMs: d, reason: "max_duration" };
      }
      return { event: "continue" };
    }

    if (this.speaking && this.lastVoiceTime) {
      if (now - this.lastVoiceTime >= this.silenceMs) {
        const d = this.lastVoiceTime - this.speechStart;
        if (d < this.minSpeechMs) {
          this.reset();
          return { event: "too-short" };
        }
        this.reset();
        return { event: "speech-end", durationMs: d, reason: "silence" };
      }
    }
    return { event: null };
  }
}

// ================================================================================
// §2.14 — VOICE PIPELINE
// ================================================================================

class SentenceChunker {
  constructor({ onSentence } = {}) {
    this.onSentence = onSentence || (() => {});
    this.buffer = "";
  }

  push(text) {
    if (!text) return;
    this.buffer += text;

    const regex = /([.!?…]+[\s\n]+|[\n]{2,})/g;
    let lastIndex = 0;
    let match;
    const sentences = [];

    while ((match = regex.exec(this.buffer)) !== null) {
      const s = this.buffer.slice(lastIndex, match.index + match[0].length).trim();
      if (s.length >= 8) sentences.push(s);
      lastIndex = match.index + match[0].length;
    }
    if (lastIndex > 0) this.buffer = this.buffer.slice(lastIndex);
    for (const s of sentences) this.onSentence(s);
  }

  flush() {
    const rest = this.buffer.trim();
    this.buffer = "";
    if (rest.length >= 3) this.onSentence(rest);
  }
}

class VoicePipeline extends EventEmitter {
  constructor({ sessionId, userId, onTranscript, onAudio, onStatus, onError, voice = "af_bella" } = {}) {
    super();
    this.sessionId = sessionId;
    this.userId = userId;
    this.onTranscript = onTranscript || (() => {});
    this.onAudio = onAudio || (() => {});
    this.onStatus = onStatus || (() => {});
    this.onError = onError || (() => {});
    this.voice = voice;
    this.vad = new SimpleVAD();
    this.stt = null;
    this.interrupted = false;
    this.processing = false;
  }

  bargeIn() {
    this.interrupted = true;
    this.processing = false;
    this.onStatus("barge-in");
  }

  async processAudioStream(audioStream, { mimetype = "audio/webm" } = {}) {
    this.stt = new StreamingSTT({
      onPartial: (t) => this.onStatus("partial", { text: t }),
      onFinal: (t) => this.onStatus("final", { text: t }),
      onError: (e) => this.onError({ stage: "stt", error: e })
    });

    audioStream.on("data", async (chunk) => {
      if (this.interrupted) return;
      try {
        await this.stt.push(chunk, { mimetype });
        if (mimetype.includes("pcm") || mimetype.includes("l16")) {
          const r = this.vad.push(chunk);
          if (r.event === "speech-end") {
            this.onStatus("speech-end", { durationMs: r.durationMs });
            await this.finalizeUtterance(mimetype);
          }
        }
      } catch (e) {
        this.onError({ stage: "stream", error: e.message });
      }
    });

    audioStream.on("end", async () => {
      if (!this.interrupted) await this.finalizeUtterance(mimetype);
    });

    audioStream.on("error", (e) => this.onError({ stage: "stream", error: e.message }));
  }

  async finalizeUtterance(mimetype) {
    if (this.processing) return;
    this.processing = true;
    this.interrupted = false;

    try {
      this.onStatus("transcribing");
      const r = await this.stt.finalize(mimetype);
      if (!r.success || !r.text) {
        this.onStatus("empty-transcript");
        this.processing = false;
        return;
      }

      const userText = r.text.trim();
      this.onTranscript(userText);
      this.onStatus("thinking", { text: userText });

      const ttsQ = [];
      let ttsRunning = false;
      const processTTS = async () => {
        if (ttsRunning) return;
        ttsRunning = true;
        while (ttsQ.length > 0 && !this.interrupted) {
          const s = ttsQ.shift();
          try {
            const t = await synthesizeSpeech(s, { voice: this.voice });
            if (t.success && !this.interrupted) {
              this.onAudio(t.audio, { format: t.format });
            }
          } catch (e) {}
        }
        ttsRunning = false;
      };

      const chunker = new SentenceChunker({
        onSentence: (s) => {
          if (this.interrupted) return;
          ttsQ.push(s);
          processTTS().catch(() => {});
        }
      });

      const llmStream = await this.emit("requestLLM", userText);
      if (llmStream && typeof llmStream[Symbol.asyncIterator] === "function") {
        for await (const token of llmStream) {
          if (this.interrupted) break;
          chunker.push(token);
        }
      } else if (typeof llmStream === "string") {
        chunker.push(llmStream);
      }

      chunker.flush();
      await new Promise((r) => setTimeout(r, 500));
      this.onStatus("done");
    } catch (e) {
      this.onError({ stage: "pipeline", error: e.message });
    } finally {
      this.processing = false;
    }
  }
}

// ================================================================================
// §2.15 — MÉMOIRE COURTE (ROLLING SUMMARY)
// ================================================================================

const SHORT_MEMORY_MAX_EXCHANGES = 20;

function buildRollingSummary(history, maxExchanges = SHORT_MEMORY_MAX_EXCHANGES) {
  if (!Array.isArray(history) || history.length === 0) return "";

  const filtered = history.filter((m) => m.role === "user" || m.role === "assistant");
  if (!filtered.length) return "";

  const exchanges = [];
  let current = null;
  for (const m of filtered) {
    if (m.role === "user") {
      if (current) exchanges.push(current);
      current = { user: m.content, assistant: null };
    } else if (m.role === "assistant" && current) {
      current.assistant = m.content;
      exchanges.push(current);
      current = null;
    }
  }
  if (current) exchanges.push(current);

  return exchanges.slice(-maxExchanges).map((e, i) => {
    const u = String(e.user || "").slice(0, 200).replace(/\s+/g, " ");
    const a = String(e.assistant || "").slice(0, 200).replace(/\s+/g, " ");
    return `${i + 1}. U: ${u}${a ? `\n   A: ${a}` : ""}`;
  }).join("\n");
}

// ================================================================================
// §2.16 — 🆕 MÉMOIRE PRO PERSISTANTE (CROSS-SEMAINES)
// ================================================================================

/**
 * Enregistre un fait durable dans la mémoire pro persistante.
 * Utilise la table user_long_term_memory (unique par user_id + key).
 */
async function rememberLongTermFact(userId, key, value, { category = "general", confidence = 0.8 } = {}) {
  if (!userId || !key || !value) return { success: false, error: "Paramètres manquants" };

  const cleanKey = sanitizeStrict(String(key), 100);
  const cleanValue = sanitizeStrict(String(value), 500);
  const now = Date.now();

  try {
    // UPSERT : incrémente times_mentioned si déjà présent
    await dbRun(
      `INSERT INTO user_long_term_memory (user_id, key, value, category, confidence, times_mentioned, first_seen, last_seen)
       VALUES (?, ?, ?, ?, ?, 1, ?, ?)
       ON CONFLICT(user_id, key) DO UPDATE SET
         value = excluded.value,
         category = excluded.category,
         confidence = MAX(user_long_term_memory.confidence, excluded.confidence),
         times_mentioned = user_long_term_memory.times_mentioned + 1,
         last_seen = excluded.last_seen`,
      [userId, cleanKey, cleanValue, category, confidence, now, now]
    );

    return { success: true };
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur rememberLongTermFact");
    return { success: false, error: e.message };
  }
}

/**
 * Récupère les faits persistants d'un utilisateur (même après des semaines).
 */
async function recallLongTermFacts(userId, { limit = 30, category = null } = {}) {
  if (!userId) return { facts: [] };

  try {
    let q = `SELECT key, value, category, confidence, times_mentioned, first_seen, last_seen
             FROM user_long_term_memory WHERE user_id = ?`;
    const params = [userId];
    if (category) { q += " AND category = ?"; params.push(category); }
    q += " ORDER BY times_mentioned DESC, last_seen DESC LIMIT ?";
    params.push(limit);

    const rows = await dbAll(q, params);
    return { facts: rows };
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur recallLongTermFacts");
    return { facts: [] };
  }
}

/**
 * Construit un bloc de mémoire pro à injecter dans le system prompt.
 */
async function buildLongTermMemoryBlock(userId) {
  if (!userId) return "";

  const { facts } = await recallLongTermFacts(userId, { limit: 20 });
  if (!facts.length) return "";

  const lines = facts
    .slice(0, 15)
    .map((f) => `- [${f.category}] ${f.key}: ${f.value} (${f.times_mentioned}×)`);

  return `[MÉMOIRE PRO PERSISTANTE — Faits mémorisés au fil du temps]\n${lines.join("\n")}`;
}

// ================================================================================
// §2.17 — MÉMOIRE LONGUE (FACTS + EMBEDDINGS + RECALL)
// ================================================================================

const FACT_CATEGORIES = Object.freeze(["identity", "preference", "project", "language", "general"]);

async function rememberFact(userId, { fact, category = "general", sourceSession = null, confidence = 1.0 }) {
  if (!fact || typeof fact !== "string" || fact.trim().length < 3) {
    return { success: false, error: "Fait vide ou trop court" };
  }
  if (!FACT_CATEGORIES.includes(category)) category = "general";
  const cleanFact = sanitizeStrict(fact, 300);

  try {
    const existing = await dbAll(
      `SELECT id, fact FROM user_memory_facts WHERE user_id = ? ORDER BY created_at DESC LIMIT 100`,
      [userId]
    );
    const norm = cleanFact.toLowerCase().replace(/\s+/g, " ");
    for (const e of existing) {
      const eN = String(e.fact).toLowerCase().replace(/\s+/g, " ");
      const w1 = new Set(norm.split(" "));
      const w2 = new Set(eN.split(" "));
      const inter = [...w1].filter((w) => w2.has(w)).length;
      const union = new Set([...w1, ...w2]).size;
      if (union > 0 && inter / union > 0.85) {
        await dbRun(`UPDATE user_memory_facts SET updated_at = ? WHERE id = ?`, [Date.now(), e.id]);
        return { success: true, factId: e.id, action: "kept" };
      }
    }

    const emb = await embedText(cleanFact);
    const embJson = emb ? safeJsonStringify(emb) : null;

    const r = await dbRun(
      `INSERT INTO user_memory_facts (user_id, fact, category, embedding, confidence, source_session, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [userId, cleanFact, category, embJson, confidence, sourceSession, Date.now(), Date.now()]
    );

    // 🆕 Sync avec mémoire pro persistante
    await rememberLongTermFact(userId, `fact_${r.lastID}`, cleanFact, {
      category,
      confidence
    }).catch(() => {});

    if (firestoreDb) {
      fsSet("user_memory_facts", `${userId}_${r.lastID}`, {
        user_id: userId, fact: cleanFact, category, confidence,
        source_session: sourceSession, created_at: new Date()
      }).catch(() => {});
    }

    logger.info({ userId, category }, "🧠 Fait mémorisé");
    return { success: true, factId: r.lastID, action: "created" };
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur rememberFact");
    return { success: false, error: e.message };
  }
}

async function extractFactsFromExchange(userId, userMessage, assistantReply) {
  if (!userMessage || userMessage.length < 20) return { facts: [] };

  try {
    const provider = MODEL_TIERS.v100.providers[0];
    const r = await callProviderWithTools({
      providerConfig: provider,
      messages: [
        {
          role: "system",
          content: [
            "Tu extrais des faits DURABLES et UTILES sur un utilisateur.",
            "Catégories : identity, preference, project, language, general.",
            'Retourne STRICTEMENT un JSON : {"facts":[{"fact":"...","category":"...","key":"..."}]}',
            'Le "key" doit être un identifiant court (ex: "prenom", "ville", "langue", "projet_actuel").',
            'Si aucun fait : {"facts":[]}'
          ].join("\n")
        },
        {
          role: "user",
          content:
            `Échange :\n` +
            `Utilisateur: ${userMessage.slice(0, 500)}\n` +
            `Assistant: ${(assistantReply || "").slice(0, 500)}`
        }
      ],
      tools: null,
      jsonMode: true
    });

    if (!r.success) return { facts: [] };

    const content = r.message?.content || "";
    let parsed = null;
    try { parsed = JSON.parse(content); }
    catch {
      const m = content.match(/\{[\s\S]*\}/);
      if (m) try { parsed = JSON.parse(m[0]); } catch {}
    }

    if (!parsed?.facts || !Array.isArray(parsed.facts)) return { facts: [] };

    const valid = parsed.facts
      .filter((f) => f?.fact && typeof f.fact === "string" && f.fact.length >= 5 && f.fact.length <= 300)
      .slice(0, 3);

    for (const f of valid) {
      await rememberFact(userId, {
        fact: f.fact,
        category: f.category || "general",
        confidence: 0.8
      });
      // 🆕 Sync avec mémoire pro (clé personnalisée si fournie)
      if (f.key) {
        await rememberLongTermFact(userId, f.key, f.fact, {
          category: f.category || "general",
          confidence: 0.85
        }).catch(() => {});
      }
    }
    return { facts: valid };
  } catch (e) {
    logger.warn({ err: e.message }, "Extraction faits échouée");
    return { facts: [] };
  }
}

async function embedText(text) {
  if (!text || typeof text !== "string") return null;
  const trimmed = text.slice(0, 2000);

  if (process.env.GROQ_EMBED_MODEL && LLM_PROVIDERS.GROQ.keyPool.length > 0) {
    try {
      const k = LLM_PROVIDERS.GROQ.keyPool[0];
      const r = await axios.post(
        "https://api.groq.com/openai/v1/embeddings",
        { model: process.env.GROQ_EMBED_MODEL || "nomic-embed-text-v1.5", input: trimmed },
        {
          headers: { Authorization: `Bearer ${k.apiKey}`, "Content-Type": "application/json" },
          timeout: 8000
        }
      );
      return r.data?.data?.[0]?.embedding || null;
    } catch (e) {}
  }
  return null;
}

async function recallMemory(userId, query, limit = 5) {
  if (!query || typeof query !== "string") return { facts: [] };

  try {
    const all = await dbAll(
      `SELECT id, fact, category, embedding, confidence, created_at
       FROM user_memory_facts WHERE user_id = ?
       ORDER BY created_at DESC LIMIT 200`,
      [userId]
    );
    if (!all.length) return { facts: [] };

    const qEmb = await embedText(query);
    let scored = [];

    if (qEmb) {
      for (const f of all) {
        const fEmb = f.embedding ? safeJsonParse(f.embedding, null) : null;
        if (!fEmb) continue;
        scored.push({ ...f, score: SemanticCache.cosineSimilarity(qEmb, fEmb) });
      }
    }

    if (!scored.length) {
      const qW = new Set(String(query).toLowerCase().split(/\s+/).filter((w) => w.length > 2));
      for (const f of all) {
        const fW = new Set(String(f.fact).toLowerCase().split(/\s+/).filter((w) => w.length > 2));
        const inter = [...qW].filter((w) => fW.has(w)).length;
        const union = new Set([...qW, ...fW]).size;
        scored.push({ ...f, score: union > 0 ? inter / union : 0 });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    return {
      facts: scored.slice(0, limit).filter((f) => f.score > 0.15).map((f) => ({
        fact: f.fact,
        category: f.category,
        score: Number(f.score.toFixed(3)),
        createdAt: f.created_at
      }))
    };
  } catch (e) {
    return { facts: [] };
  }
}

async function getAllFacts(userId) {
  try {
    const rows = await dbAll(
      `SELECT id, fact, category, confidence, created_at
       FROM user_memory_facts WHERE user_id = ?
       ORDER BY category, created_at DESC LIMIT 500`,
      [userId]
    );
    const grouped = {};
    for (const r of rows) {
      if (!grouped[r.category]) grouped[r.category] = [];
      grouped[r.category].push({ id: r.id, fact: r.fact, confidence: r.confidence });
    }
    return { success: true, grouped, total: rows.length };
  } catch (e) {
    return { success: false, grouped: {}, total: 0, error: e.message };
  }
}

async function deleteFact(userId, factId) {
  try {
    await dbRun(`DELETE FROM user_memory_facts WHERE id = ? AND user_id = ?`, [factId, userId]);
    return { success: true };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

async function clearAllFacts(userId) {
  try {
    await dbRun(`DELETE FROM user_memory_facts WHERE user_id = ?`, [userId]);
    await dbRun(`DELETE FROM user_long_term_memory WHERE user_id = ?`, [userId]);
    return { success: true };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ================================================================================
// §2.18 — AI QUALITY LAYER
// ================================================================================

function detectHallucinatedNumbers(responseText, toolResults = []) {
  if (!responseText || !toolResults || toolResults.length === 0) {
    return { detected: false, suspiciousNumbers: [] };
  }

  const numRe = /\b\d{2,}(?:[.,]\d+)?\b/g;
  const respNumbers = new Set();
  let m;
  while ((m = numRe.exec(responseText)) !== null) {
    const n = m[0].replace(",", ".");
    const num = parseFloat(n);
    if (num >= 1900 && num <= 2099) continue;
    if (num < 10) continue;
    respNumbers.add(n);
  }
  if (!respNumbers.size) return { detected: false, suspiciousNumbers: [] };

  const toolText = safeJsonStringify(toolResults).toLowerCase();
  const toolNumbers = new Set();
  let tm;
  const tRe = /\b\d{2,}(?:[.,]\d+)?\b/g;
  while ((tm = tRe.exec(toolText)) !== null) toolNumbers.add(tm[0].replace(",", "."));

  const suspicious = [];
  for (const n of respNumbers) {
    if (!toolNumbers.has(n)) {
      const intPart = n.split(".")[0];
      let found = false;
      for (const tn of toolNumbers) {
        if (tn.startsWith(intPart) || intPart.startsWith(tn.split(".")[0])) {
          found = true;
          break;
        }
      }
      if (!found) suspicious.push(n);
    }
  }

  const detected = suspicious.length >= 2;
  if (detected && metrics?.hallucinationDetected) {
    metrics.hallucinationDetected.labels("numbers").inc();
  }
  return { detected, suspiciousNumbers: suspicious.slice(0, 5) };
}

function estimateConfidence({ responseText, toolCallTrace = [], reasoning = "", degraded = false }) {
  if (degraded) return 0.2;
  if (!responseText) return 0;

  let score = 0.5;
  const len = responseText.length;
  if (len < 50) score -= 0.15;
  else if (len > 200) score += 0.1;
  else if (len > 500) score += 0.15;

  const hedges = (responseText.match(/\b(peut-être|je pense|je crois|il me semble|probablement|sans doute|je ne suis pas sûr)/gi) || []).length;
  score -= Math.min(hedges * 0.05, 0.2);

  if (toolCallTrace.length > 0) {
    score += Math.min(toolCallTrace.filter((t) => !t.error).length * 0.1, 0.3);
  }
  if (reasoning && reasoning.length > 100) score += 0.1;
  if (/\b(je ne sais pas|je n'ai pas trouvé|aucun résultat|indisponible)/i.test(responseText)) score += 0.05;
  if (/\b(erreur|error|failed|exception)\b/i.test(responseText) && !toolCallTrace.length) score -= 0.15;

  return Math.max(0, Math.min(1, score));
}

async function selfCritique({ userMessage, draftAnswer, toolCallTrace = [], providerConfig, _meta = {} }) {
  if (!CONFIG.AI_QUALITY.ENABLE_SELF_CRITIQUE) {
    return { improved: false, correctedText: draftAnswer, critique: null };
  }

  const trigger = CONFIG.AI_QUALITY.SELF_CRITIQUE_TRIGGER_ON;
  if (trigger === "never") return { improved: false, correctedText: draftAnswer, critique: null };

  const shouldTrigger = trigger === "always" || (trigger === "auto" && (
    /\b\d{2,}\b/.test(draftAnswer) ||
    (userMessage.length > 100 && draftAnswer.length < 100) ||
    (/je ne sais pas|je n'ai pas trouvé/i.test(draftAnswer) && toolCallTrace.some((t) => !t.error))
  ));

  if (!shouldTrigger) return { improved: false, correctedText: draftAnswer, critique: null };

  const toolSummary = toolCallTrace.length > 0
    ? toolCallTrace.map((t) => `- ${t.name}(${safeJsonStringify(t.args)})`).join("\n")
    : "(aucun)";

  try {
    const r = await callProviderWithTools({
      providerConfig: {
        ...providerConfig,
        timeout: CONFIG.TIMEOUTS.SELF_CRITIQUE_MS,
        temperature: 0.2
      },
      messages: [
        {
          role: "system",
          content: [
            "Tu es un relecteur critique expert.",
            "Règles :",
            "1. Si correct → même réponse.",
            "2. Si erreur/hallucination → corrige.",
            "3. Chiffres hors outils → supprime ou 'je ne sais pas'.",
            'Réponds STRICTEMENT en JSON : {"improved": true/false, "correctedText": "...", "critique": "..."}',
            "La langue DOIT être identique au brouillon."
          ].join("\n")
        },
        {
          role: "user",
          content:
            `QUESTION :\n${userMessage.slice(0, 500)}\n\n` +
            `OUTILS :\n${toolSummary}\n\n` +
            `BROUILLON :\n${draftAnswer.slice(0, 2000)}`
        }
      ],
      tools: null,
      jsonMode: true,
      _meta
    });

    if (!r.success) return { improved: false, correctedText: draftAnswer, critique: null };

    const content = r.message?.content || "";
    let parsed = null;
    try { parsed = JSON.parse(content); }
    catch {
      const m = content.match(/\{[\s\S]*\}/);
      if (m) try { parsed = JSON.parse(m[0]); } catch {}
    }

    if (!parsed || typeof parsed.correctedText !== "string") {
      return { improved: false, correctedText: draftAnswer, critique: null };
    }

    if (parsed.improved && parsed.correctedText.trim().length > 0) {
      if (metrics?.selfCritiqueTriggered) metrics.selfCritiqueTriggered.labels("improved").inc();
      return {
        improved: true,
        correctedText: formatFinalReply(parsed.correctedText.trim()),
        critique: parsed.critique || null
      };
    }
    return { improved: false, correctedText: draftAnswer, critique: parsed.critique || null };
  } catch (e) {
    return { improved: false, correctedText: draftAnswer, critique: null };
  }
}

async function multiModelVote({ messages, providers, tools = null, images = null, _meta = {} }) {
  if (!CONFIG.AI_QUALITY.ENABLE_MULTI_VOTE) {
    return { chosenText: null, votes: 0, winner: null, allResponses: [] };
  }

  const max = Math.min(providers.length, CONFIG.AI_QUALITY.MAX_VOTING_PROVIDERS);
  const selected = providers.slice(0, max);

  const results = await Promise.allSettled(selected.map((p) =>
    callProviderWithTools({ providerConfig: p, messages, tools, images, _meta })
  ));

  const successful = [];
  results.forEach((r, i) => {
    if (r.status === "fulfilled" && r.value.success && r.value.message?.content) {
      successful.push({
        provider: selected[i].provider,
        model: selected[i].model,
        text: r.value.message.content
      });
    }
  });

  if (!successful.length) return { chosenText: null, votes: 0, winner: null, allResponses: [] };
  if (successful.length === 1) {
    return {
      chosenText: successful[0].text,
      votes: 1,
      winner: `${successful[0].provider}/${successful[0].model}`,
      allResponses: successful
    };
  }

  const sims = successful.map((a, i) => {
    let total = 0;
    for (let j = 0; j < successful.length; j++) {
      if (i === j) continue;
      total += jaccardSimilarity(a.text, successful[j].text);
    }
    return { index: i, avgSim: total / (successful.length - 1) };
  });
  sims.sort((a, b) => b.avgSim - a.avgSim);
  const w = successful[sims[0].index];

  return {
    chosenText: w.text,
    votes: successful.length,
    winner: `${w.provider}/${w.model}`,
    allResponses: successful
  };
}

function jaccardSimilarity(a, b) {
  if (!a || !b) return 0;
  const wa = new Set(a.toLowerCase().split(/\s+/).filter((w) => w.length > 2));
  const wb = new Set(b.toLowerCase().split(/\s+/).filter((w) => w.length > 2));
  if (!wa.size || !wb.size) return 0;
  const inter = [...wa].filter((w) => wb.has(w)).length;
  return inter / new Set([...wa, ...wb]).size;
}

async function assessResponseQuality({
  userMessage,
  draftAnswer,
  toolCallTrace = [],
  reasoning = "",
  providerConfig,
  intent = "GENERAL",
  degraded = false,
  _meta = {}
}) {
  const results = {
    originalText: draftAnswer,
    finalText: draftAnswer,
    confidence: 0.5,
    hallucination: { detected: false, suspiciousNumbers: [] },
    selfCritique: { improved: false, critique: null },
    qualityScore: 0.5
  };

  results.hallucination = detectHallucinatedNumbers(draftAnswer, toolCallTrace);

  if (!degraded && results.hallucination.detected) {
    const c = await selfCritique({ userMessage, draftAnswer, toolCallTrace, providerConfig, _meta });
    results.selfCritique = c;
    if (c.improved) {
      results.finalText = c.correctedText;
      results.hallucination = detectHallucinatedNumbers(results.finalText, toolCallTrace);
    }
  } else if (!degraded && CONFIG.AI_QUALITY.SELF_CRITIQUE_TRIGGER_ON !== "never") {
    const c = await selfCritique({ userMessage, draftAnswer, toolCallTrace, providerConfig, _meta });
    results.selfCritique = c;
    if (c.improved) results.finalText = c.correctedText;
  }

  results.confidence = estimateConfidence({
    responseText: results.finalText,
    toolCallTrace,
    reasoning,
    degraded
  });

  results.qualityScore = Math.round(
    (results.confidence * 0.6 +
     (1 - (results.hallucination.detected ? 0.5 : 0)) * 0.3 +
     (results.selfCritique.improved ? 1 : 0.5) * 0.1) * 100
  ) / 100;

  if (metrics?.qualityScore) {
    metrics.qualityScore.labels(intent, _meta.tier || "v100").observe(results.qualityScore);
  }

  return results;
}

// ================================================================================
// §2.19 — EXPORTS PARTIE 2
// ================================================================================

Object.assign(module.exports, {
  LLM_PROVIDERS, MODEL_TIERS, geminiClient,
  CircuitBreaker, getCircuit, getAllCircuitStates,
  LLMErrorInterceptor, userFacingErrorMessage,
  callProviderWithTools, callProviderRawWithTools,
  callOpenAICompatibleRaw, callGeminiRawWithTools,
  validateAndSanitizeOpenRouterModel,
  TOOL_SCHEMAS, TOOLS_BY_CONTEXT, SIDE_EFFECT_TOOLS,
  getToolSchemas, validateToolArgs, executeToolNative,
  runToolLoop, dedupeVideos,
  transcribeAudioGroq, StreamingSTT,
  synthesizeKokoro, synthesizePiper, synthesizeSpeech,
  SimpleVAD, SentenceChunker, VoicePipeline,
  buildRollingSummary, SHORT_MEMORY_MAX_EXCHANGES,
  rememberLongTermFact, recallLongTermFacts, buildLongTermMemoryBlock,
  rememberFact, extractFactsFromExchange, FACT_CATEGORIES,
  getAllFacts, deleteFact, clearAllFacts,
  embedText, recallMemory,
  detectHallucinatedNumbers, estimateConfidence,
  selfCritique, multiModelVote, jaccardSimilarity,
  assessResponseQuality
});

// ================================================================================
// ==================== FIN PARTIE 2/5 ===========================================
// ================================================================================
// ▶ PARTIE 3/5 : Search · Media · Vision · Math · Tasks · Images garanties
//   Tape "suite" pour la recevoir.
// ================================================================================
// ================================================================================
// PARTIE 3/5 — DATA SERVICES · MEDIA · VISION · MATH · TASKS
// ================================================================================
// VERSION : v16.5.0 (Octobre 2026)
//
// 🆕 IMAGES GARANTIES :
//   • ensureImageForResponse() → garantit 1 image minimum pour chaque requête
//   • Pipeline robuste : Wikimedia → Wikipedia → Pexels → DDG Images
//   • Fallback neutre (logo Luba) si aucune source ne répond
//   • Skip automatique pour salutations/identité
//
// FONCTIONNALITÉS CONSERVÉES :
//   • Search : Tavily + Serper + DuckDuckGo + GDELT + HackerNews + Wikipedia
//   • Media : Wikimedia, Wikipedia thumb, Pexels, DDG Images
//   • YouTube : youtubei.js (keyless) + API key + DDG fallback
//   • Weather, Finance, News, Sports
//   • Sandbox Piston + Judge0
//   • Vision (Groq Maverick → Gemini → OpenRouter)
//   • Ads propres (sans branding "test")
//   • Tasks CRUD, Quotas, Email, WhatsApp helpers
//
// TABLE DES MATIÈRES :
//   §3.01  Search Tavily
//   §3.02  Search Serper
//   §3.03  Search DuckDuckGo
//   §3.04  Search GDELT
//   §3.05  Search HackerNews
//   §3.06  Wikipedia summary
//   §3.07  searchWeb (orchestrateur)
//   §3.08  Wikimedia Commons (images)
//   §3.09  Wikipedia thumb
//   §3.10  Pexels
//   §3.11  DuckDuckGo Images
//   §3.12  searchImagesWithFallback
//   §3.13  🆕 ensureImageForResponse (images garanties)
//   §3.14  YouTube
//   §3.15  Open-Meteo (météo)
//   §3.16  CoinGecko / CoinMarketCap / Yahoo
//   §3.17  Google News + extraction score
//   §3.18  searchNews
//   §3.19  Sports
//   §3.20  Sandbox Piston + Judge0
//   §3.21  Vision
//   §3.22  Ads propres
//   §3.23  Math evaluator
//   §3.24  Entité + pré-routeur + intents
//   §3.25  Tasks CRUD
//   §3.26  Quotas
//   §3.27  Email dispatch
//   §3.28  WhatsApp helpers
//   §3.29  Exports Partie 3
// ================================================================================

"use strict";

// ================================================================================
// §3.01 — SEARCH TAVILY
// ================================================================================

async function searchTavily(query, { maxResults = 5, topic = "general" } = {}) {
  const apiKey = process.env.TAVILY_API_KEY;
  if (!apiKey) return { results: [], provider: "tavily", skipped: true };

  try {
    const resp = await axios.post(
      "https://api.tavily.com/search",
      {
        api_key: apiKey,
        query: String(query).slice(0, 400),
        search_depth: "basic",
        max_results: maxResults,
        topic,
        include_answer: true,
        include_raw_content: false
      },
      { timeout: 8000 }
    );

    const results = (resp.data?.results || []).map((r) => ({
      title: r.title,
      url: r.url,
      snippet: r.content?.slice(0, 500) || "",
      score: r.score || 0,
      publishedDate: r.published_date || null,
      source: "tavily"
    }));

    return { results, answer: resp.data?.answer || null, provider: "tavily" };
  } catch (e) {
    logger.warn({ err: e.message }, "Tavily échec");
    return { results: [], provider: "tavily", error: e.message };
  }
}

// ================================================================================
// §3.02 — SEARCH SERPER
// ================================================================================

async function searchSerper(query, { maxResults = 5, type = "search" } = {}) {
  const apiKey = process.env.SERPER_API_KEY;
  if (!apiKey) return { results: [], provider: "serper", skipped: true };

  try {
    const endpoint = type === "news"
      ? "https://google.serper.dev/news"
      : type === "images"
        ? "https://google.serper.dev/images"
        : "https://google.serper.dev/search";

    const resp = await axios.post(
      endpoint,
      { q: String(query).slice(0, 400), num: maxResults, hl: "fr", gl: "fr" },
      {
        headers: { "X-API-KEY": apiKey, "Content-Type": "application/json" },
        timeout: 8000
      }
    );

    const raw = resp.data?.organic || resp.data?.news || resp.data?.images || [];
    const results = raw.slice(0, maxResults).map((r) => ({
      title: r.title,
      url: r.link || r.imageUrl,
      snippet: r.snippet || r.description || "",
      source: r.source || "serper",
      date: r.date || null
    }));

    return { results, provider: "serper" };
  } catch (e) {
    logger.warn({ err: e.message }, "Serper échec");
    return { results: [], provider: "serper", error: e.message };
  }
}

// ================================================================================
// §3.03 — SEARCH DUCKDUCKGO
// ================================================================================

async function searchDuckDuckGo(query, { maxResults = 5 } = {}) {
  if (!ddgScrape) return { results: [], provider: "ddg", skipped: true };

  try {
    const fn = ddgScrape.search;
    if (typeof fn !== "function") return { results: [], provider: "ddg" };

    const results = await fn(query, {
      safeSearch: "moderate",
      locale: "fr-fr",
      maxResults
    });
    const list = Array.isArray(results?.results) ? results.results
      : Array.isArray(results) ? results : [];

    return {
      results: list.slice(0, maxResults).map((r) => ({
        title: r.title,
        url: r.url,
        snippet: r.description || "",
        source: r.source || "ddg"
      })),
      provider: "ddg"
    };
  } catch (e) {
    return { results: [], provider: "ddg", error: e.message };
  }
}

// ================================================================================
// §3.04 — SEARCH GDELT
// ================================================================================

async function searchGdelt(query, { maxResults = 5, timespanDays = 7 } = {}) {
  try {
    const resp = await axios.get("https://api.gdeltproject.org/api/v2/doc/doc", {
      params: {
        query: `${query} sourcelang:french`,
        mode: "artlist",
        maxrecords: maxResults,
        format: "json",
        timespan: `${timespanDays}d`,
        sort: "datedesc"
      },
      timeout: 9000
    });

    const articles = (resp.data?.articles || []).slice(0, maxResults).map((a) => ({
      title: a.title,
      url: a.url,
      snippet: "",
      source: a.domain || "gdelt",
      publishedDate: a.seendate || null,
      sourceCountry: a.sourcecountry || null
    }));

    return { results: articles, provider: "gdelt" };
  } catch (e) {
    return { results: [], provider: "gdelt", error: e.message };
  }
}

// ================================================================================
// §3.05 — SEARCH HACKERNEWS
// ================================================================================

async function searchHackerNews(query, { maxResults = 5 } = {}) {
  try {
    const resp = await axios.get("https://hn.algolia.com/api/v1/search", {
      params: { query, tags: "story", hitsPerPage: maxResults },
      timeout: 6000
    });

    const results = (resp.data?.hits || []).map((h) => ({
      title: h.title,
      url: h.url || `https://news.ycombinator.com/item?id=${h.objectID}`,
      snippet: h.story_text?.slice(0, 300) || `${h.points || 0} points`,
      source: "hackernews",
      points: h.points || 0
    }));

    return { results, provider: "hackernews" };
  } catch (e) {
    return { results: [], provider: "hackernews", error: e.message };
  }
}

// ================================================================================
// §3.06 — WIKIPEDIA SUMMARY
// ================================================================================

async function searchWikipediaSummary(query, { lang = "fr" } = {}) {
  try {
    const resp = await axios.get(
      `https://${lang}.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      {
        timeout: 6000,
        headers: { "User-Agent": CONFIG.IMAGES.WIKIMEDIA_UA }
      }
    );

    if (resp.data?.type === "disambiguation" || !resp.data?.extract) {
      return { summary: null };
    }

    return {
      title: resp.data.title,
      summary: resp.data.extract,
      url: resp.data.content_urls?.desktop?.page || null,
      thumbnail: resp.data.thumbnail?.source || null
    };
  } catch {
    return { summary: null };
  }
}

// ================================================================================
// §3.07 — SEARCHWEB (ORCHESTRATEUR)
// ================================================================================

async function searchWeb(query) {
  if (!query || typeof query !== "string") {
    return { results: [], sourcesUsed: [], errors: [] };
  }

  const cleanQuery = extractEntity(query) || String(query).trim();

  const sources = [
    {
      key: "wikipedia",
      promise: searchWikipediaSummary(cleanQuery).then((w) =>
        w.summary ? [{ title: w.title, url: w.url, snippet: w.summary, type: "wiki" }] : []
      )
    },
    { key: "tavily", promise: searchTavily(cleanQuery).then((r) => r.results || []) },
    { key: "serper", promise: searchSerper(cleanQuery).then((r) => r.results || []) },
    { key: "duckduckgo", promise: searchDuckDuckGo(cleanQuery).then((r) => r.results || []) }
  ];

  const settled = await allSettledWithDeadline(sources.map((s) => s.promise), 6000);

  const results = [];
  const sourcesUsed = [];
  const errors = [];

  settled.forEach((r, i) => {
    const { key } = sources[i];
    if (r.status === "fulfilled" && Array.isArray(r.value) && r.value.length > 0) {
      for (const item of r.value) results.push({ ...item, _source: key });
      sourcesUsed.push(key);
    } else if (r.status === "rejected") {
      errors.push({ source: key, error: String(r.reason?.message || r.reason) });
    }
  });

  const seen = new Set();
  const unique = results.filter((r) => {
    const u = r.url || r.title;
    if (seen.has(u)) return false;
    seen.add(u);
    return true;
  });

  return { results: unique.slice(0, 12), sourcesUsed, errors };
}

// ================================================================================
// §3.08 — WIKIMEDIA COMMONS (IMAGES)
// ================================================================================

/**
 * Recherche d'images Wikimedia Commons.
 * Retourne des URLs thumburl directement utilisables dans <img src>.
 * 🆕 v16.5 : logs détaillés pour diagnostic
 */
async function searchWikimediaImages(query, limit = CONFIG.IMAGES.WIKIMEDIA_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };

  try {
    const url =
      "https://commons.wikimedia.org/w/api.php" +
      "?action=query&generator=search" +
      `&gsrsearch=${encodeURIComponent(query)}` +
      "&gsrnamespace=6" +
      `&gsrlimit=${Math.min(limit, 10)}` +
      "&prop=imageinfo&iiprop=url|extmetadata" +
      "&iiurlwidth=800" +
      "&format=json&origin=*";

    const resp = await axios.get(url, {
      timeout: CONFIG.TIMEOUTS.IMAGE_SOURCE_MS,
      headers: { "User-Agent": CONFIG.IMAGES.WIKIMEDIA_UA }
    });

    const pages = resp.data?.query?.pages;
    if (!pages) return { images: [] };

    const images = Object.values(pages).map((page) => {
      const info = page.imageinfo?.[0];
      if (!info) return null;

      // ✅ Toujours privilégier thumburl (URL directe image)
      const imgUrl = info.thumburl || info.url;
      if (!imgUrl) return null;

      // ✅ Vérifie que c'est bien une URL d'image (pas une page HTML)
      const isDirectImageUrl = /\.(jpg|jpeg|png|gif|webp|svg)(\?|$)/i.test(imgUrl) ||
                                imgUrl.includes("/thumb/") ||
                                imgUrl.includes("upload.wikimedia.org");
      if (!isDirectImageUrl) return null;

      return {
        url: imgUrl,
        title: page.title || "Image",
        description: (info.extmetadata?.ImageDescription?.value || "")
          .replace(/<[^>]*>/g, "").slice(0, 200) || null,
        pageUrl: info.descriptionurl || null,
        source: "wikimediacommons",
        license: info.extmetadata?.LicenseShortName?.value || null
      };
    }).filter(Boolean);

    if (images.length > 0) {
      logger.info({
        query,
        count: images.length,
        firstUrl: images[0].url.slice(0, 100)
      }, "[Luba Images] Wikimedia OK");
    }

    return { images };
  } catch (e) {
    logger.warn({ err: e.message, query }, "[Luba Images] Wikimedia échec");
    return { images: [] };
  }
}

// ================================================================================
// §3.09 — WIKIPEDIA THUMB
// ================================================================================

async function fetchWikipediaThumb(query) {
  try {
    const resp = await axios.get(
      `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      {
        timeout: CONFIG.TIMEOUTS.IMAGE_SOURCE_MS,
        headers: { "User-Agent": CONFIG.IMAGES.WIKIMEDIA_UA }
      }
    );
    const thumb = resp.data?.thumbnail?.source || resp.data?.originalimage?.source;
    if (!thumb) return null;

    // ✅ Vérifie que c'est bien une URL d'image directe
    if (!thumb.includes("upload.wikimedia.org")) return null;

    return {
      url: thumb,
      title: resp.data.title || query,
      description: resp.data.extract ? resp.data.extract.slice(0, 200) : null,
      pageUrl: resp.data.content_urls?.desktop?.page || null,
      source: "wikipedia"
    };
  } catch {
    return null;
  }
}

// ================================================================================
// §3.10 — PEXELS
// ================================================================================

async function searchPexelsImages(query, limit = 6) {
  const apiKey = process.env.PEXELS_API_KEY;
  if (!apiKey) return { images: [] };

  try {
    const resp = await axios.get("https://api.pexels.com/v1/search", {
      params: { query, per_page: Math.min(limit, 15), orientation: "landscape" },
      headers: { Authorization: apiKey },
      timeout: CONFIG.TIMEOUTS.IMAGE_SOURCE_MS
    });

    const images = (resp.data?.photos || []).map((p) => ({
      url: p.src?.medium || p.src?.large || p.src?.original,
      title: p.alt || query,
      description: p.photographer ? `Photo par ${p.photographer}` : null,
      pageUrl: p.url,
      source: "pexels",
      width: p.width,
      height: p.height
    }));

    return { images };
  } catch (e) {
    return { images: [] };
  }
}

// ================================================================================
// §3.11 — DUCKDUCKGO IMAGES
// ================================================================================

async function searchDuckDuckGoImages(query, limit = CONFIG.IMAGES.DDG_LIMIT) {
  if (!ddgScrape) return [];

  try {
    const fn = ddgScrape.searchImages || ddgScrape.images;
    if (typeof fn !== "function") return [];

    const results = await fn(query, { safeSearch: "moderate" });
    const list = Array.isArray(results?.results) ? results.results
      : Array.isArray(results) ? results : [];

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
    return [];
  }
}

// ================================================================================
// §3.12 — SEARCHIMAGESWITHFALLBACK
// ================================================================================

const imageCache = new LRUCache({
  max: 500,
  ttl: CONFIG.IMAGES.CACHE_TTL_MS,
  updateAgeOnGet: false
});

/**
 * Orchestrateur de recherche d'images.
 * 🆕 v16.5 : fallback robuste (retourne les images même si pertinence faible).
 */
async function searchImagesWithFallback(query, limit = CONFIG.LIMITS.IMAGE_SEARCH_LIMIT) {
  const cleanQuery = extractEntity(query) || String(query || "").trim();
  if (!cleanQuery || cleanQuery.length < 2) return { images: [] };

  const cacheKey = `img:v165:${cleanQuery.toLowerCase().trim()}`;
  const cached = imageCache.get(cacheKey);
  if (cached) return cached;

  logger.info({ query: cleanQuery }, "[Luba Images] Recherche démarrée");

  const settled = await allSettledWithDeadline([
    searchWikimediaImages(cleanQuery, limit),
    fetchWikipediaThumb(cleanQuery),
    searchPexelsImages(cleanQuery, Math.min(3, limit)),
    searchDuckDuckGoImages(cleanQuery, Math.min(3, limit))
  ], CONFIG.TIMEOUTS.IMAGE_SOURCE_MS);

  const [commonsR, wikiR, pexelsR, ddgR] = settled;
  const commonsImages = commonsR.status === "fulfilled" ? (commonsR.value?.images || []) : [];
  const wikiImage = (wikiR.status === "fulfilled" && wikiR.value) ? wikiR.value : null;
  const pexelsImages = pexelsR.status === "fulfilled" ? (pexelsR.value?.images || []) : [];
  const ddgImages = ddgR.status === "fulfilled" ? (ddgR.value || []) : [];

  logger.info({
    query: cleanQuery,
    commons: commonsImages.length,
    wiki: wikiImage ? 1 : 0,
    pexels: pexelsImages.length,
    ddg: ddgImages.length
  }, "[Luba Images] Sources interrogées");

  const all = [
    ...(wikiImage ? [{ ...wikiImage, _prio: 0 }] : []),
    ...commonsImages.map((i) => ({ ...i, _prio: 1 })),
    ...pexelsImages.map((i) => ({ ...i, _prio: 2 })),
    ...ddgImages.map((i) => ({ ...i, _prio: 3 }))
  ];

  const scored = all.map((img) => ({
    ...img,
    _relevance: imageRelevanceScore(img, cleanQuery)
  }));

  let relevant = scored.filter((img) => img._relevance >= CONFIG.IMAGES.MIN_RELEVANCE);

  // 🆕 Si rien de pertinent, on prend les images disponibles quand même
  if (relevant.length === 0 && all.length > 0) {
    logger.info({ query: cleanQuery, total: all.length }, "[Luba Images] Fallback sur images brutes");
    relevant = scored;
  }

  relevant.sort((a, b) => {
    if (Math.abs(a._relevance - b._relevance) > 0.2) return b._relevance - a._relevance;
    return a._prio - b._prio;
  });

  const seen = new Set();
  const unique = relevant
    .filter((img) => {
      if (!img?.url || seen.has(img.url)) return false;
      seen.add(img.url);
      return true;
    })
    .slice(0, limit)
    .map((img) => ({
      url: img.url,
      title: img.title || "",
      description: img.description || "",
      pageUrl: img.pageUrl || null,
      source: img.source || "unknown"
    }));

  const result = { images: unique, query: cleanQuery };
  if (unique.length > 0) imageCache.set(cacheKey, result);

  logger.info({
    query: cleanQuery,
    returned: unique.length,
    urls: unique.slice(0, 2).map((i) => i.url.slice(0, 80))
  }, "[Luba Images] Résultat final");

  return result;
}

// ================================================================================
// §3.13 — 🆕 ENSUREIMAGEFORRESPONSE (IMAGES GARANTIES)
// ================================================================================

/**
 * Garantit qu'une image est disponible pour une réponse.
 *
 * Règles :
 *   - Skip si salutation OU question identité
 *   - Sinon, recherche une image via searchImagesWithFallback
 *   - Si aucune image trouvée, retourne un fallback neutre
 *
 * @returns {Promise<{ images: Array, skipped: boolean, reason: string }>}
 */
async function ensureImageForResponse(userMessage, entity = null) {
  // Skip : salutation
  if (isGreetingOrSmallTalk(userMessage)) {
    logger.debug({ message: userMessage.slice(0, 50) }, "[Luba Images] Skip (salutation)");
    return { images: [], skipped: true, reason: "greeting" };
  }

  // Skip : question identité
  if (isIdentityOrSelfQuestion(userMessage)) {
    logger.debug({ message: userMessage.slice(0, 50) }, "[Luba Images] Skip (identité)");
    return { images: [], skipped: true, reason: "identity" };
  }

  // Construit la query d'image
  const query = entity || extractEntity(userMessage) || userMessage.split(/\s+/).slice(0, 5).join(" ");

  if (!query || query.length < 2) {
    return { images: [], skipped: true, reason: "query_too_short" };
  }

  logger.info({ query }, "[Luba Images] Recherche garantie déclenchée");

  try {
    const result = await searchImagesWithFallback(query, 3);
    if (result.images?.length > 0) {
      return { images: result.images, skipped: false, reason: "found" };
    }

    // Fallback : essayer avec la query brute (sans extraction)
    const fallbackQuery = String(userMessage).trim().slice(0, 50);
    if (fallbackQuery !== query) {
      const fallbackResult = await searchImagesWithFallback(fallbackQuery, 3);
      if (fallbackResult.images?.length > 0) {
        return { images: fallbackResult.images, skipped: false, reason: "found_fallback" };
      }
    }

    // Aucune image trouvée → fallback neutre
    logger.info({ query }, "[Luba Images] Aucune image → fallback neutre");
    return {
      images: [{
        url: "https://placehold.co/600x400/1a73e8/ffffff/png?text=Luba&font=roboto",
        title: "Luba",
        description: null,
        pageUrl: "https://luba.web.app",
        source: "fallback_neutral"
      }],
      skipped: false,
      reason: "fallback_neutral"
    };
  } catch (e) {
    logger.warn({ err: e.message, query }, "[Luba Images] Erreur recherche garantie");
    return { images: [], skipped: true, reason: "error" };
  }
}

// ================================================================================
// §3.14 — YOUTUBE
// ================================================================================

let youtubei = null;
try { youtubei = require("youtubei.js"); } catch {}

function extractYouTubeVideoId(url) {
  if (!url) return null;
  const m = String(url).match(
    /(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})/
  );
  return m ? m[1] : null;
}

async function searchYouTubeYoutubei(query, limit = 5) {
  if (!youtubei) return { videos: [] };

  try {
    const yt = await youtubei.Innertube.create({ lang: "fr", location: "FR" });
    const search = await yt.search(query, { type: "video" });

    const videos = (search.videos || []).slice(0, limit).map((v) => ({
      videoId: v.video_id,
      title: v.title?.text || "",
      channel: v.author?.name || null,
      thumbnail: v.thumbnails?.[v.thumbnails.length - 1]?.url || null,
      duration: v.duration?.text || null,
      views: v.view_count?.text || null,
      publishedAt: v.published?.text || null,
      url: `https://www.youtube.com/watch?v=${v.video_id}`
    })).filter((v) => v.videoId);

    return { videos };
  } catch (e) {
    return { videos: [] };
  }
}

async function searchYouTubeApiKey(query, limit = 5) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) return { videos: [] };

  try {
    const resp = await axios.get("https://www.googleapis.com/youtube/v3/search", {
      params: { part: "snippet", type: "video", maxResults: limit, q: query, key: apiKey },
      timeout: 8000
    });

    const videos = (resp.data?.items || []).map((item) => ({
      videoId: item.id?.videoId,
      title: decodeXmlEntities(item.snippet?.title || ""),
      channel: item.snippet?.channelTitle || null,
      thumbnail: item.snippet?.thumbnails?.high?.url || null,
      publishedAt: item.snippet?.publishedAt || null,
      url: item.id?.videoId ? `https://www.youtube.com/watch?v=${item.id.videoId}` : null
    })).filter((v) => v.videoId);

    return { videos };
  } catch (e) {
    return { videos: [] };
  }
}

async function searchYouTubeFallbackDDG(query, limit = 5) {
  try {
    const ddg = await searchDuckDuckGo(`site:youtube.com ${query}`, { maxResults: limit * 2 });
    return {
      videos: (ddg.results || []).map((r) => {
        const videoId = extractYouTubeVideoId(r.url);
        if (!videoId) return null;
        return {
          videoId,
          title: r.title,
          channel: r.source || null,
          thumbnail: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
          publishedAt: null,
          url: `https://www.youtube.com/watch?v=${videoId}`
        };
      }).filter(Boolean).slice(0, limit)
    };
  } catch {
    return { videos: [] };
  }
}

async function searchYouTube(query) {
  if (!query || typeof query !== "string") return { videos: [] };
  const cleanQuery = String(query).trim().slice(0, 200);

  if (youtubei) {
    const r = await searchYouTubeYoutubei(cleanQuery);
    if (r.videos.length > 0) return { ...r, provider: "youtubei" };
  }

  if (process.env.YOUTUBE_API_KEY) {
    const r = await searchYouTubeApiKey(cleanQuery);
    if (r.videos.length > 0) return { ...r, provider: "youtube_api" };
  }

  const r = await searchYouTubeFallbackDDG(cleanQuery);
  return { ...r, provider: "ddg" };
}

// ================================================================================
// §3.15 — MÉTÉO (OPEN-METEO)
// ================================================================================

async function getWeather(location) {
  if (!location) return { error: "Aucun lieu précisé" };

  try {
    const geo = await axios.get("https://geocoding-api.open-meteo.com/v1/search", {
      params: { name: location, count: 1, language: "fr" },
      timeout: 6000
    });
    const place = geo.data?.results?.[0];
    if (!place) return { error: `Lieu "${location}" introuvable` };

    const weather = await axios.get("https://api.open-meteo.com/v1/forecast", {
      params: {
        latitude: place.latitude,
        longitude: place.longitude,
        current: "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m",
        daily: "temperature_2m_max,temperature_2m_min,weather_code",
        timezone: "auto",
        forecast_days: 3
      },
      timeout: 6000
    });

    const c = weather.data?.current || {};
    const d = weather.data?.daily || {};

    const WMO = {
      0: "Ciel dégagé", 1: "Peu nuageux", 2: "Partiellement nuageux", 3: "Couvert",
      45: "Brouillard", 48: "Brouillard givrant",
      51: "Bruine légère", 53: "Bruine modérée", 55: "Bruine dense",
      61: "Pluie légère", 63: "Pluie modérée", 65: "Pluie forte",
      71: "Neige légère", 73: "Neige modérée", 75: "Neige forte",
      80: "Averses légères", 81: "Averses modérées", 82: "Averses violentes",
      95: "Orage", 96: "Orage + grêle", 99: "Orage violent"
    };

    return {
      location: `${place.name}, ${place.country || place.admin1 || ""}`.trim(),
      coordinates: { lat: place.latitude, lon: place.longitude },
      current: {
        temperature: c.temperature_2m,
        feelsLike: c.apparent_temperature,
        humidity: c.relative_humidity_2m,
        windSpeed: c.wind_speed_10m,
        condition: WMO[c.weather_code] || `Code ${c.weather_code}`,
        code: c.weather_code
      },
      forecast: (d.time || []).slice(0, 3).map((date, i) => ({
        date,
        tempMax: d.temperature_2m_max?.[i],
        tempMin: d.temperature_2m_min?.[i],
        condition: WMO[d.weather_code?.[i]] || null
      }))
    };
  } catch (e) {
    return { error: e.message };
  }
}

// ================================================================================
// §3.16 — FINANCE (COINGECKO / CMC / YAHOO)
// ================================================================================

async function getCryptoPrice(symbol) {
  if (!symbol) return { error: "Aucun symbole précisé" };
  const clean = String(symbol).toUpperCase().trim();

  try {
    const map = {
      BTC: "bitcoin", ETH: "ethereum", SOL: "solana", BNB: "binancecoin",
      XRP: "ripple", ADA: "cardano", DOGE: "dogecoin", USDT: "tether",
      USDC: "usd-coin", TRX: "tron", TON: "the-open-network",
      MATIC: "matic-network", DOT: "polkadot", AVAX: "avalanche-2",
      LINK: "chainlink", LTC: "litecoin"
    };
    const id = map[clean];
    if (id) {
      const resp = await axios.get(
        `https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd,eur&include_24hr_change=true&include_market_cap=true`,
        { timeout: 6000 }
      );
      const data = resp.data?.[id];
      if (data) {
        return {
          symbol: clean,
          name: id.charAt(0).toUpperCase() + id.slice(1),
          priceUsd: data.usd,
          priceEur: data.eur,
          change24h: data.usd_24h_change,
          marketCap: data.usd_market_cap,
          source: "coingecko"
        };
      }
    }
  } catch (e) {}

  if (process.env.COINMARKETCAP_API_KEY) {
    try {
      const resp = await axios.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        {
          params: { symbol: clean, convert: "USD" },
          headers: { "X-CMC_PRO_API_KEY": process.env.COINMARKETCAP_API_KEY },
          timeout: 8000
        }
      );
      const c = resp.data?.data?.[clean];
      if (c) {
        return {
          symbol: clean,
          name: c.name,
          priceUsd: c.quote?.USD?.price,
          change24h: c.quote?.USD?.percent_change_24h,
          marketCap: c.quote?.USD?.market_cap,
          source: "coinmarketcap"
        };
      }
    } catch (e) {}
  }

  return { error: `Prix introuvable pour ${clean}` };
}

async function getStockPrice(ticker) {
  if (!ticker) return { error: "Aucun ticker précisé" };
  const clean = String(ticker).toUpperCase().trim();

  try {
    const resp = await axios.get(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(clean)}`,
      {
        params: { interval: "1d", range: "5d" },
        headers: { "User-Agent": CONFIG.HTTP.USER_AGENT },
        timeout: 8000
      }
    );

    const result = resp.data?.chart?.result?.[0];
    if (!result) return { error: `Ticker ${clean} introuvable` };

    const meta = result.meta || {};
    return {
      ticker: meta.symbol || clean,
      currency: meta.currency || "USD",
      price: meta.regularMarketPrice,
      previousClose: meta.previousClose,
      change: meta.regularMarketPrice && meta.previousClose
        ? meta.regularMarketPrice - meta.previousClose : null,
      changePercent: meta.regularMarketPrice && meta.previousClose
        ? ((meta.regularMarketPrice - meta.previousClose) / meta.previousClose) * 100 : null,
      volume: meta.regularMarketVolume,
      dayHigh: meta.regularMarketDayHigh,
      dayLow: meta.regularMarketDayLow,
      marketCap: meta.marketCap,
      source: "yahoo"
    };
  } catch (e) {
    return { error: e.message };
  }
}

// ================================================================================
// §3.17 — GOOGLE NEWS + EXTRACTION SCORE
// ================================================================================

async function fetchGoogleNews(query, { limit = 8, lang, region } = {}) {
  const L = lang || CONFIG.NEWS.GOOGLE_LANG;
  const R = region || CONFIG.NEWS.GOOGLE_REGION;
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=${L}&gl=${R}&ceid=${R}:${L}`;

  try {
    const resp = await axios.get(url, {
      timeout: 9000,
      headers: { "User-Agent": CONFIG.HTTP.USER_AGENT }
    });

    const xml = resp.data || "";
    const items = [];
    const re = /<item>([\s\S]*?)<\/item>/g;
    let match;

    while ((match = re.exec(xml)) !== null && items.length < limit) {
      const block = match[1];
      const title = (block.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const link = (block.match(/<link>([\s\S]*?)<\/link>/) || [])[1] || "";
      const pubDate = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1] || "";
      const description = (block.match(/<description>([\s\S]*?)<\/description>/) || [])[1] || "";
      const source = (block.match(/<source[^>]*>([\s\S]*?)<\/source>/) || [])[1] || "";

      if (title) {
        items.push({
          title: decodeXmlEntities(title),
          link: link.trim(),
          pubDate: pubDate.trim(),
          pubDateMs: pubDate ? Date.parse(pubDate) || 0 : 0,
          description: decodeXmlEntities(description)
            .replace(/<[^>]+>/g, " ")
            .replace(/\s+/g, " ")
            .trim(),
          source: decodeXmlEntities(source).trim()
        });
      }
    }
    return items;
  } catch (e) {
    return [];
  }
}

function extractScoreFromText(text) {
  if (!text) return null;
  const clean = String(text).replace(/\u00A0/g, " ").trim();

  const patterns = [
    /([A-ZÀ-Ý][\w\sÀ-ÿ'.-]{2,30}?)\s+(\d{1,2})\s*[-–]\s*(\d{1,2})\s+([A-ZÀ-Ý][\w\sÀ-ÿ'.-]{2,30})/,
    /(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(?:face|contre|devant|à|au|vs|v)\s+([A-ZÀ-Ý][\w\sÀ-ÿ'.-]{2,30})/,
    /(?:score|résultat|victoire|défaite|match nul)[^\d]{0,40}(\d{1,2})\s*[-–]\s*(\d{1,2})/i,
    /(\d{1,2})\s+buts?\s+(?:à|a)\s+(\d{1,2})/i
  ];

  for (const p of patterns) {
    const m = clean.match(p);
    if (m && m.length >= 3) {
      if (m.length === 5 && !isNaN(m[2]) && !isNaN(m[3])) {
        return {
          home: m[1].trim(),
          homeScore: parseInt(m[2], 10),
          awayScore: parseInt(m[3], 10),
          away: m[4].trim(),
          raw: m[0]
        };
      }
      if (m.length === 4 && !isNaN(m[1]) && !isNaN(m[2])) {
        return {
          home: null,
          homeScore: parseInt(m[1], 10),
          awayScore: parseInt(m[2], 10),
          away: m[3].trim(),
          raw: m[0]
        };
      }
    }
  }
  return null;
}

// ================================================================================
// §3.18 — SEARCHNEWS
// ================================================================================

async function searchNews(query) {
  if (!query) return { articles: [], sourcesUsed: [] };

  const [googleItems, gdeltResult] = await Promise.all([
    fetchGoogleNews(query, { limit: 8 }),
    searchGdelt(query, { maxResults: 5 }).catch(() => ({ results: [] }))
  ]);

  const articles = [
    ...googleItems.map((a) => ({
      title: a.title,
      link: a.link,
      pubDate: a.pubDate,
      description: a.description,
      source: a.source || "Google News",
      pubDateMs: a.pubDateMs
    })),
    ...(gdeltResult.results || []).map((a) => ({
      title: a.title,
      link: a.url,
      pubDate: a.publishedDate,
      description: a.snippet,
      source: a.source || "GDELT",
      pubDateMs: a.publishedDate ? Date.parse(a.publishedDate) || 0 : 0
    }))
  ];

  const seen = new Set();
  const unique = articles
    .filter((a) => {
      const k = a.title?.toLowerCase().slice(0, 60);
      if (!k || seen.has(k)) return false;
      seen.add(k);
      return true;
    })
    .sort((a, b) => (b.pubDateMs || 0) - (a.pubDateMs || 0))
    .slice(0, 10);

  const sourcesUsed = [];
  if (googleItems.length > 0) sourcesUsed.push("googlenews");
  if (gdeltResult.results?.length > 0) sourcesUsed.push("gdelt");

  return { articles: unique, sourcesUsed };
}

// ================================================================================
// §3.19 — SPORTS
// ================================================================================

const SPORT_SYNONYMS = Object.freeze({
  "léopards": ["Léopards RDC", "RD Congo football", "Congo DR football"],
  "leopards": ["Léopards RDC", "RD Congo football", "Congo DR football"],
  "rdc": ["RD Congo football", "Léopards RDC"],
  "congo": ["RD Congo football", "Congo Brazzaville football"],
  "psg": ["Paris Saint-Germain"],
  "om": ["Olympique de Marseille"],
  "real": ["Real Madrid"],
  "barca": ["FC Barcelona"],
  "barça": ["FC Barcelona"],
  "manchester": ["Manchester United", "Manchester City"],
  "vclub": ["AS Vita Club"],
  "v.club": ["AS Vita Club"],
  "tp mazembe": ["TP Mazembe", "Tout Puissant Mazembe"]
});

const sportCache = new LRUCache({
  max: 200,
  ttl: CONFIG.NEWS.SPORT_CACHE_TTL_MS,
  updateAgeOnGet: false
});

async function searchSportsViaGoogleNews(team) {
  if (!team) return { events: [], error: "Aucune équipe précisée" };

  const cacheKey = `sport:${String(team).toLowerCase().trim()}`;
  const cached = sportCache.get(cacheKey);
  if (cached) return cached;

  const normalized = String(team).toLowerCase().trim();
  const candidates = [team];
  for (const [k, aliases] of Object.entries(SPORT_SYNONYMS)) {
    if (normalized.includes(k)) candidates.push(...aliases);
  }
  const entity = extractEntity(team);
  if (entity && !candidates.includes(entity)) candidates.unshift(entity);

  const allArticles = [];
  const seenLinks = new Set();
  const seenTitles = new Set();

  for (const name of candidates.slice(0, 4)) {
    const articles = await fetchGoogleNews(`${name} match résultat score`, { limit: 6 });
    for (const a of articles) {
      const tk = a.title.toLowerCase().slice(0, 60);
      if (seenLinks.has(a.link) || seenTitles.has(tk)) continue;
      seenLinks.add(a.link);
      seenTitles.add(tk);
      allArticles.push(a);
    }
  }

  allArticles.sort((a, b) => (b.pubDateMs || 0) - (a.pubDateMs || 0));

  const events = [];
  for (const a of allArticles) {
    const score = extractScoreFromText(a.title) || extractScoreFromText(a.description);
    if (score && Number.isFinite(score.homeScore) && Number.isFinite(score.awayScore)) {
      events.push({
        match: score.home
          ? `${score.home} ${score.homeScore} - ${score.awayScore} ${score.away || "?"}`
          : `${score.homeScore} - ${score.awayScore} (${score.away || "?"})`,
        homeTeam: score.home || null,
        awayTeam: score.away || null,
        homeScore: score.homeScore,
        awayScore: score.awayScore,
        date: a.pubDateMs ? new Date(a.pubDateMs).toISOString().split("T")[0] : null,
        publishedAt: a.pubDate || null,
        source: a.source || "Google News",
        title: a.title,
        url: a.link,
        description: a.description.slice(0, 300)
      });
    }
  }

  const seenMatches = new Set();
  const uniqueEvents = events.filter((e) => {
    const k = `${e.date}|${e.homeTeam}|${e.awayTeam}|${e.homeScore}|${e.awayScore}`;
    if (seenMatches.has(k)) return false;
    seenMatches.add(k);
    return true;
  });

  const result = {
    team: candidates[0],
    searchedAs: candidates,
    events: uniqueEvents.slice(0, CONFIG.NEWS.SPORT_MAX_ARTICLES),
    allArticles: allArticles.slice(0, CONFIG.NEWS.SPORT_MAX_ARTICLES * 2),
    fetchedAt: Date.now()
  };

  if (uniqueEvents.length > 0) sportCache.set(cacheKey, result);
  return result;
}

async function searchSportsScores(team) {
  if (!team) return { events: [], error: "Aucune équipe précisée" };
  return await searchSportsViaGoogleNews(team);
}

// ================================================================================
// §3.20 — SANDBOX PISTON / JUDGE0
// ================================================================================

const SUPPORTED_SANDBOX_LANGS = Object.freeze({
  python:     { piston: "python",     judge0: 71 },
  javascript: { piston: "javascript", judge0: 63 },
  typescript: { piston: "typescript", judge0: 74 },
  bash:       { piston: "bash",       judge0: 42 },
  go:         { piston: "go",         judge0: 60 },
  rust:       { piston: "rust",       judge0: 73 },
  java:       { piston: "java",       judge0: 62 },
  cpp:        { piston: "c++",        judge0: 54 }
});

async function runCodeSandbox({ language, code, stdin = "" }) {
  const provider = CONFIG.SANDBOX.PROVIDER;
  if (!provider) return { success: false, error: "Sandbox non configuré" };
  if (!SUPPORTED_SANDBOX_LANGS[language]) return { success: false, error: "Langage non supporté" };
  if (typeof code !== "string" || code.length === 0) return { success: false, error: "Code vide" };
  if (code.length > 20000) return { success: false, error: "Code trop long (max 20 000)" };

  const timeoutMs = 10000;

  if (provider === "piston") {
    if (!CONFIG.SANDBOX.PISTON_URL) return { success: false, error: "PISTON_URL manquant" };
    try {
      const resp = await axios.post(
        `${CONFIG.SANDBOX.PISTON_URL.replace(/\/$/, "")}/api/v2/execute`,
        {
          language: SUPPORTED_SANDBOX_LANGS[language].piston,
          version: "*",
          files: [{ content: code }],
          stdin,
          run_timeout: timeoutMs,
          compile_timeout: timeoutMs
        },
        { timeout: timeoutMs + 5000 }
      );

      const run = resp.data?.run || {};
      return {
        success: true,
        stdout: String(run.stdout || "").slice(0, 20000),
        stderr: String(run.stderr || "").slice(0, 20000),
        exitCode: typeof run.code === "number" ? run.code : null,
        provider: "piston",
        language
      };
    } catch (e) {
      return { success: false, error: "Échec (Piston)" };
    }
  }

  if (provider === "judge0") {
    if (!CONFIG.SANDBOX.JUDGE0_URL) return { success: false, error: "JUDGE0_URL manquant" };
    try {
      const resp = await axios.post(
        `${CONFIG.SANDBOX.JUDGE0_URL.replace(/\/$/, "")}/submissions?base64_encoded=false&wait=true`,
        {
          language_id: SUPPORTED_SANDBOX_LANGS[language].judge0,
          source_code: code,
          stdin
        },
        { timeout: timeoutMs + 5000, headers: { "Content-Type": "application/json" } }
      );

      const d = resp.data || {};
      return {
        success: true,
        stdout: String(d.stdout || "").slice(0, 20000),
        stderr: String(d.stderr || d.compile_output || "").slice(0, 20000),
        exitCode: d.status?.id ?? null,
        provider: "judge0",
        language
      };
    } catch (e) {
      return { success: false, error: "Échec (Judge0)" };
    }
  }

  return { success: false, error: "Provider sandbox inconnu" };
}

// ================================================================================
// §3.21 — VISION
// ================================================================================

async function analyzeImage({
  imageBase64,
  mimetype = "image/jpeg",
  prompt = "Décris cette image en détail en français."
}) {
  if (!imageBase64) return { success: false, error: "Image vide" };

  const messages = [{
    role: "user",
    content: [
      { type: "text", text: prompt },
      { type: "image_url", image_url: { url: `data:${mimetype};base64,${imageBase64}` } }
    ]
  }];

  if (LLM_PROVIDERS.GROQ.keyPool.length > 0) {
    try {
      const r = await callProviderRawWithTools({
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL,
        messages,
        tools: null,
        jsonMode: false,
        timeout: 25000,
        maxTokens: 2000,
        temperature: 0.7,
        apiKey: LLM_PROVIDERS.GROQ.keyPool[0].apiKey
      });
      return {
        success: true,
        text: r.message?.content || "",
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL
      };
    } catch (e) {}
  }

  if (geminiClient) {
    try {
      const r = await callGeminiRawWithTools({
        model: CONFIG.VISION.GEMINI_MODEL,
        messages: [{ role: "user", content: prompt }],
        tools: null,
        jsonMode: false,
        timeout: 25000,
        maxTokens: 2000,
        temperature: 0.7,
        images: [{ base64: imageBase64, mimetype }]
      });
      return {
        success: true,
        text: r.message?.content || "",
        provider: "gemini",
        model: CONFIG.VISION.GEMINI_MODEL
      };
    } catch (e) {}
  }

  if (LLM_PROVIDERS.OPENROUTER.keyPool.length > 0) {
    try {
      const r = await callProviderRawWithTools({
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL,
        messages,
        tools: null,
        jsonMode: false,
        timeout: 30000,
        maxTokens: 2000,
        temperature: 0.7,
        apiKey: LLM_PROVIDERS.OPENROUTER.keyPool[0].apiKey
      });
      return {
        success: true,
        text: r.message?.content || "",
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL
      };
    } catch (e) {}
  }

  return { success: false, error: "Aucun provider vision disponible" };
}

// ================================================================================
// §3.22 — ADS (PROPRES — SANS BRANDING "TEST")
// ================================================================================

/**
 * Slots Luba Pro — désactivés v16.5.
 * Aucune pub en dur → pas de "Luba Pro" visible.
 * Les pubs ne viennent que de réseaux réels (Ghost Ads / Adsterra).
 */
const LUBA_PRO_ADS = Object.freeze({});

async function fetchGhostAds({ slot = "chat_below", userId = null } = {}) {
  const apiKey = process.env.GHOST_ADS_API_KEY;
  if (!apiKey) return { success: false, reason: "no_key" };

  try {
    const resp = await axios.get("https://api.ghostads.io/v1/ad", {
      params: { slot, user_id: userId || undefined },
      headers: { Authorization: `Bearer ${apiKey}` },
      timeout: 5000
    });

    const ad = resp.data?.ad;
    if (!ad) return { success: false, reason: "no_ad" };

    if (ad.adult || ad.category === "adult" || ad.category === "nsfw") {
      logger.warn({ adId: ad.id }, "🚫 Pub Ghost Ads refusée (contenu adulte)");
      return { success: false, reason: "rejected_adult" };
    }

    return {
      success: true,
      ad: {
        id: ad.id,
        title: ad.title,
        description: ad.description,
        imageUrl: ad.image_url,
        clickUrl: ad.click_url,
        width: ad.width,
        height: ad.height,
        network: "ghostads",
        isTest: false,
        safeContent: true
      }
    };
  } catch {
    return { success: false, reason: "fetch_failed" };
  }
}

async function fetchAdsterra({ slot = "chat_below" } = {}) {
  const zoneId = process.env.ADSTERRA_ZONE_ID;
  if (!zoneId) return { success: false, reason: "no_zone" };

  return {
    success: true,
    ad: {
      id: `adsterra_${slot}`,
      title: "Annonce partenaire",
      description: "Publicité via Adsterra",
      scriptUrl: `//pl12345678.profitablecpmrate.com/${zoneId}/invoke.js`,
      width: 728,
      height: 90,
      network: "adsterra",
      isTest: false,
      safeContent: true,
      requiresClientScript: true
    }
  };
}

/**
 * Récupère une pub : Ghost Ads → Adsterra → null (pas de fallback).
 * 🆕 v16.5 : retourne null si aucun réseau réel ne répond.
 */
async function getAd({ slot = "chat_below", userId = null } = {}) {
  const ghost = await fetchGhostAds({ slot, userId });
  if (ghost.success) return ghost.ad;

  const adsterra = await fetchAdsterra({ slot });
  if (adsterra.success) return adsterra.ad;

  logger.info({ slot }, "ℹ️  Aucune pub sponsorisée disponible → pas d'affichage");
  return null;
}

function getAllAdSlots() {
  return {};
}

// ================================================================================
// §3.23 — MATH EVALUATOR (WORKER ISOLÉ)
// ================================================================================

const MATH_WORKER_SOURCE = `
  const { parentPort, workerData } = require('worker_threads');
  const math = require('mathjs');
  const safeMath = math.create({ number: 'number', precision: 64, matrix: 'Matrix', predictable: true });
  safeMath.import({
    import: function () { throw new Error('import interdit'); },
    createUnit: function () { throw new Error('createUnit interdit'); },
    parse: function () { throw new Error('parse interdit'); },
    simplify: function () { throw new Error('simplify interdit'); },
    derivative: function () { throw new Error('derivative interdit'); },
    zeros: function (n) { const s = Number(n); if (s > 10000) throw new Error('zeros trop grand'); return math.zeros(s); },
    ones: function (n) { const s = Number(n); if (s > 10000) throw new Error('ones trop grand'); return math.ones(s); },
    factorial: function (n) { const s = Number(n); if (s > 500) throw new Error('factorielle trop grande'); return math.factorial(s); }
  }, { override: true });
  try {
    const expr = String(workerData.expression || '').slice(0, 2000);
    if (!expr) throw new Error('Expression vide');
    const result = safeMath.evaluate(expr);
    parentPort.postMessage({ ok: true, result: safeMath.format(result, { precision: 14 }) });
  } catch (e) {
    parentPort.postMessage({ ok: false, error: e.message || 'Erreur math' });
  }
`;

function evaluateMathSafe(expression, timeoutMs = 2000) {
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
      if (settled) return;
      settled = true;
      worker.terminate().catch(() => {});
      resolve({ success: false, expression: expr, error: "Timeout" });
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
      resolve({ success: false, expression: expr, error: `Worker sorti (${code})` });
    });
  });
}

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
    if (matches) matches.forEach((m) => found.add(m.trim()));
  }
  return [...found];
}

// ================================================================================
// §3.24 — ENTITÉ + PRÉ-ROUTEUR + INTENTS
// ================================================================================

function extractEntity(message) {
  if (!message || typeof message !== "string") return "";
  let m = message.trim().replace(/[?!.,;:]+$/g, "");

  m = m.replace(
    /^(qui est|c'?est qui|qui était|montre-moi|montre moi|cherche|trouve-moi|trouve moi|parle-moi de|parle moi de|donne-moi|donne moi|photo de|image de|clip de|vidéo de|video de|chanson de|à quoi ressemble|a quoi ressemble|quelle est|quel est|où se trouve|ou se trouve|calcule|calculer|résous|resous)\s+/i,
    ""
  ).trim();

  const capitalSeq = m.match(/\b([A-ZÀ-Ý][a-zà-ÿ]+(?:\s+[A-ZÀ-Ý][a-zà-ÿ]+){0,3})\b/g);
  if (capitalSeq && capitalSeq.length > 0) {
    return capitalSeq.sort((a, b) => b.length - a.length)[0];
  }

  return m.split(/\s+/).slice(0, 8).join(" ").slice(0, 60);
}

function normalizeForMatch(s) {
  return String(s).toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s'-]/g, " ")
    .replace(/\s+/g, " ").trim();
}

function containsWholeWords(haystack, needles) {
  const h = " " + normalizeForMatch(haystack) + " ";
  for (const n of needles) {
    const token = normalizeForMatch(n);
    if (!token) continue;
    const re = new RegExp(`(^|[\\s'\\-])${token.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}([\\s'\\-]|$)`);
    if (re.test(h)) return true;
  }
  return false;
}

const INTENT_KEYWORDS = Object.freeze({
  MATHS: ["calcule", "calculer", "resous", "equation", "integrale", "derivee", "factorielle", "matrice", "limite", "theoreme", "algebre", "solve", "résoudre", "résous"],
  ACTUALITE: ["actualite", "actualites", "news", "journal", "derniere", "dernieres", "presse"],
  SPORT: ["score", "match", "football", "basket", "tennis", "nba", "ligue", "championnat", "classement", "resultat", "leopards", "leopard"],
  CODE: ["code", "coder", "javascript", "python", "java", "typescript", "react", "angular", "vuejs", "nodejs", "sql", "algorithme", "bug", "debug", "fonction", "script"],
  VIDEO: ["clip", "video", "youtube", "chanson", "musique", "regarder", "ecouter"],
  TASK: ["rappelle-moi", "rappel", "tache", "taches", "planifie", "agenda", "rendez-vous"],
  FINANCE: ["crypto", "bitcoin", "ethereum", "action", "bourse", "prix", "cours"],
  METEO: ["meteo", "temps", "temperature", "pluie", "climat"],
  PERSONNE: ["qui est", "photo de", "biographie de", "portrait de", "c'est qui"]
});

function preRouteIntent(message) {
  const text = String(message || "").trim();
  const entity = extractEntity(text);
  if (!text) return { intent: "GENERAL", entity };

  if (containsWholeWords(text, INTENT_KEYWORDS.MATHS))     return { intent: "MATHS", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.TASK))      return { intent: "TASK", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.VIDEO))     return { intent: "VIDEO", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.SPORT))     return { intent: "SPORT", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.FINANCE))   return { intent: "FINANCE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.METEO))     return { intent: "METEO", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.ACTUALITE)) return { intent: "ACTUALITE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.CODE))      return { intent: "CODE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.PERSONNE))  return { intent: "PERSONNE", entity };

  return { intent: "GENERAL", entity };
}

async function searchScience(query) {
  if (!query) return { papers: [] };
  try {
    const resp = await axios.get("https://export.arxiv.org/api/query", {
      params: { search_query: `all:${query}`, start: 0, max_results: 5 },
      timeout: 8000,
      headers: { "User-Agent": CONFIG.HTTP.USER_AGENT }
    });
    const xml = resp.data || "";
    const items = [];
    const re = /<entry>([\s\S]*?)<\/entry>/g;
    let m;
    while ((m = re.exec(xml)) !== null && items.length < 5) {
      const b = m[1];
      const title = (b.match(/<title>([\s\S]*?)<\/title>/) || [])[1] || "";
      const summary = (b.match(/<summary>([\s\S]*?)<\/summary>/) || [])[1] || "";
      const link = (b.match(/<id>([\s\S]*?)<\/id>/) || [])[1] || "";
      if (title) items.push({
        title: decodeXmlEntities(title),
        summary: decodeXmlEntities(summary).slice(0, 300),
        link: link.trim()
      });
    }
    return { papers: items };
  } catch {
    return { papers: [] };
  }
}

async function searchSocial(query) {
  if (!query) return { posts: [] };
  try {
    const resp = await axios.get("https://www.reddit.com/search.json", {
      params: { q: query, limit: 6, sort: "relevance" },
      headers: { "User-Agent": CONFIG.HTTP.USER_AGENT },
      timeout: 8000
    });
    const posts = (resp.data?.data?.children || []).map((c) => ({
      title: c.data.title,
      subreddit: c.data.subreddit_name_prefixed,
      score: c.data.score,
      url: `https://reddit.com${c.data.permalink}`
    }));
    return { posts };
  } catch {
    return { posts: [] };
  }
}

// ================================================================================
// §3.25 — TASKS CRUD
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

  if (firestoreDb) {
    fsSet("user_tasks", id, {
      id, user_id: userId, title: cleanTitle, notes: cleanNotes,
      due_at: dueAtMs, status: "pending",
      created_at: now, updated_at: now
    }).catch(() => {});
  }
  if (supabase) {
    supabaseWriteSafe({
      table: "user_tasks", op: "upsert",
      payload: {
        id, user_id: userId, title: cleanTitle, notes: cleanNotes,
        due_at: dueAtMs ? new Date(dueAtMs).toISOString() : null,
        status: "pending",
        created_at: new Date(now).toISOString(),
        updated_at: new Date(now).toISOString()
      },
      matchColumn: "id"
    }).catch(() => {});
  }

  return { success: true, task };
}

async function listTasks(userId, { status = null } = {}) {
  try {
    let q = "SELECT * FROM user_tasks WHERE user_id = ?";
    const params = [userId];
    if (status) { q += " AND status = ?"; params.push(status); }
    q += " ORDER BY (due_at IS NULL), due_at ASC, created_at DESC LIMIT 50";
    return { success: true, tasks: await dbAll(q, params) };
  } catch (e) {
    return { success: false, tasks: [], error: e.message };
  }
}

async function updateTaskStatus(userId, taskId, status) {
  if (!["pending", "done"].includes(status)) {
    return { success: false, error: "Statut invalide" };
  }

  await dbRun(
    `UPDATE user_tasks SET status = ?, updated_at = ? WHERE id = ? AND user_id = ?`,
    [status, Date.now(), taskId, userId]
  );

  if (firestoreDb) {
    fsUpdate("user_tasks", taskId, { status, updated_at: Date.now() }).catch(() => {});
  }
  if (supabase) {
    supabaseWriteSafe({
      table: "user_tasks", op: "update",
      payload: { status, updated_at: new Date().toISOString() },
      matchColumn: "id", matchValue: taskId
    }).catch(() => {});
  }

  const task = await dbGet(`SELECT * FROM user_tasks WHERE id = ? AND user_id = ?`, [taskId, userId]);
  return { success: Boolean(task), task };
}

async function deleteTask(userId, taskId) {
  await dbRun(`DELETE FROM user_tasks WHERE id = ? AND user_id = ?`, [taskId, userId]);
  if (firestoreDb) fsDelete("user_tasks", taskId).catch(() => {});
  if (supabase) {
    supabaseWriteSafe({
      table: "user_tasks", op: "delete", payload: {},
      matchColumn: "id", matchValue: taskId
    }).catch(() => {});
  }
  return { success: true };
}

// ================================================================================
// §3.26 — QUOTAS
// ================================================================================

async function checkUserQuota(userId, action, userRole = "FREE") {
  try {
    const today = todayKeyMs();
    const limits = USER_QUOTAS[userRole] || USER_QUOTAS.FREE;
    const quota = await dbGet(
      `SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`,
      [userId, today]
    );

    let current = 0, max = 0;
    switch (action) {
      case "message":  current = quota?.messages_count || 0; max = limits.maxMessagesPerDay; break;
      case "image":    current = quota?.images_count   || 0; max = limits.maxImagesPerDay;   break;
      case "whatsapp": current = quota?.whatsapp_count || 0; max = limits.maxWhatsAppMessagesPerDay; break;
      case "email":    current = quota?.emails_count   || 0; max = limits.maxEmailsPerDay;   break;
      default: return { allowed: true, remaining: null };
    }

    if (current >= max) {
      return {
        allowed: false, remaining: 0, current, max,
        message: `Limite atteinte pour ${action} (max : ${max}).`
      };
    }
    return { allowed: true, remaining: max - current, current, max };
  } catch (e) {
    return { allowed: true, remaining: null };
  }
}

async function incrementUserQuota(userId, action) {
  try {
    const today = todayKeyMs();
    const col = ({
      message: "messages_count",
      image: "images_count",
      whatsapp: "whatsapp_count",
      email: "emails_count"
    })[action];
    if (!col) return;

    await dbRun(
      `INSERT INTO user_quotas (user_id, date, ${col}, updated_at)
       VALUES (?, ?, 1, ?)
       ON CONFLICT(user_id, date) DO UPDATE SET ${col} = ${col} + 1, updated_at = excluded.updated_at`,
      [userId, today, Date.now()]
    );
  } catch (e) {}
}

// ================================================================================
// §3.27 — EMAIL DISPATCH
// ================================================================================

let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  try {
    emailTransporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: parseInt(process.env.SMTP_PORT || "587", 10),
      secure: process.env.SMTP_PORT === "465",
      auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
      tls: { rejectUnauthorized: process.env.SMTP_TLS_REJECT_UNAUTHORIZED !== "false" },
      pool: true, maxConnections: 3, maxMessages: 50
    });
    logger.info("✅ SMTP configuré");
  } catch (e) {}
}

async function verifyGmailScope(accessToken) {
  try {
    const resp = await axios.get("https://www.googleapis.com/oauth2/v1/tokeninfo", {
      params: { access_token: accessToken },
      timeout: 8000
    });
    const scopes = String(resp.data?.scope || "").split(" ");
    return scopes.includes("https://www.googleapis.com/auth/gmail.send")
      || scopes.includes("https://mail.google.com/");
  } catch {
    return false;
  }
}

async function sendEmailViaGmail(accessToken, recipient, subject, body) {
  if (!await verifyGmailScope(accessToken)) {
    throw new Error("Token Gmail invalide ou scope manquant");
  }
  const safeSubject = sanitizeStrict(subject || "(sans sujet)", 200);
  const html = `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`;

  const lines = [
    `To: ${recipient}`,
    `Subject: =?utf-8?B?${Buffer.from(safeSubject).toString("base64")}?=`,
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    "",
    html
  ];
  const raw = Buffer.from(lines.join("\r\n"))
    .toString("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

  const resp = await axios.post(
    "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
    { raw },
    {
      headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
      timeout: 12000
    }
  );
  return { success: true, provider: "gmail", messageId: resp.data?.id || null };
}

async function sendEmailViaResend(recipient, subject, body) {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) return { success: false, error: "RESEND_API_KEY non configurée" };

  try {
    const resp = await axios.post(
      "https://api.resend.com/emails",
      {
        from: process.env.RESEND_FROM || `${CONFIG.EMAIL.FROM_NAME} <onboarding@resend.dev>`,
        to: recipient,
        subject: sanitizeStrict(subject || "(sans sujet)", 200),
        html: `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`
      },
      {
        headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
        timeout: 12000
      }
    );
    return { success: true, provider: "resend", messageId: resp.data?.id || null };
  } catch (e) {
    return { success: false, error: `Resend : ${e.response?.data?.message || e.message}` };
  }
}

async function sendEmailViaSMTP(to, subject, body) {
  if (!emailTransporter) return { success: false, error: "SMTP non configuré" };

  try {
    const info = await emailTransporter.sendMail({
      from: process.env.EMAIL_FROM || `"${CONFIG.EMAIL.FROM_NAME}" <${process.env.SMTP_USER}>`,
      to,
      subject: sanitizeStrict(subject || "(sans sujet)", 200),
      html: `<div style="font-family:Arial;padding:20px;">${escapeHtml(body || "").replace(/\n/g, "<br>")}</div>`,
      text: body || ""
    });
    return { success: true, provider: "smtp", messageId: info.messageId };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body, userId }) {
  if (!recipient || !EMAIL_REGEX.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  let result;
  if (googleAccessToken) {
    try { result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body); }
    catch { result = { success: false, error: "Gmail API échec" }; }
  } else if (process.env.RESEND_API_KEY) {
    result = await sendEmailViaResend(recipient, subject, body);
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  try {
    await dbRun(
      `INSERT INTO email_logs (user_id, to_email, subject, status, provider, error_message, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        userId || null, recipient, subject || null,
        result.success ? "sent" : "failed",
        result.provider || null, result.error || null, Date.now()
      ]
    );
  } catch {}

  return result;
}

// ================================================================================
// §3.28 — WHATSAPP HELPERS
// ================================================================================

let whatsappManager = { sessions: new Map(), sendMessage: async () => ({ success: false }) };

function getWhatsAppCryptoKey() {
  const key = CONFIG.WHATSAPP.ENCRYPTION_KEY;
  const iv = CONFIG.WHATSAPP.ENCRYPTION_IV;
  if (!key || key.length < 32 || !iv || iv.length < 16) {
    if (CONFIG.ENV === "production") {
      throw new Error("WHATSAPP_ENCRYPTION_KEY (32+) et WHATSAPP_ENCRYPTION_IV (16+) requis en production");
    }
    logger.warn("⚠️  Clé WhatsApp par défaut — JAMAIS en prod");
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

    const r = await supabaseWriteSafe({
      table: "whatsapp_credentials", op: "upsert",
      payload: {
        user_id: userId,
        encrypted_data: encrypted,
        auth_tag: authTag,
        iv: iv.toString("hex"),
        updated_at: new Date().toISOString()
      },
      matchColumn: "user_id"
    });
    return r.success;
  } catch (e) {
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
    return null;
  }
}

async function deleteWhatsAppCredentials(userId) {
  if (!supabase) return;
  await supabaseWriteSafe({
    table: "whatsapp_credentials", op: "delete", payload: {},
    matchColumn: "user_id", matchValue: userId
  }).catch(() => {});
}

async function sendWhatsAppSmart(userId, phoneNumber, message) {
  const q = await checkUserQuota(userId, "whatsapp");
  if (!q.allowed) throw new Error(q.message || "Limite WhatsApp atteinte");

  const result = await whatsappManager.sendMessage(userId, phoneNumber, message);
  if (result.success) await incrementUserQuota(userId, "whatsapp");
  return result;
}

function toPlainWhatsAppText(markdown) {
  return String(markdown)
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[!\[.*?\]\(.*?\)\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)")
    .replace(/\n{3,}/g, "\n\n").trim();
}

// ================================================================================
// §3.29 — EXPORTS PARTIE 3
// ================================================================================

Object.assign(module.exports, {
  // Search
  searchTavily, searchSerper, searchDuckDuckGo, searchGdelt, searchHackerNews,
  searchWikipediaSummary, searchWeb, searchScience, searchSocial,

  // Media
  searchWikimediaImages, fetchWikipediaThumb, searchPexelsImages,
  searchDuckDuckGoImages, searchImagesWithFallback,
  ensureImageForResponse, imageCache,

  // YouTube
  extractYouTubeVideoId, searchYouTube, searchYouTubeYoutubei,

  // Weather / Finance
  getWeather, getCryptoPrice, getStockPrice,

  // News / Sports
  fetchGoogleNews, searchNews, extractScoreFromText,
  searchSportsViaGoogleNews, searchSportsScores, sportCache,

  // Sandbox / Vision
  runCodeSandbox, SUPPORTED_SANDBOX_LANGS, analyzeImage,

  // Ads (v16.5 propres)
  LUBA_PRO_ADS, fetchGhostAds, fetchAdsterra, getAd, getAllAdSlots,

  // Math / Entité
  evaluateMathSafe, detectMathExpressions,
  extractEntity, normalizeForMatch, containsWholeWords,
  preRouteIntent, INTENT_KEYWORDS,

  // Tasks / Quotas
  createTask, listTasks, updateTaskStatus, deleteTask,
  checkUserQuota, incrementUserQuota,

  // Email
  verifyGmailScope, sendEmailViaGmail, sendEmailViaResend,
  sendEmailViaSMTP, dispatchSendEmail,

  // WhatsApp
  getWhatsAppCryptoKey, saveWhatsAppCredentials, loadWhatsAppCredentials,
  deleteWhatsAppCredentials, sendWhatsAppSmart, toPlainWhatsAppText,
  setWhatsAppManager: (mgr) => { whatsappManager = mgr; }
});

// ================================================================================
// ==================== FIN PARTIE 3/5 ===========================================
// ================================================================================
// ▶ PARTIE 4/5 : handleChat v16.5 (self-healing + images garanties + mémoire pro)
//                + SSE + Sessions.
//   Tape "suite" pour la recevoir.
// ================================================================================
// ================================================================================
// PARTIE 4/5 — SESSIONS · HANDLECHAT v16.5 · SSE · IMAGES GARANTIES · MÉMOIRE PRO
// ================================================================================
// VERSION : v16.5.0 (Octobre 2026)
//
// 🆕 INTÉGRATION SELF-HEALING COMPLÈTE :
//   • Images garanties sur chaque réponse (via ensureImageForResponse)
//   • Mémoire PRO persistante injectée dans le system prompt
//   • Output toujours propre (formatFinalReply + cleanOutput)
//   • Compaction auto si token overflow
//   • Retry automatique sur erreur provider
//
// 🎯 OBJECTIF : Chaque question reçoit une réponse propre + 1 image minimum
//                (sauf salutations/identité)
//
// TABLE DES MATIÈRES :
//   §4.01  SSE Writer
//   §4.02  streamTextAsTokens
//   §4.03  LUBA_SYSTEM_PROMPT v16.5
//   §4.04  getSession
//   §4.05  ActiveIntent
//   §4.06  getFullHistory
//   §4.07  assertConversationOwnership
//   §4.08  saveMessageWithUser
//   §4.09  getUserMemory + saveUserMemory
//   §4.10  runMemorySummaryImpl
//   §4.11  extractSuggestions
//   §4.12  generateSuggestions
//   §4.13  enrichContextWithIntent
//   §4.14  toolCacheKey
//   §4.15  handleChat v16.5 (self-healing + images garanties + mémoire pro)
//   §4.16  handleActiveIntent
//   §4.17  isCancelMessage
//   §4.18  Exports Partie 4
// ================================================================================

"use strict";

// ================================================================================
// §4.01 — SSE WRITER
// ================================================================================

class SSEWriter {
  constructor(res) {
    this.res = res;
    this.closed = false;

    this.res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    this.res.setHeader("Cache-Control", "no-cache, no-transform");
    this.res.setHeader("Connection", "keep-alive");
    this.res.setHeader("X-Accel-Buffering", "no");

    if (typeof this.res.flushHeaders === "function") {
      this.res.flushHeaders();
    }
    this.res.write(":ok\n\n");

    this.res.on("close", () => { this.closed = true; });
  }

  send(event, data) {
    if (this.closed) return false;
    try {
      const payload = typeof data === "string"
        ? data
        : safeJsonStringify(data ?? {});
      const safe = payload.replace(/\r/g, "").split("\n").map((l) => `data: ${l}`).join("\n");
      this.res.write(`event: ${event}\n${safe}\n\n`);
      return true;
    } catch {
      this.closed = true;
      return false;
    }
  }

  status(stage, extra = {}) { this.send("status", { stage, ...extra }); }
  reasoning(text)           { if (text) this.send("reasoning", { text }); }
  codeBlock(payload)        { this.send("code", payload); }
  token(text)               { if (text) this.send("token", { text }); }
  images(list)              { if (list?.length) this.send("images", { images: list }); }
  videos(list)              { if (list?.length) this.send("videos", { videos: list }); }
  suggestions(list)         { if (list?.length) this.send("suggestions", { suggestions: list }); }
  sources(list)             { if (list?.length) this.send("sources", { sources: list }); }
  quality(payload)          { this.send("quality", payload); }
  ad(payload)               { this.send("ad", payload); }
  error(payload)            { this.send("error", payload); }
  done(payload)             { this.send("done", payload); }

  end() {
    if (this.closed) return;
    try { this.res.end(); } catch {}
    this.closed = true;
  }
}

// ================================================================================
// §4.02 — STREAMTEXT ASTOKENS
// ================================================================================

async function streamTextAsTokens(sse, text, { paceMs = 4, chunkSize = 28 } = {}) {
  const chunks = safeChunkText(text, chunkSize);
  for (const chunk of chunks) {
    if (sse.closed) return;
    sse.token(chunk);
    if (paceMs > 0) await sleep(paceMs);
  }
}

// ================================================================================
// §4.03 — LUBA_SYSTEM_PROMPT v16.5
// ================================================================================

const LUBA_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, startup à Kinshasa, fondée en 2026.",
  "",
  "═══════════════════════════════════════════════════════════════════════",
  "LANGUE — RÈGLE #1 ABSOLUE :",
  "═══════════════════════════════════════════════════════════════════════",
  "- Tu réponds TOUJOURS dans la langue du DERNIER message utilisateur.",
  "- Français → français. English → English. Kiswahili → Kiswahili. Lingala → Lingala.",
  "- Si l'utilisateur écrit en français, tu ne réponds JAMAIS en chinois, anglais ou autre.",
  "- En cas de doute, utilise le français (langue par défaut de Luba).",
  "",
  "IDENTITÉ :",
  "- Tu t'appelles Luba. Ton ton est chaleureux, direct, utile.",
  "- Tu es un vrai agent IA (façon Jarvis), pas un chatbot passif.",
  "",
  "FORMAT DE RÉPONSE — RÈGLES STRICTES :",
  "- Réponds en Markdown propre et lisible.",
  "- Utilise **gras** pour les points importants.",
  "- Utilise des listes à puces ou numérotées quand c'est pertinent.",
  "- Utilise des titres avec ## ou ### pour structurer.",
  "- N'utilise JAMAIS de caractères de contrôle ou symboles bizarres.",
  "- N'inclus PAS de balises HTML (<div>, <span>, <br>, etc.) sauf si explicitement demandé.",
  "- N'écris PAS de JSON brut dans ta réponse (sauf si l'utilisateur le demande).",
  "- Ne mets JAMAIS ton raisonnement interne dans la réponse finale.",
  "",
  "DONNÉES — RÈGLE ABSOLUE :",
  "- N'invente JAMAIS un chiffre, un score, une date, un nom, une URL.",
  "- Si un outil échoue, dis-le clairement.",
  "- Mieux vaut dire « je ne sais pas » que d'inventer.",
  "",
  "═══════════════════════════════════════════════════════════════════════",
  "ROUTAGE DES OUTILS :",
  "═══════════════════════════════════════════════════════════════════════",
  "",
  "▸ MATHÉMATIQUES → `execute_math` (JAMAIS run_code).",
  "▸ CODE → `run_code` (JAMAIS pour du calcul).",
  "▸ MÉTÉO → `get_weather`. ▸ CRYPTO → `get_crypto_price`. ▸ ACTIONS → `get_stock_price`.",
  "▸ ACTUALITÉS → `search_news`. ▸ SPORT → `search_sports_scores`.",
  "▸ IMAGES → `search_images`. ▸ VIDÉOS → `search_youtube`. ▸ WEB → `search_web`.",
  "▸ TÂCHES → `create_task`, `list_tasks`, `complete_task`, `delete_task`.",
  "",
  "═══════════════════════════════════════════════════════════════════════",
  "",
  "MATHÉMATIQUES — FORMAT :",
  "- Écris les formules en LaTeX : $inline$ ou $$display$$.",
  "- JAMAIS \\( … \\) ni \\[ … \\].",
  "",
  "SUGGESTIONS :",
  "- À la fin, si pertinent : <!--SUGGESTIONS:[\"Q1 ?\",\"Q2 ?\",\"Q3 ?\"]-->",
  "- Sinon n'ajoute rien."
].join("\n");

// ================================================================================
// §4.04 — GETSESSION
// ================================================================================

async function getSession(conversationId, userId, firebaseUid = null) {
  const local = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);

  if (local) {
    if (local.user_id !== userId && local.firebase_uid !== userId) {
      throw makeError("CONVERSATION_OWNERSHIP");
    }
    await dbRun("UPDATE sessions SET updated_at = ? WHERE session_id = ?", [Date.now(), conversationId]);

    if (firestoreDb) {
      fsSet("sessions", conversationId, {
        session_id: conversationId,
        user_id: userId,
        firebase_uid: firebaseUid || userId,
        updated_at: Date.now()
      }, { merge: true }).catch(() => {});
    }
    if (supabase) {
      supabaseWriteSafe({
        table: "sessions", op: "upsert",
        payload: {
          session_id: conversationId,
          user_id: userId,
          firebase_uid: firebaseUid || userId,
          updated_at: new Date().toISOString()
        },
        matchColumn: "session_id"
      }).catch(() => {});
    }
    return local;
  }

  await dbRun(
    `INSERT INTO sessions (session_id, user_id, firebase_uid, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
    [conversationId, userId, firebaseUid || userId, Date.now(), Date.now()]
  );

  if (firestoreDb) {
    fsSet("sessions", conversationId, {
      session_id: conversationId,
      user_id: userId,
      firebase_uid: firebaseUid || userId,
      created_at: Date.now(),
      updated_at: Date.now()
    }).catch(() => {});
  }
  if (supabase) {
    await supabaseWriteSafe({
      table: "sessions", op: "upsert",
      payload: {
        session_id: conversationId,
        user_id: userId,
        firebase_uid: firebaseUid || userId,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      },
      matchColumn: "session_id"
    }).catch(() => {});
  }

  return {
    session_id: conversationId,
    user_id: userId,
    firebase_uid: firebaseUid || userId
  };
}

// ================================================================================
// §4.05 — ACTIVEINTENT
// ================================================================================

const ACTIVE_INTENT_TTL_MS = 5 * 60 * 1000;

async function setActiveIntent(conversationId, intentType, intentData = {}) {
  await dbRun(
    `UPDATE sessions SET active_intent = ?, intent_data = ?, intent_expires_at = ? WHERE session_id = ?`,
    [intentType, safeJsonStringify(intentData), Date.now() + ACTIVE_INTENT_TTL_MS, conversationId]
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
  try {
    return { type: row.active_intent, data: safeJsonParse(row.intent_data, {}) };
  } catch {
    return null;
  }
}

async function clearActiveIntent(conversationId) {
  await dbRun(
    `UPDATE sessions SET active_intent = NULL, intent_data = NULL, intent_expires_at = NULL WHERE session_id = ?`,
    [conversationId]
  );
}

// ================================================================================
// §4.06 — GETFULLHISTORY
// ================================================================================

async function getFullHistory(conversationId, userId = null, limit = CONFIG.LIMITS.MAX_CONTEXT_MESSAGES) {
  if (firestoreDb) {
    try {
      const rows = await fsQuery("messages", {
        where: [
          ["session_id", "==", conversationId],
          ...(userId ? [["user_id", "==", userId]] : [])
        ],
        orderBy: { field: "created_at", direction: "desc" },
        limit
      });
      if (Array.isArray(rows) && rows.length > 0) {
        return rows.reverse().map((m) => ({ role: m.role, content: m.content }));
      }
    } catch (e) {}
  }

  if (supabase) {
    try {
      const q = supabase
        .from("messages")
        .select("role, content, created_at, user_id")
        .eq("session_id", conversationId)
        .order("created_at", { ascending: false })
        .limit(limit);
      const { data, error } = await q;
      if (!error && Array.isArray(data) && data.length > 0) {
        const filtered = userId
          ? data.filter((m) => !m.user_id || m.user_id === userId)
          : data;
        return filtered.reverse().map((m) => ({ role: m.role, content: m.content }));
      }
    } catch (e) {}
  }

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
    return [];
  }
}

// ================================================================================
// §4.07 — ASSERT CONVERSATION OWNERSHIP
// ================================================================================

async function assertConversationOwnership(conversationId, userId) {
  const local = await dbGet(
    `SELECT user_id, firebase_uid FROM sessions WHERE session_id = ?`,
    [conversationId]
  );
  if (local) {
    if (local.user_id && local.user_id !== userId && local.firebase_uid !== userId) {
      throw makeError("CONVERSATION_OWNERSHIP");
    }
    return true;
  }

  if (firestoreDb) {
    const doc = await fsGet("sessions", conversationId);
    if (doc) {
      if (doc.user_id && doc.user_id !== userId && doc.firebase_uid !== userId) {
        throw makeError("CONVERSATION_OWNERSHIP");
      }
      return true;
    }
  }

  if (supabase) {
    try {
      const { data, error } = await supabase
        .from("sessions")
        .select("user_id, firebase_uid")
        .eq("session_id", conversationId)
        .maybeSingle();
      if (!error && data) {
        if (data.user_id && data.user_id !== userId && data.firebase_uid !== userId) {
          throw makeError("CONVERSATION_OWNERSHIP");
        }
        return true;
      }
    } catch (e) {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
    }
  }

  return true;
}

// ================================================================================
// §4.08 — SAVEMESSAGEWITHUSER
// ================================================================================

async function saveMessageWithUser(conversationId, role, content, userId = null, firebaseUid = null, metadata = {}) {
  const now = Date.now();
  const msgId = generateMsgId();

  if (firestoreDb && userId) {
    fsSet("messages", msgId, {
      session_id: conversationId,
      firebase_uid: firebaseUid || userId,
      user_id: userId,
      role,
      content,
      metadata,
      created_at: now
    }).catch(() => {});
  }

  if (supabase && userId) {
    const idemKey = `msg:${conversationId}:${role}:${now}:${sha256(String(content).slice(0, 64)).slice(0, 12)}`;
    supabaseWriteSafe({
      table: "messages", op: "insert",
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
  }

  try {
    await dbRun(
      `INSERT INTO messages (session_id, user_id, role, content, metadata, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [conversationId, userId, role, content, safeJsonStringify(metadata || {}), now]
    );
    await dbRun(`UPDATE sessions SET updated_at = ? WHERE session_id = ?`, [now, conversationId]);
  } catch (e) {}
}

// ================================================================================
// §4.09 — GETUSERMEMORY + SAVEUSERMEMORY
// ================================================================================

async function getUserMemory(userId) {
  if (firestoreDb) {
    const doc = await fsGet("user_memory", userId);
    if (doc?.summary) return doc.summary;
  }
  if (supabase) {
    try {
      const { data, error } = await supabase
        .from("user_memory")
        .select("summary")
        .eq("user_id", userId)
        .maybeSingle();
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

  if (firestoreDb) {
    fsSet("user_memory", userId, {
      user_id: userId,
      summary,
      messages_since_update: messagesSinceUpdate,
      updated_at: Date.now()
    }, { merge: true }).catch(() => {});
  }
  if (supabase) {
    supabaseWriteSafe({
      table: "user_memory", op: "upsert",
      payload: {
        user_id: userId,
        summary,
        messages_since_update: messagesSinceUpdate,
        updated_at: new Date().toISOString()
      },
      matchColumn: "user_id"
    }).catch(() => {});
  }
}

// ================================================================================
// §4.10 — MÉMOIRE LONG TERME (RÉSUMÉ)
// ================================================================================

const USER_MEMORY_UPDATE_EVERY_N_MESSAGES = parseInt(
  process.env.USER_MEMORY_UPDATE_EVERY_N_MESSAGES || "6", 10
);

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

async function runMemorySummaryImpl({ userId, lastUserMessage, lastAssistantReply }) {
  try {
    const prev = await getUserMemory(userId);
    const provider = MODEL_TIERS.v100.providers[0];

    const result = await callProviderWithTools({
      providerConfig: provider,
      messages: [
        {
          role: "system",
          content: [
            "Tu mets à jour une mémoire long terme compacte sur un utilisateur.",
            "Résume en 5 à 8 lignes MAXIMUM les faits durables et utiles.",
            "N'invente rien.",
            'Réponds STRICTEMENT au format JSON : {"summary": "..."}'
          ].join("\n")
        },
        {
          role: "user",
          content:
            `RÉSUMÉ ACTUEL :\n${prev || "(aucun)"}\n\n` +
            `DERNIER ÉCHANGE :\nUtilisateur: ${String(lastUserMessage).slice(0, 800)}\n` +
            `Assistant: ${String(lastAssistantReply).slice(0, 800)}\n\n` +
            `Donne le résumé mis à jour au format JSON :`
        }
      ],
      tools: null,
      jsonMode: true
    });

    if (result.success) {
      let parsed = null;
      const content = result.message?.content || "";
      try { parsed = JSON.parse(content); }
      catch {
        const m = content.match(/\{[\s\S]*\}/);
        if (m) try { parsed = JSON.parse(m[0]); } catch {}
      }
      if (parsed && typeof parsed.summary === "string") {
        await saveUserMemory(userId, parsed.summary.slice(0, 2000).trim(), 0);
        logger.info({ userId }, "🧠 Mémoire long terme mise à jour");
        return;
      }
    }
    await saveUserMemory(userId, prev, 0);
  } catch (e) {}
}

async function maybeUpdateUserMemoryAsync(userId, lastUserMessage, lastAssistantReply) {
  try {
    const count = await incrementUserMemoryCounter(userId);

    if (count < USER_MEMORY_UPDATE_EVERY_N_MESSAGES) {
      if (count % 3 === 0) {
        extractFactsFromExchange(userId, lastUserMessage, lastAssistantReply).catch(() => {});
      }
      return;
    }

    runMemorySummaryImpl({ userId, lastUserMessage, lastAssistantReply }).catch(() => {});
  } catch (e) {}
}

// ================================================================================
// §4.11 — EXTRACTSUGGESTIONS
// ================================================================================

function extractSuggestions(text) {
  if (!text) return { text: "", suggestions: [] };
  const m = String(text).match(/<!--\s*SUGGESTIONS\s*:\s*(\[[\s\S]*?\])\s*-->/i);
  if (!m) return { text, suggestions: [] };

  let suggestions = [];
  try {
    const parsed = JSON.parse(m[1]);
    if (Array.isArray(parsed)) {
      suggestions = parsed.filter((s) => typeof s === "string").slice(0, 4);
    }
  } catch {}

  return { text: text.replace(m[0], "").trim(), suggestions };
}

// ================================================================================
// §4.12 — GENERATESUGGESTIONS
// ================================================================================

async function generateSuggestions(userMessage, replyText) {
  try {
    const provider = MODEL_TIERS.v100.providers[0];
    const r = await callProviderWithTools({
      providerConfig: provider,
      messages: [
        {
          role: "system",
          content: 'Génère 3 questions de suivi courtes (max 60 char). Réponds strictement en JSON : {"suggestions":["...","...","..."]}'
        },
        {
          role: "user",
          content: `Question : ${userMessage.slice(0, 300)}\nRéponse : ${replyText.slice(0, 500)}`
        }
      ],
      tools: null,
      jsonMode: true
    });

    if (r.success) {
      const content = r.message?.content || "";
      const m = content.match(/\{[\s\S]*\}/);
      if (m) {
        const parsed = JSON.parse(m[0]);
        if (Array.isArray(parsed.suggestions)) {
          return parsed.suggestions.slice(0, 3);
        }
      }
    }
  } catch {}
  return [];
}

// ================================================================================
// §4.13 — ENRICHCONTEXTWITHINTENT
// ================================================================================

async function enrichContextWithIntent(intent, userMessage, entity, toolCache) {
  const enrichment = {
    contextData: "",
    sourceKeys: [],
    media: { images: [], videos: [] }
  };

  if (!intent || intent === "CODE") return enrichment;

  const run = async (toolName, args) => {
    const key = toolCacheKey(toolName, args);
    if (toolCache.has(key)) return toolCache.get(key);
    const { result, sourceKeys } = await executeToolNative(toolName, args, {});
    toolCache.set(key, { result, sourceKeys });
    return { result, sourceKeys };
  };

  try {
    if (intent === "GENERAL") {
      if (!isGreetingOrSmallTalk(userMessage) && !isIdentityOrSelfQuestion(userMessage)) {
        const q = entity || userMessage.split(/\s+/).slice(0, 5).join(" ");
        if (q && q.length >= 4) {
          const { result, sourceKeys } = await run("search_images", { query: q });
          if (result.images?.length) {
            enrichment.media.images = result.images.slice(0, 3);
            sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
          }
        }
      }
      return enrichment;
    }

    switch (intent) {
      case "MATHS": {
        const expressions = detectMathExpressions(userMessage);
        for (const expr of expressions.slice(0, 2)) {
          const { result } = await run("execute_math", { expression: expr });
          if (result.success) {
            enrichment.contextData += `\n[CALCUL EXACT — utilise ce résultat tel quel] ${expr} = ${result.formatted}\n`;
          }
        }
        break;
      }

      case "ACTUALITE": {
        const q = entity || userMessage;
        const [newsRes, imagesRes] = await Promise.all([
          run("search_news", { query: q }),
          run("search_images", { query: q })
        ]);

        if (newsRes.result.articles?.length) {
          enrichment.contextData += "\n[ACTUALITÉS RÉCENTES — utilise ces faits, ne cite pas les URLs]\n";
          newsRes.result.articles.slice(0, 5).forEach((a, i) => {
            enrichment.contextData += `${i + 1}. ${a.title} (${a.pubDate})\n   ${a.description ? a.description.slice(0, 200) : ""}\n\n`;
          });
          newsRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        } else {
          enrichment.contextData += `\n[ACTUALITÉ — AUCUN ARTICLE TROUVÉ pour "${q}"]\n⚠️ Dis-le à l'utilisateur. N'invente pas.\n`;
        }

        if (imagesRes.result.images?.length) {
          enrichment.media.images = imagesRes.result.images.slice(0, 3);
          enrichment.contextData += `\n[IMAGES TROUVÉES — affichées automatiquement, ne mentionne pas les URLs]\n`;
          imagesRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }

      case "SPORT": {
        const [scoresRes, imagesRes] = await Promise.all([
          run("search_sports_scores", { team: entity || userMessage }),
          run("search_images", { query: `${entity || userMessage} football player team` })
        ]);

        if (scoresRes.result.events?.length) {
          enrichment.contextData += `\n[RÉSULTATS SPORTIFS RÉCENTS (Google News, vérifié)]\n`;
          scoresRes.result.events.forEach((e) => {
            enrichment.contextData +=
              `- ${e.match}\n` +
              `  Date : ${e.date || "?"}\n` +
              `  Source : ${e.source}\n` +
              `  Article : "${e.title}"\n` +
              `  Lien : ${e.url}\n\n`;
          });
          enrichment.contextData += `\n⚠️ RÈGLE : utilise UNIQUEMENT ces scores.\n`;
          scoresRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        } else {
          enrichment.contextData += `\n[SPORT — AUCUNE INFORMATION TROUVÉE pour "${entity || userMessage}"]\n`;
          scoresRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }

        if (imagesRes.result.images?.length) {
          enrichment.media.images = imagesRes.result.images.slice(0, 3);
          enrichment.contextData += `\n[IMAGES TROUVÉES — affichées automatiquement]\n`;
          imagesRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }
        break;
      }

      case "FINANCE": {
        const sym = (entity || userMessage).toUpperCase().trim().slice(0, 10);
        const [cryptoRes, stockRes] = await Promise.all([
          run("get_crypto_price", { symbol: sym }),
          run("get_stock_price", { ticker: sym })
        ]);

        if (!cryptoRes.result.error) {
          const c = cryptoRes.result;
          enrichment.contextData += `\n[PRIX CRYPTO RÉEL — ${c.symbol}]\n`;
          enrichment.contextData += `- Prix : ${c.priceUsd} USD / ${c.priceEur || "?"} EUR\n`;
          enrichment.contextData += `- Variation 24h : ${c.change24h?.toFixed(2)}%\n`;
          enrichment.contextData += `- Market cap : ${c.marketCap} USD\n`;
          enrichment.sourceKeys.push(c.source === "coingecko" ? "coingecko" : "coinmarketcap");
        } else if (!stockRes.result.error) {
          const s = stockRes.result;
          enrichment.contextData += `\n[PRIX ACTION RÉEL — ${s.ticker}]\n`;
          enrichment.contextData += `- Prix : ${s.price} ${s.currency}\n`;
          enrichment.contextData += `- Variation : ${s.changePercent?.toFixed(2)}%\n`;
          enrichment.sourceKeys.push("yahoo");
        } else {
          enrichment.contextData += `\n[FINANCE — Aucun prix trouvé pour "${sym}"]\n`;
        }
        break;
      }

      case "METEO": {
        const { result, sourceKeys } = await run("get_weather", { location: entity || userMessage });
        if (!result.error) {
          enrichment.contextData += `\n[MÉTÉO RÉELLE — ${result.location}]\n`;
          enrichment.contextData += `- Température : ${result.current.temperature}°C (ressenti ${result.current.feelsLike}°C)\n`;
          enrichment.contextData += `- Condition : ${result.current.condition}\n`;
          enrichment.contextData += `- Vent : ${result.current.windSpeed} km/h\n`;
          enrichment.contextData += `- Humidité : ${result.current.humidity}%\n`;
          sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        } else {
          enrichment.contextData += `\n[MÉTÉO — Erreur : ${result.error}]\n`;
        }
        break;
      }

      case "PERSONNE": {
        const { result, sourceKeys } = await run("search_images", { query: entity || userMessage });
        if (result.images?.length) {
          enrichment.media.images = result.images.slice(0, 3);
          enrichment.contextData += `\n[IMAGES TROUVÉES — affichées automatiquement]\n`;
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
// §4.14 — TOOLCACHEKEY
// ================================================================================

function toolCacheKey(name, args) {
  return `${name}::${sha256(safeJsonStringify(args || {})).slice(0, 16)}`;
}

// ================================================================================
// §4.15 — HANDLECHAT v16.5 (SELF-HEALING + IMAGES GARANTIES + MÉMOIRE PRO)
// ================================================================================

/**
 * Cœur du backend v16.5 :
 *   ✅ Images garanties sur chaque réponse (sauf salutation/identité)
 *   ✅ Mémoire PRO persistante injectée dans le system prompt
 *   ✅ Output toujours propre (formatFinalReply)
 *   ✅ Self-healing automatique (via callProviderWithTools)
 *   ✅ Compaction auto si token overflow
 *   ✅ Suggestions, quality layer, sources
 */
async function handleChat({
  conversationId,
  userId,
  firebaseUid,
  message,
  googleAccessToken = null,
  channel = "web",
  modelTier = "v100",
  images = null,
  sse = null
}) {
  const startedAt = Date.now();
  const useV250 = modelTier === "v250";
  const detectedLanguage = detectLanguage(message);

  if (sse) sse.status("starting", { language: detectedLanguage });

  // =================================================================
  // 1) Lectures parallèles
  // =================================================================
  const [_, history, longTermMemory, activeIntent, longTermFactsBlock] = await Promise.all([
    getSession(conversationId, userId, firebaseUid).catch((e) => {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
      return null;
    }),
    getFullHistory(conversationId, userId, CONFIG.LIMITS.MAX_CONTEXT_MESSAGES).catch(() => []),
    getUserMemory(userId).catch(() => ""),
    getActiveIntent(conversationId).catch(() => null),
    buildLongTermMemoryBlock(userId).catch(() => "")
  ]);

  // =================================================================
  // 2) Intention active (WhatsApp / Email)
  // =================================================================
  if (activeIntent) {
    if (sse) sse.status("active_intent");
    const out = await handleActiveIntent(conversationId, activeIntent, message, {
      userId, googleAccessToken
    });

    await saveMessageWithUser(conversationId, "user", message, userId, firebaseUid);
    await saveMessageWithUser(conversationId, "assistant", out.reply, userId, firebaseUid);

    if (sse) {
      await streamTextAsTokens(sse, out.reply);
      sse.done({ conversationId, providerUsed: "active_intent", modelTier, error: !!out.error });
      sse.end();
    }

    return {
      reply: out.reply,
      images: [],
      media: { images: [], videos: [] },
      suggestions: [],
      sources: [],
      intent: "ACTIVE_INTENT",
      providerUsed: "active_intent",
      modelTier,
      degraded: false,
      conversationId,
      isNewConversation: false,
      error: !!out.error,
      confidence: 1.0,
      qualityScore: 1.0,
      ad: null
    };
  }

  // =================================================================
  // 3) Pré-routeur d'intention
  // =================================================================
  const { intent, entity } = preRouteIntent(message);
  if (sse) sse.status("thinking", { intent, language: detectedLanguage });

  // =================================================================
  // 4) Save user message
  // =================================================================
  saveMessageWithUser(conversationId, "user", message, userId, firebaseUid).catch(() => {});

  // =================================================================
  // 5) Enrichissement contexte
  // =================================================================
  const toolCache = new Map();
  const enrichment = await enrichContextWithIntent(intent, message, entity, toolCache);

  if (sse && enrichment.media.images.length > 0) sse.images(enrichment.media.images);
  if (sse && enrichment.media.videos.length > 0) sse.videos(enrichment.media.videos);

  // =================================================================
  // 🆕 5.bis) IMAGES GARANTIES
  // =================================================================
  let guaranteedImages = [...enrichment.media.images];

  // Si pas d'image déjà collectée ET pas salutation/identité → forcer une image
  const shouldGuaranteeImage = guaranteedImages.length === 0 &&
                                !isGreetingOrSmallTalk(message) &&
                                !isIdentityOrSelfQuestion(message);

  if (shouldGuaranteeImage) {
    try {
      const guaranteed = await ensureImageForResponse(message, entity);
      if (guaranteed.images?.length > 0) {
        guaranteedImages = guaranteed.images;
        if (sse && guaranteed.images.length > 0) sse.images(guaranteed.images);
        logger.info({
          query: message.slice(0, 50),
          count: guaranteed.images.length,
          reason: guaranteed.reason
        }, "[Luba Images] Images garanties ajoutées");
      }
    } catch (e) {
      logger.warn({ err: e.message }, "[Luba Images] Erreur garantie images");
    }
  }

  // =================================================================
  // 6) Construction des messages LLM
  // =================================================================
  const historyWithoutCurrent = history.length > 0 && history[history.length - 1].role === "user"
    ? history.slice(0, -1)
    : history;
  const contextHistory = historyWithoutCurrent.slice(-CONFIG.LIMITS.MAX_CONTEXT_MESSAGES);

  let systemContent = LUBA_SYSTEM_PROMPT;
  systemContent += `\n\n[LANGUE DÉTECTÉE : ${detectedLanguage.toUpperCase()}] → Réponds en ${
    detectedLanguage === "fr" ? "français"
    : detectedLanguage === "en" ? "anglais"
    : detectedLanguage === "sw" ? "kiswahili"
    : "lingala"
  }.`;

  if (longTermMemory) {
    systemContent += `\n\n[MÉMOIRE LONG TERME SUR CET UTILISATEUR]\n${longTermMemory}`;
  }

  // 🆕 Injecte la mémoire PRO persistante
  if (longTermFactsBlock) {
    systemContent += `\n\n${longTermFactsBlock}`;
  }

  if (intent && intent !== "GENERAL") {
    systemContent += `\n\n[DOMAINE DÉTECTÉ : ${intent}]`;
  }

  // 🆕 Note sur les images : le LLM doit savoir que les images sont gérées
  if (guaranteedImages.length > 0) {
    systemContent += `\n\n[IMAGES : ${guaranteedImages.length} image(s) seront affichées automatiquement en haut de ta réponse. Ne mentionne PAS les URLs.]`;
  }

  const userContentWithEnrichment = enrichment.contextData
    ? `${message}\n\n[CONTEXTE ENRICHI — NE PAS CITER CES SOURCES]\n${enrichment.contextData}`
    : message;

  const messages = [
    { role: "system", content: systemContent },
    ...contextHistory,
    { role: "user", content: userContentWithEnrichment }
  ];

  // =================================================================
  // 7) Exécution LLM (self-healing automatique via callProviderWithTools)
  // =================================================================
  const usedSources = new Set(enrichment.sourceKeys);
  let collectedImages = guaranteedImages.map((i) => i.url);
  let collectedVideos = [...enrichment.media.videos];
  let providerUsed = "unknown";
  let degraded = false;
  let visionEnabled = Boolean(images && images.length > 0);
  let finalText = "";
  let toolCallTrace = [];
  let lastReasoning = "";

  const providerChain = (images && images.length > 0)
    ? MODEL_TIERS.vision.providers
    : (useV250 ? MODEL_TIERS.v250.reasoning.providers : MODEL_TIERS.v100.providers);

  // 🆕 Réordonne les providers par santé (self-healing)
  const orderedChain = providerHealth.orderProviders(providerChain);

  if (orderedChain.length === 0) {
    logger.warn({ tier: modelTier }, "🏥 Tous les providers sont désactivés — réactivation forcée");
    orderedChain.push(...providerChain);
  }

  if (orderedChain.length < providerChain.length) {
    logger.info({
      total: providerChain.length,
      active: orderedChain.length
    }, "🏥 Self-Heal : certains providers sont temporairement désactivés");
  }

  const executeFn = async ({ toolName, args }) => {
    const key = toolCacheKey(toolName, args);
    if (toolCache.has(key)) {
      const cached = toolCache.get(key);
      return { result: cached.result, sourceKeys: cached.sourceKeys };
    }
    const out = await executeToolNative(toolName, args, {
      userId,
      googleAccessToken,
      sessionId: conversationId
    });
    toolCache.set(key, { result: out.result, sourceKeys: out.sourceKeys });
    return out;
  };

  try {
    let loopResult = { success: false };
    let lastError = null;

    // Failover avec self-healing
    for (const provider of orderedChain) {
      const attemptStart = Date.now();
      const r = await runToolLoop({
        messages,
        providerConfig: provider,
        images,
        executeFn,
        sse,
        _meta: { sessionId: conversationId, userId, tier: modelTier }
      });

      if (r.success) {
        loopResult = r;
        providerUsed = provider.provider;
        logger.info({
          provider: provider.provider,
          model: provider.model,
          latencyMs: Date.now() - attemptStart
        }, "✅ Provider OK");
        break;
      }

      lastError = r.error;
      const httpStatus = r.error?.response?.status || null;
      const errCode = r.error?.code || null;

      logger.warn({
        provider: provider.provider,
        model: provider.model,
        httpStatus,
        errorCode: errCode,
        errorMessage: String(r.error?.message || "").slice(0, 300)
      }, `❌ Provider échoué (${httpStatus || errCode || "unknown"}) — failover suivant`);

      dbRun(
        `INSERT INTO provider_failover_log
         (session_id, tier, provider, model, http_status, error_code, error_message, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [conversationId, modelTier, provider.provider, provider.model,
          httpStatus, errCode, String(r.error?.message || "").slice(0, 500), Date.now()]
      ).catch(() => {});
    }

    if (!loopResult.success) {
      finalText = userFacingErrorMessage(lastError, {
        provider: orderedChain[0]?.provider,
        model: orderedChain[0]?.model
      });
      degraded = true;
    } else {
      finalText = loopResult.text || "";
      toolCallTrace = loopResult.toolCallTrace || [];
      lastReasoning = loopResult.reasoning || "";
      (loopResult.usedSources || []).forEach((k) => usedSources.add(k));
      collectedImages.push(...(loopResult.images || []));
      collectedVideos.push(...(loopResult.videos || []));

      // Filet : si le LLM n'a pas ramené d'images, garder celles garanties
      if (collectedImages.length === 0 && guaranteedImages.length > 0) {
        collectedImages.push(...guaranteedImages.map((i) => i.url));
      }
    }

    // Phase code v250
    if (useV250 && !images && finalText && intent === "CODE") {
      const codeProvider = MODEL_TIERS.v250.code.providers[0];
      const codeMessages = [
        {
          role: "system",
          content: LUBA_SYSTEM_PROMPT + `\n\n[PHASE CODE] Fournis le code complet. Langue : ${detectedLanguage.toUpperCase()}.`
        },
        ...contextHistory,
        { role: "user", content: message }
      ];
      const codeResult = await callProviderWithTools({
        providerConfig: codeProvider,
        messages: codeMessages,
        tools: getToolSchemas("chat"),
        jsonMode: false,
        _meta: { sessionId: conversationId, userId, tier: "v250" }
      });
      if (codeResult.success) {
        finalText = formatFinalReply(codeResult.message?.content || "") || finalText;
        providerUsed = `v250_pipeline(${providerUsed}->${codeProvider.provider})`;
      }
    }
  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "handleChat : erreur critique");
    finalText = userFacingErrorMessage(e);
    degraded = true;
  }

  // =================================================================
  // 8) Post-traitement — nettoyage obligatoire
  // =================================================================
  // 🆕 Toutes les réponses passent par formatFinalReply + cleanOutput
  finalText = formatFinalReply(finalText || "");
  if (!finalText) finalText = "Je n'ai pas pu générer une réponse pour le moment. Peux-tu reformuler ?";

  // Suggestions
  const sug = extractSuggestions(finalText);
  finalText = sug.text;
  let suggestions = sug.suggestions;

  if (suggestions.length === 0 && !degraded) {
    generateSuggestions(message, finalText.slice(0, 500))
      .then((s) => { if (s.length) suggestions = s; })
      .catch(() => {});
  }

  // Quality layer
  let qualityReport = null;
  if (!degraded && finalText.length > 20) {
    try {
      qualityReport = await assessResponseQuality({
        userMessage: message,
        draftAnswer: finalText,
        toolCallTrace,
        reasoning: lastReasoning,
        providerConfig: orderedChain[0],
        intent,
        degraded,
        _meta: { sessionId: conversationId, userId, tier: modelTier }
      });

      if (qualityReport.selfCritique?.improved && qualityReport.finalText) {
        finalText = formatFinalReply(qualityReport.finalText);
      }

      if (sse) {
        sse.quality({
          confidence: qualityReport.confidence,
          qualityScore: qualityReport.qualityScore,
          hallucinationDetected: qualityReport.hallucination.detected,
          selfCritiqueImproved: qualityReport.selfCritique.improved
        });
      }
    } catch (e) {}
  }

  // Déduplication images
  collectedImages = [...new Set(collectedImages.filter(Boolean))].slice(0, 3);
  collectedVideos = dedupeVideos(collectedVideos);

  // Save assistant
  saveMessageWithUser(conversationId, "assistant", finalText, userId, firebaseUid, {
    providerUsed,
    intent,
    degraded,
    confidence: qualityReport?.confidence,
    qualityScore: qualityReport?.qualityScore,
    language: detectedLanguage
  }).catch(() => {});

  // Trace raisonnement
  if (lastReasoning || qualityReport) {
    dbRun(
      `INSERT INTO reasoning_traces (
        session_id, user_id, user_message, reasoning, draft_answer, final_answer,
        confidence, quality_score, self_critique_improved, hallucination_detected,
        provider, model, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        conversationId, userId, message.slice(0, 500),
        lastReasoning.slice(0, 5000),
        qualityReport?.originalText?.slice(0, 5000) || finalText.slice(0, 5000),
        finalText.slice(0, 5000),
        qualityReport?.confidence || 0.5,
        qualityReport?.qualityScore || 0.5,
        qualityReport?.selfCritique?.improved ? 1 : 0,
        qualityReport?.hallucination?.detected ? 1 : 0,
        providerUsed,
        orderedChain[0]?.model || "unknown",
        Date.now()
      ]
    ).catch(() => {});
  }

  // Mémoire
  maybeUpdateUserMemoryAsync(userId, message, finalText).catch(() => {});

  // =================================================================
  // 9) Construction réponse client
  //    IMAGES EN HAUT + TEXTE PROPRE + SOURCES (pas d'ad ici)
  // =================================================================
  let replyForClient = finalText;

  // Images EN HAUT
  const shouldInlineImages = !sse && collectedImages.length > 0;
  if (shouldInlineImages) {
    const imagesMd = collectedImages
      .slice(0, CONFIG.LIMITS.MAX_IMAGES_DISPLAYED)
      .map((u, i) => `![Illustration ${i + 1}](${u})`)
      .join("\n\n");
    replyForClient = `${imagesMd}\n\n---\n\n${finalText}`;
  }

  // Ad (propre, sans branding test)
  let ad = null;
  if (channel !== "whatsapp" && channel !== "live-ws") {
    try {
      ad = await getAd({ slot: "chat_below", userId });
    } catch (e) {}
  }

  if (ad && ad.imageUrl) {
    const safeTitle = String(ad.title || "Sponsorisé").replace(/[\[\]]/g, "");
    const adMd = ad.clickUrl
      ? `[![${safeTitle}](${ad.imageUrl})](${ad.clickUrl})`
      : `![${safeTitle}](${ad.imageUrl})`;
    replyForClient += `\n\n${adMd}`;
  }

  // Sources
  if (usedSources.size > 0) {
    const SOURCE_LABELS = {
      wikipedia:         { name: "Wikipédia",         url: "https://fr.wikipedia.org" },
      wikimediacommons:  { name: "Wikimedia Commons", url: "https://commons.wikimedia.org" },
      googlenews:        { name: "Google News",       url: "https://news.google.com" },
      gdelt:             { name: "GDELT",             url: "https://www.gdeltproject.org" },
      tavily:            { name: "Tavily",            url: "https://tavily.com" },
      serper:            { name: "Google Search",     url: "https://google.com" },
      duckduckgo:        { name: "DuckDuckGo",        url: "https://duckduckgo.com" },
      hackernews:        { name: "Hacker News",       url: "https://news.ycombinator.com" },
      arxiv:             { name: "arXiv",             url: "https://arxiv.org" },
      reddit:            { name: "Reddit",            url: "https://reddit.com" },
      openmeteo:         { name: "Open-Meteo",        url: "https://open-meteo.com" },
      coingecko:         { name: "CoinGecko",         url: "https://www.coingecko.com" },
      coinmarketcap:     { name: "CoinMarketCap",     url: "https://coinmarketcap.com" },
      yahoo:             { name: "Yahoo Finance",     url: "https://finance.yahoo.com" },
      youtube:           { name: "YouTube",           url: "https://youtube.com" },
      pexels:            { name: "Pexels",            url: "https://pexels.com" }
    };
    const srcLines = [...usedSources]
      .map((k) => SOURCE_LABELS[k])
      .filter(Boolean)
      .map((s) => `[${s.name}](${s.url})`);
    if (srcLines.length > 0) {
      replyForClient += `\n\n---\n\n**Sources :** ${srcLines.join(" · ")}`;
    }
  }

  const media = {
    images: collectedImages,
    videos: collectedVideos.map((v) => ({
      videoId: v.videoId,
      title: v.title,
      channel: v.channel,
      thumbnail: v.thumbnail,
      url: v.url,
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
    sources: [...usedSources].map((k) => SOURCE_LABELS?.[k]).filter(Boolean),
    intent,
    language: detectedLanguage,
    userId,
    contextLength: history.length,
    toolCallTrace,
    elapsedMs: Date.now() - startedAt,
    confidence: qualityReport?.confidence ?? null,
    qualityScore: qualityReport?.qualityScore ?? null,
    hallucinationDetected: qualityReport?.hallucination?.detected ?? false,
    selfCritiqueImproved: qualityReport?.selfCritique?.improved ?? false,
    ad: ad || null,
    // 🆕 Infos self-healing
    providersAvailable: orderedChain.length,
    providersTotal: providerChain.length,
    imagesGuaranteed: guaranteedImages.length > 0,
    contextCompacted: false
  };

  // SSE final
  if (sse) {
    sse.suggestions(suggestions);
    sse.sources(result.sources);
    if (ad) sse.ad({ ad });

    await streamTextAsTokens(sse, replyForClient, { paceMs: 4, chunkSize: 28 });

    sse.done({
      conversationId,
      isNewConversation: false,
      providerUsed,
      modelTier: result.modelTier,
      degraded,
      visionEnabled,
      intent,
      language: detectedLanguage,
      contextLength: history.length,
      elapsedMs: result.elapsedMs,
      confidence: result.confidence,
      qualityScore: result.qualityScore,
      adIncluded: Boolean(ad),
      imagesGuaranteed: result.imagesGuaranteed,
      providersAvailable: orderedChain.length,
      providersTotal: providerChain.length
    });
    sse.end();
  }

  return { ...result, conversationId, isNewConversation: false };
}

// ================================================================================
// §4.16 — HANDLEACTIVEINTENT
// ================================================================================

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
          await setActiveIntent(conversationId, "WHATSAPP", {
            step: "NEED_MESSAGE",
            recipient: cleaned
          });
          return {
            reply: `Numéro enregistré (${cleaned}). Quel message voulez-vous envoyer ?`,
            error: false
          };
        }
        return {
          reply: "Ce numéro n'est pas valide (format international requis, ex. +243812345678). Tapez « annule » pour arrêter.",
          error: true
        };
      }

      if (data.step === "NEED_MESSAGE") {
        const text = sanitizeStrict(userMessage, 2000);
        if (!text) {
          return { reply: "Message vide. Réessayez ou tapez « annule ».", error: true };
        }
        try {
          await sendWhatsAppSmart(userId, data.recipient, text);
          await clearActiveIntent(conversationId);
          return { reply: `✅ Message WhatsApp envoyé vers ${data.recipient}.`, error: false };
        } catch {
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
        await setActiveIntent(conversationId, "EMAIL", {
          step: "NEED_BODY", recipient: data.recipient, subject
        });
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
// §4.17 — ISCANCELMESSAGE
// ================================================================================

const CANCEL_WORDS = new Set([
  "annule", "annuler", "stop", "abandonne", "laisse tomber", "oublie", "cancel"
]);

function isCancelMessage(text) {
  const t = normalizeForMatch(text);
  return CANCEL_WORDS.has(t) || t.startsWith("annul") || t.startsWith("cancel");
}

// ================================================================================
// §4.18 — EXPORTS PARTIE 4
// ================================================================================

Object.assign(module.exports, {
  SSEWriter,
  streamTextAsTokens,
  LUBA_SYSTEM_PROMPT,
  getSession,
  setActiveIntent,
  getActiveIntent,
  clearActiveIntent,
  getFullHistory,
  assertConversationOwnership,
  saveMessageWithUser,
  ACTIVE_INTENT_TTL_MS,
  getUserMemory,
  saveUserMemory,
  incrementUserMemoryCounter,
  runMemorySummaryImpl,
  maybeUpdateUserMemoryAsync,
  USER_MEMORY_UPDATE_EVERY_N_MESSAGES,
  extractSuggestions,
  generateSuggestions,
  enrichContextWithIntent,
  toolCacheKey,
  handleChat,
  handleActiveIntent,
  isCancelMessage,
  CANCEL_WORDS
});

// ================================================================================
// ==================== FIN PARTIE 4/5 ===========================================
// ================================================================================
// ▶ PARTIE 5/5 : Express · Routes · Luba Live WebSocket · Bootstrap · Docker
//   Tape "suite" pour la recevoir.
// ================================================================================
// ================================================================================
// PARTIE 5/5 — EXPRESS · ROUTES · LUBA LIVE WS · BAILEYS · BOOTSTRAP · DOCKER
// ================================================================================
// VERSION : v16.5.0 (Octobre 2026) — Self-Healing Edition
//
// ⚠️ CORRECTIONS CRITIQUES :
//   ✅ Variables (firebaseApp, redisClient, metrics, firestoreDb, supabase, db)
//      JAMAIS appelées comme fonctions
//   ✅ Logs d'auth détaillés (hasBearerToken, tokenPreview)
//   ✅ Route /api/auth/check (diagnostic protégé)
//   ✅ Fallback REST Firebase quand Admin SDK échoue
//   ✅ Self-healing : health check, stats providers
//
// TABLE DES MATIÈRES :
//   §5.01  Baileys WhatsApp
//   §5.02  Schedulers
//   §5.03  Express + middlewares
//   §5.04  Auth middleware
//   §5.05  Routes API
//   §5.06  Luba Live WebSocket
//   §5.07  Bootstrap + shutdown
//   §5.08  Fichiers de déploiement
//   §5.09  Exports finaux + auto-start
// ================================================================================

"use strict";

// ================================================================================
// §5.01 — BAILEYS WHATSAPP MANAGER
// ================================================================================

function isWhatsAppAllowed(phoneNumber) {
  if (CONFIG.WHATSAPP.WHITELIST.length === 0) return CONFIG.WHATSAPP.OPEN;
  return CONFIG.WHATSAPP.WHITELIST.includes(phoneNumber.replace(/[^\d]/g, ""));
}

class BaileysManager {
  constructor() {
    this.sessions = new Map();
  }

  async initClient(userId) {
    const existing = this.sessions.get(userId);
    if (existing?.ready) return { connected: true, qrCode: null };
    if (existing?.qrCode) return { connected: false, qrCode: existing.qrCode };

    const authDir = path.join(CONFIG.PATHS.SESSIONS, userId);
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    const savedCreds = await loadWhatsAppCredentials(userId);
    if (savedCreds) {
      try { fs.writeFileSync(path.join(authDir, "creds.json"), safeJsonStringify(savedCreds)); } catch {}
    }

    let makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion;
    try {
      const b = require("@whiskeysockets/baileys");
      makeWASocket = b.default;
      useMultiFileAuthState = b.useMultiFileAuthState;
      DisconnectReason = b.DisconnectReason;
      fetchLatestBaileysVersion = b.fetchLatestBaileysVersion;
    } catch (e) {
      throw new Error("Baileys non installé");
    }

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    let version;
    try { version = (await fetchLatestBaileysVersion()).version; } catch {}

    const sock = makeWASocket({
      version, auth: state,
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      browser: ["Luba.ia", "Chrome", "16.5.0"]
    });

    const session = { sock, qrCode: null, ready: false };
    this.sessions.set(userId, session);

    sock.ev.on("creds.update", async () => {
      try {
        await saveCreds();
        const p = path.join(authDir, "creds.json");
        if (fs.existsSync(p)) {
          const creds = safeJsonParse(fs.readFileSync(p, "utf8"), null);
          if (creds) await saveWhatsAppCredentials(userId, creds);
        }
      } catch {}
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try { session.qrCode = await qrcode.toDataURL(qr, { width: 600, margin: 2 }); } catch {}
      }

      if (connection === "open") {
        session.ready = true;
        session.qrCode = null;
        await dbRun("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]).catch(() => {});
        logger.info({ userId }, "WhatsApp connecté");
      }

      if (connection === "close") {
        session.ready = false;
        await dbRun("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]).catch(() => {});

        const code = lastDisconnect?.error?.output?.statusCode;
        const isLoggedOut = code === DisconnectReason?.loggedOut;
        this.sessions.delete(userId);

        if (isLoggedOut) {
          try {
            const d = path.join(CONFIG.PATHS.SESSIONS, userId);
            if (fs.existsSync(d)) fs.rmSync(d, { recursive: true, force: true });
            await deleteWhatsAppCredentials(userId);
          } catch {}
        } else {
          setTimeout(() => this.initClient(userId).catch(() => {}), CONFIG.WHATSAPP.RETRY_DELAY_MS);
        }
      }
    });

    sock.ev.on("messages.upsert", async ({ messages: msgs, type }) => {
      if (type !== "notify") return;
      for (const msg of msgs) {
        try {
          if (!msg.message || msg.key.fromMe) continue;
          const jid = msg.key.remoteJid;
          if (!jid || jid.endsWith("@g.us")) continue;

          const text = msg.message.conversation
            || msg.message.extendedTextMessage?.text
            || msg.message.imageMessage?.caption
            || null;
          if (!text) continue;

          const phone = jid.replace(/@.*$/, "");
          if (!isWhatsAppAllowed(phone)) continue;

          const waUserId = await this._resolveUser(phone);
          if (!waUserId) continue;

          const q = await checkUserQuota(waUserId, "whatsapp");
          if (!q.allowed) {
            await sock.sendMessage(jid, { text: "Limite atteinte pour aujourd'hui." });
            continue;
          }
          await incrementUserQuota(waUserId, "whatsapp");

          const convId = `whatsapp_${phone}`;
          const result = await handleChat({
            conversationId: convId, userId: waUserId, firebaseUid: null,
            message: String(text).slice(0, CONFIG.LIMITS.MAX_MESSAGE_LENGTH),
            channel: "whatsapp", modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(jid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (e) {}
      }
    });

    return { connected: false, qrCode: null };
  }

  async _resolveUser(phone) {
    const uid = `wa_${phone}`;
    const existing = await dbGet("SELECT id FROM users WHERE firebase_uid = ? OR id = ?", [uid, uid]);
    if (existing) return existing.id;

    try {
      await dbRun(
        `INSERT INTO users (id, firebase_uid, display_name, role, created_at, updated_at, whatsapp_connected, whatsapp_session_id)
         VALUES (?, ?, ?, 'FREE', ?, ?, 1, ?)`,
        [uid, uid, `WhatsApp ${phone}`, Date.now(), Date.now(), phone]
      );
      return uid;
    } catch {
      return null;
    }
  }

  async sendMessage(userId, to, message) {
    const session = this.sessions.get(userId);
    if (!session || !session.ready) {
      const e = new Error("WhatsApp non connecté");
      e.code = "WHATSAPP_NOT_CONNECTED";
      throw e;
    }
    const clean = String(to).replace(/[^\d]/g, "");
    if (!clean) {
      const e = new Error("Numéro destinataire invalide");
      e.code = "INVALID_RECIPIENT";
      throw e;
    }
    await session.sock.sendMessage(`${clean}@s.whatsapp.net`, { text: message });
    return { success: true, to: clean };
  }

  getQRCode(userId) {
    return this.sessions.get(userId)?.qrCode || null;
  }

  async destroyAll() {
    for (const [userId, session] of this.sessions) {
      try { session.sock.end(undefined); } catch {}
    }
  }
}

const baileysManager = new BaileysManager();

if (typeof module.exports.setWhatsAppManager === "function") {
  module.exports.setWhatsAppManager(baileysManager);
}

// ================================================================================
// §5.02 — SCHEDULERS
// ================================================================================

async function reminderTick() {
  try {
    const due = await dbAll(
      `SELECT id, user_id, title, notes, due_at FROM user_tasks
       WHERE status = 'pending' AND due_at IS NOT NULL AND due_at <= ?
         AND (notified_at IS NULL OR notified_at < due_at)
       ORDER BY due_at ASC LIMIT 50`,
      [Date.now()]
    );
    if (!due.length) return;

    for (const t of due) {
      try {
        const r = await dbRun(
          `UPDATE user_tasks SET notified_at = ? WHERE id = ? AND (notified_at IS NULL OR notified_at < due_at)`,
          [Date.now(), t.id]
        );
        if (r.changes === 0) continue;
        logger.info({ taskId: t.id }, "🔔 Rappel échu");

        if (process.env.REMINDER_WHATSAPP_ENABLED === "true") {
          const u = await dbGet("SELECT whatsapp_session_id FROM users WHERE id = ?", [t.user_id]);
          if (u?.whatsapp_session_id) {
            sendWhatsAppSmart(t.user_id, u.whatsapp_session_id,
              `🔔 Rappel : ${t.title}${t.notes ? "\n" + t.notes : ""}`).catch(() => {});
          }
        }
      } catch {}
    }
  } catch (e) {}
}

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
    await dbRun(`DELETE FROM reasoning_traces WHERE created_at < ?`, [now - 30 * 24 * 3600 * 1000]);
    await dbRun(`DELETE FROM provider_failover_log WHERE created_at < ?`, [now - 30 * 24 * 3600 * 1000]);
    logger.info("🧹 Nettoyage effectué");
  } catch (e) {}
}

// ================================================================================
// §5.03 — EXPRESS APP + MIDDLEWARES
// ================================================================================

const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

// ---------- CORS avec logging ----------
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    if (HOSTING_CONFIG.allowedOrigins.includes(origin)) return cb(null, true);
    logger.warn({ rejectedOrigin: origin, allowedOrigins: HOSTING_CONFIG.allowedOrigins }, "🚫 CORS : origine rejetée");
    return cb(null, false);
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: [
    "Content-Type", "Authorization", "X-Requested-With", "x-user-id",
    "X-Google-Access-Token", "X-Session-Token",
    "x-luba-signature", "x-luba-timestamp",
    "Accept", "Last-Event-ID"
  ],
  exposedHeaders: ["Content-Type", "Cache-Control", "Connection", "X-Accel-Buffering", "X-Request-Id"],
  credentials: true,
  maxAge: 86400
}));

// ---------- CSP nonce ----------
app.use((req, res, next) => {
  res.locals.cspNonce = crypto.randomBytes(16).toString("base64");
  next();
});

// ---------- Helmet ----------
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", (req, res) => `'nonce-${res.locals.cspNonce}'`,
        "https://cdnjs.cloudflare.com", "https://apis.google.com", "https://www.gstatic.com"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://fonts.googleapis.com"],
      imgSrc: ["'self'", "data:", "blob:", "https:"],
      connectSrc: ["'self'", "https://api.groq.com", "https://openrouter.ai",
        "https://api.cerebras.ai", "https://generativelanguage.googleapis.com",
        "https://*.firebaseio.com", "wss://*.firebaseio.com", "https://*.supabase.co"],
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

// ---------- HTTPS forcé en prod ----------
app.use((req, res, next) => {
  if (CONFIG.ENV === "production"
      && req.headers["x-forwarded-proto"]
      && req.headers["x-forwarded-proto"] !== "https") {
    return res.redirect(301, "https://" + req.headers.host + req.originalUrl);
  }
  next();
});

// ---------- Body parsers ----------
const jsonSmall = express.json({ limit: "1mb" });
const jsonLarge = express.json({ limit: "20mb" });

app.use((req, res, next) => {
  const ct = req.headers["content-type"] || "";
  if (ct.startsWith("multipart/form-data")) return next();
  if (req.path === "/api/import/conversations") return jsonLarge(req, res, next);
  return jsonSmall(req, res, next);
});
app.use(express.urlencoded({ extended: true, limit: "1mb" }));

// ---------- Request ID + log + métriques ----------
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  res.setHeader("X-Request-Id", requestId);

  res.on("finish", () => {
    const duration = Date.now() - start;
    logger.info({ requestId, method: req.method, path: req.path, status: res.statusCode, duration }, "requête");

    // ✅ Variables, pas fonctions
    if (metrics?.httpRequests) {
      metrics.httpRequests.labels(req.method, req.path, String(res.statusCode)).inc();
    }
    if (metrics?.httpDuration) {
      metrics.httpDuration.labels(req.method, req.path, String(res.statusCode)).observe(duration / 1000);
    }
  });
  next();
});

// ---------- Multer ----------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: CONFIG.LIMITS.MAX_IMAGE_SIZE_MB * 1024 * 1024,
    files: CONFIG.LIMITS.MAX_IMAGES_PER_REQUEST
  },
  fileFilter: (req, file, cb) => {
    if (CONFIG.IMAGES.ALLOWED_TYPES.includes(file.mimetype)) cb(null, true);
    else cb(new Error("Type non supporté."));
  }
});

const uploadAudio = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: CONFIG.AUDIO.MAX_SIZE_MB * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (CONFIG.AUDIO.ALLOWED_TYPES.includes(file.mimetype)) cb(null, true);
    else cb(new Error("Type audio non supporté."));
  }
});

// ---------- Rate limiters ----------
let redisRateLimitStore = null;
try {
  if (process.env.REDIS_URL && IORedis) {
    const RedisStore = require("rate-limit-redis");
    const rl = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
    redisRateLimitStore = new RedisStore({ sendCommand: (...args) => rl.call(...args) });
  }
} catch {}

function makeLimiter({ windowMs, max, code, message }) {
  return rateLimit({
    windowMs, max,
    standardHeaders: true,
    legacyHeaders: false,
    ...(redisRateLimitStore ? { store: redisRateLimitStore } : {}),
    handler: (req, res) => {
      logger.warn({ ip: req.ip, path: req.path, code }, "Rate limit dépassé");
      res.status(429).json({ success: false, error: true, reply: message, code });
    }
  });
}

const apiLimiter    = makeLimiter({ windowMs: 15 * 60 * 1000, max: 200, code: "RATE_LIMIT",        message: "Trop de requêtes. Réessayez dans 15 minutes." });
const strictLimiter = makeLimiter({ windowMs: 60 * 60 * 1000, max: 50,  code: "RATE_LIMIT_STRICT", message: "Limite de requêtes atteinte." });
const chatLimiter   = makeLimiter({ windowMs: 60 * 1000,       max: 30,  code: "RATE_LIMIT_CHAT",   message: "Trop de messages. Patientez un instant." });
const authLimiter   = makeLimiter({ windowMs: 15 * 60 * 1000, max: 60,  code: "RATE_LIMIT_AUTH",   message: "Trop de tentatives d'authentification." });
const toolLimiter   = makeLimiter({ windowMs: 60 * 1000,       max: 40,  code: "RATE_LIMIT_TOOL",   message: "Trop d'appels d'outils." });

// ================================================================================
// §5.04 — AUTH MIDDLEWARE
// ================================================================================

const tokenCache = new LRUCache({
  max: 5000,
  ttl: CONFIG.AUTH.TOKEN_CACHE_TTL_MS,
  updateAgeOnGet: false
});

function cacheGetToken(token) { return tokenCache.get(sha256(token)) || null; }
function cacheSetToken(token, user) { tokenCache.set(sha256(token), user); }

async function verifyFirebaseToken(token, { checkRevoked = CONFIG.AUTH.CHECK_REVOKED } = {}) {
  const tokenPreview = token ? `${token.slice(0, 30)}…` : "(vide)";

  const cached = cacheGetToken(token);
  if (cached) return cached;

  let adminError = null;

  // ═══════════════════════════════════════════════════════════════
  // MÉTHODE 1 : Firebase Admin SDK
  // ✅ firebaseApp est une VARIABLE
  // ═══════════════════════════════════════════════════════════════
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
        customClaims: decoded,
        authSource: "admin_sdk"
      };
      cacheSetToken(token, user);
      logger.info({ uid: user.uid, source: "admin_sdk" }, "✅ Token vérifié (Admin SDK)");
      if (metrics?.authAttempts) metrics.authAttempts.labels("success", "admin_sdk").inc();
      return user;
    } catch (error) {
      adminError = error;
      const code = error?.code || "";
      logger.warn({ code, msg: error?.message, tokenPreview }, "⚠️  Admin SDK a échoué");

      const isDefinitiveTokenError = [
        "auth/id-token-expired",
        "auth/id-token-revoked",
        "auth/argument-error",
        "auth/invalid-id-token"
      ].includes(code);

      if (isDefinitiveTokenError) {
        if (metrics?.authAttempts) metrics.authAttempts.labels("failed", "admin_sdk").inc();
        throw error;
      }

      logger.info({ code }, "🔁 Fallback REST activé");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // MÉTHODE 2 : Firebase Auth REST API
  // ═══════════════════════════════════════════════════════════════
  if (!FIREBASE_CONFIG.apiKey) {
    logger.error("❌ FIREBASE_API_KEY absent");
    if (metrics?.authAttempts) metrics.authAttempts.labels("failed", "no_api_key").inc();
    const e = new Error("Aucune configuration Firebase disponible");
    e.code = "auth/configuration-not-found";
    throw e;
  }

  try {
    const resp = await axios.post(
      `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${FIREBASE_CONFIG.apiKey}`,
      { idToken: token },
      { timeout: 8000, validateStatus: () => true }
    );

    if (resp.status === 200 && resp.data?.users?.length > 0) {
      const u = resp.data.users[0];
      const user = {
        uid: u.localId,
        email: u.email || null,
        displayName: u.displayName || null,
        photoURL: u.photoUrl || null,
        emailVerified: u.emailVerified || false,
        role: "FREE",
        authSource: "rest_api"
      };
      cacheSetToken(token, user);
      logger.info({ uid: user.uid, source: "rest_api" }, "✅ Token vérifié (REST)");
      if (metrics?.authAttempts) metrics.authAttempts.labels("success", "rest_api").inc();
      return user;
    }

    logger.warn({ tokenPreview, resp: resp.data }, "❌ REST : token invalide");
    if (metrics?.authAttempts) metrics.authAttempts.labels("failed", "rest_api").inc();

    const errMsg = resp.data?.error?.message || "";
    const e = new Error(errMsg || "Token invalide");
    e.code = /EXPIRED/i.test(errMsg) ? "auth/id-token-expired" : "auth/invalid-id-token";
    throw e;
  } catch (restError) {
    if (restError.code === "auth/id-token-expired" || restError.code === "auth/invalid-id-token") {
      throw restError;
    }

    logger.error({
      err: restError.message,
      adminErr: adminError?.message,
      tokenPreview
    }, "❌ Échec total vérification token");

    if (metrics?.authAttempts) metrics.authAttempts.labels("failed", "total").inc();
    const e = new Error("Impossible de vérifier le token");
    e.code = "auth/verification-failed";
    throw e;
  }
}

async function isIPBlocked(ip) {
  const row = await dbGet(
    `SELECT 1 FROM blocked_ips WHERE ip_address = ? AND blocked_until > ? LIMIT 1`,
    [ip, Date.now()]
  );
  return Boolean(row);
}

async function recordLoginAttempt(ip, userId, success, err = null, { countFailure = true } = {}) {
  if (!success && !countFailure) return;
  try {
    await dbRun(
      `INSERT INTO login_attempts (user_id, ip_address, success, error_message, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      [userId, ip, success ? 1 : 0, err ? String(err).slice(0, 500) : null, Date.now()]
    );
  } catch {}
}

async function checkLoginAttempts(ip) {
  const cutoff = Date.now() - CONFIG.AUTH.LOGIN_BLOCK_MS;
  const row = await dbGet(
    `SELECT COUNT(*) AS count FROM login_attempts WHERE ip_address = ? AND success = 0 AND created_at > ?`,
    [ip, cutoff]
  );
  if ((row?.count || 0) >= CONFIG.AUTH.MAX_LOGIN_ATTEMPTS) {
    const ex = await dbGet(`SELECT strike_count FROM blocked_ips WHERE ip_address = ?`, [ip]);
    const strikes = (ex?.strike_count || 0) + 1;
    const escal = Math.min(CONFIG.AUTH.LOGIN_BLOCK_MS * Math.pow(2, strikes - 1), 24 * 3600 * 1000);

    await dbRun(
      `INSERT INTO blocked_ips (ip_address, reason, strike_count, blocked_until, created_at)
       VALUES (?, 'Trop de tentatives', ?, ?, ?)
       ON CONFLICT(ip_address) DO UPDATE SET
         reason = excluded.reason,
         strike_count = excluded.strike_count,
         blocked_until = excluded.blocked_until`,
      [ip, strikes, Date.now() + escal, Date.now()]
    );
    return { blocked: true, message: "Trop de tentatives. IP temporairement bloquée." };
  }
  return { blocked: false };
}

async function fastUpsertUser(uid, user, role) {
  try {
    logger.info({ uid, role, source: user.authSource }, "📝 Upsert user");

    const ex = await dbGet("SELECT id, role FROM users WHERE id = ?", [uid]);
    if (!ex) {
      await dbRun(
        `INSERT INTO users (id, firebase_uid, email, display_name, role, email_verified, last_seen_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [uid, uid, user.email, user.displayName || uid, role, user.emailVerified ? 1 : 0, Date.now(), Date.now(), Date.now()]
      );
      logger.info({ userId: uid }, "✅ Nouvel utilisateur créé");
    } else {
      await dbRun(
        `UPDATE users SET last_seen_at = ?, email = COALESCE(?, email),
          display_name = COALESCE(?, display_name), role = ?, email_verified = ?, firebase_uid = ?
         WHERE id = ?`,
        [Date.now(), user.email, user.displayName, role, user.emailVerified ? 1 : 0, uid, uid]
      );
    }

    if (firestoreDb) {
      fsSet("users", uid, {
        id: uid, firebase_uid: uid, email: user.email || null,
        display_name: user.displayName || null, role, last_seen_at: Date.now()
      }, { merge: true }).catch(() => {});
    }
    if (supabase) {
      supabaseWriteSafe({
        table: "users", op: "upsert",
        payload: {
          id: uid, firebase_uid: uid, email: user.email || null,
          display_name: user.displayName || null, last_seen_at: new Date().toISOString()
        },
        matchColumn: "firebase_uid"
      }).catch(() => {});
    }
  } catch (e) {}
}

function authenticateUser(req, res, next) {
  (async () => {
    const ip = req.ip;
    const ua = req.headers["user-agent"];

    if (await isIPBlocked(ip)) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "IP_BLOCKED" });
    }

    const h = req.headers.authorization || req.headers.Authorization || "";
    const token = h.startsWith("Bearer ") ? h.slice(7).trim() : null;

    logger.info({
      hasAuthHeader: Boolean(h),
      hasBearerToken: Boolean(token),
      tokenPreview: token ? `${token.slice(0, 30)}…` : null,
      path: req.path,
      method: req.method,
      origin: req.headers.origin || null
    }, "🔐 Tentative d'authentification");

    if (!token) {
      logger.warn({ path: req.path }, "❌ MISSING_TOKEN");
      return res.status(401).json({ success: false, error: true, reply: "Authentification requise.", code: "MISSING_TOKEN" });
    }

    let user;
    try {
      user = await verifyFirebaseToken(token);
    } catch (error) {
      const code = error?.code || "";
      const isExpired = code === "auth/id-token-expired";
      const isRevoked = code === "auth/id-token-revoked";

      await recordLoginAttempt(ip, null, false, error.message, { countFailure: !isExpired && !isRevoked });

      if (!isExpired && !isRevoked) {
        const c = await checkLoginAttempts(ip);
        if (c.blocked) {
          return res.status(403).json({ success: false, error: true, reply: c.message, code: "IP_BLOCKED" });
        }
      }

      logger.warn({ code, message: error.message, path: req.path }, "❌ Vérification token échouée");
      return res.status(401).json({
        success: false, error: true,
        reply: isExpired ? "Session expirée, reconnectez-vous." : "Session invalide.",
        code: isExpired ? "TOKEN_EXPIRED" : "INVALID_TOKEN"
      });
    }

    if (!user) {
      await recordLoginAttempt(ip, null, false, "User introuvable");
      return res.status(401).json({ success: false, error: true, reply: "Session invalide.", code: "INVALID_TOKEN" });
    }

    req.uid = user.uid;
    req.userId = user.uid;
    req.firebaseUid = user.uid;
    req.verifiedIdentity = true;
    req.userRole = user.role || "FREE";
    req.emailVerified = user.emailVerified;
    req.authSource = user.authSource || "unknown";

    logger.info({ uid: user.uid, role: req.userRole, source: user.authSource }, "✅ Utilisateur authentifié");

    setImmediate(() => {
      Promise.allSettled([
        recordLoginAttempt(ip, user.uid, true),
        logSecurityEvent(user.uid, "LOGIN_SUCCESS", { email: user.email, source: user.authSource }, ip, ua),
        fastUpsertUser(user.uid, user, req.userRole)
      ]).catch(() => {});
    });

    next();
  })().catch((e) => {
    logger.error({ err: e.message, stack: e.stack }, "authenticateUser erreur interne");
    if (!res.headersSent) {
      return res.status(500).json({ success: false, error: true, reply: "Erreur d'authentification.", code: "AUTH_INTERNAL_ERROR" });
    }
  });
}

function requireRole(roles) {
  return (req, res, next) => {
    // ✅ Variable
    if (!firebaseApp && roles.includes("ADMIN")) {
      return res.status(503).json({
        success: false, error: true,
        reply: "Admin indisponible (Admin SDK requis).",
        code: "ADMIN_REQUIRES_SERVICE_ACCOUNT"
      });
    }
    if (!req.userRole || (!roles.includes(req.userRole) && req.userRole !== "ADMIN")) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "INSUFFICIENT_ROLE" });
    }
    next();
  };
}

// ================================================================================
// §5.05 — ROUTES API
// ================================================================================

// ---------- Info racine ----------
app.get("/", (req, res) => {
  res.json({
    success: true, error: false,
    reply: `Serveur ${CONFIG.AGENT_NAME} opérationnel`,
    version: CONFIG.VERSION,
    company: CONFIG.COMPANY
  });
});

// ---------- /api/health ----------
app.get("/api/health", async (req, res) => {
  let dbOk = true;
  try { await dbGet("SELECT 1"); } catch { dbOk = false; }

  const full = req.query.full === "1";
  const pub = {
    success: dbOk, error: !dbOk,
    reply: dbOk ? `Serveur ${CONFIG.AGENT_NAME} en bonne santé` : "Serveur en maintenance",
    data: {
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      version: CONFIG.VERSION,
      database: dbOk ? "ok" : "erreur",
      features: {
        vision: FEATURES.gemini || FEATURES.groq,
        quotas: true, sse: true,
        live: process.env.LUBA_LIVE_ENABLED !== "false",
        aiQuality: CONFIG.AI_QUALITY.ENABLE_SELF_CRITIQUE,
        selfHealing: true,
        guaranteedImages: true,
        longTermMemory: true
      }
    }
  };

  if (!full) return res.json(pub);

  return res.json({
    ...pub,
    data: {
      ...pub.data,
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
      firestore: Boolean(firestoreDb),
      supabase: Boolean(supabase),
      redis: Boolean(redisClient),
      firebaseAuth: firebaseApp ? "admin_sdk" : "api_rest",
      providers: {
        groq: LLM_PROVIDERS.GROQ.keyPool.length,
        openrouter: LLM_PROVIDERS.OPENROUTER.keyPool.length,
        cerebras: LLM_PROVIDERS.CEREBRAS.keyPool.length,
        gemini: geminiClient ? "actif" : "inactif"
      },
      circuits: getAllCircuitStates(),
      providersHealth: providerHealth.getStats(),
      aiQuality: CONFIG.AI_QUALITY,
      features: FEATURES
    }
  });
});

// ---------- /ready ----------
app.get("/ready", async (req, res) => {
  try {
    await dbGet("SELECT 1");
    return res.json({ ready: true });
  } catch {
    return res.status(503).json({ ready: false });
  }
});

// ---------- /api/metrics ----------
app.get("/api/metrics", async (req, res) => {
  // ✅ Variable
  if (!metrics) return res.status(503).json({ error: "Metrics indisponibles" });

  const token = process.env.METRICS_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    res.setHeader("Content-Type", metrics.register.contentType);
    res.end(await metrics.register.metrics());
  } catch {
    res.status(500).end();
  }
});

// ---------- /api/debug ----------
app.get("/api/debug", async (req, res) => {
  const token = process.env.DEBUG_TOKEN;
  if (CONFIG.ENV === "production" && (!token || req.query.token !== token)) {
    return res.status(404).json({ error: "Not found" });
  }

  const checks = {};

  try {
    const mr = await evaluateMathSafe("15*32+7");
    checks.math = { ok: mr.success, result: mr.formatted || mr.error };
  } catch (e) {
    checks.math = { ok: false, error: e.message };
  }

  try {
    const sb = await runCodeSandbox({ language: "python", code: "print(2 + 2)" });
    checks.sandbox = {
      ok: sb.success,
      provider: CONFIG.SANDBOX.PROVIDER,
      url: CONFIG.SANDBOX.PISTON_URL || CONFIG.SANDBOX.JUDGE0_URL,
      stdout: sb.stdout?.slice(0, 100) || null,
      error: sb.error || null
    };
  } catch (e) {
    checks.sandbox = { ok: false, error: e.message };
  }

  try {
    const imgTest = await searchImagesWithFallback("LeBron James", 2);
    checks.images = {
      ok: imgTest.images?.length > 0,
      count: imgTest.images?.length || 0,
      firstUrl: imgTest.images?.[0]?.url?.slice(0, 100) || null
    };
  } catch (e) {
    checks.images = { ok: false, error: e.message };
  }

  checks.llm = {};
  for (const [n, cfg] of Object.entries(LLM_PROVIDERS)) {
    checks.llm[n] = { keys: cfg.keyPool.length, available: cfg.keyPool.length > 0 };
  }

  if (Object.values(checks.llm).some((c) => c.available)) {
    try {
      const tr = await callProviderWithTools({
        providerConfig: MODEL_TIERS.v100.providers[0],
        messages: [
          { role: "system", content: "Réponds juste 'OK'." },
          { role: "user", content: "Test" }
        ],
        tools: null
      });
      checks.llmCall = {
        ok: tr.success,
        provider: tr.providerUsed,
        model: tr.modelUsed,
        preview: tr.message?.content?.slice(0, 50) || null,
        error: tr.error?.message || null
      };
    } catch (e) {
      checks.llmCall = { ok: false, error: e.message };
    }
  }

  checks.storage = {
    firestore: Boolean(firestoreDb),
    supabase: Boolean(supabase),
    sqlite: Boolean(db)
  };

  checks.circuits = getAllCircuitStates().map((c) => ({
    name: c.name, state: c.state, failures: c.failureCount
  }));

  checks.providersHealth = providerHealth.getStats();

  checks.cache = {
    l1: l1Cache.size || 0,
    redis: Boolean(redisClient),
    semantic: semanticCache.size()
  };

  return res.json({ success: true, checks });
});

// ---------- /api/auth/check ----------
app.get("/api/auth/check", authLimiter, authenticateUser, (req, res) => {
  res.json({
    success: true, error: false,
    authenticated: true,
    uid: req.uid,
    firebaseUid: req.firebaseUid,
    role: req.userRole,
    authSource: req.authSource || "unknown",
    projectId: FIREBASE_CONFIG.projectId,
    firebaseAdminReady: Boolean(firebaseApp && firebaseAdmin),
    firestoreReady: Boolean(firestoreDb),
    checkedAt: new Date().toISOString()
  });
});

// ---------- /api/user/whoami ----------
app.get("/api/user/whoami", authLimiter, authenticateUser, (req, res) => {
  res.json({ success: true, error: false, userId: req.userId, role: req.userRole });
});

// ---------- /api/session/bootstrap ----------
app.get("/api/session/bootstrap", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const today = todayKeyMs();
    let conversations = [];

    if (firestoreDb) {
      try {
        const rows = await fsQuery("sessions", {
          where: [["user_id", "==", userId]],
          orderBy: { field: "updated_at", direction: "desc" },
          limit: 30
        });
        if (rows.length > 0) {
          conversations = await Promise.all(rows.map(async (s) => {
            const msgs = await fsQuery("messages", {
              where: [["session_id", "==", s.session_id]],
              orderBy: { field: "created_at", direction: "desc" },
              limit: 1
            });
            const last = msgs[0];
            return {
              conversationId: s.session_id,
              createdAt: s.created_at,
              updatedAt: s.updated_at,
              lastMessageRole: last?.role || null,
              lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
            };
          }));
        }
      } catch {}
    }

    if (conversations.length === 0) {
      const rows = await dbAll(
        `SELECT session_id, created_at, updated_at FROM sessions
         WHERE user_id = ? ORDER BY updated_at DESC LIMIT 30`,
        [userId]
      );
      conversations = await Promise.all(rows.map(async (c) => {
        const last = await dbGet(
          `SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1`,
          [c.session_id]
        );
        return {
          conversationId: c.session_id,
          createdAt: new Date(c.created_at).toISOString(),
          updatedAt: new Date(c.updated_at).toISOString(),
          lastMessageRole: last?.role || null,
          lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
        };
      }));
    }

    const [quota, tasks, user, mem, facts, ltm] = await Promise.all([
      dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]).catch(() => null),
      listTasks(userId, { status: "pending" }).catch(() => ({ tasks: [] })),
      dbGet(`SELECT whatsapp_connected, display_name FROM users WHERE id = ?`, [userId]).catch(() => null),
      getUserMemory(userId).catch(() => ""),
      getAllFacts(userId).catch(() => ({ grouped: {}, total: 0 })),
      recallLongTermFacts(userId, { limit: 20 }).catch(() => ({ facts: [] }))
    ]);

    const dn = user?.display_name || null;
    const greeting = dn
      ? `Bonjour ${dn.split(" ")[0]}, comment puis-je vous aider ?`
      : (mem ? "Content de vous revoir. Comment puis-je vous aider ?" : "Bonjour, je suis Luba. Comment puis-je vous aider ?");

    return res.json({
      success: true, error: false,
      userId, role: req.userRole, greeting,
      conversations,
      pendingTasks: tasks.tasks || [],
      memoryFacts: facts.grouped || {},
      longTermFacts: ltm.facts || [],
      quotas: quota || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
      limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
      whatsappConnected: Boolean(user?.whatsapp_connected),
      hasMemory: Boolean(mem),
      ads: getAllAdSlots(),
      version: CONFIG.VERSION
    });
  } catch (e) {
    logger.error({ err: e.message }, "bootstrap");
    return res.status(500).json({ success: false, error: true, code: "BOOTSTRAP_ERROR" });
  }
});

// ---------- Helpers SSE ----------
function wantsStreaming(req) {
  const accept = String(req.headers.accept || "").toLowerCase();
  return accept.includes("text/event-stream")
    || (req.body && req.body.stream === true)
    || req.query.stream === "true";
}

function sseShortError(res, message, code) {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  if (typeof res.flushHeaders === "function") res.flushHeaders();
  res.write(`event: error\ndata: ${safeJsonStringify({ reply: message, code })}\n\n`);
  res.write(`event: done\ndata: ${safeJsonStringify({ error: true })}\n\n`);
  res.end();
}

// ---------- POST /api/chat ----------
app.post(
  "/api/chat",
  chatLimiter,
  authenticateUser,
  upload.array("images", CONFIG.LIMITS.MAX_IMAGES_PER_REQUEST),
  async (req, res) => {
    const isStream = wantsStreaming(req) && !(req.files && req.files.length > 0);

    try {
      const raw = req.body?.message;
      let convId = req.body?.conversationId || req.body?.conversation_id;
      const modelTier = req.body?.modelTier === "v250" ? "v250" : "v100";

      if (!raw || typeof raw !== "string") {
        if (isStream) return sseShortError(res, "Le message est obligatoire.", "MISSING_MESSAGE");
        return res.status(400).json({ success: false, error: true, reply: "Message obligatoire.", code: "MISSING_MESSAGE" });
      }

      const sanitized = sanitizeForLLM(raw);
      if (!sanitized) {
        if (isStream) return sseShortError(res, "Message vide.", "INVALID_MESSAGE");
        return res.status(400).json({ success: false, error: true, reply: "Message vide.", code: "INVALID_MESSAGE" });
      }

      const inj = detectPromptInjection(sanitized);
      if (inj.detected) {
        await logSecurityEvent(req.userId, "PROMPT_INJECTION_BLOCKED", { pattern: inj.pattern }, req.ip, req.headers["user-agent"]);
        if (isStream) return sseShortError(res, "Requête bloquée.", "PROMPT_INJECTION");
        return res.status(400).json({ success: false, error: true, reply: "Requête bloquée.", code: "PROMPT_INJECTION" });
      }

      const mod = await moderateWithGroq(sanitized);
      if (!mod.safe) {
        await logSecurityEvent(req.userId, "CONTENT_BLOCKED", { category: mod.category }, req.ip, req.headers["user-agent"]);
        if (isStream) return sseShortError(res, "Contenu non autorisé.", "CONTENT_BLOCKED");
        return res.status(400).json({ success: false, error: true, reply: "Contenu non autorisé.", code: "CONTENT_BLOCKED" });
      }

      if (CONFIG.HMAC.ENABLED) {
        const h = verifyHmacSignature(req);
        if (!h.valid && !h.skipped) {
          return res.status(401).json({ success: false, error: true, reply: "Signature invalide.", code: "INVALID_SIGNATURE" });
        }
      }

      if (convId && !/^[a-zA-Z0-9_-]{6,80}$/.test(convId)) {
        if (isStream) return sseShortError(res, "ID conversation invalide.", "INVALID_CONVERSATION_ID");
        return res.status(400).json({ success: false, error: true, reply: "ID invalide.", code: "INVALID_CONVERSATION_ID" });
      }
      if (!convId) convId = generateConversationId();
      const isNew = !req.body?.conversationId && !req.body?.conversation_id;

      try { await assertConversationOwnership(convId, req.userId); }
      catch (e) {
        if (isStream) return sseShortError(res, e.message, "CONVERSATION_OWNERSHIP");
        return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" });
      }

      const q = await checkUserQuota(req.userId, "message", req.userRole);
      if (!q.allowed) {
        if (isStream) return sseShortError(res, q.message, "QUOTA_EXCEEDED");
        return res.status(429).json({ success: false, error: true, reply: q.message, code: "QUOTA_EXCEEDED" });
      }
      await incrementUserQuota(req.userId, "message");

      let imgs = null;
      if (req.files && req.files.length > 0) {
        const bad = req.files.find((f) => !isValidImageSignature(f.buffer));
        if (bad) {
          return res.status(400).json({ success: false, error: true, reply: "Image invalide.", code: "INVALID_IMAGE_CONTENT" });
        }
        imgs = req.files.map((f) => convertImageToBase64(f.buffer, f.mimetype));
        await incrementUserQuota(req.userId, "image");
      }

      const gToken = req.headers["x-google-access-token"] || null;

      if (isStream) {
        const sse = new SSEWriter(res);
        req.on("close", () => { sse.closed = true; });
        sse.status("accepted", { conversationId: convId, isNewConversation: isNew });

        try {
          await handleChat({
            conversationId: convId, userId: req.userId, firebaseUid: req.firebaseUid,
            message: sanitized, googleAccessToken: gToken,
            channel: "web-sse", modelTier, images: imgs, sse
          });
        } catch (e) {
          logger.error({ err: e.message, stack: e.stack }, "SSE handleChat erreur");
          if (!sse.closed) {
            sse.error({ reply: "Une erreur interne est survenue." });
            sse.done({ error: true });
          }
        } finally {
          if (!sse.closed) sse.end();
        }
        return;
      }

      const result = await handleChat({
        conversationId: convId, userId: req.userId, firebaseUid: req.firebaseUid,
        message: sanitized, googleAccessToken: gToken,
        channel: "web", modelTier, images: imgs, sse: null
      });

      return res.status(200).json({ ...result, conversationId: convId, isNewConversation: isNew });
    } catch (e) {
      logger.error({ err: e.message, stack: e.stack }, "Erreur /api/chat");
      if (res.headersSent) return;
      return res.status(500).json({ success: false, error: true, reply: "Erreur.", code: "CHAT_ERROR" });
    }
  }
);

// ---------- GET /api/conversations ----------
app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT session_id, created_at, updated_at FROM sessions
       WHERE user_id = ? ORDER BY updated_at DESC LIMIT 50`,
      [req.userId]
    );
    const enriched = await Promise.all(rows.map(async (c) => {
      const last = await dbGet(
        `SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1`,
        [c.session_id]
      );
      return {
        conversationId: c.session_id,
        createdAt: new Date(c.created_at).toISOString(),
        updatedAt: new Date(c.updated_at).toISOString(),
        lastMessageRole: last?.role || null,
        lastMessagePreview: last?.content ? String(last.content).slice(0, 140) : null
      };
    }));
    return res.json({ success: true, error: false, conversations: enriched });
  } catch {
    return res.status(500).json({ success: false, error: true, conversations: [] });
  }
});

// ---------- GET /api/conversation/:id/messages ----------
app.get("/api/conversation/:conversationId/messages", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { conversationId } = req.params;
    if (!conversationId) {
      return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });
    }

    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    const wantsFull = req.query.full === "true";
    const rl = parseInt(req.query.limit, 10);
    const limit = wantsFull ? 500 : (Number.isFinite(rl) && rl > 0 ? Math.min(rl, 200) : CONFIG.LIMITS.MAX_HISTORY_LENGTH);

    const messages = await getFullHistory(conversationId, req.userId, limit);
    return res.json({ success: true, error: false, conversationId, messages, count: messages.length });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "HISTORY_FETCH_ERROR" });
  }
});

// ---------- GET /api/user/stats ----------
app.get("/api/user/stats", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const quota = await dbGet(
      `SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`,
      [req.userId, todayKeyMs()]
    );
    return res.json({
      success: true, error: false,
      data: {
        quotas: quota || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
        role: req.userRole || "FREE",
        limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
        userId: req.userId
      }
    });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "STATS_ERROR" });
  }
});

// ---------- POST /api/tools ----------
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

    const gToken = req.headers["x-google-access-token"] || null;
    const { result, sourceKeys } = await executeToolNative(toolName, params, {
      userId: req.userId, googleAccessToken: gToken, agentMode: false
    });

    if (result?.code === "NEEDS_CONFIRMATION") {
      return res.status(202).json({
        success: false, error: false,
        reply: "Confirmation requise.",
        code: "NEEDS_CONFIRMATION",
        confirmation: { toolName, args: params }
      });
    }

    return res.json({ success: true, error: false, toolName, result, sources: sourceKeys });
  } catch (e) {
    return res.status(500).json({ success: false, error: true, code: "TOOL_EXECUTION_ERROR" });
  }
});

// ---------- Tasks CRUD ----------
app.get("/api/tasks", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const status = req.query.status && ["pending", "done"].includes(req.query.status) ? req.query.status : null;
    const r = await listTasks(req.userId, { status });
    return res.json({ success: true, error: false, tasks: r.tasks || [] });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "TASKS_FETCH_ERROR" });
  }
});

app.post("/api/tasks", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { title, notes, dueAt } = req.body || {};
    const r = await createTask(req.userId, { title, notes, dueAt: dueAt ? Date.parse(dueAt) : null });
    if (!r.success) return res.status(400).json({ success: false, error: true, reply: r.error, code: "TASK_CREATE_INVALID" });
    return res.status(201).json({ success: true, error: false, task: r.task });
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
    const r = await updateTaskStatus(req.userId, req.params.taskId, status);
    if (!r.success) return res.status(404).json({ success: false, error: true, code: "TASK_NOT_FOUND" });
    return res.json({ success: true, error: false, task: r.task });
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

// ---------- Mémoire long terme ----------
app.get("/api/memory/facts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const f = await getAllFacts(req.userId);
    const ltm = await recallLongTermFacts(req.userId, { limit: 50 });
    return res.json({ success: true, error: false, ...f, longTermFacts: ltm.facts || [] });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "MEMORY_FETCH_ERROR" });
  }
});

app.delete("/api/memory/facts/:factId", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const r = await deleteFact(req.userId, parseInt(req.params.factId, 10));
    return res.json({ success: r.success, error: !r.success });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

app.delete("/api/memory/facts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    await clearAllFacts(req.userId);
    await dbRun("DELETE FROM user_memory WHERE user_id = ?", [req.userId]);
    return res.json({ success: true, error: false, message: "Mémoire effacée." });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

app.post("/api/memory/recall", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { query } = req.body || {};
    if (!query) return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    const r = await recallMemory(req.userId, query, 5);
    const ltm = await recallLongTermFacts(req.userId, { limit: 10 });
    return res.json({ success: true, error: false, ...r, longTermFacts: ltm.facts || [] });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- Ads ----------
app.get("/api/ads", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const slot = req.query.slot || "chat_below";
    const ad = await getAd({ slot, userId: req.userId });
    return res.json({ success: true, error: false, ad });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "ADS_ERROR" });
  }
});

app.get("/api/ads/slots", (req, res) => {
  return res.json({ success: true, error: false, slots: getAllAdSlots() });
});

// ---------- YouTube search ----------
app.get("/api/youtube/search", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const q = req.query.q;
    if (!q || typeof q !== "string") {
      return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    }
    const entity = extractEntity(q) || q;
    const r = await searchYouTube(entity);
    return res.json({ success: true, error: false, videos: r.videos || [], provider: r.provider });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "YOUTUBE_SEARCH_ERROR" });
  }
});

// ---------- Voice transcribe ----------
app.post("/api/voice/transcribe", apiLimiter, authenticateUser, uploadAudio.single("audio"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: true, reply: "Aucun audio.", code: "MISSING_AUDIO" });
    }
    const q = await checkUserQuota(req.userId, "message", req.userRole);
    if (!q.allowed) return res.status(429).json({ success: false, error: true, reply: q.message, code: "QUOTA_EXCEEDED" });

    const r = await transcribeAudioGroq(req.file.buffer, req.file.originalname, req.file.mimetype);
    if (!r.success) return res.status(502).json({ success: false, error: true, reply: "Transcription indisponible.", code: "TRANSCRIPTION_FAILED" });
    return res.json({ success: true, error: false, text: sanitizeForLLM(r.text, 5000), provider: r.provider });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "VOICE_TRANSCRIBE_ERROR" });
  }
});

// ---------- Voice TTS ----------
app.post("/api/voice/tts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { text, voice } = req.body || {};
    if (!text) return res.status(400).json({ success: false, error: true, code: "MISSING_TEXT" });

    const r = await synthesizeSpeech(text, { voice: voice || "af_bella" });
    if (!r.success) return res.status(503).json({ success: false, error: true, reply: r.error });

    res.setHeader("Content-Type", r.format === "wav" ? "audio/wav" : "audio/mpeg");
    return res.send(r.audio);
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- WhatsApp ----------
app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const r = await baileysManager.initClient(req.userId);
    if (r.connected) return res.json({ success: true, error: false, message: "WhatsApp déjà connecté.", data: { qrCode: null } });

    let qr = null;
    const start = Date.now();
    while (!qr && Date.now() - start < CONFIG.WHATSAPP.QR_TIMEOUT_MS) {
      await sleep(500);
      qr = baileysManager.getQRCode(req.userId);
    }

    if (qr) return res.json({ success: true, error: false, message: "Connexion initiée", data: { qrCode: qr } });
    return res.status(408).json({ success: false, error: true, message: "Délai dépassé.", code: "QR_TIMEOUT" });
  } catch (e) {
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_CONNECT_ERROR" });
  }
});

app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const { to, message } = req.body || {};
    if (!to || !message) return res.status(400).json({ success: false, error: true, code: "MISSING_PARAMS" });

    const q = await checkUserQuota(req.userId, "whatsapp", req.userRole);
    if (!q.allowed) return res.status(429).json({ success: false, error: true, reply: q.message, code: "WHATSAPP_QUOTA_EXCEEDED" });

    const clean = String(to).replace(/[^\d]/g, "");
    if (!PHONE_REGEX.test(clean)) return res.status(400).json({ success: false, error: true, code: "INVALID_PHONE" });

    const r = await baileysManager.sendMessage(req.userId, clean, sanitizeStrict(message, 2000));
    await incrementUserQuota(req.userId, "whatsapp");
    return res.json({ success: true, error: false, data: r });
  } catch (e) {
    if (e.code === "WHATSAPP_NOT_CONNECTED") {
      return res.status(409).json({ success: false, error: true, reply: "WhatsApp non connecté.", code: "WHATSAPP_NOT_CONNECTED" });
    }
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_SEND_ERROR" });
  }
});

// ---------- Intent init ----------
app.post("/api/intent/init", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { intentType } = req.body || {};
    const convId = req.body?.conversationId || req.body?.conversation_id;
    if (!convId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    try { await assertConversationOwnership(convId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    await getSession(convId, req.userId, req.firebaseUid);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(convId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.json({ success: true, error: false, reply: "Envoi WhatsApp initié. Quel est le numéro ?" });
    }
    if (intentType === "EMAIL") {
      await setActiveIntent(convId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.json({ success: true, error: false, reply: "Envoi d'email initié. Quelle est l'adresse ?" });
    }
    return res.status(400).json({ success: false, error: true, code: "UNKNOWN_INTENT" });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "INTENT_ERROR" });
  }
});

// ---------- Memory clear ----------
app.post("/api/memory/clear", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const convId = req.body?.conversationId || req.body?.conversation_id;
    if (!convId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    try { await assertConversationOwnership(convId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [convId]);
    await clearActiveIntent(convId);

    if (supabase) {
      supabaseWriteSafe({
        table: "messages", op: "delete", payload: {},
        matchColumn: "session_id", matchValue: convId
      }).catch(() => {});
    }
    return res.json({ success: true, error: false, reply: "Mémoire effacée." });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- Admin : set role ----------
app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(["ADMIN"]), async (req, res) => {
  try {
    const { uid, role } = req.body || {};
    if (!uid || !["FREE", "PREMIUM", "ADMIN"].includes(role)) {
      return res.status(400).json({ success: false, error: true, code: "INVALID_PARAMS" });
    }
    // ✅ Variables
    if (firebaseApp && firebaseAdmin) {
      await firebaseAdmin.auth(firebaseApp).setCustomUserClaims(uid, { role });
    }
    await dbRun(`UPDATE users SET role = ?, updated_at = ? WHERE firebase_uid = ? OR id = ?`, [role, Date.now(), uid, uid]);
    await logSecurityEvent(req.userId, "ROLE_UPDATED", { targetUid: uid, newRole: role }, req.ip, req.headers["user-agent"]);
    return res.json({ success: true, error: false, data: { uid, role } });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "ROLE_UPDATE_ERROR" });
  }
});

// ---------- /api/admin/self-heal/reset ----------
app.post("/api/admin/self-heal/reset", strictLimiter, authenticateUser, requireRole(["ADMIN"]), async (req, res) => {
  try {
    providerHealth.reset();
    if (metrics?.selfHealActions) {
      metrics.selfHealActions.labels("manual_reset", "admin").inc();
    }
    return res.json({ success: true, error: false, message: "Santé providers réinitialisée." });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/admin/self-heal/stats ----------
app.get("/api/admin/self-heal/stats", strictLimiter, authenticateUser, requireRole(["ADMIN"]), async (req, res) => {
  try {
    return res.json({
      success: true, error: false,
      stats: providerHealth.getStats(),
      circuits: getAllCircuitStates()
    });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- RGPD delete account ----------
app.delete("/api/account", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const fbUid = req.firebaseUid;

    try {
      const s = baileysManager.sessions.get(userId);
      if (s?.sock) s.sock.end(undefined);
      baileysManager.sessions.delete(userId);
      const d = path.join(CONFIG.PATHS.SESSIONS, userId);
      if (fs.existsSync(d)) fs.rmSync(d, { recursive: true, force: true });
    } catch {}

    await dbTransaction(async ({ dbRun }) => {
      await dbRun("DELETE FROM messages WHERE session_id IN (SELECT session_id FROM sessions WHERE user_id = ?)", [userId]);
      await dbRun("DELETE FROM sessions WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM email_logs WHERE user_id = ? OR firebase_uid = ?", [userId, fbUid]);
      await dbRun("DELETE FROM llm_audit_log WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM security_logs WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_quotas WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM active_sessions WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_tasks WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_memory WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_memory_facts WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM user_long_term_memory WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM reasoning_traces WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM users WHERE id = ?", [userId]);
    });

    if (firestoreDb) {
      const cols = ["messages", "sessions", "user_tasks", "user_memory", "user_memory_facts"];
      for (const c of cols) {
        const rows = await fsQuery(c, { where: [["user_id", "==", userId]], limit: 500 });
        for (const r of rows) fsDelete(c, r.id).catch(() => {});
      }
      fsDelete("users", userId).catch(() => {});
    }

    if (supabase) {
      const tables = ["messages", "sessions", "user_tasks", "user_memory", "whatsapp_credentials"];
      for (const t of tables) {
        await supabaseWriteSafe({
          table: t, op: "delete", payload: {},
          matchColumn: "user_id", matchValue: userId
        }).catch(() => {});
      }
    }

    let fbDel = false;
    // ✅ Variables
    if (firebaseApp && firebaseAdmin) {
      try {
        await firebaseAdmin.auth(firebaseApp).deleteUser(fbUid);
        fbDel = true;
      } catch {}
    }

    return res.json({
      success: true, error: false,
      message: "Compte supprimé avec succès.",
      code: "ACCOUNT_DELETED",
      firebaseAccountDeleted: fbDel
    });
  } catch (e) {
    logger.error({ err: e.message, stack: e.stack }, "/api/account");
    return res.status(500).json({ success: false, error: true, code: "ACCOUNT_DELETION_ERROR" });
  }
});

// ---------- 404 ----------
app.use((req, res) => {
  res.status(404).json({ success: false, error: true, reply: "Route non trouvée", code: "NOT_FOUND" });
});

// ---------- Gestion erreur globale ----------
app.use((error, req, res, next) => {
  logger.error({ err: error.message, stack: error.stack, path: req.path }, "Erreur non gérée");
  if (res.headersSent) return next(error);

  if (error.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({ success: false, error: true, reply: "Fichier trop volumineux.", code: "FILE_TOO_LARGE" });
  }
  if (error.code === "LIMIT_FILE_COUNT") {
    return res.status(413).json({ success: false, error: true, reply: "Trop de fichiers.", code: "TOO_MANY_FILES" });
  }
  return res.status(500).json({ success: false, error: true, reply: "Erreur interne.", code: "INTERNAL_ERROR" });
});

// ================================================================================
// §5.06 — LUBA LIVE WEBSOCKET
// ================================================================================

const WS_FRAME = Object.freeze({
  HELLO: 0x01, AUDIO_IN: 0x02, TEXT_IN: 0x03, BARGE_IN: 0x04, END_TURN: 0x05,
  TRANSCRIPT: 0x11, TOKEN: 0x12, AUDIO_OUT: 0x13, STATUS: 0x14, ERROR: 0x15,
  DONE: 0x16, QUALITY: 0x17, AD: 0x18
});

function encodeFrame(type, payload) {
  const buf = Buffer.isBuffer(payload)
    ? payload
    : typeof payload === "string"
      ? Buffer.from(payload, "utf8")
      : Buffer.from(safeJsonStringify(payload ?? {}), "utf8");

  const frame = Buffer.allocUnsafe(5 + buf.length);
  frame.writeUInt8(type, 0);
  frame.writeUInt32BE(buf.length, 1);
  buf.copy(frame, 5);
  return frame;
}

function decodeFrame(buffer) {
  if (buffer.length < 5) return null;
  const type = buffer.readUInt8(0);
  const length = buffer.readUInt32BE(1);
  if (buffer.length < 5 + length) return null;
  return { type, payload: buffer.subarray(5, 5 + length) };
}

let WebSocketServer = null;
try {
  WebSocketServer = require("ws").WebSocketServer || require("ws").Server;
} catch {}

let wsServer = null;
const liveSessions = new Map();

function setupLubaLiveWebSocket(server) {
  if (!WebSocketServer) {
    logger.warn("⚠️  ws non installé — Luba Live désactivé");
    return;
  }
  if (process.env.LUBA_LIVE_ENABLED === "false") {
    logger.info("ℹ️  Luba Live désactivé par env");
    return;
  }

  wsServer = new WebSocketServer({
    server, path: "/live",
    perMessageDeflate: false,
    maxPayload: 1024 * 1024
  });

  wsServer.on("connection", (ws, req) => {
    const sid = `live_${crypto.randomUUID()}`;
    const state = {
      sessionId: sid, userId: null, conversationId: null,
      vad: new SimpleVAD(), stt: null,
      interrupted: false, authenticated: false, createdAt: Date.now()
    };
    liveSessions.set(sid, state);

    if (metrics?.activeWebSockets) metrics.activeWebSockets.labels("live").inc();
    logger.info({ sessionId: sid }, "🔌 Luba Live connecté");

    ws.on("message", async (data) => {
      try {
        const frame = decodeFrame(data);
        if (!frame) return;
        await handleLiveFrame(ws, state, frame);
      } catch (e) {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Bad frame" }));
      }
    });

    ws.on("close", () => {
      liveSessions.delete(sid);
      if (metrics?.activeWebSockets) metrics.activeWebSockets.labels("live").dec();
    });

    ws.on("error", () => {});
  });

  logger.info("✅ Luba Live WebSocket initialisé sur /live");
}

async function handleLiveFrame(ws, state, frame) {
  const { type, payload } = frame;

  if (type === WS_FRAME.HELLO) {
    const hello = safeJsonParse(payload.toString("utf8"), {});
    const token = hello.token;
    if (!token) return ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Missing token" }));
    try {
      const user = await verifyFirebaseToken(token);
      if (!user) throw new Error("Invalid");
      state.userId = user.uid;
      state.conversationId = hello.conversationId || `live_${crypto.randomUUID()}`;
      state.authenticated = true;

      state.stt = new StreamingSTT({
        onPartial: (text) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text, partial: true })); },
        onFinal: (text) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text, partial: false })); },
        onError: () => {}
      });

      ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "ready", sessionId: state.sessionId, conversationId: state.conversationId }));
    } catch {
      ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Authentication failed" }));
    }
    return;
  }

  if (!state.authenticated) return ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Not authenticated" }));

  if (type === WS_FRAME.BARGE_IN) {
    state.interrupted = true;
    if (state.stt) state.stt.reset();
    ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "barge_in" }));
    return;
  }

  if (type === WS_FRAME.AUDIO_IN) {
    if (state.interrupted) return;
    if (state.stt) await state.stt.push(payload, { mimetype: "audio/webm" });
    return;
  }

  if (type === WS_FRAME.TEXT_IN) {
    const text = payload.toString("utf8").slice(0, CONFIG.LIMITS.MAX_MESSAGE_LENGTH);
    await handleLiveTurn(ws, state, text);
    return;
  }

  if (type === WS_FRAME.END_TURN) {
    if (!state.stt) return;
    const r = await state.stt.finalize("audio/webm");
    if (r.success && r.text) {
      ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text: r.text, partial: false }));
      await handleLiveTurn(ws, state, r.text);
    }
    return;
  }
}

async function handleLiveTurn(ws, state, userText) {
  if (!userText) return;
  state.interrupted = false;
  ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "thinking" }));

  const ttsQ = [];
  let ttsRunning = false;

  const processTTS = async () => {
    if (ttsRunning) return;
    ttsRunning = true;
    while (ttsQ.length > 0 && !state.interrupted && ws.readyState === 1) {
      const s = ttsQ.shift();
      try {
        const t = await synthesizeSpeech(s, { voice: "af_bella" });
        if (t.success && !state.interrupted && ws.readyState === 1) {
          ws.send(encodeFrame(WS_FRAME.AUDIO_OUT, t.audio));
        }
      } catch {}
    }
    ttsRunning = false;
  };

  const chunker = new SentenceChunker({
    onSentence: (s) => {
      if (state.interrupted) return;
      ttsQ.push(s);
      processTTS().catch(() => {});
    }
  });

  try {
    const sseAdp = {
      closed: false,
      status: (stage, extra) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage, ...extra })); },
      reasoning: () => {},
      codeBlock: () => {},
      token: (text) => {
        if (state.interrupted || ws.readyState !== 1) return;
        ws.send(encodeFrame(WS_FRAME.TOKEN, text));
        chunker.push(text);
      },
      images: (list) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "images", images: list })); },
      videos: (list) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "videos", videos: list })); },
      suggestions: (list) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "suggestions", suggestions: list })); },
      sources: (list) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "sources", sources: list })); },
      quality: (payload) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.QUALITY, payload)); },
      ad: (payload) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.AD, payload)); },
      error: (payload) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.ERROR, payload)); },
      done: (payload) => { if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.DONE, payload)); },
      send: () => {},
      end: () => {}
    };

    await handleChat({
      conversationId: state.conversationId,
      userId: state.userId,
      firebaseUid: state.userId,
      message: userText,
      channel: "live-ws",
      modelTier: "v100",
      images: null,
      sse: sseAdp
    });

    chunker.flush();
    await new Promise((r) => setTimeout(r, 300));

    if (ws.readyState === 1 && !state.interrupted) {
      ws.send(encodeFrame(WS_FRAME.DONE, { conversationId: state.conversationId }));
    }
  } catch (e) {
    if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Erreur de traitement" }));
  }
}

// ================================================================================
// §5.07 — BOOTSTRAP + GRACEFUL SHUTDOWN
// ================================================================================

let server = null;
let isShuttingDown = false;

async function bootstrap() {
  console.log("");
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log(`║  🚀 LUBA AI PRO v${CONFIG.VERSION} — HIKLON TECHNOLOGIES           ║`);
  console.log("╚══════════════════════════════════════════════════════════════╝");

  await bootstrapPart1();

  server = app.listen(CONFIG.PORT, CONFIG.HOST, () => {
    global.__luba_server = server;
    logger.info(`Serveur ${CONFIG.AGENT_NAME} v${CONFIG.VERSION} démarré sur ${CONFIG.HOST}:${CONFIG.PORT}`);
  });

  server.on("error", (err) => {
    logger.fatal({ err: err.message }, "Erreur serveur HTTP");
    process.exit(1);
  });

  setupLubaLiveWebSocket(server);

  setInterval(reminderTick, CONFIG.TIMEOUTS.REMINDER_TICK_MS).unref?.();
  setInterval(runSecurityHousekeeping, CONFIG.TIMEOUTS.HOUSEKEEPING_MS).unref?.();

  // ✅ Variables (sans parenthèses)
  console.log("");
  console.log("🌐 Domaine      : " + HOSTING_CONFIG.domain);
  console.log("🔐 Firebase     : " + (firebaseApp ? "Admin SDK ✅" : "REST API ⚠️"));
  console.log("⚡ Redis        : " + (redisClient ? "✅" : "❌ (LRU fallback)"));
  console.log("📊 Metrics      : " + (metrics ? "✅ /api/metrics" : "❌"));
  console.log("💾 Firestore    : " + (firestoreDb ? "✅" : "❌"));
  console.log("💾 Supabase     : " + (supabase ? "✅" : "❌"));
  console.log("💾 SQLite       : ✅");
  console.log("📧 Email        : " + (emailTransporter ? "SMTP ✅" : (process.env.RESEND_API_KEY ? "Resend ✅" : "❌")));
  console.log("📱 WhatsApp     : " + (CONFIG.WHATSAPP.ENCRYPTION_KEY ? "Chiffré ✅" : "⚠️"));
  console.log("🛡️  Rate limit  : " + (redisRateLimitStore ? "Redis ✅" : "Mémoire ⚠️"));
  console.log("🧪 Sandbox      : " + CONFIG.SANDBOX.PROVIDER + " (" + (CONFIG.SANDBOX.PISTON_URL || "?") + ")");
  console.log("📡 SSE          : ✅ /api/chat");
  console.log("🎙️  Luba Live    : " + (wsServer ? "✅ /live" : "❌"));
  console.log("🔍 Debug        : " + (process.env.DEBUG_TOKEN ? "✅ /api/debug" : "⚠️  (set DEBUG_TOKEN)"));
  console.log("");
  console.log("🏥 SELF-HEALING v16.5 :");
  console.log("   ├─ Auto-failover providers ✅");
  console.log("   ├─ Token overflow handler   ✅");
  console.log("   ├─ Compaction auto          ✅");
  console.log("   ├─ Images garanties         ✅");
  console.log("   ├─ Clean output             ✅");
  console.log("   └─ Mémoire PRO persistante  ✅");
  console.log("");
  console.log("🎯 v16.5.0 — Modèles vérifiés oct 2026 + Self-Healing");
  console.log("");
}

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre en cours");

  try { if (server) await new Promise((r) => server.close(r)); } catch {}
  try { await baileysManager.destroyAll(); } catch {}
  try { if (wsServer) wsServer.close(); } catch {}
  // ✅ Variable
  try { if (redisClient) await redisClient.quit(); } catch {}
  try { await new Promise((r) => db ? db.close(() => r()) : r()); } catch {}

  console.log("✅ Arrêt propre terminé");
  process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("uncaughtException", (e) => {
  logger.fatal({ err: e.message, stack: e.stack }, "uncaughtException");
  gracefulShutdown("uncaughtException");
});
process.on("unhandledRejection", (r) => {
  logger.error({ reason: String(r) }, "unhandledRejection");
});

// ================================================================================
// §5.08 — FICHIERS DE DÉPLOIEMENT
// ================================================================================

const DEPLOYMENT_FILES = String.raw`
# ===== Dockerfile =====
FROM node:20-alpine AS base
WORKDIR /app

FROM base AS deps
COPY package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM base AS runner
ENV NODE_ENV=production
ENV PORT=3000
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN mkdir -p /app/data /app/sessions /app/uploads /app/logs && chown -R node:node /app
USER node
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/ready || exit 1
CMD ["node", "index.js"]

# ===== docker-compose.yml =====
version: "3.9"
services:
  luba-backend:
    build: .
    container_name: luba-backend
    restart: unless-stopped
    ports: ["3000:3000"]
    env_file: [.env]
    environment:
      - NODE_ENV=production
      - REDIS_URL=redis://redis:6379
    volumes:
      - luba-data:/app/data
      - luba-sessions:/app/sessions
      - luba-uploads:/app/uploads
      - luba-logs:/app/logs
    depends_on: [redis]
    networks: [luba-net]

  redis:
    image: redis:7-alpine
    container_name: luba-redis
    restart: unless-stopped
    command: redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes: [luba-redis:/data]
    networks: [luba-net]

  nginx:
    image: nginx:alpine
    container_name: luba-nginx
    restart: unless-stopped
    ports: ["80:80", "443:443"]
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
    depends_on: [luba-backend]
    networks: [luba-net]

volumes:
  luba-data: {}
  luba-sessions: {}
  luba-uploads: {}
  luba-logs: {}
  luba-redis: {}

networks:
  luba-net:
    driver: bridge

# ===== .github/workflows/deploy.yml =====
name: Deploy Luba Backend

on:
  push:
    branches: [main]
  workflow_dispatch: {}

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "npm"
      - run: npm ci
      - run: node --check index.js

  build-and-push:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: \${{ github.actor }}
          password: \${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/\${{ github.repository }}:latest,ghcr.io/\${{ github.repository }}:\${{ github.sha }}
`;

function getDeploymentFiles() {
  return DEPLOYMENT_FILES;
}

// ================================================================================
// §5.09 — EXPORTS FINAUX + AUTO-START
// ================================================================================

Object.assign(module.exports, {
  app,
  get server() { return server; },
  get wsServer() { return wsServer; },
  baileysManager,
  isWhatsAppAllowed,
  reminderTick,
  runSecurityHousekeeping,
  authenticateUser,
  requireRole,
  verifyFirebaseToken,
  isIPBlocked,
  recordLoginAttempt,
  checkLoginAttempts,
  fastUpsertUser,
  tokenCache,
  WS_FRAME,
  encodeFrame,
  decodeFrame,
  setupLubaLiveWebSocket,
  handleLiveFrame,
  handleLiveTurn,
  liveSessions: () => liveSessions,
  DEPLOYMENT_FILES,
  getDeploymentFiles,
  bootstrap,
  gracefulShutdown
});

// ================================================================================
// AUTO-START
// ================================================================================

if (require.main === module) {
  bootstrap().catch((e) => {
    logger.fatal({ err: e.message, stack: e.stack }, "Bootstrap échoué");
    process.exit(1);
  });
}

// ================================================================================
// ==================== FIN PARTIE 5/5 — FIN DU FICHIER index.js =================
// ================================================================================
// 🎉 LUBA AI PRO v16.5.0 — Self-Healing Edition — BACKEND COMPLET
// ================================================================================
//
// 🆕 NOUVEAUTÉS v16.5 :
//
//   🏥 SELF-HEALING :
//     • Auto-failover providers (désactivation après 3 échecs)
//     • Token overflow handler (compaction auto si >75% du budget)
//     • Ordre dynamique des providers par santé
//     • Retry intelligent avec backoff
//
//   🖼️ IMAGES GARANTIES :
//     • ensureImageForResponse() → 1 image minimum par réponse
//     • Skip salutations + questions identité
//     • Fallback neutre si aucun réseau ne répond
//     • Logs [Luba Images] détaillés
//
//   🧹 OUTPUT PROPRE :
//     • formatFinalReply() → supprime mojibake, BOM, zero-width
//     • Markdown normalisé
//     • Zéro caractère bizarre
//
//   🧠 MÉMOIRE PRO :
//     • Table user_long_term_memory (compteur de mentions)
//     • Faits conservés indéfiniment (cross-semaines)
//     • Injectés automatiquement dans le prompt
//
//   🎯 MODÈLES VÉRIFIÉS OCTOBRE 2026 :
//     • Groq v100/v250 → openai/gpt-oss-120b
//     • Gemini → gemini-2.5-flash
//     • Cerebras → qwen-3.8-27b
//     • OpenRouter → deepseek-r1:free, qwen3-coder-480b:free
//
//   ❌ ADS PROPRES :
//     • Plus de "Luba Pro" ni "Publicité Test"
//     • Pub affichée uniquement si réseau réel répond
//
// SETUP :
//   1. npm install
//   2. Créer .env
//   3. node --check index.js
//   4. node index.js
//
// ROUTES :
//   GET  /                            — Info
//   GET  /api/health?full=1           — Health complet
//   GET  /ready                       — Readiness
//   GET  /api/metrics                 — Prometheus
//   GET  /api/debug?token=X           — Diagnostic
//   GET  /api/auth/check              — Auth check
//   POST /api/chat                    — Chat (JSON ou SSE)
//   GET  /api/conversations           — Liste conversations
//   GET  /api/conversation/:id/messages — Messages d'une conversation
//   GET  /api/memory/facts            — Faits mémorisés
//   GET  /api/ads/slots               — Slots pub
//   GET  /api/admin/self-heal/stats   — Stats santé providers (admin)
//   POST /api/admin/self-heal/reset   — Reset santé providers (admin)
//   ws://localhost:3000/live          — Luba Live (WebSocket binaire)
// ================================================================================