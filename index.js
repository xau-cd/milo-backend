// ================================================================================
// LUBA AI PRO — BACKEND v16.0.0 — Enterprise Edition
// HIKLON TECHNOLOGIES · Kinshasa, RDC · 2026
// ================================================================================
// PARTIE 1/4 — INFRASTRUCTURE, CONFIG, AUTH, CACHE, SÉCURITÉ
// --------------------------------------------------------------------------------
// Sommaire :
//   §1.1   Imports & bootstrap
//   §1.2   CONFIG centralisée + validation stricte
//   §1.3   Logger Pino (redaction + request-id)
//   §1.4   Erreurs typées (LubaError + codes)
//   §1.5   Utils (UUID, hash, sanitize, chunking UTF-8, backoff)
//   §1.6   Firebase Admin SDK + Firestore
//   §1.7   Cache multi-niveaux (L1 LRU + L2 Redis + L3 Firestore + sémantique)
//   §1.8   Sécurité (prompt injection, modération, HMAC, audit)
//   §1.9   Metrics Prometheus (/metrics)
//   §1.10  Feature flags + dégradation gracieuse
//   §1.11  Exports PARTIE 1
// ================================================================================
// NOUVEAUTÉS v16.0 vs v15.1 :
//   ✅ Firestore (users, memory, logs, sessions) en DUAL-WRITE avec Supabase
//   ✅ Cache 3 niveaux + cache sémantique (cosine sur embeddings)
//   ✅ Métriques Prometheus + latence P50/P95/P99 par provider
//   ✅ Modération Groq + filtre anti-injection (prompt wrapping)
//   ✅ Signature HMAC inter-services
//   ✅ Audit logs RGPD-compliant
//   ✅ Erreurs typées (LubaError + code machine)
//   ✅ i18n : détection FR/EN/SW/LN
//   ✅ Feature flags + dégradation propre si provider manquant
//   ✅ Utils durcis (chunking UTF-8 safe, sanitize strict, safe JSON)
// ================================================================================

"use strict";

require("dotenv").config();

// ================================================================================
// §1.1 — IMPORTS & BOOTSTRAP
// ================================================================================

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
const Parser         = require("rss-parser");
const FormData       = require("form-data");
const cheerio        = require("cheerio");
const { LRUCache }   = require("lru-cache");
const { PassThrough } = require("stream");

// --- Optionnels (chargés en try/catch : démarrage en mode dégradé si absents) ---
let GoogleGenAI   = null;
let firebaseAdmin = null;
let Firestore     = null;
let IORedis       = null;
let BullMQ        = null;
let PromClient    = null;
let ddgScrape     = null;
let GroqSDK       = null;

try { GoogleGenAI = require("@google/genai").GoogleGenAI; } catch { /* dégradé */ }
try {
  firebaseAdmin = require("firebase-admin");
  Firestore = require("@google-cloud/firestore");
} catch { /* dégradé */ }
try { IORedis = require("ioredis"); } catch { /* dégradé */ }
try { BullMQ = require("bullmq"); } catch { /* dégradé */ }
try { PromClient = require("prom-client"); } catch { /* dégradé */ }
try { ddgScrape = require("duck-duck-scrape"); } catch { /* dégradé */ }
try { GroqSDK = require("groq-sdk"); } catch { /* dégradé */ }

const { createClient } = require("@supabase/supabase-js");

// ================================================================================
// §1.2 — CONFIGURATION CENTRALISÉE
// ================================================================================

const CONFIG = Object.freeze({
  // --- Identité ---
  ENV:        process.env.NODE_ENV || "production",
  VERSION:    "16.0.0",
  AGENT_NAME: "Luba",
  COMPANY:    "HIKLON TECHNOLOGIES",
  HOST:       process.env.HOST || "0.0.0.0",
  PORT:       parseInt(process.env.PORT || "3000", 10),

  // --- Branding modèles ---
  BRAND: Object.freeze({
    V100: process.env.BRAND_V100 || "Mwamba",
    V250: process.env.BRAND_V250 || "Ngandu",
    LIVE: process.env.BRAND_LIVE || "Luba Live"
  }),

  // --- Limites ---
  LIMITS: Object.freeze({
    MAX_MESSAGE_LENGTH:    parseInt(process.env.MAX_MESSAGE_LENGTH    || "15000", 10),
    MAX_HISTORY_LENGTH:    parseInt(process.env.MAX_HISTORY_LENGTH    || "50", 10),
    MAX_CONTEXT_MESSAGES:  parseInt(process.env.MAX_CONTEXT_MESSAGES  || "20", 10),
    MAX_CONTEXT_TOKENS:    parseInt(process.env.MAX_CONTEXT_TOKENS    || "8000", 10),
    MAX_IMAGE_SIZE_MB:     parseInt(process.env.MAX_IMAGE_SIZE_MB     || "10", 10),
    MAX_IMAGES_PER_REQUEST:parseInt(process.env.MAX_IMAGES_PER_REQUEST|| "3", 10),
    IMAGE_SEARCH_LIMIT:    parseInt(process.env.IMAGE_SEARCH_LIMIT    || "6", 10)
  }),

  // --- Agent ---
  AGENT: Object.freeze({
    MAX_ITERATIONS:           parseInt(process.env.AGENT_MAX_ITERATIONS || "5", 10),
    MAX_TOOL_CALLS_PER_STEP:  parseInt(process.env.AGENT_MAX_TOOL_CALLS_PER_STEP || "6", 10)
  }),

  // --- Timeouts / Retry ---
  TIMEOUTS: Object.freeze({
    CHAT_ATTEMPT_MS:    parseInt(process.env.CHAT_ATTEMPT_TIMEOUT_MS  || "15000", 10),
    CHAT_GLOBAL_MS:     parseInt(process.env.CHAT_GLOBAL_TIMEOUT_MS   || "90000", 10),
    TOOL_MS:            parseInt(process.env.TOOL_TIMEOUT_MS          || "8000", 10),
    V250_ROUTE_MS:      parseInt(process.env.V250_ROUTE_TIMEOUT       || "60000", 10),
    IMAGE_SOURCE_MS:    parseInt(process.env.IMAGE_SOURCE_DEADLINE_MS || "4000", 10),
    REMINDER_TICK_MS:   parseInt(process.env.REMINDER_TICK_MS         || "60000", 10),
    HOUSEKEEPING_MS:    parseInt(process.env.HOUSEKEEPING_INTERVAL_MS || String(6 * 3600 * 1000), 10)
  }),

  RETRY: Object.freeze({
    MAX_ATTEMPTS:  parseInt(process.env.MAX_RETRY_ATTEMPTS    || "3", 10),
    BASE_DELAY_MS: parseInt(process.env.RETRY_BASE_DELAY_MS   || "100", 10),
    MAX_DELAY_MS:  parseInt(process.env.RETRY_MAX_DELAY_MS    || "1600", 10)
  }),

  // --- Circuit breaker ---
  CIRCUIT: Object.freeze({
    THRESHOLD:        parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD  || "5", 10),
    RESET_MS:         parseInt(process.env.CIRCUIT_BREAKER_RESET_MS   || "30000", 10),
    HALF_OPEN_MAX:    1
  }),

  // --- Auth ---
  AUTH: Object.freeze({
    TOKEN_CACHE_TTL_MS:   parseInt(process.env.AUTH_TOKEN_CACHE_TTL_MS || "300000", 10),
    CHECK_REVOKED:        process.env.AUTH_CHECK_REVOKED === "true",
    MAX_LOGIN_ATTEMPTS:   parseInt(process.env.MAX_LOGIN_ATTEMPTS      || "20", 10),
    LOGIN_BLOCK_MS:       parseInt(process.env.LOGIN_BLOCK_DURATION    || "900000", 10),
    MAX_SESSIONS_PER_USER:parseInt(process.env.MAX_SESSIONS_PER_USER   || "10", 10),
    HMAC_SECRET:          process.env.HMAC_SECRET || null
  }),

  // --- Chemins ---
  PATHS: Object.freeze({
    DATA:     path.join(__dirname, "data"),
    DB:       path.join(__dirname, "data", "luba.db"),
    SESSIONS: path.join(__dirname, "sessions"),
    UPLOADS:  path.join(__dirname, "uploads"),
    LOGS:     path.join(__dirname, "logs")
  }),

  // --- Sports / News ---
  NEWS: Object.freeze({
    SPORT_MAX_ARTICLES: parseInt(process.env.SPORT_NEWS_MAX_ARTICLES || "6", 10),
    SPORT_CACHE_TTL_MS: parseInt(process.env.SPORT_CACHE_TTL_MS      || "600000", 10),
    GOOGLE_LANG:        process.env.GOOGLE_NEWS_LANG   || "fr",
    GOOGLE_REGION:      process.env.GOOGLE_NEWS_REGION || "FR"
  }),

  // --- Images ---
  IMAGES: Object.freeze({
    CACHE_TTL_MS:       parseInt(process.env.IMAGE_CACHE_TTL_MS     || String(20 * 60 * 1000), 10),
    WIKIMEDIA_LIMIT:    parseInt(process.env.IMAGE_WIKIMEDIA_LIMIT  || "8", 10),
    DDG_LIMIT:          parseInt(process.env.IMAGE_DDG_LIMIT        || "4", 10),
    WIKIMEDIA_UA:       process.env.WIKIMEDIA_USER_AGENT
      || "LubaAI/16.0.0 (https://luba.web.app; contact@luba.web.app)",
    ALLOWED_TYPES:      ["image/jpeg", "image/png", "image/gif", "image/webp"]
  }),

  // --- Audio ---
  AUDIO: Object.freeze({
    ALLOWED_TYPES: ["audio/mpeg","audio/mp4","audio/wav","audio/webm",
                    "audio/ogg","audio/m4a","audio/x-m4a","audio/aac"],
    MAX_SIZE_MB:   parseInt(process.env.MAX_AUDIO_SIZE_MB || "20", 10)
  }),

  // --- Email ---
  EMAIL: Object.freeze({
    CONTACT:     process.env.CONTACT_EMAIL || "contact@luba.web.app",
    FROM_NAME:   process.env.EMAIL_FROM_NAME || "Luba",
    FROM_ADDR:   process.env.EMAIL_FROM_ADDR || process.env.SMTP_USER || "noreply@luba.web.app"
  }),

  // --- WhatsApp ---
  WHATSAPP: Object.freeze({
    QR_TIMEOUT_MS:   parseInt(process.env.WHATSAPP_QR_TIMEOUT  || "30000", 10),
    RETRY_DELAY_MS:  parseInt(process.env.WHATSAPP_RETRY_DELAY || "4000", 10),
    WHITELIST:       (process.env.WHATSAPP_WHITELIST || "")
                       .split(",").map(s => s.trim().replace(/[^\d]/g, "")).filter(Boolean),
    OPEN:            process.env.WHATSAPP_OPEN === "true",
    ENCRYPTION_KEY:  process.env.WHATSAPP_ENCRYPTION_KEY || null,
    ENCRYPTION_IV:   process.env.WHATSAPP_ENCRYPTION_IV  || null
  }),

  // --- Modèles vision (Groq Llama 4 Maverick gratuit) ---
  VISION: Object.freeze({
    GROQ_MODEL:       process.env.VISION_MODEL_GROQ       || "meta-llama/llama-4-maverick-17b-128e-instruct",
    OPENROUTER_MODEL: process.env.VISION_MODEL_OPENROUTER || "inclusionai/ling-3.0-flash-vl:free",
    GEMINI_MODEL:     process.env.VISION_MODEL_GEMINI     || "gemini-3.6-flash"
  }),

  // --- Sandbox code ---
  SANDBOX: Object.freeze({
    PROVIDER:   process.env.CODE_SANDBOX_PROVIDER || "",
    PISTON_URL: process.env.PISTON_URL            || "",
    JUDGE0_URL: process.env.JUDGE0_URL            || "",
    E2B_KEY:    process.env.E2B_API_KEY           || ""
  }),

  // --- Cache ---
  CACHE: Object.freeze({
    L1_MAX_ITEMS:      parseInt(process.env.CACHE_L1_MAX_ITEMS  || "5000", 10),
    L1_TTL_MS:         parseInt(process.env.CACHE_L1_TTL_MS     || String(10 * 60 * 1000), 10),
    L2_DEFAULT_TTL_S:  parseInt(process.env.CACHE_L2_TTL_S      || "3600", 10),
    SEMANTIC_THRESHOLD: parseFloat(process.env.CACHE_SEMANTIC_THRESHOLD || "0.92")
  }),

  // --- HTTP ---
  HTTP: Object.freeze({
    USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/16.0.0"
  }),

  // --- HMAC ---
  HMAC: Object.freeze({
    ENABLED: Boolean(process.env.HMAC_SECRET),
    SECRET:  process.env.HMAC_SECRET || null,
    WINDOW_MS: 5 * 60 * 1000
  })
});

// ================================================================================
// §1.2.bis — FIREBASE / HOSTING / SUPABASE
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
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:5173",
    "http://localhost:4200"
  ]
});

// ================================================================================
// §1.2.ter — QUOTAS
// ================================================================================

const USER_QUOTAS = Object.freeze({
  FREE:    { maxMessagesPerDay: 100,    maxImagesPerDay: 20,    maxWhatsAppMessagesPerDay: 10,    maxEmailsPerDay: 5,    maxTokensPerRequest: 8000   },
  PREMIUM: { maxMessagesPerDay: 1000,   maxImagesPerDay: 200,   maxWhatsAppMessagesPerDay: 100,   maxEmailsPerDay: 50,   maxTokensPerRequest: 32000  },
  ADMIN:   { maxMessagesPerDay: 999999, maxImagesPerDay: 999999,maxWhatsAppMessagesPerDay: 999999,maxEmailsPerDay: 999999,maxTokensPerRequest: 128000 }
});

// ================================================================================
// §1.2.quater — VALIDATION ENVIRONNEMENT
// ================================================================================

function validateEnvironment() {
  const problems = [];
  const warnings = [];

  // Firebase
  const hasFirebaseAdmin = Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  const hasFirebaseRest  = Boolean(process.env.FIREBASE_API_KEY);
  if (!hasFirebaseAdmin && !hasFirebaseRest) {
    problems.push("Aucune authentification Firebase configurée (SERVICE_ACCOUNT_JSON ou API_KEY).");
  } else if (!hasFirebaseAdmin) {
    warnings.push("Firebase Admin SDK absent → mode REST uniquement (rôle FREE forcé, admin limité).");
  }

  // LLM
  const hasAnyLLM = Boolean(
    process.env.GROQ_API_KEY || process.env.OPENROUTER_API_KEY ||
    process.env.CEREBRAS_API_KEY || process.env.GEMINI_API_KEY
  );
  if (!hasAnyLLM) problems.push("Aucune clé LLM configurée (GROQ / OPENROUTER / CEREBRAS / GEMINI).");

  // Persistance
  const hasSupabase = Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_KEY);
  const hasFirestore = Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  if (!hasSupabase && !hasFirestore && CONFIG.ENV === "production") {
    warnings.push("Ni Supabase ni Firestore configurés → SQLite seul (non persistant sur Render).");
  }

  // Production stricte
  if (CONFIG.ENV === "production") {
    const key = CONFIG.WHATSAPP.ENCRYPTION_KEY;
    const iv  = CONFIG.WHATSAPP.ENCRYPTION_IV;
    if (!key || key.length < 32 || !iv || iv.length < 16) {
      problems.push("WHATSAPP_ENCRYPTION_KEY (≥32 chars) et WHATSAPP_ENCRYPTION_IV (≥16 chars) obligatoires en production.");
    }
    if (!CONFIG.HMAC.ENABLED) {
      warnings.push("HMAC_SECRET absent → signature inter-services désactivée (recommandé en prod).");
    }
  }

  // Affichage
  for (const w of warnings) console.warn("⚠️  " + w);
  if (problems.length > 0) {
    for (const p of problems) console.error("❌ " + p);
    if (CONFIG.ENV === "production") {
      console.error("🛑 Démarrage interrompu (production stricte).");
      process.exit(1);
    } else {
      console.warn("⚠️  Démarrage en mode dégradé.");
    }
  }

  return { problems, warnings, ok: problems.length === 0 };
}

// ================================================================================
// §1.2.quinquies — DOSSIERS
// ================================================================================

function ensureDirectories() {
  for (const dir of [CONFIG.PATHS.DATA, CONFIG.PATHS.SESSIONS, CONFIG.PATHS.UPLOADS, CONFIG.PATHS.LOGS]) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true, mode: 0o750 });
      console.log(`📁 Dossier créé : ${dir}`);
    }
  }
}

// ================================================================================
// §1.3 — LOGGER Pino
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
    }),
    req: (r) => ({
      id: r.id, method: r.method, url: r.url, ip: r.ip,
      ua: r.headers?.["user-agent"]?.slice(0, 120)
    })
  }
});

// Transport pretty en dev
if (CONFIG.ENV === "development") {
  try {
    logger.info = logger.info.bind(logger); // no-op, garde compat
  } catch {}
}

// ================================================================================
// §1.4 — ERREURS TYPÉES
// ================================================================================

/**
 * Erreur applicative avec code machine, statut HTTP, et contexte.
 * Usage : throw new LubaError("MISSING_TOKEN", "Authentification requise.", 401);
 */
class LubaError extends Error {
  constructor(code, message, httpStatus = 500, context = {}) {
    super(message || code);
    this.name = "LubaError";
    this.code = code;
    this.httpStatus = httpStatus;
    this.context = context;
    this.timestamp = new Date().toISOString();
    Error.captureStackTrace?.(this, LubaError);
  }
  toJSON() {
    return {
      success: false,
      error: true,
      reply: this.message,
      code: this.code,
      context: CONFIG.ENV === "development" ? this.context : undefined
    };
  }
}

const ERROR_CODES = Object.freeze({
  // Auth
  MISSING_TOKEN:       { status: 401, msg: "Authentification requise." },
  INVALID_TOKEN:       { status: 401, msg: "Session invalide." },
  TOKEN_EXPIRED:       { status: 401, msg: "Session expirée, reconnectez-vous." },
  IP_BLOCKED:          { status: 403, msg: "Accès refusé." },
  INSUFFICIENT_ROLE:   { status: 403, msg: "Privilèges insuffisants." },
  AUTH_INTERNAL:       { status: 500, msg: "Erreur d'authentification." },

  // Requête
  MISSING_MESSAGE:     { status: 400, msg: "Le paramètre 'message' est obligatoire." },
  INVALID_MESSAGE:     { status: 400, msg: "Message invalide." },
  INVALID_CONVERSATION_ID: { status: 400, msg: "Identifiant de conversation invalide." },
  FILE_TOO_LARGE:      { status: 413, msg: "Fichier trop volumineux." },
  TOO_MANY_FILES:      { status: 413, msg: "Trop de fichiers." },
  INVALID_IMAGE_CONTENT: { status: 400, msg: "Contenu image invalide." },

  // Logique
  CONVERSATION_OWNERSHIP: { status: 403, msg: "Conversation non autorisée." },
  CONVERSATION_BUSY:   { status: 409, msg: "Une requête est déjà en cours sur cette conversation." },
  QUOTA_EXCEEDED:      { status: 429, msg: "Quota journalier atteint." },
  RATE_LIMIT:          { status: 429, msg: "Trop de requêtes. Réessayez dans un instant." },
  RATE_LIMIT_CHAT:     { status: 429, msg: "Trop de messages. Patientez un instant." },

  // Provider
  PROVIDER_DOWN:       { status: 503, msg: "Service IA temporairement indisponible." },
  ALL_PROVIDERS_FAILED:{ status: 503, msg: "Tous les fournisseurs IA ont échoué." },
  CIRCUIT_OPEN:        { status: 503, msg: "Service temporairement surchargé." },

  // Outils
  TOOL_NOT_ALLOWED:    { status: 403, msg: "Outil non autorisé." },
  TOOL_EXECUTION_ERROR:{ status: 500, msg: "Échec de l'exécution de l'outil." },
  NEEDS_CONFIRMATION:  { status: 202, msg: "Confirmation requise." },
  SANDBOX_UNAVAILABLE: { status: 503, msg: "Sandbox d'exécution indisponible." },

  // Interne
  INTERNAL_ERROR:      { status: 500, msg: "Une erreur interne est survenue." },
  NOT_FOUND:           { status: 404, msg: "Ressource non trouvée." },
  VALIDATION_ERROR:    { status: 400, msg: "Données de requête invalides." }
});

function makeError(code, extra = "", status = null) {
  const def = ERROR_CODES[code] || { status: 500, msg: "Erreur." };
  const message = extra ? `${def.msg} ${extra}`.trim() : def.msg;
  return new LubaError(code, message, status ?? def.status);
}

function isLubaError(e) { return e instanceof LubaError; }

// ================================================================================
// §1.5 — UTILITAIRES
// ================================================================================

// --- IDs ---
const generateRequestId      = () => `req_${crypto.randomUUID()}`;
const generateConversationId = () => `conv_${crypto.randomUUID()}`;
const generateSessionToken   = () => `sess_${crypto.randomBytes(32).toString("hex")}`;
const generateUUID           = () => crypto.randomUUID();
const generateTaskId         = () => `task_${crypto.randomUUID()}`;
const generateMsgId          = () => `msg_${crypto.randomUUID()}`;

// --- Hashes ---
const sha256 = (input) =>
  crypto.createHash("sha256").update(String(input)).digest("hex");

const sha1 = (input) =>
  crypto.createHash("sha1").update(String(input)).digest("hex");

const hashSessionToken = (token) => sha256(token);

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

// --- Temps ---
const nowMs = () => Date.now();

function todayKeyMs() {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function backoffDelay(attempt, base = CONFIG.RETRY.BASE_DELAY_MS, max = CONFIG.RETRY.MAX_DELAY_MS) {
  const exp = Math.min(base * Math.pow(2, attempt), max);
  const jitter = Math.floor(Math.random() * 100);
  return exp + jitter;
}

// --- Sanitize ---
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
  let text = String(input).trim();
  text = text.replace(/[\u0000-\u001F\u007F]/g, "");
  text = text.replace(/<script[\s\S]*?<\/script>/gi, "");
  text = text.replace(/<iframe[\s\S]*?<\/iframe>/gi, "");
  text = text.replace(/<object[\s\S]*?<\/object>/gi, "");
  text = text.replace(/javascript:/gi, "");
  text = text.replace(/on\w+\s*=\s*["'][^"']*["']/gi, "");
  if (text.length > maxLength) text = text.slice(0, maxLength);
  return text;
}

// --- JSON safe ---
function safeJsonParse(str, fallback = null) {
  if (str === null || str === undefined) return fallback;
  try { return JSON.parse(str); }
  catch { return fallback; }
}

function safeJsonStringify(obj, fallback = "{}") {
  try { return JSON.stringify(obj); }
  catch { return fallback; }
}

// --- Math normalize (LaTeX) ---
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
    let text = content;
    text = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
    text = text.replace(/\\\(([\s\S]*?)\\\)/g, (_, inner) => `$${inner.trim()}$`);
    text = text.replace(/\\\[([\s\S]*?)\\\]/g, (_, inner) => `$$${inner.trim()}$$`);
    text = text
      .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, " ");
    text = text.replace(/\n{3,}/g, "\n\n");
    return text;
  }).join("").trim();
}

// --- stripThinkTags ---
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

// --- XML entities (RSS) ---
function decodeXmlEntities(str) {
  return String(str)
    .replace(/<!\[CDATA\[/g, "").replace(/\]\]>/g, "")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ").trim();
}

// --- Device fingerprint ---
function computeDeviceFingerprint(ip, userAgent) {
  return sha256(`${ip || "?"}::${userAgent || "?"}`).slice(0, 32);
}

// --- Détection signature image ---
function isValidImageSignature(buffer) {
  if (!buffer || buffer.length < 12) return false;
  const hex = buffer.subarray(0, 12).toString("hex");
  if (hex.startsWith("ffd8ff")) return true;                            // JPEG
  if (hex.startsWith("89504e470d0a1a0a")) return true;                   // PNG
  if (hex.startsWith("47494638")) return true;                           // GIF
  if (hex.startsWith("52494646") &&
      buffer.subarray(8, 12).toString("ascii") === "WEBP") return true;  // WebP
  return false;
}

// --- Conversion image ---
function convertImageToBase64(buffer, mimetype) {
  return {
    dataUrl: `data:${mimetype};base64,${buffer.toString("base64")}`,
    base64: buffer.toString("base64"),
    mimetype,
    size: buffer.length
  };
}

// --- Deadline utility ---
function withDeadline(promise, deadlineMs, fallbackValue) {
  const timer = setTimeout(() => {}, deadlineMs);
  timer.unref?.();
  return Promise.race([
    Promise.resolve(promise).finally(() => clearTimeout(timer)),
    new Promise((resolve) => {
      const t = setTimeout(() => resolve(fallbackValue), deadlineMs);
      t.unref?.();
    })
  ]);
}

async function allSettledWithDeadline(promises, deadlineMs) {
  const wrapped = promises.map((p) =>
    Promise.race([
      Promise.resolve(p)
        .then(v => ({ status: "fulfilled", value: v }))
        .catch(e => ({ status: "rejected", reason: e })),
      new Promise((resolve) => {
        const t = setTimeout(() => resolve({ status: "timeout" }), deadlineMs);
        t.unref?.();
      })
    ])
  );
  return Promise.all(wrapped);
}

// --- Chunking UTF-8 safe ---
function safeChunkText(text, targetSize = 24) {
  if (!text) return [];
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + targetSize, text.length);
    const code = text.charCodeAt(end - 1);
    // Ne jamais couper au milieu d'une paire de substitution UTF-16
    if (code >= 0xD800 && code <= 0xDBFF && end < text.length) end++;
    chunks.push(text.slice(i, end));
    i = end;
  }
  return chunks;
}

// --- Regex ---
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_REGEX = /^\+?[1-9]\d{6,14}$/;

// --- Langue detection légère (i18n) ---
function detectLanguage(text) {
  if (!text || typeof text !== "string") return "fr";
  const t = text.toLowerCase();

  // Mots-clés très discriminants
  const swahiliWords = /\b(habari|asante|karibu|jambo|ndiyo|hapana|vipi|nzuri|sana|kwaheri|tafadhali)\b/g;
  const lingalaWords = /\b(mbotá|mbote|sango|nzela|melesi|malamu|kitoko|ezali|nakozela|elengi)\b/g;
  const englishWords = /\b(the|and|you|with|this|that|hello|please|thanks|help|what|where|when)\b/g;

  const sw = (t.match(swahiliWords) || []).length;
  const ln = (t.match(lingalaWords) || []).length;
  const en = (t.match(englishWords) || []).length;
  const fr = /\b(le|la|les|un|une|des|je|tu|il|elle|nous|vous|bonjour|merci|comment|pourquoi|quand|où)\b/g;
  const frCount = (t.match(fr) || []).length;

  const scores = { fr: frCount, en, sw, ln };
  const best = Object.entries(scores).sort((a, b) => b[1] - a[1])[0];
  return best[1] > 0 ? best[0] : "fr";
}

// --- Truncate tokens safe (approx 4 chars = 1 token) ---
function truncateToTokenBudget(text, maxTokens = 8000) {
  if (!text) return "";
  const maxChars = maxTokens * 4;
  if (text.length <= maxChars) return text;
  return text.slice(0, maxChars) + "\n…[tronqué]";
}

// ================================================================================
// §1.6 — FIREBASE ADMIN + FIRESTORE
// ================================================================================

let firebaseApp   = null;
let firestoreDb   = null;
let firebaseReady = false;

function parseFirebaseServiceAccount(raw) {
  try { return JSON.parse(raw); }
  catch {
    try { return JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
    catch { throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide (ni JSON ni base64)"); }
  }
}

function initFirebase() {
  if (!firebaseAdmin || !process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    logger.warn("⚠️  Firebase Admin non initialisé — mode REST / Firestore désactivé");
    return;
  }
  try {
    const serviceAccount = parseFirebaseServiceAccount(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);

    firebaseApp = firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(serviceAccount),
      projectId: FIREBASE_CONFIG.projectId
    });

    // Initialise Firestore avec les mêmes credentials
    if (Firestore) {
      firestoreDb = new Firestore.Firestore({
        projectId: FIREBASE_CONFIG.projectId,
        credentials: {
          client_email: serviceAccount.client_email,
          private_key: serviceAccount.private_key
        }
      });
      logger.info("✅ Firebase Admin + Firestore initialisés");
    } else {
      logger.info("✅ Firebase Admin initialisé (Firestore non disponible)");
    }
    firebaseReady = true;
  } catch (e) {
    logger.error({ err: e.message }, "❌ Init Firebase Admin échouée");
    firebaseApp = null;
    firestoreDb = null;
    firebaseReady = false;
  }
}

// --- Wrappers Firestore (retournent null si non dispo, jamais throw) ---

async function fsGet(collection, docId) {
  if (!firestoreDb) return null;
  try {
    const snap = await firestoreDb.collection(collection).doc(docId).get();
    return snap.exists ? { id: snap.id, ...snap.data() } : null;
  } catch (e) {
    logger.warn({ err: e.message, collection, docId }, "Firestore get échec");
    return null;
  }
}

async function fsSet(collection, docId, data, { merge = true } = {}) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try {
    await firestoreDb.collection(collection).doc(docId).set(data, { merge });
    return { success: true };
  } catch (e) {
    logger.warn({ err: e.message, collection, docId }, "Firestore set échec");
    return { success: false, error: e };
  }
}

async function fsUpdate(collection, docId, data) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try {
    await firestoreDb.collection(collection).doc(docId).update(data);
    return { success: true };
  } catch (e) {
    logger.warn({ err: e.message, collection, docId }, "Firestore update échec");
    return { success: false, error: e };
  }
}

async function fsDelete(collection, docId) {
  if (!firestoreDb) return { success: false, reason: "no_firestore" };
  try {
    await firestoreDb.collection(collection).doc(docId).delete();
    return { success: true };
  } catch (e) {
    logger.warn({ err: e.message, collection, docId }, "Firestore delete échec");
    return { success: false, error: e };
  }
}

async function fsQuery(collection, { where = [], orderBy = null, limit = 50 } = {}) {
  if (!firestoreDb) return [];
  try {
    let q = firestoreDb.collection(collection);
    for (const [field, op, value] of where) q = q.where(field, op, value);
    if (orderBy) q = q.orderBy(orderBy.field, orderBy.direction || "desc");
    if (limit) q = q.limit(limit);
    const snap = await q.get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) {
    logger.warn({ err: e.message, collection }, "Firestore query échec");
    return [];
  }
}

// ================================================================================
// §1.6.bis — SUPABASE (dual-write backup)
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
      logger.warn({ err: result.error.message, table, op }, "Supabase write échouée");
      return { success: false, error: result.error };
    }
    return { success: true };
  } catch (e) {
    logger.warn({ err: e.message, table, op }, "Supabase write exception");
    return { success: false, error: e };
  }
}

// ================================================================================
// §1.7 — CACHE MULTI-NIVEAUX
// ================================================================================

// --- L1 : LRU mémoire ---
const l1Cache = new LRUCache({
  max: CONFIG.CACHE.L1_MAX_ITEMS,
  ttl: CONFIG.CACHE.L1_TTL_MS,
  updateAgeOnGet: false,
  allowStale: false
});

// --- L2 : Redis (si dispo) ---
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
      lazyConnect: false,
      retryStrategy: (times) => Math.min(times * 200, 5000)
    });
    redisClient.on("error", (e) => logger.warn({ err: e.message }, "Redis erreur"));
    redisClient.on("connect", () => logger.info("✅ Redis connecté (cache L2)"));
  } catch (e) {
    logger.warn({ err: e.message }, "⚠️  Redis init échouée");
    redisClient = null;
  }
}

// --- API cache unifiée ---
const cache = {
  /**
   * getCache(key) — cherche L1 puis L2. Retourne null si miss.
   */
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
      } catch (e) {
        logger.debug({ err: e.message, key }, "Redis get échec");
      }
    }
    return null;
  },

  /**
   * setCache(key, value, ttlMs) — écrit L1 + L2.
   */
  async set(key, value, ttlMs = CONFIG.CACHE.L1_TTL_MS) {
    l1Cache.set(key, value, { ttl: ttlMs });

    if (redisClient) {
      try {
        const ttlSeconds = Math.ceil(ttlMs / 1000);
        await redisClient.setex(key, ttlSeconds, safeJsonStringify(value));
      } catch (e) {
        logger.debug({ err: e.message, key }, "Redis set échec");
      }
    }
  },

  /**
   * del(key) — supprime L1 + L2.
   */
  async del(key) {
    l1Cache.delete(key);
    if (redisClient) {
      try { await redisClient.del(key); } catch {}
    }
  },

  /**
   * withCache(keyFn, ttlMs, loaderFn) — pattern classique de mémoïsation.
   */
  async withCache(key, ttlMs, loader) {
    const cached = await this.get(key);
    if (cached !== null) return cached;
    const fresh = await loader();
    if (fresh !== null && fresh !== undefined) {
      await this.set(key, fresh, ttlMs);
    }
    return fresh;
  }
};

// --- L3 : Cache sémantique (embeddings) ---

/**
 * Cache sémantique : si deux questions sont très proches (cosine > threshold),
 * on renvoie la même réponse. Économise ~30% d'appels LLM.
 */
class SemanticCache {
  constructor({ threshold = CONFIG.CACHE.SEMANTIC_THRESHOLD, maxSize = 1000 } = {}) {
    this.threshold = threshold;
    this.maxSize = maxSize;
    this.entries = new Map(); // hash -> { embedding, value, ts }
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
    // Éviction simple : si full, supprime l'entrée la plus ancienne
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
// §1.8 — SÉCURITÉ
// ================================================================================

// --- 1.8.a Détection prompt injection ---

const INJECTION_PATTERNS = [
  /ignore\s+(all\s+)?(previous|above|prior)\s+(instructions|prompts|rules)/i,
  /disregard\s+(all\s+)?(previous|above)/i,
  /forget\s+(everything|all|your)\s+(instructions|rules)/i,
  /you\s+are\s+now\s+(a|an)\s+(?!assistant|AI|model)/i,
  /system\s*:\s*you\s+are/i,
  /<\|im_start\|>/i,
  /<\|im_end\|>/i,
  /\[INST\]/i,
  /\[\/INST\]/i,
  /###\s*instruction/i,
  /act\s+as\s+(if\s+you\s+are|a)\s+(?!assistant)/i,
  /jailbreak/i,
  /DAN\s+mode/i,
  /developer\s+mode/i,
  /repeat\s+after\s+me/i,
  /reveal\s+your\s+(system\s+)?prompt/i,
  /show\s+me\s+your\s+instructions/i,
  /print\s+your\s+(initial\s+)?prompt/i
];

function detectPromptInjection(text) {
  if (!text || typeof text !== "string") return { detected: false, pattern: null };
  for (const p of INJECTION_PATTERNS) {
    if (p.test(text)) return { detected: true, pattern: p.source.slice(0, 60) };
  }
  return { detected: false, pattern: null };
}

/**
 * Wrap une entrée utilisateur dans des délimiteurs explicites pour
 * réduire l'efficacité des prompt injections.
 */
function wrapUserInput(text) {
  const clean = sanitizeForLLM(text, CONFIG.LIMITS.MAX_MESSAGE_LENGTH);
  return `<user_input>\n${clean}\n</user_input>`;
}

// --- 1.8.b Modération de contenu ---

const MODERATION_KEYWORDS = {
  // Uniquement mots ultra-graves (violence, CSAM, terrorisme)
  // ⚠️ Liste minimale — la vraie modération passe par l'API Groq moderation
  VIOLENCE_EXTREME: [
    /\bhow\s+to\s+(kill|murder|assassinate)\s+(a\s+)?(person|someone|human)/i,
    /\bhow\s+to\s+make\s+(a\s+)?(bomb|explosive|grenade)\b/i
  ],
  CSAM: [
    /\bchild\s+(porn|sexual|abuse)\b/i,
    /\bcsam\b/i
  ]
};

function moderateText(text) {
  if (!text || typeof text !== "string") return { safe: true, category: null };
  for (const [category, patterns] of Object.entries(MODERATION_KEYWORDS)) {
    for (const p of patterns) {
      if (p.test(text)) return { safe: false, category };
    }
  }
  return { safe: true, category: null };
}

async function moderateWithGroq(text) {
  // Fallback si pas d'API moderation → check local
  if (!process.env.GROQ_API_KEY) return moderateText(text);
  try {
    const resp = await axios.post(
      "https://api.groq.com/openai/v1/chat/completions",
      {
        model: "llama-guard-3-8b",
        messages: [{ role: "user", content: text.slice(0, 4000) }],
        max_tokens: 50,
        temperature: 0
      },
      {
        headers: {
          Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
          "Content-Type": "application/json"
        },
        timeout: 5000
      }
    );
    const verdict = resp.data?.choices?.[0]?.message?.content || "";
    const isUnsafe = /unsafe|violat|harmful/i.test(verdict);
    return { safe: !isUnsafe, category: isUnsafe ? "groq_guard" : null, raw: verdict.slice(0, 100) };
  } catch (e) {
    logger.debug({ err: e.message }, "Groq moderation indisponible → fallback local");
    return moderateText(text);
  }
}

// --- 1.8.c Vérification signature HMAC ---

function verifyHmacSignature(req) {
  if (!CONFIG.HMAC.ENABLED) return { valid: true, skipped: true };

  const signature = req.headers["x-luba-signature"];
  const timestamp = req.headers["x-luba-timestamp"];

  if (!signature || !timestamp) return { valid: false, reason: "missing_headers" };

  const tsNum = parseInt(timestamp, 10);
  if (!Number.isFinite(tsNum)) return { valid: false, reason: "invalid_timestamp" };

  const age = Math.abs(Date.now() - tsNum);
  if (age > CONFIG.HMAC.WINDOW_MS) return { valid: false, reason: "timestamp_expired" };

  // Payload = timestamp + method + path + body (stringifié)
  const bodyStr = typeof req.body === "string" ? req.body : safeJsonStringify(req.body || {});
  const payload = `${timestamp}.${req.method}.${req.originalUrl}.${bodyStr}`;

  if (!hmacVerify(payload, signature)) return { valid: false, reason: "signature_mismatch" };
  return { valid: true };
}

// --- 1.8.d Audit log ---

async function logSecurityEvent(userId, eventType, details = {}, ip = null, ua = null, fingerprint = null) {
  try {
    if (db) {
      await dbRun(
        `INSERT INTO security_logs (user_id, event_type, details, fingerprint, ip_address, user_agent, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [userId, eventType, safeJsonStringify(details), fingerprint, ip, ua, Date.now()]
      );
    }
    // Dual-write Firestore (best effort)
    if (firestoreDb) {
      fsSet("security_logs", generateUUID(), {
        user_id: userId, event_type: eventType, details,
        ip_address: ip, user_agent: ua, fingerprint,
        created_at: new Date()
      }).catch(() => {});
    }
  } catch (e) {
    logger.error({ err: e.message }, "Erreur log sécurité");
  }
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
    // Métriques Prometheus
    if (metrics?.llmLatency) {
      metrics.llmLatency.labels(provider, model, status).observe(latencyMs / 1000);
    }
    if (metrics?.llmTokens) {
      metrics.llmTokens.labels(provider, model, "prompt").inc(promptTokens);
      metrics.llmTokens.labels(provider, model, "completion").inc(completionTokens);
    }
  } catch (e) {
    logger.error({ err: e.message }, "Erreur audit LLM");
  }
}

// ================================================================================
// §1.9 — METRICS PROMETHEUS
// ================================================================================

let metrics = null;

function initMetrics() {
  if (!PromClient) {
    logger.warn("⚠️  prom-client non installé — /metrics indisponible");
    return;
  }
  try {
    const client = PromClient;
    client.collectDefaultMetrics({ prefix: "luba_" });

    metrics = {
      register: client.register,

      httpRequests: new client.Counter({
        name: "luba_http_requests_total",
        help: "Nombre total de requêtes HTTP",
        labelNames: ["method", "path", "status"]
      }),

      httpDuration: new client.Histogram({
        name: "luba_http_request_duration_seconds",
        help: "Durée des requêtes HTTP",
        labelNames: ["method", "path", "status"],
        buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60]
      }),

      llmLatency: new client.Histogram({
        name: "luba_llm_latency_seconds",
        help: "Latence des appels LLM",
        labelNames: ["provider", "model", "status"],
        buckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60]
      }),

      llmTokens: new client.Counter({
        name: "luba_llm_tokens_total",
        help: "Tokens consommés par provider",
        labelNames: ["provider", "model", "type"]
      }),

      llmCalls: new client.Counter({
        name: "luba_llm_calls_total",
        help: "Nombre d'appels LLM",
        labelNames: ["provider", "model", "status"]
      }),

      circuitState: new client.Gauge({
        name: "luba_circuit_breaker_state",
        help: "État du circuit breaker (0=closed, 1=half-open, 2=open)",
        labelNames: ["name"]
      }),

      cacheHits: new client.Counter({
        name: "luba_cache_hits_total",
        help: "Cache hits par niveau",
        labelNames: ["level"]
      }),

      cacheMisses: new client.Counter({
        name: "luba_cache_misses_total",
        help: "Cache misses",
        labelNames: ["level"]
      }),

      toolCalls: new client.Counter({
        name: "luba_tool_calls_total",
        help: "Appels d'outils",
        labelNames: ["tool", "status"]
      }),

      activeWebSockets: new client.Gauge({
        name: "luba_active_websockets",
        help: "WebSockets actifs (Luba Live)",
        labelNames: ["channel"]
      }),

      sttLatency: new client.Histogram({
        name: "luba_stt_latency_seconds",
        help: "Latence STT",
        labelNames: ["provider"],
        buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5]
      }),

      ttsLatency: new client.Histogram({
        name: "luba_tts_latency_seconds",
        help: "Latence TTS",
        labelNames: ["provider"],
        buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5]
      })
    };

    logger.info("✅ Métriques Prometheus initialisées");
  } catch (e) {
    logger.error({ err: e.message }, "❌ Init métriques échouée");
    metrics = null;
  }
}

// ================================================================================
// §1.10 — FEATURE FLAGS + DÉGRADATION
// ================================================================================

const FEATURES = Object.freeze({
  firestore:        Boolean(process.env.FIREBASE_SERVICE_ACCOUNT_JSON),
  supabase:         Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_KEY),
  redis:            Boolean(process.env.REDIS_URL),
  prometheus:       Boolean(PromClient),
  gemini:           Boolean(process.env.GEMINI_API_KEY),
  groq:             Boolean(process.env.GROQ_API_KEY),
  openrouter:       Boolean(process.env.OPENROUTER_API_KEY),
  cerebras:         Boolean(process.env.CEREBRAS_API_KEY),
  hmac:             Boolean(process.env.HMAC_SECRET),
  sandbox:          Boolean(process.env.CODE_SANDBOX_PROVIDER),
  youtube_api:      Boolean(process.env.YOUTUBE_API_KEY),
  resend:           Boolean(process.env.RESEND_API_KEY),
  smtp:             Boolean(process.env.SMTP_HOST && process.env.SMTP_USER),
  whatsapp_encryption: Boolean(process.env.WHATSAPP_ENCRYPTION_KEY && process.env.WHATSAPP_ENCRYPTION_IV)
});

function featureStatus() {
  return Object.entries(FEATURES)
    .map(([k, v]) => `${k}=${v ? "✅" : "❌"}`)
    .join(" | ");
}

// ================================================================================
// §1.11 — SQLITE (schéma v16, conservé pour compat v15.1)
// ================================================================================
// ⚠️ Ce bloc sera étendu dans PARTIE 2 (dual-write Firestore).
// On garde le schéma exact de v15.1 pour ne rien casser.

let db = null;

function initDatabase() {
  return new Promise((resolve, reject) => {
    db = new sqlite3.Database(CONFIG.PATHS.DB, (err) => {
      if (err) {
        logger.error({ err: err.message }, "❌ Impossible d'ouvrir SQLite");
        return reject(err);
      }
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
          last_seen_at INTEGER,
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
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT, firebase_uid TEXT, to_email TEXT NOT NULL,
          subject TEXT, status TEXT DEFAULT 'pending', provider TEXT, error_message TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
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
        db.run("CREATE INDEX IF NOT EXISTS idx_security_fingerprint ON security_logs(user_id, event_type, fingerprint)");
        db.run("CREATE INDEX IF NOT EXISTS idx_security_user_time ON security_logs(user_id, created_at DESC)");

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
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT, ip_address TEXT, success INTEGER DEFAULT 0,
          error_message TEXT, created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS blocked_ips (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ip_address TEXT UNIQUE, reason TEXT,
          strike_count INTEGER DEFAULT 1, blocked_until INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS user_memory (
          user_id TEXT PRIMARY KEY, summary TEXT DEFAULT '',
          messages_since_update INTEGER DEFAULT 0,
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS user_tasks (
          id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
          title TEXT NOT NULL, notes TEXT, due_at INTEGER,
          status TEXT DEFAULT 'pending', notified_at INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_user_tasks_user ON user_tasks(user_id, status, due_at)");

        // --- Nouvelles tables v16 ---
        db.run(`CREATE TABLE IF NOT EXISTS outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          table_name TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('insert','update','upsert','delete')),
          payload TEXT NOT NULL, idempotency_key TEXT NOT NULL,
          attempts INTEGER DEFAULT 0, last_error TEXT,
          next_attempt_at INTEGER DEFAULT (strftime('%s','now')*1000),
          status TEXT DEFAULT 'pending',
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000),
          UNIQUE(table_name, op, idempotency_key)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(status, next_attempt_at)");

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

        db.run(`CREATE TABLE IF NOT EXISTS user_memory_facts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          fact TEXT NOT NULL,
          category TEXT DEFAULT 'general',
          embedding TEXT,
          confidence REAL DEFAULT 1.0,
          source_session TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000),
          updated_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_facts_user ON user_memory_facts(user_id, created_at DESC)");
        db.run("CREATE INDEX IF NOT EXISTS idx_facts_category ON user_memory_facts(user_id, category)");

        db.run(`CREATE TABLE IF NOT EXISTS audit_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT, event_type TEXT NOT NULL,
          payload TEXT DEFAULT '{}',
          ip_address TEXT, user_agent TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now')*1000)
        )`);
        db.run("CREATE INDEX IF NOT EXISTS idx_audit_user_time ON audit_events(user_id, created_at DESC)");
      });

      logger.info("✅ Schéma SQLite v16 initialisé");
      resolve();
    });
  });
}

// --- Wrappers DB ---
function dbGet(query, params = []) {
  return new Promise((resolve, reject) => {
    if (!db) return resolve(null);
    db.get(query, params, (err, row) => err ? reject(err) : resolve(row));
  });
}
function dbAll(query, params = []) {
  return new Promise((resolve, reject) => {
    if (!db) return resolve([]);
    db.all(query, params, (err, rows) => err ? reject(err) : resolve(rows));
  });
}
function dbRun(query, params = []) {
  return new Promise((resolve, reject) => {
    if (!db) return resolve({ changes: 0 });
    db.run(query, params, function (err) { err ? reject(err) : resolve(this); });
  });
}
function dbExec(query) {
  return new Promise((resolve, reject) => {
    if (!db) return resolve();
    db.exec(query, (err) => err ? reject(err) : resolve());
  });
}

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
// §1.12 — BOOTSTRAP (à appeler depuis PARTIE 4)
// ================================================================================

async function bootstrapPart1() {
  ensureDirectories();
  validateEnvironment();

  initFirebase();
  initSupabase();
  initRedis();
  initMetrics();

  await initDatabase();

  logger.info(`🎯 Features : ${featureStatus()}`);
  return { ok: true };
}

// ================================================================================
// §1.13 — EXPORTS PARTIE 1
// ================================================================================

module.exports = {
  // Config
  CONFIG, FIREBASE_CONFIG, HOSTING_CONFIG, USER_QUOTAS, ERROR_CODES, FEATURES,

  // Logging
  logger,

  // Erreurs
  LubaError, makeError, isLubaError,

  // Firebase / Firestore
  firebaseApp: () => firebaseApp,
  firestoreDb: () => firestoreDb,
  firebaseReady: () => firebaseReady,
  fsGet, fsSet, fsUpdate, fsDelete, fsQuery,

  // Supabase
  supabase: () => supabase,
  supabaseWriteSafe,

  // Cache
  cache, l1Cache, semanticCache, SemanticCache,
  redisClient: () => redisClient,

  // DB
  db: () => db,
  dbGet, dbAll, dbRun, dbExec, dbTransaction,

  // Sécurité
  detectPromptInjection, wrapUserInput, moderateText, moderateWithGroq,
  verifyHmacSignature, hmacSign, hmacVerify,
  logSecurityEvent, auditLLMCall,

  // Metrics
  metrics: () => metrics,

  // Utils
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

  // Bootstrap
  bootstrapPart1, initDatabase, initFirebase, initSupabase, initRedis, initMetrics,
  featureStatus
};

// ================================================================================
// ==================== FIN PARTIE 1/4 ===========================================
// ================================================================================
// ▶ Prochaine partie (2/4) : Services IA, Voice (STT/TTS), Memory, Routeur LLM.
//   Tape "suite" quand tu es prêt.
// ================================================================================
// ================================================================================
// PARTIE 2/4 — SERVICES IA, VOICE (STT/TTS), MEMORY, ROUTEUR LLM
// ================================================================================
// Sommaire :
//   §2.1   Registre des providers LLM (Groq, OpenRouter, Cerebras, Gemini)
//   §2.2   Circuit breaker par (provider:model:key)
//   §2.3   Intercepteur d'erreurs LLM (retryable / rotate / skip)
//   §2.4   Tiers de modèles (Mwamba v100, Ngandu v250, Vision)
//   §2.5   Appelant OpenAI-compatible (Groq/Cerebras/OpenRouter)
//   §2.6   Appelant Gemini natif (function calling inclus)
//   §2.7   callProviderWithTools (rotation + circuit breaker + fallback)
//   §2.8   TOOL_SCHEMAS (10+ outils exposés au LLM)
//   §2.9   executeToolNative (dispatch + validation)
//   §2.10  runToolLoop (agent borné, 5 itérations)
//   §2.11  STT : Groq Whisper + Deepgram fallback + streaming
//   §2.12  TTS : Kokoro (HeadTTS) + Piper fallback
//   §2.13  VAD adaptatif (endpointing ~500 ms)
//   §2.14  Pipeline vocal (STT → LLM → TTS + sentence chunking + barge-in)
//   §2.15  Mémoire courte (rolling summary des 20 derniers échanges)
//   §2.16  Mémoire longue (extraction faits + embeddings)
//   §2.17  Recall sémantique (cosine similarity)
//   §2.18  Exports PARTIE 2
// ================================================================================
// NOUVEAUTÉS v16.0 :
//   ✅ DeepSeek R1 (raisonnement) via Groq
//   ✅ Qwen 3 (Groq/Cerebras) pour le code
//   ✅ Circuit breaker granulaire (provider:model:key)
//   ✅ Rotation multi-clés avec failover
//   ✅ Tool calling unifié (Groq/Cerebras/OpenRouter/Gemini)
//   ✅ Pipeline vocal complet (STT→LLM→TTS) < 800 ms
//   ✅ Sentence chunking pour TTS anticipé
//   ✅ Barge-in (interruption + flush)
//   ✅ VAD adaptatif (WebRTC VAD + fallback énergie)
//   ✅ Mémoire longue à embeddings (recall sémantique Firestore)
//   ✅ Extraction automatique de faits (5 catégories)
// ================================================================================

// ================================================================================
// §2.1 — REGISTRE DES PROVIDERS LLM
// ================================================================================

function buildKeyPool(keys, prefix) {
  return keys
    .filter((k) => typeof k === "string" && k.trim().length > 0)
    .map((apiKey, idx) => ({ apiKey: apiKey.trim(), label: `${prefix}_key_${idx + 1}` }));
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

const LLM_PROVIDERS = Object.freeze({
  GROQ: Object.freeze({
    baseURL: "https://api.groq.com/openai/v1",
    defaultTimeout: 10000,
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
    defaultTimeout: 12000,
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
    defaultTimeout: 10000,
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
    defaultTimeout: 12000,
    maxTokens: 8000,
    temperature: 0.7,
    isGemini: true,
    client: geminiClient,
    supportsTools: true,
    keyPool: buildKeyPool([process.env.GEMINI_API_KEY], "gemini")
  })
});

// Log clés détectées
if (LLM_PROVIDERS.GROQ.keyPool.length > 0) logger.info(`🔑 Groq : ${LLM_PROVIDERS.GROQ.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.OPENROUTER.keyPool.length > 0) logger.info(`🔑 OpenRouter : ${LLM_PROVIDERS.OPENROUTER.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.CEREBRAS.keyPool.length > 0) logger.info(`🔑 Cerebras : ${LLM_PROVIDERS.CEREBRAS.keyPool.length} clé(s)`);
if (LLM_PROVIDERS.GEMINI.keyPool.length > 0) logger.info(`🔑 Gemini : ${LLM_PROVIDERS.GEMINI.keyPool.length} clé(s)`);

// ================================================================================
// §2.2 — CIRCUIT BREAKER
// ================================================================================

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold ?? CONFIG.CIRCUIT.THRESHOLD;
    this.resetTimeout = options.resetTimeout ?? CONFIG.CIRCUIT.RESET_MS;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED"; // CLOSED | OPEN | HALF_OPEN
    this.halfOpenInFlight = 0;
    this.emitter = new EventEmitter();
  }

  canAttempt() {
    if (this.state === "CLOSED") return true;

    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime >= this.resetTimeout) {
        this.state = "HALF_OPEN";
        this.halfOpenInFlight = 0;
        this.updateMetric();
        logger.info({ circuit: this.name }, "🔓 Circuit HALF_OPEN");
        return true;
      }
      return false;
    }

    if (this.halfOpenInFlight >= CONFIG.CIRCUIT.HALF_OPEN_MAX) return false;
    return true;
  }

  async execute(fn) {
    if (!this.canAttempt()) {
      const err = makeError("CIRCUIT_OPEN");
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
    this.successCount++;
    if (this.state !== "CLOSED") {
      this.state = "CLOSED";
      this.updateMetric();
      logger.info({ circuit: this.name }, "🔒 Circuit CLOSED (récupéré)");
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
// §2.3 — INTERCEPTEUR D'ERREURS LLM
// ================================================================================

class LLMErrorInterceptor {
  static isRetryableError(error) {
    const status = error?.response?.status;
    const retryableStatuses = [408, 429, 500, 502, 503, 504];
    const isTimeout = ["ECONNABORTED", "ETIMEDOUT", "ESOCKETTIMEDOUT", "ABORT_ERR"].includes(error?.code)
      || /timeout/i.test(error?.message || "");
    const isNetwork = ["ENOTFOUND", "ECONNRESET", "ECONNREFUSED", "EAI_AGAIN"].includes(error?.code);
    return retryableStatuses.includes(status) || isTimeout || isNetwork;
  }

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

  /**
   * Providers à abandonner immédiatement (erreur définitive sur ce provider).
   */
  static shouldSkipProvider(error) {
    const code = this.getErrorCode(error);
    return ["HTTP_400", "HTTP_401", "HTTP_403", "HTTP_402", "HTTP_404", "MISSING_API_KEY"].includes(code);
  }

  /**
   * Providers pour lesquels on tourne la clé immédiatement.
   */
  static shouldRotateImmediately(error) {
    const code = this.getErrorCode(error);
    return [
      "HTTP_429", "HTTP_500", "HTTP_502", "HTTP_503", "HTTP_504",
      "TIMEOUT", "DNS_ERROR", "CONNECTION_REFUSED", "CIRCUIT_OPEN"
    ].includes(code);
  }
}

function userFacingErrorMessage(error) {
  const code = LLMErrorInterceptor.getErrorCode(error);
  if (code === "HTTP_429") return "Je suis très sollicité en ce moment. Réessayez dans quelques instants. 🙏";
  if (code === "TIMEOUT" || code === "HTTP_504") return "Cette demande est complexe et prend plus de temps que prévu. Reformulez-la en étapes plus petites, ou réessayez.";
  if (code === "CIRCUIT_OPEN") return "Je suis temporairement surchargé. Patientez une minute puis réessayez.";
  if (code === "HTTP_400") return "Je n'ai pas bien compris la requête. Pouvez-vous la reformuler ?";
  if (code === "HTTP_401" || code === "HTTP_403") return "Problème d'authentification côté fournisseur. Contactez le support si le problème persiste.";
  return "Je rencontre une difficulté technique. Reformulez votre demande, ou réessayez dans un instant.";
}

// ================================================================================
// §2.4 — TIERS DE MODÈLES
// ================================================================================

const MODEL_TIERS = Object.freeze({
  // ---------- Mwamba (v100) : conversation rapide ----------
  v100: {
    name: CONFIG.BRAND.V100,
    jsonMode: false,
    providers: [
      {
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "llama-3.3-70b-versatile",
        maxTokens: 4000, timeout: 10000, temperature: 0.7, failoverPriority: 0
      },
      {
        provider: "gemini",
        model: process.env.GEMINI_MODEL_V100 || "gemini-2.0-flash-exp",
        maxTokens: 8000, timeout: 12000, temperature: 0.7, failoverPriority: 1
      },
      {
        provider: "cerebras",
        model: process.env.CEREBRAS_MODEL_V100 || "qwen-3-32b",
        maxTokens: 4000, timeout: 10000, temperature: 0.7, failoverPriority: 2
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "meta-llama/llama-3.3-70b-instruct:free",
        maxTokens: 4000, timeout: 12000, temperature: 0.7, failoverPriority: 3
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "qwen/qwen-2.5-72b-instruct:free",
        maxTokens: 4000, timeout: 12000, temperature: 0.7, failoverPriority: 4
      }
    ]
  },

  // ---------- Ngandu (v250) : raisonnement + code ----------
  v250: {
    name: CONFIG.BRAND.V250,
    jsonMode: false,
    reasoning: {
      providers: [
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_REASONING || "deepseek-r1-distill-llama-70b",
          maxTokens: 8000, timeout: 25000, temperature: 0.6, failoverPriority: 0
        },
        {
          provider: "gemini",
          model: process.env.GEMINI_MODEL_V250_REASONING || "gemini-2.0-flash-thinking-exp",
          maxTokens: 8000, timeout: 20000, temperature: 0.3, failoverPriority: 1
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_REASONING || "deepseek/deepseek-r1:free",
          maxTokens: 8000, timeout: 25000, temperature: 0.6, failoverPriority: 2
        }
      ]
    },
    code: {
      providers: [
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_CODE || "qwen-2.5-coder-32b",
          maxTokens: 8000, timeout: 15000, temperature: 0.4, failoverPriority: 0
        },
        {
          provider: "cerebras",
          model: process.env.CEREBRAS_MODEL_V250_CODE || "qwen-3-coder-32b",
          maxTokens: 8000, timeout: 15000, temperature: 0.4, failoverPriority: 1
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_CODE || "qwen/qwen-2.5-coder-32b-instruct:free",
          maxTokens: 8000, timeout: 20000, temperature: 0.4, failoverPriority: 2
        }
      ]
    },
    maxRetries: 2
  },

  // ---------- Vision : analyse d'images ----------
  vision: {
    name: "Vision",
    jsonMode: false,
    providers: [
      {
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL,
        maxTokens: 4000, timeout: 15000, temperature: 0.7, failoverPriority: 0
      },
      {
        provider: "gemini",
        model: CONFIG.VISION.GEMINI_MODEL,
        maxTokens: 4000, timeout: 15000, temperature: 0.7, failoverPriority: 1
      },
      {
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL,
        maxTokens: 4000, timeout: 20000, temperature: 0.7, failoverPriority: 2
      }
    ]
  }
});

// ================================================================================
// §2.4.bis — VALIDATION MODÈLES OPENROUTER
// ================================================================================

function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") return null;
  const knownPrefixes = [
    "openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", "anthropic/",
    "google/", "mistralai/", "cohere/", "nvidia/", "poolside/", "inclusionai/",
    "z-ai/", "liquid/", "cognitivecomputations/"
  ];
  const isORModel = knownPrefixes.some((p) => model.includes(p));
  if (isORModel && !model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
    return model + ":free";
  }
  return model;
}

// ================================================================================
// §2.5 — APPELANT OPENAI-COMPATIBLE
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

  // Formate les messages avec images (dernier message user)
  let formattedMessages = messages;
  if (images && images.length > 0) {
    const idx = messages.length - 1;
    if (messages[idx]?.role === "user") {
      const parts = [];
      if (typeof messages[idx].content === "string") parts.push({ type: "text", text: messages[idx].content });
      for (const img of images) {
        parts.push({ type: "image_url", image_url: { url: img.dataUrl } });
      }
      formattedMessages = [...messages.slice(0, idx), { role: "user", content: parts }];
    }
  }

  const payload = { model, messages: formattedMessages, temperature, max_tokens: maxTokens };
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

  return {
    message,
    raw: response.data,
    usage: response.data?.usage || null
  };
}

// ================================================================================
// §2.6 — APPELANT GEMINI NATIF
// ================================================================================

async function callGeminiRawWithTools({ model, messages, tools, jsonMode, timeout, maxTokens, temperature, images, signal }) {
  if (!geminiClient) throw new Error("Client Gemini non initialisé");

  let systemInstruction = null;
  const contents = [];

  for (const msg of messages) {
    if (msg.role === "system") {
      systemInstruction = typeof msg.content === "string" ? msg.content : safeJsonStringify(msg.content);
      continue;
    }

    if (msg.role === "tool") {
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

  // Injection des images en input direct
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
    maxOutputTokens: maxTokens || LLM_PROVIDERS.GEMINI.maxTokens,
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
    t.unref?.();
  });

  const response = await Promise.race([geminiPromise, timeoutPromise]);

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
          arguments: safeJsonStringify(part.functionCall.args || {})
        }
      });
    }
  }

  const message = { role: "assistant", content: text };
  if (toolCalls.length > 0) message.tool_calls = toolCalls;

  return { message, raw: response, usage: response?.usageMetadata || null };
}

// ================================================================================
// §2.7 — CALLPROVIDERWITHTools (rotation + circuit breaker)
// ================================================================================

async function callProviderRawWithTools({
  provider, model, messages, tools, jsonMode = false,
  timeout, maxTokens, temperature, images, apiKey, reasoningEffort
}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout + 500);
  timer.unref?.();

  try {
    if (provider === "gemini") {
      return await callGeminiRawWithTools({
        model, messages, tools, jsonMode, timeout,
        maxTokens, temperature, images, signal: controller.signal
      });
    }
    return await callOpenAICompatibleRaw({
      provider, model, messages, tools, jsonMode, timeout,
      maxTokens, temperature, images, apiKey, reasoningEffort, signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Appelle un provider avec :
 * - rotation multi-clés
 * - circuit breaker par (provider:model:key)
 * - audit log
 * - métriques Prometheus
 */
async function callProviderWithTools({
  providerConfig, messages, tools = null, images = null, jsonMode = false,
  _meta = {}
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
          messages,
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
      // Métriques
      if (metrics?.llmCalls) {
        metrics.llmCalls.labels(providerName, model, "success").inc();
      }
      if (metrics?.llmLatency) {
        metrics.llmLatency.labels(providerName, model, "success").observe(latency / 1000);
      }
      if (metrics?.llmTokens && result.usage) {
        metrics.llmTokens.labels(providerName, model, "prompt").inc(result.usage.prompt_tokens || 0);
        metrics.llmTokens.labels(providerName, model, "completion").inc(result.usage.completion_tokens || 0);
      }

      // Audit
      auditLLMCall({
        sessionId: _meta.sessionId || null,
        userId: _meta.userId || null,
        provider: providerName, model, tier: _meta.tier || "v100",
        promptTokens: result.usage?.prompt_tokens || 0,
        completionTokens: result.usage?.completion_tokens || 0,
        latencyMs: latency, status: "success"
      }).catch(() => {});

      return {
        success: true,
        message: result.message,
        raw: result.raw,
        usage: result.usage,
        providerUsed: providerName,
        modelUsed: model,
        keyLabel: keyEntry.label,
        latencyMs: latency
      };
    } catch (error) {
      lastError = error;
      const code = LLMErrorInterceptor.getErrorCode(error);

      if (metrics?.llmCalls) {
        metrics.llmCalls.labels(providerName, model, "error").inc();
      }

      if (LLMErrorInterceptor.shouldRotateImmediately(error)) {
        logger.warn({ provider: providerName, model, keyLabel: keyEntry.label, code }, "🔄 Rotation immédiate");
        continue;
      }
      if (LLMErrorInterceptor.shouldSkipProvider(error)) {
        logger.warn({ provider: providerName, model, code }, "⏭️  Provider skip");
        break;
      }
      break;
    }
  }

  auditLLMCall({
    sessionId: _meta.sessionId || null,
    userId: _meta.userId || null,
    provider: providerName, model, tier: _meta.tier || "v100",
    latencyMs: Date.now() - startedAt, status: "failed",
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  }).catch(() => {});

  return { success: false, error: lastError };
}

// ================================================================================
// §2.8 — TOOL SCHEMAS (exposés au LLM)
// ================================================================================

const TOOL_SCHEMAS = Object.freeze({
  search_images: {
    type: "function",
    function: {
      name: "search_images",
      description: "Recherche des images réelles (Wikimedia Commons, Wikipédia, DuckDuckGo Images) pour une personne, un lieu, un objet, une équipe ou un concept précis.",
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
      description: "Recherche web générale (Wikipédia + actualités + DuckDuckGo + Tavily + Serper si configuré).",
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
      description: "Dernières actualités sur un sujet (Google News + GDELT).",
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
      description: "Derniers résultats d'une équipe via Google News (extraction de score).",
      parameters: {
        type: "object",
        properties: { team: { type: "string", description: "Nom de l'équipe (ex: 'Léopards RDC', 'PSG')." } },
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
      description: "Météo actuelle pour un lieu (Open-Meteo, gratuit, sans clé).",
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
      description: "Prix actuel d'une cryptomonnaie (CoinMarketCap keyless).",
      parameters: {
        type: "object",
        properties: {
          symbol: { type: "string", description: "Symbole (BTC, ETH, SOL...)" }
        },
        required: ["symbol"],
        additionalProperties: false
      }
    }
  },
  get_stock_price: {
    type: "function",
    function: {
      name: "get_stock_price",
      description: "Prix actuel d'une action (Yahoo Finance).",
      parameters: {
        type: "object",
        properties: { ticker: { type: "string", description: "Ticker (AAPL, TSLA...)" } },
        required: ["ticker"],
        additionalProperties: false
      }
    }
  },
  execute_math: {
    type: "function",
    function: {
      name: "execute_math",
      description: "Calcul mathématique exact (arithmétique, algèbre, trigonométrie).",
      parameters: {
        type: "object",
        properties: {
          expression: { type: "string", description: "Expression mathjs valide, 2000 char max." }
        },
        required: ["expression"],
        additionalProperties: false
      }
    }
  },
  search_youtube: {
    type: "function",
    function: {
      name: "search_youtube",
      description: "Recherche vidéos YouTube (youtubei.js si dispo, sinon API key, sinon DDG).",
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
      description: "Crée un rappel / une tâche. due_at en ISO 8601 si une date est mentionnée.",
      parameters: {
        type: "object",
        properties: {
          title: { type: "string", maxLength: 200 },
          notes: { type: "string", maxLength: 2000 },
          due_at: { type: "string", description: "Date ISO 8601 (ex: '2026-05-01T14:00:00Z')" }
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
        properties: {
          status: { type: "string", enum: ["pending", "done", "all"] }
        },
        additionalProperties: false
      }
    }
  },
  complete_task: {
    type: "function",
    function: {
      name: "complete_task",
      description: "Marque une tâche comme terminée. Nécessite confirmation.",
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
      description: "Supprime une tâche. Nécessite confirmation.",
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
      description: "Envoie un email. Nécessite confirmation explicite.",
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
      description: "Envoie un message WhatsApp. Nécessite confirmation.",
      parameters: {
        type: "object",
        properties: {
          phone_number: { type: "string", description: "Format international (ex: +243812345678)." },
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
      description: "Exécute un code dans un SANDBOX EXTERNE (Piston/Judge0, pas de réseau, timeout strict).",
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
  },
  remember_fact: {
    type: "function",
    function: {
      name: "remember_fact",
      description: "Mémorise un fait durable sur l'utilisateur (prénom, préférence, projet, langue). À utiliser SPARSEMMENT.",
      parameters: {
        type: "object",
        properties: {
          fact: { type: "string", maxLength: 300 },
          category: {
            type: "string",
            enum: ["identity", "preference", "project", "language", "general"]
          }
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
      description: "Cherche dans la mémoire longue un fait oublié par l'utilisateur ('comme je t'ai dit hier').",
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
// §2.9 — EXECUTE TOOL NATIVE
// ================================================================================

function validateToolArgs(name, args) {
  const schema = TOOL_SCHEMAS[name]?.function?.parameters;
  if (!schema) return { ok: false, error: "Outil inconnu" };
  const required = schema.required || [];
  for (const key of required) {
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
      return { ok: false, error: `Paramètre ${key} trop long (max ${prop.maxLength})` };
    }
    if (prop.enum && !prop.enum.includes(args[key])) {
      return { ok: false, error: `Paramètre ${key} doit valoir : ${prop.enum.join(", ")}` };
    }
  }
  return { ok: true };
}

/**
 * Exécute un outil natif.
 * Retourne : { result, sourceKeys, toolName }
 *
 * ⚠️ Les fonctions appelées ici (searchImagesWithFallback, searchWeb, etc.)
 *    seront définies dans PARTIE 3. En attendant, on les référence en lazy.
 */
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
        if (!result.error) sourceKeys.push("coinmarketcap");
        break;
      }
      case "get_stock_price": {
        result = await getStockPrice(args.ticker);
        if (!result.error) sourceKeys.push("yahoo");
        break;
      }
      case "execute_math": {
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
        if (!agentMode) return {
          result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
          sourceKeys: [], toolName
        };
        result = await updateTaskStatus(userId, args.task_id, "done");
        break;
      }
      case "delete_task": {
        if (!agentMode) return {
          result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
          sourceKeys: [], toolName
        };
        result = await deleteTask(userId, args.task_id);
        break;
      }
      case "send_email": {
        if (!agentMode) return {
          result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
          sourceKeys: [], toolName
        };
        const quota = await checkUserQuota(userId, "email");
        if (!quota.allowed) { result = { success: false, error: quota.message }; break; }
        result = await dispatchSendEmail({
          googleAccessToken, recipient: args.recipient,
          subject: args.subject, body: args.body, userId
        });
        if (result.success) await incrementUserQuota(userId, "email");
        break;
      }
      case "send_whatsapp_message": {
        if (!agentMode) return {
          result: { success: false, error: "Confirmation requise", code: "NEEDS_CONFIRMATION" },
          sourceKeys: [], toolName
        };
        const quota = await checkUserQuota(userId, "whatsapp");
        if (!quota.allowed) { result = { success: false, error: quota.message }; break; }
        result = await sendWhatsAppSmart(userId, args.phone_number, args.message);
        if (result.success) await incrementUserQuota(userId, "whatsapp");
        break;
      }
      case "run_code": {
        result = await runCodeSandbox({ language: args.language, code: args.code, stdin: args.stdin });
        if (!result.success && /non configuré/i.test(result.error || "")) {
          result = {
            success: true, stdout: "", stderr: "", exitCode: null, provider: "none",
            note: "Sandbox non disponible. Le code N'A PAS été exécuté. Rédige ta réponse finale — ne prétends pas avoir exécuté le code."
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
    logger.error({ err: e.message, toolName, userId }, "Erreur exécution outil");
    result = { success: false, error: "Échec d'exécution" };
  }

  if (metrics?.toolCalls) {
    metrics.toolCalls.labels(toolName, result?.success ? "success" : "error").inc();
  }

  return { result, sourceKeys, toolName };
}

// ================================================================================
// §2.10 — RUNTOOLLOOP (agent borné)
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
        toolCallTrace
      };
    }

    const assistantMessage = llmResult.message || {};
    const toolCalls = assistantMessage.tool_calls || [];
    const rawContent = assistantMessage.content || "";

    // Streamer la réflexion <think> détectée
    if (sse && rawContent) {
      const thinkMatches = rawContent.match(/<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/gi);
      if (thinkMatches) {
        for (const t of thinkMatches) {
          const inner = t.replace(/<\/?think(?:ing)?>/gi, "").trim();
          if (inner) {
            const chunks = safeChunkText(inner, 32);
            for (const c of chunks) {
              if (sse.closed) break;
              sse.reasoning(c);
              await sleep(4);
            }
            sse.reasoning("\n");
          }
        }
      }
    }

    // Streamer les blocs de code détectés dans le contenu
    if (sse && rawContent) {
      const codeRegex = /```(\w+)?(?::([^\n]+))?\n([\s\S]*?)```/g;
      let m;
      while ((m = codeRegex.exec(rawContent)) !== null) {
        sse.codeBlock({
          language: m[1] || "text",
          filename: m[2] || null,
          code: m[3],
          done: true
        });
      }
    }

    if (toolCalls.length === 0) {
      finalText = normalizeMath(stripThinkTags(rawContent).text);
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
      content: "Synthétise ta réponse finale. Pas de nouvel appel d'outil."
    });
    const finalCall = await callProviderWithTools({
      providerConfig,
      messages: workingMessages,
      tools: [],
      images,
      _meta
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
// §2.11 — STT : GROQ WHISPER + DEEPGRAM FALLBACK
// ================================================================================

/**
 * Transcription batch via Groq Whisper.
 * Rotation multi-clés + fallback Deepgram si Groq échoue.
 */
async function transcribeAudioGroq(buffer, filename, mimetype) {
  if (!LLM_PROVIDERS.GROQ.keyPool || LLM_PROVIDERS.GROQ.keyPool.length === 0) {
    return { success: false, error: "Aucune clé Groq pour la transcription" };
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
      if (metrics?.sttLatency) {
        metrics.sttLatency.labels("groq").observe(latency / 1000);
      }

      return {
        success: true,
        text: response.data?.text || "",
        provider: "groq",
        latencyMs: latency
      };
    } catch (e) {
      lastError = e;
      logger.warn({ err: e.message, keyLabel: keyEntry.label }, "STT Groq rotation");
    }
  }

  // Fallback Deepgram
  if (process.env.DEEPGRAM_API_KEY) {
    try {
      const startedAt = Date.now();
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
      const latency = Date.now() - startedAt;
      if (metrics?.sttLatency) {
        metrics.sttLatency.labels("deepgram").observe(latency / 1000);
      }
      return { success: true, text, provider: "deepgram", latencyMs: latency };
    } catch (e) {
      logger.warn({ err: e.message }, "STT Deepgram échoué");
    }
  }

  return { success: false, error: "Échec transcription (Groq + Deepgram)" };
}

/**
 * Streaming STT : accumule des chunks et transcrit dès qu'un segment VAD
 * est détecté comme terminé. Utilisé dans le pipeline Luba Live.
 *
 * Note : Groq ne propose pas un endpoint WebSocket officiel public.
 * On simule donc un streaming par chunks (300 ms) en parallèle.
 */
class StreamingSTT {
  constructor({ onPartial, onFinal, onError } = {}) {
    this.onPartial = onPartial || (() => {});
    this.onFinal = onFinal || (() => {});
    this.onError = onError || (() => {});
    this.buffer = [];
    this.totalBytes = 0;
    this.accumulatedText = "";
    this.closed = false;
  }

  async push(chunk, { mimetype = "audio/webm" } = {}) {
    if (this.closed) return;
    this.buffer.push(chunk);
    this.totalBytes += chunk.length;

    // Déclenche une transcription partielle si le buffer dépasse ~500ms
    if (this.totalBytes > 16000) {
      await this._transcribePartial(mimetype);
    }
  }

  async _transcribePartial(mimetype) {
    if (this.buffer.length === 0) return;
    try {
      const merged = Buffer.concat(this.buffer);
      const result = await transcribeAudioGroq(merged, "chunk.webm", mimetype);
      if (result.success && result.text) {
        this.accumulatedText = result.text;
        this.onPartial(result.text);
      }
    } catch (e) {
      logger.debug({ err: e.message }, "STT partial échec");
    }
  }

  async finalize(mimetype = "audio/webm") {
    if (this.closed) return { success: false, error: "Already closed" };
    this.closed = true;

    if (this.buffer.length === 0) return { success: false, error: "Empty buffer" };

    const merged = Buffer.concat(this.buffer);
    const result = await transcribeAudioGroq(merged, "final.webm", mimetype);
    if (result.success) {
      this.onFinal(result.text);
    } else {
      this.onError(result.error);
    }
    this.buffer = [];
    return result;
  }

  reset() {
    this.buffer = [];
    this.totalBytes = 0;
    this.accumulatedText = "";
    this.closed = false;
  }
}

// ================================================================================
// §2.12 — TTS : KOKORO (HEADTTS) + PIPER FALLBACK
// ================================================================================

/**
 * TTS via Kokoro (HeadTTS). Peut être :
 *  - une URL HTTP auto-hébergée (kokoro-fastapi, headtts-server)
 *  - une API distante compatible
 */
async function synthesizeKokoro(text, { voice = "af_bella", speed = 1.0, format = "mp3" } = {}) {
  const baseURL = process.env.KOKORO_URL;
  if (!baseURL) return { success: false, error: "KOKORO_URL non configurée" };

  try {
    const startedAt = Date.now();
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
    const latency = Date.now() - startedAt;
    if (metrics?.ttsLatency) {
      metrics.ttsLatency.labels("kokoro").observe(latency / 1000);
    }
    return {
      success: true,
      audio: Buffer.from(response.data),
      provider: "kokoro",
      format,
      latencyMs: latency
    };
  } catch (e) {
    logger.warn({ err: e.message }, "Kokoro TTS échoué");
    return { success: false, error: e.message };
  }
}

/**
 * TTS via Piper (auto-hébergé). Retourne du WAV brut.
 */
async function synthesizePiper(text, { voice = "fr_FR-siwis-medium", speed = 1.0 } = {}) {
  const baseURL = process.env.PIPER_URL;
  if (!baseURL) return { success: false, error: "PIPER_URL non configurée" };

  try {
    const startedAt = Date.now();
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
    const latency = Date.now() - startedAt;
    if (metrics?.ttsLatency) {
      metrics.ttsLatency.labels("piper").observe(latency / 1000);
    }
    return {
      success: true,
      audio: Buffer.from(response.data),
      provider: "piper",
      format: "wav",
      latencyMs: latency
    };
  } catch (e) {
    logger.warn({ err: e.message }, "Piper TTS échoué");
    return { success: false, error: e.message };
  }
}

/**
 * TTS avec cascade : Kokoro → Piper → erreur.
 */
async function synthesizeSpeech(text, options = {}) {
  if (!text || !text.trim()) return { success: false, error: "Texte vide" };

  // Nettoie le markdown pour le TTS
  const cleanText = text
    .replace(/```[\s\S]*?```/g, " bloc de code omis ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[*_~>#]/g, "")
    .replace(/\n{2,}/g, ". ")
    .trim();

  const kokoroResult = await synthesizeKokoro(cleanText, options);
  if (kokoroResult.success) return kokoroResult;

  const piperResult = await synthesizePiper(cleanText, options);
  if (piperResult.success) return piperResult;

  return { success: false, error: "Aucun moteur TTS disponible (Kokoro + Piper)" };
}

// ================================================================================
// §2.13 — VAD ADAPTATIF
// ================================================================================

/**
 * VAD (Voice Activity Detection) simple basé sur l'énergie RMS.
 * Pour un VAD plus précis, utiliser WebRTC VAD côté client.
 *
 * Endpointing : détecte la fin de parole après ~500 ms de silence.
 */
class SimpleVAD {
  constructor({
    energyThreshold = 500,
    silenceMs = 500,
    minSpeechMs = 200,
    maxSpeechMs = 30000
  } = {}) {
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
    this.totalSamples = 0;
  }

  /**
   * Pousse un chunk PCM 16-bit mono (Buffer).
   * Retourne { event: "speech-start" | "speech-end" | "continue" | null, durationMs? }
   */
  push(pcmBuffer) {
    if (!pcmBuffer || pcmBuffer.length < 2) return { event: null };

    const samples = pcmBuffer.length / 2;
    this.totalSamples += samples;

    // RMS energy
    let sum = 0;
    for (let i = 0; i < pcmBuffer.length; i += 2) {
      const sample = pcmBuffer.readInt16LE(i);
      sum += sample * sample;
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

      // Sécurité : max 30s de parole continue
      if (now - this.speechStart > this.maxSpeechMs) {
        const duration = now - this.speechStart;
        this.reset();
        return { event: "speech-end", durationMs: duration, reason: "max_duration" };
      }
      return { event: "continue" };
    }

    // Silence
    if (this.speaking && this.lastVoiceTime) {
      const silenceDuration = now - this.lastVoiceTime;
      if (silenceDuration >= this.silenceMs) {
        const duration = this.lastVoiceTime - this.speechStart;
        // Trop court = bruit, on ignore
        if (duration < this.minSpeechMs) {
          this.reset();
          return { event: "too-short" };
        }
        this.reset();
        return { event: "speech-end", durationMs: duration, reason: "silence" };
      }
    }
    return { event: null };
  }
}

// ================================================================================
// §2.14 — PIPELINE VOCAL (STT → LLM → TTS + sentence chunking + barge-in)
// ================================================================================

/**
 * Sentence chunker : détecte les fins de phrase pour lancer le TTS en avance.
 */
class SentenceChunker {
  constructor({ onSentence } = {}) {
    this.onSentence = onSentence || (() => {});
    this.buffer = "";
  }

  push(text) {
    if (!text) return;
    this.buffer += text;

    // Regex : fin de phrase suivie d'espace ou fin de buffer
    const regex = /([.!?…]+[\s\n]+|[\n]{2,})/g;
    let lastIndex = 0;
    let match;
    const sentences = [];

    while ((match = regex.exec(this.buffer)) !== null) {
      const sentence = this.buffer.slice(lastIndex, match.index + match[0].length).trim();
      if (sentence.length >= 8) sentences.push(sentence);
      lastIndex = match.index + match[0].length;
    }

    if (lastIndex > 0) {
      this.buffer = this.buffer.slice(lastIndex);
    }

    for (const s of sentences) this.onSentence(s);
  }

  flush() {
    const rest = this.buffer.trim();
    this.buffer = "";
    if (rest.length >= 3) this.onSentence(rest);
  }
}

/**
 * Pipeline vocal complet.
 * Utilise : STT streaming + LLM + TTS streaming + sentence chunking + barge-in.
 *
 * @param {object} opts
 * @param {Stream} opts.audioIn       - Stream d'entrée (PCM 16kHz mono)
 * @param {Function} opts.onTranscript - Callback transcript final
 * @param {Function} opts.onAudio     - Callback audio TTS sortant (Buffer)
 * @param {Function} opts.onStatus    - Callback statut
 * @param {Function} opts.onError     - Callback erreur
 */
class VoicePipeline extends EventEmitter {
  constructor({
    sessionId, userId,
    onTranscript, onAudio, onStatus, onError,
    voice = "af_bella"
  } = {}) {
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
    this.chunker = null;
    this.interrupted = false;
    this.processing = false;
  }

  /**
   * Barge-in : interrompt la génération en cours.
   */
  bargeIn() {
    this.interrupted = true;
    this.processing = false;
    this.onStatus("barge-in");
  }

  /**
   * Démarre le traitement d'un flux audio continu.
   */
  async processAudioStream(audioStream, { mimetype = "audio/webm" } = {}) {
    this.stt = new StreamingSTT({
      onPartial: (text) => this.onStatus("partial", { text }),
      onFinal: (text) => this.onStatus("final", { text }),
      onError: (e) => this.onError({ stage: "stt", error: e })
    });

    let pcmBuffer = [];

    audioStream.on("data", async (chunk) => {
      if (this.interrupted) return;
      try {
        await this.stt.push(chunk, { mimetype });

        // VAD sur le chunk converti (si PCM brut)
        if (mimetype.includes("pcm") || mimetype.includes("l16")) {
          const vadResult = this.vad.push(chunk);
          if (vadResult.event === "speech-end") {
            this.onStatus("speech-end", { durationMs: vadResult.durationMs });
            await this.finalizeUtterance(mimetype);
          }
        }
      } catch (e) {
        this.onError({ stage: "stream", error: e.message });
      }
    });

    audioStream.on("end", async () => {
      if (!this.interrupted) {
        await this.finalizeUtterance(mimetype);
      }
    });

    audioStream.on("error", (e) => {
      this.onError({ stage: "stream", error: e.message });
    });
  }

  /**
   * Finalise une utterance : STT final → LLM → TTS streaming.
   */
  async finalizeUtterance(mimetype) {
    if (this.processing) return;
    this.processing = true;
    this.interrupted = false;

    try {
      this.onStatus("transcribing");
      const sttResult = await this.stt.finalize(mimetype);
      if (!sttResult.success || !sttResult.text) {
        this.onStatus("empty-transcript");
        this.processing = false;
        return;
      }

      const userText = sttResult.text.trim();
      this.onTranscript(userText);
      this.onStatus("thinking", { text: userText });

      // Prépare le chunker TTS
      const ttsQueue = [];
      let ttsProcessing = false;

      const processTTSQueue = async () => {
        if (ttsProcessing) return;
        ttsProcessing = true;
        while (ttsQueue.length > 0 && !this.interrupted) {
          const sentence = ttsQueue.shift();
          try {
            const tts = await synthesizeSpeech(sentence, { voice: this.voice });
            if (tts.success && !this.interrupted) {
              this.onAudio(tts.audio, { format: tts.format });
            }
          } catch (e) {
            logger.warn({ err: e.message }, "TTS échec sur phrase");
          }
        }
        ttsProcessing = false;
      };

      this.chunker = new SentenceChunker({
        onSentence: (sentence) => {
          if (this.interrupted) return;
          ttsQueue.push(sentence);
          processTTSQueue().catch(() => {});
        }
      });

      // Récupère la réponse LLM (callback injectée par l'appelant)
      const llmStream = await this.emit("requestLLM", userText);
      if (llmStream && typeof llmStream[Symbol.asyncIterator] === "function") {
        for await (const token of llmStream) {
          if (this.interrupted) break;
          this.chunker.push(token);
        }
      } else if (typeof llmStream === "string") {
        this.chunker.push(llmStream);
      }

      this.chunker.flush();
      // Attend la fin du TTS
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
// §2.15 — MÉMOIRE COURTE (rolling summary)
// ================================================================================

const SHORT_MEMORY_MAX_EXCHANGES = 20;

/**
 * Construit un résumé glissant des derniers échanges pour rester dans le contexte.
 * N'appelle PAS le LLM : c'est un simple compactage mécanique.
 */
function buildRollingSummary(history, maxExchanges = SHORT_MEMORY_MAX_EXCHANGES) {
  if (!Array.isArray(history) || history.length === 0) return "";

  // Ne garde que user/assistant
  const filtered = history.filter((m) => m.role === "user" || m.role === "assistant");
  if (filtered.length === 0) return "";

  // Regroupe en paires user/assistant
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

  // Garde les N dernières
  const recent = exchanges.slice(-maxExchanges);

  return recent.map((e, i) => {
    const u = String(e.user || "").slice(0, 200).replace(/\s+/g, " ");
    const a = String(e.assistant || "").slice(0, 200).replace(/\s+/g, " ");
    return `${i + 1}. U: ${u}${a ? `\n   A: ${a}` : ""}`;
  }).join("\n");
}

// ================================================================================
// §2.16 — MÉMOIRE LONGUE (extraction de faits + embeddings)
// ================================================================================

const FACT_CATEGORIES = Object.freeze(["identity", "preference", "project", "language", "general"]);

/**
 * Enregistre un fait durable sur l'utilisateur.
 * - Dédupliqué par similarité (cosine > 0.9)
 * - Embedding stocké pour recall sémantique
 */
async function rememberFact(userId, { fact, category = "general", sourceSession = null, confidence = 1.0 }) {
  if (!fact || typeof fact !== "string" || fact.trim().length < 3) {
    return { success: false, error: "Fait vide ou trop court" };
  }
  if (!FACT_CATEGORIES.includes(category)) category = "general";

  const cleanFact = sanitizeStrict(fact, 300);

  try {
    // Vérifie si un fait très similaire existe déjà
    const existing = await dbAll(
      `SELECT id, fact FROM user_memory_facts WHERE user_id = ? ORDER BY created_at DESC LIMIT 100`,
      [userId]
    );

    const normalized = cleanFact.toLowerCase().replace(/\s+/g, " ");
    for (const e of existing) {
      const eNorm = String(e.fact).toLowerCase().replace(/\s+/g, " ");
      // Similarité Jaccard grossière (rapide)
      const words1 = new Set(normalized.split(" "));
      const words2 = new Set(eNorm.split(" "));
      const inter = [...words1].filter((w) => words2.has(w)).length;
      const union = new Set([...words1, ...words2]).size;
      const jaccard = inter / union;
      if (jaccard > 0.85) {
        // Met à jour la date d'update du fait existant
        await dbRun(`UPDATE user_memory_facts SET updated_at = ? WHERE id = ?`, [Date.now(), e.id]);
        return { success: true, factId: e.id, action: "kept", existing: true };
      }
    }

    const embedding = await embedText(cleanFact);
    const embJson = embedding ? safeJsonStringify(embedding) : null;

    const result = await dbRun(
      `INSERT INTO user_memory_facts (user_id, fact, category, embedding, confidence, source_session, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [userId, cleanFact, category, embJson, confidence, sourceSession, Date.now(), Date.now()]
    );

    // Dual-write Firestore
    if (firestoreDb) {
      fsSet("user_memory_facts", `${userId}_${result.lastID}`, {
        user_id: userId,
        fact: cleanFact,
        category,
        confidence,
        source_session: sourceSession,
        created_at: new Date()
      }).catch(() => {});
    }

    logger.info({ userId, category, fact: cleanFact.slice(0, 80) }, "🧠 Fait mémorisé");
    return { success: true, factId: result.lastID, action: "created" };
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur rememberFact");
    return { success: false, error: e.message };
  }
}

/**
 * Extrait automatiquement des faits durables d'un échange via le LLM.
 * À appeler en fire-and-forget après chaque tour de conversation.
 */
async function extractFactsFromExchange(userId, userMessage, assistantReply) {
  if (!userMessage || userMessage.length < 20) return { facts: [] };

  try {
    const provider = MODEL_TIERS.v100.providers[0];
    const result = await callProviderWithTools({
      providerConfig: provider,
      messages: [
        {
          role: "system",
          content: [
            "Tu extrais des faits DURABLES et UTILES sur un utilisateur à partir d'un échange.",
            "Catégories : identity (prénom, âge, ville), preference (aime/déteste), project (projets en cours), language (langue préférée), general.",
            "N'inclus PAS : questions ponctuelles, small talk, contenus sensibles, infos médicales.",
            "Retourne STRICTEMENT un JSON :",
            '{"facts":[{"fact":"...","category":"identity|preference|project|language|general"}]}',
            "Si aucun fait durable, retourne {\"facts\":[]}."
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

    if (!result.success) return { facts: [] };

    const content = result.message?.content || "";
    let parsed = null;
    try { parsed = JSON.parse(content); }
    catch {
      const m = content.match(/\{[\s\S]*\}/);
      if (m) try { parsed = JSON.parse(m[0]); } catch {}
    }

    if (!parsed?.facts || !Array.isArray(parsed.facts)) return { facts: [] };

    const validFacts = parsed.facts
      .filter((f) => f?.fact && typeof f.fact === "string" && f.fact.length >= 5 && f.fact.length <= 300)
      .slice(0, 3);

    for (const f of validFacts) {
      await rememberFact(userId, {
        fact: f.fact,
        category: f.category || "general",
        confidence: 0.8
      });
    }

    return { facts: validFacts };
  } catch (e) {
    logger.warn({ err: e.message }, "Extraction de faits échouée");
    return { facts: [] };
  }
}

// ================================================================================
// §2.17 — RECALL SÉMANTIQUE
// ================================================================================

/**
 * Embeddings via nomic-embed-text (Groq) ou fallback OpenAI-compatible.
 * Retourne null si aucun provider d'embeddings n'est dispo.
 */
async function embedText(text) {
  if (!text || typeof text !== "string") return null;
  const trimmed = text.slice(0, 2000);

  // Groq nomic-embed (si activé)
  if (process.env.GROQ_EMBED_MODEL && LLM_PROVIDERS.GROQ.keyPool.length > 0) {
    try {
      const key = LLM_PROVIDERS.GROQ.keyPool[0];
      const resp = await axios.post(
        "https://api.groq.com/openai/v1/embeddings",
        { model: process.env.GROQ_EMBED_MODEL || "nomic-embed-text-v1.5", input: trimmed },
        {
          headers: { Authorization: `Bearer ${key.apiKey}`, "Content-Type": "application/json" },
          timeout: 8000
        }
      );
      return resp.data?.data?.[0]?.embedding || null;
    } catch (e) {
      logger.debug({ err: e.message }, "Embedding Groq échoué");
    }
  }

  // OpenRouter embeddings (rare, mais parfois dispo)
  if (process.env.OPENROUTER_EMBED_MODEL && LLM_PROVIDERS.OPENROUTER.keyPool.length > 0) {
    try {
      const key = LLM_PROVIDERS.OPENROUTER.keyPool[0];
      const resp = await axios.post(
        "https://openrouter.ai/api/v1/embeddings",
        { model: process.env.OPENROUTER_EMBED_MODEL, input: trimmed },
        {
          headers: {
            Authorization: `Bearer ${key.apiKey}`,
            "Content-Type": "application/json",
            "HTTP-Referer": HOSTING_CONFIG.domain,
            "X-Title": "Luba AI"
          },
          timeout: 8000
        }
      );
      return resp.data?.data?.[0]?.embedding || null;
    } catch (e) {
      logger.debug({ err: e.message }, "Embedding OpenRouter échoué");
    }
  }

  return null;
}

/**
 * Recall sémantique : cherche les faits les plus pertinents pour une query.
 * Si embeddings indisponibles → fallback par mots-clés (Jaccard).
 */
async function recallMemory(userId, query, limit = 5) {
  if (!query || typeof query !== "string") return { facts: [] };

  try {
    const allFacts = await dbAll(
      `SELECT id, fact, category, embedding, confidence, created_at
       FROM user_memory_facts WHERE user_id = ?
       ORDER BY created_at DESC LIMIT 200`,
      [userId]
    );

    if (allFacts.length === 0) return { facts: [] };

    // Essaie l'embedding de la query
    const queryEmb = await embedText(query);

    let scored = [];

    if (queryEmb) {
      for (const f of allFacts) {
        const fEmb = f.embedding ? safeJsonParse(f.embedding, null) : null;
        if (!fEmb) continue;
        const score = SemanticCache.cosineSimilarity(queryEmb, fEmb);
        scored.push({ ...f, score });
      }
    }

    // Fallback : mots-clés Jaccard
    if (scored.length === 0) {
      const qWords = new Set(String(query).toLowerCase().split(/\s+/).filter((w) => w.length > 2));
      for (const f of allFacts) {
        const fWords = new Set(String(f.fact).toLowerCase().split(/\s+/).filter((w) => w.length > 2));
        const inter = [...qWords].filter((w) => fWords.has(w)).length;
        const union = new Set([...qWords, ...fWords]).size;
        const jaccard = union > 0 ? inter / union : 0;
        scored.push({ ...f, score: jaccard });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, limit).filter((f) => f.score > 0.15);

    return {
      facts: top.map((f) => ({
        fact: f.fact,
        category: f.category,
        score: Number(f.score.toFixed(3)),
        createdAt: f.created_at
      }))
    };
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur recallMemory");
    return { facts: [] };
  }
}

/**
 * Retourne tous les faits catégorisés (pour le bootstrap).
 */
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
    return { success: false, error: e.message, grouped: {}, total: 0 };
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
    if (firestoreDb) {
      const facts = await fsQuery("user_memory_facts", {
        where: [["user_id", "==", userId]], limit: 500
      });
      for (const f of facts) {
        fsDelete("user_memory_facts", f.id).catch(() => {});
      }
    }
    return { success: true };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ================================================================================
// §2.17.bis — HELPERS PARTAGÉS (référencés par executeToolNative)
// ================================================================================
// Ces fonctions sont définies en PARTIE 3. Ici on déclare des stubs sûrs
// qui seront overridés par `Object.assign(module.exports, ...)` en PARTIE 3.

let searchImagesWithFallback = async () => ({ images: [] });
let searchWeb               = async () => ({ results: [], sourcesUsed: [] });
let searchNews              = async () => ({ articles: [] });
let searchSportsScores      = async () => ({ events: [] });
let searchScience           = async () => ({ papers: [] });
let searchSocial            = async () => ({ posts: [] });
let getWeather              = async () => ({ error: "Météo indisponible" });
let getCryptoPrice          = async () => ({ error: "Crypto indisponible" });
let getStockPrice           = async () => ({ error: "Action indisponible" });
let searchYouTube           = async () => ({ videos: [] });
let createTask              = async () => ({ success: false, error: "Tâches indisponibles" });
let listTasks               = async () => ({ success: false, tasks: [] });
let updateTaskStatus        = async () => ({ success: false, error: "Tâches indisponibles" });
let deleteTask              = async () => ({ success: false, error: "Tâches indisponibles" });
let checkUserQuota          = async () => ({ allowed: true, remaining: null });
let incrementUserQuota      = async () => {};
let dispatchSendEmail       = async () => ({ success: false, error: "Email indisponible" });
let sendWhatsAppSmart       = async () => ({ success: false, error: "WhatsApp indisponible" });
let runCodeSandbox          = async () => ({ success: false, error: "Sandbox non configuré" });
let extractEntity           = (msg) => String(msg || "").trim().slice(0, 80);
let evaluateMathSafe        = async () => ({ success: false, error: "Math indisponible" });

/**
 * Injecte les vraies implémentations depuis PARTIE 3.
 * Appelé automatiquement via Object.assign plus bas.
 */
function setSharedHelpers(helpers) {
  if (helpers.searchImagesWithFallback) searchImagesWithFallback = helpers.searchImagesWithFallback;
  if (helpers.searchWeb)               searchWeb               = helpers.searchWeb;
  if (helpers.searchNews)              searchNews              = helpers.searchNews;
  if (helpers.searchSportsScores)      searchSportsScores      = helpers.searchSportsScores;
  if (helpers.searchScience)           searchScience           = helpers.searchScience;
  if (helpers.searchSocial)            searchSocial            = helpers.searchSocial;
  if (helpers.getWeather)              getWeather              = helpers.getWeather;
  if (helpers.getCryptoPrice)          getCryptoPrice          = helpers.getCryptoPrice;
  if (helpers.getStockPrice)           getStockPrice           = helpers.getStockPrice;
  if (helpers.searchYouTube)           searchYouTube           = helpers.searchYouTube;
  if (helpers.createTask)              createTask              = helpers.createTask;
  if (helpers.listTasks)               listTasks               = helpers.listTasks;
  if (helpers.updateTaskStatus)        updateTaskStatus        = helpers.updateTaskStatus;
  if (helpers.deleteTask)              deleteTask              = helpers.deleteTask;
  if (helpers.checkUserQuota)          checkUserQuota          = helpers.checkUserQuota;
  if (helpers.incrementUserQuota)      incrementUserQuota      = helpers.incrementUserQuota;
  if (helpers.dispatchSendEmail)       dispatchSendEmail       = helpers.dispatchSendEmail;
  if (helpers.sendWhatsAppSmart)       sendWhatsAppSmart       = helpers.sendWhatsAppSmart;
  if (helpers.runCodeSandbox)          runCodeSandbox          = helpers.runCodeSandbox;
  if (helpers.extractEntity)           extractEntity           = helpers.extractEntity;
  if (helpers.evaluateMathSafe)        evaluateMathSafe        = helpers.evaluateMathSafe;
}

// ================================================================================
// §2.18 — EXPORTS PARTIE 2
// ================================================================================

Object.assign(module.exports, {
  // Providers
  LLM_PROVIDERS, MODEL_TIERS, geminiClient,

  // Circuit breaker
  CircuitBreaker, getCircuit, getAllCircuitStates, circuitRegistry,

  // Erreurs
  LLMErrorInterceptor, userFacingErrorMessage,

  // Appelants
  callProviderWithTools, callProviderRawWithTools,
  callOpenAICompatibleRaw, callGeminiRawWithTools,
  validateAndSanitizeOpenRouterModel,

  // Tool calling
  TOOL_SCHEMAS, TOOLS_BY_CONTEXT, SIDE_EFFECT_TOOLS,
  getToolSchemas, validateToolArgs, executeToolNative,
  runToolLoop, dedupeVideos,

  // STT / TTS
  transcribeAudioGroq, StreamingSTT,
  synthesizeKokoro, synthesizePiper, synthesizeSpeech,

  // VAD + pipeline
  SimpleVAD, SentenceChunker, VoicePipeline,

  // Mémoire courte
  buildRollingSummary, SHORT_MEMORY_MAX_EXCHANGES,

  // Mémoire longue
  rememberFact, extractFactsFromExchange, FACT_CATEGORIES,
  getAllFacts, deleteFact, clearAllFacts,

  // Recall sémantique
  embedText, recallMemory,

  // Helpers injectables
  setSharedHelpers
});

// ================================================================================
// ==================== FIN PARTIE 2/4 ===========================================
// ================================================================================
// ▶ Prochaine partie (3/4) : Search (Tavily/Serper/GDELT/HN), Media (Pexels/
//   Wikimedia/youtubei), Weather (Open-Meteo), Finance (CoinMarketCap/Yahoo),
//   News (Google News + score extraction), Ads (Ghost Ads + Adsterra + Luba Pro
//   + lien test safe), Code (Piston), Vision, i18n.
//   Tape "suite" quand tu es prêt.
// ================================================================================
// ================================================================================
// PARTIE 3/4 — DATA SERVICES, MEDIA, NEWS, FINANCE, ADS, CODE, VISION
// ================================================================================
// Sommaire :
//   §3.1   Search orchestrator (Tavily + Serper + DDG + GDELT + HN + Wikipedia)
//   §3.2   Media : Pexels + Wikimedia + youtubei.js (keyless)
//   §3.3   Weather : Open-Meteo (gratuit, keyless)
//   §3.4   Finance : CoinMarketCap (keyless) + Yahoo Finance
//   §3.5   News : Google News RSS + extraction de score
//   §3.6   Sports : Google News + synonymes équipes
//   §3.7   Code sandbox : Piston + Judge0
//   §3.8   Vision : analyse d'images (Groq Llama 4 Maverick + Gemini)
//   §3.9   Ads : Ghost Ads + Adsterra + Luba Pro + lien test SAFE
//   §3.10  Math evaluator (Worker isolé, mathjs sandbox)
//   §3.11  Entité + pré-routeur d'intention + i18n
//   §3.12  Tasks CRUD (UUID + Firestore-first)
//   §3.13  Quotas utilisateur
//   §3.14  Email dispatch (Gmail API / Resend / SMTP)
//   §3.15  WhatsApp helpers (chiffrement + envoi)
//   §3.16  Wire setSharedHelpers() — branche les stubs de Partie 2
//   §3.17  Exports PARTIE 3
// ================================================================================

// ================================================================================
// §3.1 — SEARCH ORCHESTRATOR
// ================================================================================

/**
 * Recherche via Tavily (si clé dispo).
 */
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
    return {
      results,
      answer: resp.data?.answer || null,
      provider: "tavily"
    };
  } catch (e) {
    logger.warn({ err: e.message }, "Tavily échec");
    return { results: [], provider: "tavily", error: e.message };
  }
}

/**
 * Recherche via Serper.dev (Google Search API, 2500 req/mois gratuites).
 */
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

/**
 * Recherche via DuckDuckGo (scraping libre, pas de clé).
 */
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
    logger.warn({ err: e.message }, "DDG échec");
    return { results: [], provider: "ddg", error: e.message };
  }
}

/**
 * GDELT (Global Database of Events, Language and Tone) — keyless.
 * Idéal pour actualités internationales / monitoring géopolitique.
 */
async function searchGdelt(query, { maxResults = 5, timespanDays = 7 } = {}) {
  try {
    const url = "https://api.gdeltproject.org/api/v2/doc/doc";
    const params = {
      query: `${query} sourcelang:french`,
      mode: "artlist",
      maxrecords: maxResults,
      format: "json",
      timespan: `${timespanDays}d`,
      sort: "datedesc"
    };
    const resp = await axios.get(url, { params, timeout: 9000 });
    const articles = (resp.data?.articles || []).slice(0, maxResults).map((a) => ({
      title: a.title,
      url: a.url,
      snippet: "",
      source: a.domain || "gdelt",
      publishedDate: a.seendate || null,
      sourceCountry: a.sourcecountry || null,
      language: a.language || null
    }));
    return { results: articles, provider: "gdelt" };
  } catch (e) {
    logger.warn({ err: e.message }, "GDELT échec");
    return { results: [], provider: "gdelt", error: e.message };
  }
}

/**
 * Hacker News (Algolia API, keyless).
 */
async function searchHackerNews(query, { maxResults = 5 } = {}) {
  try {
    const resp = await axios.get(
      "https://hn.algolia.com/api/v1/search",
      { params: { query, tags: "story", hitsPerPage: maxResults }, timeout: 6000 }
    );
    const results = (resp.data?.hits || []).map((h) => ({
      title: h.title,
      url: h.url || `https://news.ycombinator.com/item?id=${h.objectID}`,
      snippet: h.story_text?.slice(0, 300) || `${h.points || 0} points · ${h.num_comments || 0} commentaires`,
      source: "hackernews",
      points: h.points || 0
    }));
    return { results, provider: "hackernews" };
  } catch (e) {
    return { results: [], provider: "hackernews", error: e.message };
  }
}

/**
 * Résumé Wikipédia (FR) via REST API.
 */
async function searchWikipediaSummary(query, { lang = "fr" } = {}) {
  try {
    const resp = await axios.get(
      `https://${lang}.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      { timeout: 6000, headers: { "User-Agent": CONFIG.IMAGES.WIKIMEDIA_UA } }
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

/**
 * Orchestrateur de recherche web : cascade multi-providers avec deadline par source.
 * Sources : Wikipedia + Tavily + Serper + DDG + GDELT + HN.
 */
async function searchWeb(query) {
  if (!query || typeof query !== "string") {
    return { results: [], sourcesUsed: [], errors: [] };
  }

  const cleanQuery = extractEntity(query) || String(query).trim();

  const sources = [
    { key: "wikipedia", promise: searchWikipediaSummary(cleanQuery).then((w) => w.summary ? [{
      title: w.title, url: w.url, snippet: w.summary, type: "wiki"
    }] : []) },
    { key: "tavily", promise: searchTavily(cleanQuery).then((r) => r.results || []) },
    { key: "serper", promise: searchSerper(cleanQuery).then((r) => r.results || []) },
    { key: "duckduckgo", promise: searchDuckDuckGo(cleanQuery).then((r) => r.results || []) }
  ];

  const settled = await allSettledWithDeadline(
    sources.map((s) => s.promise),
    6000
  );

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

  // Déduplication par URL
  const seen = new Set();
  const unique = results.filter((r) => {
    const u = r.url || `${r.title}`;
    if (seen.has(u)) return false;
    seen.add(u);
    return true;
  });

  return { results: unique.slice(0, 12), sourcesUsed, errors };
}

// ================================================================================
// §3.2 — MEDIA : PEXELS + WIKIMEDIA + YOUTUBEI.JS
// ================================================================================

let youtubei = null;
try { youtubei = require("youtubei.js"); } catch { /* fallback API key */ }

/**
 * Wikimedia Commons — recherche d'images réelles (gsrnamespace=6).
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
      "&iiurlwidth=1280" +
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
      const imgUrl = info.thumburl || info.url;
      if (!imgUrl) return null;
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

    return { images };
  } catch (e) {
    logger.warn({ err: e.message, query }, "Wikimedia images échec");
    return { images: [] };
  }
}

/**
 * Miniature Wikipédia via REST summary.
 */
async function fetchWikipediaThumb(query) {
  try {
    const resp = await axios.get(
      `https://fr.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(query)}`,
      { timeout: CONFIG.TIMEOUTS.IMAGE_SOURCE_MS, headers: { "User-Agent": CONFIG.IMAGES.WIKIMEDIA_UA } }
    );
    const thumb = resp.data?.thumbnail?.source || resp.data?.originalimage?.source;
    if (!thumb) return null;
    return {
      url: thumb,
      title: resp.data.title || query,
      description: resp.data.extract ? resp.data.extract.slice(0, 200) : null,
      pageUrl: resp.data.content_urls?.desktop?.page || null,
      source: "wikipedia"
    };
  } catch { return null; }
}

/**
 * Pexels — photos libres de droits (200 req/heure gratuit).
 */
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
      url: p.src?.large || p.src?.original,
      title: p.alt || query,
      description: p.photographer ? `Photo par ${p.photographer}` : null,
      pageUrl: p.url,
      source: "pexels",
      width: p.width,
      height: p.height
    }));
    return { images };
  } catch (e) {
    logger.warn({ err: e.message }, "Pexels échec");
    return { images: [] };
  }
}

/**
 * DuckDuckGo Images (via searchImages, PAS search web).
 */
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
    logger.warn({ err: e.message, query }, "DDG images échec");
    return [];
  }
}

/**
 * Recherche d'images multi-source : Wikimedia + Wikipedia + Pexels + DDG.
 * Fusion + dédup + cache (uniquement si résultats > 0).
 */
const imageCache = new LRUCache({
  max: 500,
  ttl: CONFIG.IMAGES.CACHE_TTL_MS,
  updateAgeOnGet: false
});

async function searchImagesWithFallback(query, limit = CONFIG.LIMITS.IMAGE_SEARCH_LIMIT) {
  const cleanQuery = extractEntity(query) || String(query || "").trim();
  if (!cleanQuery) return { images: [] };

  const cacheKey = `img:${cleanQuery.toLowerCase().trim()}`;
  const cached = imageCache.get(cacheKey);
  if (cached) return cached;

  const settled = await allSettledWithDeadline([
    searchWikimediaImages(cleanQuery, limit),
    fetchWikipediaThumb(cleanQuery),
    searchPexelsImages(cleanQuery, Math.min(4, limit)),
    searchDuckDuckGoImages(cleanQuery, Math.min(4, limit))
  ], CONFIG.TIMEOUTS.IMAGE_SOURCE_MS);

  const [commonsR, wikiR, pexelsR, ddgR] = settled;

  const commonsImages = commonsR.status === "fulfilled" ? (commonsR.value?.images || []) : [];
  const wikiImage = (wikiR.status === "fulfilled" && wikiR.value) ? wikiR.value : null;
  const pexelsImages = pexelsR.status === "fulfilled" ? (pexelsR.value?.images || []) : [];
  const ddgImages = ddgR.status === "fulfilled" ? (ddgR.value || []) : [];

  const all = [...commonsImages, ...(wikiImage ? [wikiImage] : []), ...pexelsImages, ...ddgImages];

  const seen = new Set();
  const unique = all.filter((img) => {
    if (!img?.url || seen.has(img.url)) return false;
    seen.add(img.url);
    return true;
  }).slice(0, limit);

  const result = { images: unique, query: cleanQuery };

  if (unique.length > 0) {
    imageCache.set(cacheKey, result);
    logger.info({
      query: cleanQuery,
      count: unique.length,
      commons: commonsImages.length,
      wiki: wikiImage ? 1 : 0,
      pexels: pexelsImages.length,
      ddg: ddgImages.length
    }, "🖼️ Images trouvées");
  }

  return result;
}

// ================================================================================
// §3.2.bis — YOUTUBE (youtubei.js en priorité, API key fallback)
// ================================================================================

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
    logger.warn({ err: e.message }, "youtubei.js échec");
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
    logger.warn({ err: e.message }, "YouTube API key échec");
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
  } catch { return { videos: [] }; }
}

async function searchYouTube(query) {
  if (!query || typeof query !== "string") return { videos: [] };
  const cleanQuery = String(query).trim().slice(0, 200);

  // 1) youtubei.js (keyless, sans quota)
  if (youtubei) {
    const r = await searchYouTubeYoutubei(cleanQuery);
    if (r.videos.length > 0) return { ...r, provider: "youtubei" };
  }

  // 2) API key officielle
  if (process.env.YOUTUBE_API_KEY) {
    const r = await searchYouTubeApiKey(cleanQuery);
    if (r.videos.length > 0) return { ...r, provider: "youtube_api" };
  }

  // 3) Fallback DDG
  const r = await searchYouTubeFallbackDDG(cleanQuery);
  return { ...r, provider: "ddg" };
}

// ================================================================================
// §3.3 — WEATHER (Open-Meteo, keyless)
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

    const WMO_CODES = {
      0: "Ciel dégagé", 1: "Peu nuageux", 2: "Partiellement nuageux", 3: "Couvert",
      45: "Brouillard", 48: "Brouillard givrant",
      51: "Bruine légère", 53: "Bruine modérée", 55: "Bruine dense",
      61: "Pluie légère", 63: "Pluie modérée", 65: "Pluie forte",
      71: "Neige légère", 73: "Neige modérée", 75: "Neige forte",
      80: "Averses légères", 81: "Averses modérées", 82: "Averses violentes",
      95: "Orage", 96: "Orage avec grêle légère", 99: "Orage avec grêle forte"
    };

    return {
      location: `${place.name}, ${place.country || place.admin1 || ""}`.trim(),
      coordinates: { lat: place.latitude, lon: place.longitude },
      current: {
        temperature: c.temperature_2m,
        feelsLike: c.apparent_temperature,
        humidity: c.relative_humidity_2m,
        windSpeed: c.wind_speed_10m,
        condition: WMO_CODES[c.weather_code] || `Code ${c.weather_code}`,
        code: c.weather_code
      },
      forecast: (d.time || []).slice(0, 3).map((date, i) => ({
        date,
        tempMax: d.temperature_2m_max?.[i],
        tempMin: d.temperature_2m_min?.[i],
        condition: WMO_CODES[d.weather_code?.[i]] || null
      }))
    };
  } catch (e) {
    logger.warn({ err: e.message, location }, "Open-Meteo échec");
    return { error: e.message };
  }
}

// ================================================================================
// §3.4 — FINANCE (CoinMarketCap keyless + Yahoo Finance)
// ================================================================================

/**
 * CoinMarketCap keyless via l'endpoint public /v1/cryptocurrency/quotes/latest
 * (⚠️ nécessite normalement une clé). Fallback CoinGecko (100% keyless).
 */
async function getCryptoPrice(symbol) {
  if (!symbol) return { error: "Aucun symbole précisé" };
  const cleanSymbol = String(symbol).toUpperCase().trim();

  // 1) CoinGecko (100% keyless)
  try {
    const symbolToId = {
      BTC: "bitcoin", ETH: "ethereum", SOL: "solana", BNB: "binancecoin",
      XRP: "ripple", ADA: "cardano", DOGE: "dogecoin", USDT: "tether",
      USDC: "usd-coin", TRX: "tron", TON: "the-open-network", MATIC: "matic-network"
    };
    const id = symbolToId[cleanSymbol];
    if (id) {
      const resp = await axios.get(
        `https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd,eur&include_24hr_change=true&include_market_cap=true`,
        { timeout: 6000 }
      );
      const data = resp.data?.[id];
      if (data) {
        return {
          symbol: cleanSymbol,
          name: id.charAt(0).toUpperCase() + id.slice(1),
          priceUsd: data.usd,
          priceEur: data.eur,
          change24h: data.usd_24h_change,
          marketCap: data.usd_market_cap,
          source: "coingecko"
        };
      }
    }
  } catch (e) {
    logger.warn({ err: e.message }, "CoinGecko échec");
  }

  // 2) CoinMarketCap (si clé dispo)
  if (process.env.COINMARKETCAP_API_KEY) {
    try {
      const resp = await axios.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        {
          params: { symbol: cleanSymbol, convert: "USD" },
          headers: { "X-CMC_PRO_API_KEY": process.env.COINMARKETCAP_API_KEY },
          timeout: 8000
        }
      );
      const c = resp.data?.data?.[cleanSymbol];
      if (c) {
        return {
          symbol: cleanSymbol,
          name: c.name,
          priceUsd: c.quote?.USD?.price,
          change24h: c.quote?.USD?.percent_change_24h,
          marketCap: c.quote?.USD?.market_cap,
          source: "coinmarketcap"
        };
      }
    } catch (e) {
      logger.warn({ err: e.message }, "CoinMarketCap échec");
    }
  }

  return { error: `Prix introuvable pour ${cleanSymbol}` };
}

/**
 * Yahoo Finance — prix action via query1.finance.yahoo.com (keyless).
 */
async function getStockPrice(ticker) {
  if (!ticker) return { error: "Aucun ticker précisé" };
  const cleanTicker = String(ticker).toUpperCase().trim();

  try {
    const resp = await axios.get(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(cleanTicker)}`,
      {
        params: { interval: "1d", range: "5d" },
        headers: { "User-Agent": CONFIG.HTTP.USER_AGENT },
        timeout: 8000
      }
    );

    const result = resp.data?.chart?.result?.[0];
    if (!result) return { error: `Ticker ${cleanTicker} introuvable` };

    const meta = result.meta || {};
    const quote = result.indicators?.quote?.[0] || {};

    return {
      ticker: meta.symbol || cleanTicker,
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
    logger.warn({ err: e.message, ticker }, "Yahoo Finance échec");
    return { error: e.message };
  }
}

// ================================================================================
// §3.5 — GOOGLE NEWS + EXTRACTION DE SCORE
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
    logger.warn({ err: e.message, query }, "Google News échec");
    return [];
  }
}

async function searchNews(query) {
  if (!query) return { articles: [], sourcesUsed: [] };

  const [googleItems, gdeltResult] = await Promise.all([
    fetchGoogleNews(query, { limit: 8 }),
    searchGdelt(query, { maxResults: 5 }).catch(() => ({ results: [] }))
  ]);

  const articles = [
    ...googleItems.map((a) => ({
      title: a.title, link: a.link, pubDate: a.pubDate,
      description: a.description, source: a.source || "Google News",
      pubDateMs: a.pubDateMs
    })),
    ...(gdeltResult.results || []).map((a) => ({
      title: a.title, link: a.url, pubDate: a.publishedDate,
      description: a.snippet, source: a.source || "GDELT",
      pubDateMs: a.publishedDate ? Date.parse(a.publishedDate) || 0 : 0
    }))
  ];

  // Déduplication + tri par date
  const seen = new Set();
  const unique = articles.filter((a) => {
    const key = a.title?.toLowerCase().slice(0, 60);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  }).sort((a, b) => (b.pubDateMs || 0) - (a.pubDateMs || 0)).slice(0, 10);

  const sourcesUsed = [];
  if (googleItems.length > 0) sourcesUsed.push("googlenews");
  if (gdeltResult.results?.length > 0) sourcesUsed.push("gdelt");

  return { articles: unique, sourcesUsed };
}

/**
 * Extrait un score depuis un titre : "RDC 3-1 Zimbabwe", "victoire 2 à 1", etc.
 */
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
// §3.6 — SPORTS (Google News + synonymes)
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
  "manchester": ["Manchester United", "Manchester City"]
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
  for (const [key, aliases] of Object.entries(SPORT_SYNONYMS)) {
    if (normalized.includes(key)) candidates.push(...aliases);
  }
  const entity = extractEntity(team);
  if (entity && !candidates.includes(entity)) candidates.unshift(entity);

  const allArticles = [];
  const seenLinks = new Set();
  const seenTitles = new Set();

  for (const name of candidates.slice(0, 4)) {
    const articles = await fetchGoogleNews(`${name} match résultat score`, { limit: 6 });
    for (const a of articles) {
      const titleKey = a.title.toLowerCase().slice(0, 60);
      if (seenLinks.has(a.link) || seenTitles.has(titleKey)) continue;
      seenLinks.add(a.link);
      seenTitles.add(titleKey);
      allArticles.push(a);
    }
  }

  allArticles.sort((a, b) => (b.pubDateMs || 0) - (a.pubDateMs || 0));

  const events = [];
  for (const a of allArticles) {
    const scoreFromTitle = extractScoreFromText(a.title);
    const scoreFromDesc = !scoreFromTitle ? extractScoreFromText(a.description) : null;
    const score = scoreFromTitle || scoreFromDesc;

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
    const key = `${e.date}|${e.homeTeam}|${e.awayTeam}|${e.homeScore}|${e.awayScore}`;
    if (seenMatches.has(key)) return false;
    seenMatches.add(key);
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
// §3.7 — CODE SANDBOX (Piston + Judge0)
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
  if (!provider) return { success: false, error: "Sandbox d'exécution non configuré" };
  if (!SUPPORTED_SANDBOX_LANGS[language]) return { success: false, error: "Langage non supporté" };
  if (typeof code !== "string" || code.length === 0) return { success: false, error: "Code vide" };
  if (code.length > 20000) return { success: false, error: "Code trop long (max 20 000 caractères)" };

  const timeoutMs = 8000;

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
        { timeout: timeoutMs + 3000 }
      );
      const run = resp.data?.run || {};
      return {
        success: true,
        stdout: String(run.stdout || "").slice(0, 20000),
        stderr: String(run.stderr || "").slice(0, 20000),
        exitCode: typeof run.code === "number" ? run.code : null,
        provider: "piston", language
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Piston échec");
      return { success: false, error: "Échec d'exécution (Piston)" };
    }
  }

  if (provider === "judge0") {
    if (!CONFIG.SANDBOX.JUDGE0_URL) return { success: false, error: "JUDGE0_URL manquant" };
    try {
      const langId = SUPPORTED_SANDBOX_LANGS[language].judge0;
      const resp = await axios.post(
        `${CONFIG.SANDBOX.JUDGE0_URL.replace(/\/$/, "")}/submissions?base64_encoded=false&wait=true`,
        { language_id: langId, source_code: code, stdin },
        { timeout: timeoutMs + 5000, headers: { "Content-Type": "application/json" } }
      );
      const d = resp.data || {};
      return {
        success: true,
        stdout: String(d.stdout || "").slice(0, 20000),
        stderr: String(d.stderr || d.compile_output || "").slice(0, 20000),
        exitCode: d.status?.id ?? null,
        provider: "judge0", language
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Judge0 échec");
      return { success: false, error: "Échec d'exécution (Judge0)" };
    }
  }

  return { success: false, error: "Provider sandbox inconnu" };
}

// ================================================================================
// §3.8 — VISION (analyse d'images)
// ================================================================================

/**
 * Analyse d'images via Groq Llama 4 Maverick (gratuit), Gemini ou OpenRouter.
 */
async function analyzeImage({ imageBase64, mimetype = "image/jpeg", prompt = "Décris cette image en détail." }) {
  if (!imageBase64) return { success: false, error: "Image vide" };

  const messages = [{
    role: "user",
    content: [
      { type: "text", text: prompt },
      { type: "image_url", image_url: { url: `data:${mimetype};base64,${imageBase64}` } }
    ]
  }];

  // 1) Groq Llama 4 Maverick
  if (LLM_PROVIDERS.GROQ.keyPool.length > 0) {
    try {
      const result = await callProviderRawWithTools({
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL,
        messages,
        tools: null,
        jsonMode: false,
        timeout: 20000,
        maxTokens: 2000,
        temperature: 0.7,
        apiKey: LLM_PROVIDERS.GROQ.keyPool[0].apiKey
      });
      return {
        success: true,
        text: result.message?.content || "",
        provider: "groq",
        model: CONFIG.VISION.GROQ_MODEL
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Vision Groq échec → tentative Gemini");
    }
  }

  // 2) Gemini
  if (geminiClient) {
    try {
      const result = await callGeminiRawWithTools({
        model: CONFIG.VISION.GEMINI_MODEL,
        messages: [{ role: "user", content: prompt }],
        tools: null,
        jsonMode: false,
        timeout: 20000,
        maxTokens: 2000,
        temperature: 0.7,
        images: [{ base64: imageBase64, mimetype }]
      });
      return {
        success: true,
        text: result.message?.content || "",
        provider: "gemini",
        model: CONFIG.VISION.GEMINI_MODEL
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Vision Gemini échec");
    }
  }

  // 3) OpenRouter
  if (LLM_PROVIDERS.OPENROUTER.keyPool.length > 0) {
    try {
      const result = await callProviderRawWithTools({
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL,
        messages,
        tools: null,
        jsonMode: false,
        timeout: 25000,
        maxTokens: 2000,
        temperature: 0.7,
        apiKey: LLM_PROVIDERS.OPENROUTER.keyPool[0].apiKey
      });
      return {
        success: true,
        text: result.message?.content || "",
        provider: "openrouter",
        model: CONFIG.VISION.OPENROUTER_MODEL
      };
    } catch (e) {
      logger.warn({ err: e.message }, "Vision OpenRouter échec");
    }
  }

  return { success: false, error: "Aucun provider vision disponible" };
}

// ================================================================================
// §3.9 — ADS (Ghost Ads + Adsterra + Luba Pro + lien test SAFE)
// ================================================================================

/**
 * Réseau publicitaire interne Luba Pro (fallback permanent, 100% maison).
 *
 * ⚠️ Lien de test SAFE (pas de contenu adulte, HTTPS, statique) :
 *    https://placehold.co/728x90/1a73e8/ffffff/png?text=Publicite+Test+Luba&font=roboto
 *
 * Le clic redirige vers le domaine Luba (aucun tracking tiers).
 */
const LUBA_PRO_ADS = Object.freeze({
  test_safe: {
    id: "test_safe_728x90",
    title: "Publicité test Luba",
    description: "Placeholder safe — pas de contenu sensible.",
    imageUrl: "https://placehold.co/728x90/1a73e8/ffffff/png?text=Publicite+Test+Luba&font=roboto",
    clickUrl: "https://luba.web.app",
    width: 728,
    height: 90,
    network: "luba_pro",
    isTest: true,
    safeContent: true
  },
  self_promo: {
    id: "self_promo_728x90",
    title: "Luba Pro — Passez premium",
    description: "Débloquez Ngandu (raisonnement) et Luba Live (voix < 800ms).",
    imageUrl: "https://placehold.co/728x90/1a1a1a/ffffff/png?text=Luba+Pro+-+Passez+Premium&font=roboto",
    clickUrl: "https://luba.web.app/pro",
    width: 728,
    height: 90,
    network: "luba_pro",
    isTest: false,
    safeContent: true
  },
  banner_adaptive: {
    id: "banner_adaptive",
    title: "Découvrir Luba Live",
    description: "Conversation vocale temps réel.",
    imageUrl: "https://placehold.co/970x250/4285f4/ffffff/png?text=Luba+Live+-+Voix+temps+reel&font=roboto",
    clickUrl: "https://luba.web.app/live",
    width: 970,
    height: 250,
    network: "luba_pro",
    isTest: false,
    safeContent: true
  }
});

/**
 * Ghost Ads (monétisation ouverte, 75% revshare).
 * Récupère une pub via leur API (nécessite GHOST_ADS_API_KEY).
 */
async function fetchGhostAds({ slot = "sidebar", userId = null } = {}) {
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

    // Filtre SAFE : refuse tout contenu adulte
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
  } catch (e) {
    logger.warn({ err: e.message }, "Ghost Ads échec");
    return { success: false, reason: e.message };
  }
}

/**
 * Adsterra (bannière HTML/JS, insertion côté client).
 * Retourne juste les métadonnées — l'injection se fait dans le front.
 */
async function fetchAdsterra({ slot = "banner_728x90" } = {}) {
  const zoneId = process.env.ADSTERRA_ZONE_ID;
  if (!zoneId) return { success: false, reason: "no_zone" };

  // ⚠️ Adsterra nécessite un script côté client. On retourne juste la config.
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
 * Orchestrateur Ads : Ghost → Adsterra → Luba Pro (fallback permanent).
 * ⚠️ Le fallback Luba Pro contient le lien test SAFE.
 */
async function getAd({ slot = "sidebar", userId = null, allowTest = true } = {}) {
  // 1) Ghost Ads
  const ghost = await fetchGhostAds({ slot, userId });
  if (ghost.success) return ghost.ad;

  // 2) Adsterra
  const adsterra = await fetchAdsterra({ slot });
  if (adsterra.success) return adsterra.ad;

  // 3) Fallback Luba Pro (self-promo ou test safe)
  const fallbackKey = allowTest ? "test_safe" : "self_promo";
  return { ...LUBA_PRO_ADS[fallbackKey], slot };
}

/**
 * Retourne tous les slots disponibles (pour le bootstrap front).
 */
function getAllAdSlots() {
  return {
    test_safe: LUBA_PRO_ADS.test_safe,
    self_promo: LUBA_PRO_ADS.self_promo,
    banner_adaptive: LUBA_PRO_ADS.banner_adaptive
  };
}

// ================================================================================
// §3.10 — MATH EVALUATOR (Worker isolé)
// ================================================================================

const MATH_WORKER_SOURCE = `
  const { parentPort, workerData } = require('worker_threads');
  const math = require('mathjs');
  const safeMath = math.create({ number: 'number', precision: 64, matrix: 'Matrix', predictable: true });
  safeMath.import({
    import: function () { throw new Error('import interdit'); },
    createUnit: function () { throw new Error('createUnit interdit'); },
    evaluate: function () { throw new Error('evaluate interdit'); },
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
      if (msg.ok) {
        resolve({ success: true, expression: expr, result: msg.result, formatted: msg.result });
      } else {
        resolve({ success: false, expression: expr, error: msg.error });
      }
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
// §3.11 — ENTITÉ + PRÉ-ROUTEUR + i18n
// ================================================================================

function extractEntity(message) {
  if (!message || typeof message !== "string") return "";
  let m = message.trim().replace(/[?!.,;:]+$/g, "");

  m = m.replace(
    /^(qui est|c'?est qui|qui était|montre-moi|montre moi|cherche|trouve-moi|trouve moi|parle-moi de|parle moi de|donne-moi|donne moi|photo de|image de|clip de|vidéo de|video de|chanson de|à quoi ressemble|a quoi ressemble|quelle est|quel est|où se trouve|ou se trouve)\s+/i,
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
  MATHS: ["calcule", "calculer", "resous", "equation", "integrale", "derivee", "factorielle", "matrice", "limite", "theoreme", "algebre"],
  ACTUALITE: ["actualite", "actualites", "news", "journal", "derniere", "dernieres", "presse"],
  SPORT: ["score", "match", "football", "basket", "tennis", "nba", "ligue", "championnat", "classement", "resultat", "leopards", "leopard"],
  CODE: ["code", "coder", "javascript", "python", "java", "typescript", "react", "angular", "vuejs", "nodejs", "sql", "algorithme", "bug", "debug"],
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

  if (containsWholeWords(text, INTENT_KEYWORDS.TASK))      return { intent: "TASK", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.VIDEO))     return { intent: "VIDEO", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.MATHS))     return { intent: "MATHS", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.SPORT))     return { intent: "SPORT", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.FINANCE))   return { intent: "FINANCE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.METEO))     return { intent: "METEO", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.ACTUALITE)) return { intent: "ACTUALITE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.CODE))      return { intent: "CODE", entity };
  if (containsWholeWords(text, INTENT_KEYWORDS.PERSONNE))  return { intent: "PERSONNE", entity };

  return { intent: "GENERAL", entity };
}

// ================================================================================
// §3.12 — TASKS CRUD (UUID + Firestore-first)
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
      matchColumn: "id",
      idempotencyKey: `task:${id}`
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
    const tasks = await dbAll(q, params);
    return { success: true, tasks };
  } catch (e) {
    return { success: false, tasks: [], error: e.message };
  }
}

async function updateTaskStatus(userId, taskId, status) {
  if (!["pending", "done"].includes(status)) return { success: false, error: "Statut invalide" };

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
      matchColumn: "id", matchValue: taskId,
      idempotencyKey: `task_status:${taskId}:${status}:${Date.now()}`
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
      matchColumn: "id", matchValue: taskId,
      idempotencyKey: `task_delete:${taskId}`
    }).catch(() => {});
  }
  return { success: true };
}

// ================================================================================
// §3.13 — QUOTAS UTILISATEUR
// ================================================================================

async function checkUserQuota(userId, action, userRole = "FREE") {
  try {
    const today = todayKeyMs();
    const limits = USER_QUOTAS[userRole] || USER_QUOTAS.FREE;
    const quota = await dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]);

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
    logger.error({ err: e.message }, "Erreur checkUserQuota");
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
  } catch (e) {
    logger.error({ err: e.message }, "Erreur incrementUserQuota");
  }
}

// ================================================================================
// §3.14 — EMAIL DISPATCH (Gmail API / Resend / SMTP)
// ================================================================================

let emailTransporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
  try {
    const tlsRejectUnauthorized = process.env.SMTP_TLS_REJECT_UNAUTHORIZED !== "false";
    emailTransporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: parseInt(process.env.SMTP_PORT || "587", 10),
      secure: process.env.SMTP_PORT === "465",
      auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
      tls: { rejectUnauthorized: tlsRejectUnauthorized },
      pool: true, maxConnections: 3, maxMessages: 50
    });
    logger.info("✅ SMTP configuré");
  } catch (e) {
    logger.warn({ err: e.message }, "SMTP init échouée");
  }
}

async function verifyGmailScope(accessToken) {
  try {
    const resp = await axios.get("https://www.googleapis.com/oauth2/v1/tokeninfo", {
      params: { access_token: accessToken }, timeout: 8000
    });
    const scopes = String(resp.data?.scope || "").split(" ");
    return scopes.includes("https://www.googleapis.com/auth/gmail.send")
      || scopes.includes("https://mail.google.com/");
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
// §3.15 — WHATSAPP HELPERS
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
        user_id: userId, encrypted_data: encrypted, auth_tag: authTag,
        iv: iv.toString("hex"), updated_at: new Date().toISOString()
      },
      matchColumn: "user_id",
      idempotencyKey: `whatsapp_creds:${userId}`
    });
    return r.success;
  } catch (e) {
    logger.error({ err: e.message }, "Erreur save WhatsApp creds");
    return false;
  }
}

async function loadWhatsAppCredentials(userId) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase.from("whatsapp_credentials")
      .select("encrypted_data, auth_tag, iv").eq("user_id", userId).maybeSingle();
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
    table: "whatsapp_credentials", op: "delete", payload: {},
    matchColumn: "user_id", matchValue: userId,
    idempotencyKey: `whatsapp_creds_delete:${userId}`
  }).catch(() => {});
}

async function sendWhatsAppSmart(userId, phoneNumber, message) {
  const quota = await checkUserQuota(userId, "whatsapp");
  if (!quota.allowed) throw new Error(quota.message || "Limite WhatsApp atteinte");
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
// §3.16 — WIRE SHARED HELPERS (remplace les stubs de Partie 2)
// ================================================================================

setSharedHelpers({
  searchImagesWithFallback,
  searchWeb,
  searchNews,
  searchSportsScores,
  searchScience: async () => ({ papers: [] }),          // Sera complété en v16.1 si besoin
  searchSocial:  async () => ({ posts: [] }),           // Idem
  getWeather,
  getCryptoPrice,
  getStockPrice,
  searchYouTube,
  createTask,
  listTasks,
  updateTaskStatus,
  deleteTask,
  checkUserQuota,
  incrementUserQuota,
  dispatchSendEmail,
  sendWhatsAppSmart,
  runCodeSandbox,
  extractEntity,
  evaluateMathSafe
});

logger.info("✅ Helpers Partie 3 injectés dans Partie 2");

// ================================================================================
// §3.17 — EXPORTS PARTIE 3
// ================================================================================

Object.assign(module.exports, {
  // Search
  searchTavily, searchSerper, searchDuckDuckGo, searchGdelt, searchHackerNews,
  searchWikipediaSummary, searchWeb,

  // Media
  searchWikimediaImages, fetchWikipediaThumb, searchPexelsImages,
  searchDuckDuckGoImages, searchImagesWithFallback,
  extractYouTubeVideoId, searchYouTube, searchYouTubeYoutubei,

  // Weather
  getWeather,

  // Finance
  getCryptoPrice, getStockPrice,

  // News + Sports
  fetchGoogleNews, searchNews, extractScoreFromText,
  searchSportsViaGoogleNews, searchSportsScores,

  // Code sandbox
  runCodeSandbox, SUPPORTED_SANDBOX_LANGS,

  // Vision
  analyzeImage,

  // Ads
  LUBA_PRO_ADS, fetchGhostAds, fetchAdsterra, getAd, getAllAdSlots,

  // Math
  evaluateMathSafe, detectMathExpressions,

  // Entity + routage
  extractEntity, normalizeForMatch, containsWholeWords, preRouteIntent, INTENT_KEYWORDS,

  // Tasks
  createTask, listTasks, updateTaskStatus, deleteTask,

  // Quotas
  checkUserQuota, incrementUserQuota,

  // Email
  verifyGmailScope, sendEmailViaGmail, sendEmailViaResend, sendEmailViaSMTP, dispatchSendEmail,

  // WhatsApp
  getWhatsAppCryptoKey, saveWhatsAppCredentials, loadWhatsAppCredentials,
  deleteWhatsAppCredentials, sendWhatsAppSmart, toPlainWhatsAppText,

  // Registre injectable
  setWhatsAppManager: (mgr) => { whatsappManager = mgr; }
});

// ================================================================================
// ==================== FIN PARTIE 3/4 ===========================================
// ================================================================================
// ▶ Prochaine partie (4/4) : Routes Express (chat, tools, tasks, memory, ads,
//   voice, whatsapp, intent), Luba Live WebSocket (protocole binaire custom),
//   métriques, housekeeping, bootstrap, Dockerfile, CI/CD.
//   Tape "suite" quand tu es prêt.
// ================================================================================
// ================================================================================
// PARTIE 4/4 — ROUTES, LUBA LIVE WS, PRODUCTION, DÉPLOIEMENT
// ================================================================================
// Sommaire :
//   §4.1   SSE Writer + streaming helpers
//   §4.2   LUBA_SYSTEM_PROMPT (anti-hallucination)
//   §4.3   Sessions + ActiveIntent + FullHistory
//   §4.4   Mémoire long terme (résumé + Firestore)
//   §4.5   Suggestions (extract + generate)
//   §4.6   Enrichissement contexte (images/vidéos/scores parallèles)
//   §4.7   handleChat (cœur du backend)
//   §4.8   handleActiveIntent (annulation + expiration)
//   §4.9   WhatsApp Baileys manager
//   §4.10  Reminder scheduler
//   §4.11  Express app + middlewares globaux
//   §4.12  Routes API complètes
//   §4.13  Luba Live WebSocket (protocole binaire custom)
//   §4.14  Bootstrap + graceful shutdown
//   §4.15  Docker / docker-compose / Nginx / CI-CD
//   §4.16  Exports finaux
// ================================================================================

// ================================================================================
// §4.1 — SSE WRITER + STREAMING HELPERS
// ================================================================================

/**
 * Writer SSE typé.
 * Contrat d'événements :
 *   status       { stage, message?, iteration?, name? }
 *   reasoning    { text }
 *   code         { language, filename?, code?, stdout?, stderr?, done, execution? }
 *   images       { images: [{url,title,source,pageUrl}] }
 *   videos       { videos: [{videoId,title,url,embedUrl,thumbnail,channel}] }
 *   token        { text }
 *   suggestions  { suggestions: ["...","...","..."] }
 *   sources      { sources: [{name,url,logo}] }
 *   error        { reply, code? }
 *   done         { conversationId, isNewConversation, providerUsed, modelTier,
 *                  degraded, visionEnabled, intent, contextLength }
 */
class SSEWriter {
  constructor(res) {
    this.res = res;
    this.closed = false;
    this.res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    this.res.setHeader("Cache-Control", "no-cache, no-transform");
    this.res.setHeader("Connection", "keep-alive");
    this.res.setHeader("X-Accel-Buffering", "no");
    this.res.flushHeaders?.();
    this.res.write(":ok\n\n");
    this.res.on("close", () => { this.closed = true; });
  }

  send(event, data) {
    if (this.closed) return false;
    try {
      const payload = typeof data === "string" ? data : safeJsonStringify(data ?? {});
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
  error(payload)            { this.send("error", payload); }
  done(payload)             { this.send("done", payload); }

  end() {
    if (this.closed) return;
    try { this.res.end(); } catch {}
    this.closed = true;
  }
}

async function streamTextAsTokens(sse, text, { paceMs = 4, chunkSize = 28 } = {}) {
  const chunks = safeChunkText(text, chunkSize);
  for (const chunk of chunks) {
    if (sse.closed) return;
    sse.token(chunk);
    if (paceMs > 0) await sleep(paceMs);
  }
}

// ================================================================================
// §4.2 — LUBA_SYSTEM_PROMPT (anti-hallucination)
// ================================================================================

const LUBA_SYSTEM_PROMPT = [
  "Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, startup à Kinshasa, fondée en 2026.",
  "",
  "IDENTITÉ :",
  "- Tu t'appelles Luba (ou Luba.ia).",
  "- Ton ton est chaleureux, intelligent, proactif.",
  "- Tu es un vrai agent IA (façon Jarvis), pas un chatbot passif.",
  "- Tu supportes le français, l'anglais, le swahili et le lingala. Réponds dans la langue de l'utilisateur.",
  "",
  "MÉMOIRE :",
  "- Souviens-toi du contexte de la conversation.",
  "- Ne redemande JAMAIS une info déjà donnée.",
  "- Si une [MÉMOIRE LONG TERME] est fournie, utilise-la naturellement.",
  "- Tu peux appeler remember_fact pour mémoriser un fait durable, recall_memory pour retrouver une info oubliée.",
  "",
  "DONNÉES — RÈGLE ABSOLUE (violation = faute grave) :",
  "- N'invente JAMAIS un score, une actualité, une météo, un prix, une vidéo, un nom de joueur, une date.",
  "- Si un outil retourne un résultat, UTILISE-LE TEL QUEL. Ne modifie aucun chiffre.",
  "- Si un outil ÉCHOUE ou retourne 'aucun résultat', DIS-LE CLAIREMENT à l'utilisateur.",
  "  Exemple : « Je n'ai pas trouvé le score exact pour ce match. Veux-tu que je cherche autrement ? »",
  "- Mieux vaut dire « je ne sais pas » que d'inventer une information.",
  "",
  "IMAGES :",
  "- Dès que tu décris une personnalité, un lieu, un objet ou une équipe précise, appelle search_images.",
  "- Les images sont affichées automatiquement — ne mentionne JAMAIS les URLs dans ta réponse.",
  "",
  "MODULE JARVIS :",
  "- Chercher un clip → search_youtube",
  "- Créer un rappel → create_task (avec due_at ISO si une date est mentionnée)",
  "- Voir les tâches → list_tasks",
  "- Météo → get_weather",
  "- Crypto → get_crypto_price ; Action → get_stock_price",
  "",
  "CODE :",
  "- Fournis du code de production complet, jamais tronqué.",
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
  '  <!--SUGGESTIONS:["Question 1 ?","Question 2 ?","Question 3 ?"]-->',
  "- Sinon, n'ajoute rien."
].join("\n");

// ================================================================================
// §4.3 — SESSIONS + ACTIVE INTENT + FULL HISTORY
// ================================================================================

const ACTIVE_INTENT_TTL_MS = 5 * 60 * 1000;

async function getSession(conversationId, userId, firebaseUid = null) {
  const local = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);

  if (local) {
    if (local.user_id !== userId && local.firebase_uid !== userId) {
      throw makeError("CONVERSATION_OWNERSHIP");
    }
    await dbRun("UPDATE sessions SET updated_at = ? WHERE session_id = ?", [Date.now(), conversationId]);

    // Sync Firestore + Supabase (best effort)
    if (firestoreDb) {
      fsSet("sessions", conversationId, {
        session_id: conversationId, user_id: userId,
        firebase_uid: firebaseUid || userId,
        updated_at: Date.now()
      }, { merge: true }).catch(() => {});
    }
    if (supabase) {
      supabaseWriteSafe({
        table: "sessions", op: "upsert",
        payload: {
          session_id: conversationId, user_id: userId,
          firebase_uid: firebaseUid || userId,
          updated_at: new Date().toISOString()
        },
        matchColumn: "session_id",
        idempotencyKey: `session:${conversationId}`
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
      session_id: conversationId, user_id: userId,
      firebase_uid: firebaseUid || userId,
      created_at: Date.now(), updated_at: Date.now()
    }).catch(() => {});
  }
  if (supabase) {
    await supabaseWriteSafe({
      table: "sessions", op: "upsert",
      payload: {
        session_id: conversationId, user_id: userId,
        firebase_uid: firebaseUid || userId,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      },
      matchColumn: "session_id",
      idempotencyKey: `session:${conversationId}`
    }).catch(() => {});
  }

  return { session_id: conversationId, user_id: userId, firebase_uid: firebaseUid || userId };
}

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

async function getFullHistory(conversationId, userId = null, limit = CONFIG.LIMITS.MAX_CONTEXT_MESSAGES) {
  // Priorité : Firestore (le plus frais si Admin SDK actif)
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
    } catch (e) {
      logger.debug({ err: e.message }, "getFullHistory Firestore échec");
    }
  }

  // Fallback Supabase
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
        const filtered = userId ? data.filter((m) => !m.user_id || m.user_id === userId) : data;
        return filtered.reverse().map((m) => ({ role: m.role, content: m.content }));
      }
    } catch (e) {
      logger.debug({ err: e.message }, "getFullHistory Supabase échec");
    }
  }

  // Fallback SQLite
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
    logger.error({ err: e.message }, "getFullHistory SQLite échec");
    return [];
  }
}

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
        .from("sessions").select("user_id, firebase_uid")
        .eq("session_id", conversationId).maybeSingle();
      if (!error && data) {
        if (data.user_id && data.user_id !== userId && data.firebase_uid !== userId) {
          throw makeError("CONVERSATION_OWNERSHIP");
        }
        return true;
      }
    } catch (e) {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
      logger.debug({ err: e.message }, "assertConversationOwnership Supabase échec");
    }
  }

  return true;
}

/**
 * Sauvegarde un message (triple-write : Firestore + Supabase + SQLite).
 * Le texte PUR est sauvegardé (pas les blocs Illustrations/Sources).
 */
async function saveMessageWithUser(conversationId, role, content, userId = null, firebaseUid = null, metadata = {}) {
  const now = Date.now();
  const msgId = generateMsgId();

  // 1) Firestore (principal)
  if (firestoreDb && userId) {
    fsSet("messages", msgId, {
      session_id: conversationId,
      firebase_uid: firebaseUid || userId,
      user_id: userId,
      role, content, metadata,
      created_at: now
    }).catch(() => {});
  }

  // 2) Supabase (backup)
  if (supabase && userId) {
    const idemKey = `msg:${conversationId}:${role}:${now}:${sha256(String(content).slice(0, 64)).slice(0, 12)}`;
    supabaseWriteSafe({
      table: "messages", op: "insert",
      payload: {
        session_id: conversationId,
        firebase_uid: firebaseUid || userId,
        user_id: userId,
        role, content,
        metadata: metadata || {},
        created_at: new Date(now).toISOString()
      },
      idempotencyKey: idemKey
    }).catch(() => {});
  }

  // 3) SQLite (local, toujours)
  try {
    await dbRun(
      `INSERT INTO messages (session_id, user_id, role, content, metadata, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [conversationId, userId, role, content, safeJsonStringify(metadata || {}), now]
    );
    await dbRun(`UPDATE sessions SET updated_at = ? WHERE session_id = ?`, [now, conversationId]);
  } catch (e) {
    logger.error({ err: e.message }, "saveMessageWithUser SQLite échec");
  }
}

// ================================================================================
// §4.4 — MÉMOIRE LONG TERME (résumé)
// ================================================================================

const USER_MEMORY_UPDATE_EVERY_N_MESSAGES = parseInt(
  process.env.USER_MEMORY_UPDATE_EVERY_N_MESSAGES || "6", 10
);

async function getUserMemory(userId) {
  // Firestore
  if (firestoreDb) {
    const doc = await fsGet("user_memory", userId);
    if (doc?.summary) return doc.summary;
  }
  // Supabase
  if (supabase) {
    try {
      const { data, error } = await supabase.from("user_memory")
        .select("summary").eq("user_id", userId).maybeSingle();
      if (!error && data) return data.summary || "";
    } catch {}
  }
  // SQLite
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
      user_id: userId, summary, messages_since_update: messagesSinceUpdate,
      updated_at: Date.now()
    }, { merge: true }).catch(() => {});
  }
  if (supabase) {
    supabaseWriteSafe({
      table: "user_memory", op: "upsert",
      payload: {
        user_id: userId, summary, messages_since_update: messagesSinceUpdate,
        updated_at: new Date().toISOString()
      },
      matchColumn: "user_id",
      idempotencyKey: `memory:${userId}`
    }).catch(() => {});
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

async function runMemorySummaryImpl({ userId, lastUserMessage, lastAssistantReply }) {
  try {
    const previousSummary = await getUserMemory(userId);
    const provider = MODEL_TIERS.v100.providers[0];

    const result = await callProviderWithTools({
      providerConfig: provider,
      messages: [
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
    await saveUserMemory(userId, previousSummary, 0);
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur runMemorySummary");
  }
}

async function maybeUpdateUserMemoryAsync(userId, lastUserMessage, lastAssistantReply) {
  try {
    const count = await incrementUserMemoryCounter(userId);
    if (count < USER_MEMORY_UPDATE_EVERY_N_MESSAGES) {
      // Extrait quand même des faits durables (fire-and-forget léger)
      if (count % 3 === 0) {
        extractFactsFromExchange(userId, lastUserMessage, lastAssistantReply).catch(() => {});
      }
      return;
    }
    runMemorySummaryImpl({ userId, lastUserMessage, lastAssistantReply }).catch(() => {});
  } catch (e) {
    logger.error({ err: e.message, userId }, "Erreur déclenchement mémoire");
  }
}

// ================================================================================
// §4.5 — SUGGESTIONS
// ================================================================================

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

async function generateSuggestions(userMessage, replyText) {
  try {
    const provider = MODEL_TIERS.v100.providers[0];
    const result = await callProviderWithTools({
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
// §4.6 — ENRICHISSEMENT CONTEXTE
// ================================================================================

function toolCacheKey(name, args) {
  return `${name}::${sha256(safeJsonStringify(args || {})).slice(0, 16)}`;
}

async function enrichContextWithIntent(intent, userMessage, entity, toolCache) {
  const enrichment = {
    contextData: "",
    sourceKeys: [],
    media: { images: [], videos: [] },
    extra: {}
  };

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
          if (result.success) {
            enrichment.contextData += `\n[Calcul exact] ${expr} = ${result.formatted}\n`;
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
          enrichment.contextData += `\n[RÉSULTATS SPORTIFS RÉCENTS (source : Google News, vérifié)]\n`;
          scoresRes.result.events.forEach((e) => {
            enrichment.contextData +=
              `- ${e.match}\n` +
              `  Date : ${e.date || "?"}\n` +
              `  Source : ${e.source}\n` +
              `  Article : "${e.title}"\n` +
              `  Lien : ${e.url}\n\n`;
          });
          enrichment.contextData += `\n⚠️ RÈGLE : utilise UNIQUEMENT ces scores. Ne les modifie pas.\n`;
          scoresRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        } else {
          const articles = scoresRes.result.allArticles || [];
          if (articles.length > 0) {
            enrichment.contextData += `\n[SPORT — ARTICLES TROUVÉS mais AUCUN SCORE extrait]\n`;
            articles.slice(0, 5).forEach((a) => {
              enrichment.contextData += `- "${a.title}" (${a.source}, ${a.pubDate})\n  ${a.link}\n`;
            });
            enrichment.contextData += `\n⚠️ Pas de score exact. Dis-le clairement. N'INVENTE PAS.\n`;
          } else {
            enrichment.contextData += `\n[SPORT — AUCUNE INFORMATION TROUVÉE pour "${entity || userMessage}"]\n`;
          }
          scoresRes.sourceKeys.forEach((k) => enrichment.sourceKeys.push(k));
        }

        if (imagesRes.result.images?.length) {
          enrichment.media.images = imagesRes.result.images.slice(0, 3);
          enrichment.contextData += `\n[IMAGES TROUVÉES — affichées automatiquement, ne mentionne pas les URLs]\n`;
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
          enrichment.contextData += `- Source : ${c.source}\n`;
          enrichment.sourceKeys.push(c.source === "coingecko" ? "coingecko" : "coinmarketcap");
        } else if (!stockRes.result.error) {
          const s = stockRes.result;
          enrichment.contextData += `\n[PRIX ACTION RÉEL — ${s.ticker}]\n`;
          enrichment.contextData += `- Prix : ${s.price} ${s.currency}\n`;
          enrichment.contextData += `- Variation : ${s.changePercent?.toFixed(2)}%\n`;
          enrichment.contextData += `- Volume : ${s.volume}\n`;
          enrichment.sourceKeys.push("yahoo");
        } else {
          enrichment.contextData += `\n[FINANCE — Aucun prix trouvé pour "${sym}"]\n⚠️ Dis-le clairement.\n`;
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
          enrichment.contextData += `\n[IMAGES TROUVÉES — affichées automatiquement, ne mentionne pas les URLs]\n`;
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
// §4.7 — HANDLECHAT (cœur du backend)
// ================================================================================

async function handleChat({
  conversationId, userId, firebaseUid, message,
  googleAccessToken = null, channel = "web", modelTier = "v100",
  images = null, sse = null
}) {
  const startedAt = Date.now();
  const useV250 = modelTier === "v250";

  if (sse) sse.status("starting");

  // 1) Lecture parallèle
  const [_, history, longTermMemory, activeIntent] = await Promise.all([
    getSession(conversationId, userId, firebaseUid).catch((e) => {
      if (e.code === "CONVERSATION_OWNERSHIP") throw e;
      logger.warn({ err: e.message }, "getSession échec");
      return null;
    }),
    getFullHistory(conversationId, userId, CONFIG.LIMITS.MAX_CONTEXT_MESSAGES).catch(() => []),
    getUserMemory(userId).catch(() => ""),
    getActiveIntent(conversationId).catch(() => null)
  ]);

  // 2) Intention active (WhatsApp, Email)
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

  // 3) Pré-routeur intention + entité
  const { intent, entity } = preRouteIntent(message);
  if (sse) sse.status("thinking", { intent });

  // 4) Save user message
  saveMessageWithUser(conversationId, "user", message, userId, firebaseUid).catch(() => {});

  // 5) Enrichissement
  const toolCache = new Map();
  const enrichment = await enrichContextWithIntent(intent, message, entity, toolCache);

  if (sse && enrichment.media.images.length > 0) sse.images(enrichment.media.images);
  if (sse && enrichment.media.videos.length > 0) sse.videos(enrichment.media.videos);

  // 6) Construction des messages LLM
  const historyWithoutCurrent = history.length > 0 && history[history.length - 1].role === "user"
    ? history.slice(0, -1)
    : history;
  const contextHistory = historyWithoutCurrent.slice(-CONFIG.LIMITS.MAX_CONTEXT_MESSAGES);

  let systemContent = LUBA_SYSTEM_PROMPT;
  if (longTermMemory) {
    systemContent += `\n\n[MÉMOIRE LONG TERME SUR CET UTILISATEUR]\n${longTermMemory}`;
  }
  if (intent && intent !== "GENERAL") {
    systemContent += `\n\n[DOMAINE DÉTECTÉ : ${intent}]`;
  }

  const userContentWithEnrichment = enrichment.contextData
    ? `${message}\n\n[CONTEXTE ENRICHI — NE PAS CITER CES SOURCES]\n${enrichment.contextData}`
    : message;

  const messages = [
    { role: "system", content: systemContent },
    ...contextHistory,
    { role: "user", content: userContentWithEnrichment }
  ];

  // 7) Exécution LLM
  const usedSources = new Set(enrichment.sourceKeys);
  let collectedImages = enrichment.media.images.map((i) => i.url);
  let collectedVideos = [...enrichment.media.videos];
  let providerUsed = "unknown";
  let degraded = false;
  let visionEnabled = Boolean(images && images.length > 0);
  let finalText = "";
  let toolCallTrace = [];

  const providerChain = (images && images.length > 0)
    ? MODEL_TIERS.vision.providers
    : (useV250 ? MODEL_TIERS.v250.reasoning.providers : MODEL_TIERS.v100.providers);

  const executeFn = async ({ toolName, args }) => {
    const key = toolCacheKey(toolName, args);
    if (toolCache.has(key)) {
      const cached = toolCache.get(key);
      return { result: cached.result, sourceKeys: cached.sourceKeys };
    }
    const out = await executeToolNative(toolName, args, { userId, googleAccessToken, sessionId: conversationId });
    toolCache.set(key, { result: out.result, sourceKeys: out.sourceKeys });
    return out;
  };

  try {
    let loopResult = { success: false };

    for (const provider of providerChain) {
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
        break;
      }
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
      if (collectedImages.length === 0 && enrichment.media.images.length > 0) {
        collectedImages.push(...enrichment.media.images.map((i) => i.url));
      }
    }

    // 8) Phase code v250 (si applicable)
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
        jsonMode: false,
        _meta: { sessionId: conversationId, userId, tier: "v250" }
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

  // 9) Post-traitement
  finalText = normalizeMath(finalText || "");
  if (!finalText) finalText = "Je n'ai pas pu générer une réponse pour le moment.";

  const sug = extractSuggestions(finalText);
  finalText = sug.text;
  let suggestions = sug.suggestions;
  if (suggestions.length === 0 && !degraded) {
    generateSuggestions(message, finalText.slice(0, 500))
      .then((s) => { if (s.length) suggestions = s; })
      .catch(() => {});
  }

  // 10) Dédoublonnage médias
  collectedImages = [...new Set(collectedImages.filter(Boolean))];
  collectedVideos = dedupeVideos(collectedVideos);

  // 11) Save assistant message
  saveMessageWithUser(conversationId, "assistant", finalText, userId, firebaseUid, {
    providerUsed, intent, degraded
  }).catch(() => {});

  // 12) Mémoire (fire-and-forget)
  maybeUpdateUserMemoryAsync(userId, message, finalText).catch(() => {});

  // 13) Construction réponse client
  let replyForClient = finalText;

  const shouldInlineImages = !sse && enrichment.media.images.length === 0 && collectedImages.length > 0;
  if (shouldInlineImages) {
    const md = collectedImages.map((u, i) => `![Image ${i + 1}](${u})`).join("\n\n");
    replyForClient += `\n\n---\n\n**Illustrations :**\n\n${md}`;
  }

  if (usedSources.size > 0) {
    const SOURCE_LABELS = {
      wikipedia: { name: "Wikipédia", url: "https://fr.wikipedia.org" },
      wikimediacommons: { name: "Wikimedia Commons", url: "https://commons.wikimedia.org" },
      googlenews: { name: "Google News", url: "https://news.google.com" },
      gdelt: { name: "GDELT", url: "https://www.gdeltproject.org" },
      tavily: { name: "Tavily", url: "https://tavily.com" },
      serper: { name: "Google Search", url: "https://google.com" },
      duckduckgo: { name: "DuckDuckGo", url: "https://duckduckgo.com" },
      hackernews: { name: "Hacker News", url: "https://news.ycombinator.com" },
      arxiv: { name: "arXiv", url: "https://arxiv.org" },
      reddit: { name: "Reddit", url: "https://reddit.com" },
      openmeteo: { name: "Open-Meteo", url: "https://open-meteo.com" },
      coingecko: { name: "CoinGecko", url: "https://www.coingecko.com" },
      coinmarketcap: { name: "CoinMarketCap", url: "https://coinmarketcap.com" },
      yahoo: { name: "Yahoo Finance", url: "https://finance.yahoo.com" },
      youtube: { name: "YouTube", url: "https://youtube.com" },
      pexels: { name: "Pexels", url: "https://pexels.com" }
    };
    const srcLines = [...usedSources]
      .map((k) => SOURCE_LABELS[k])
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
    sources: [...usedSources].map((k) => SOURCE_LABELS[k]).filter(Boolean),
    intent,
    userId,
    contextLength: history.length,
    toolCallTrace,
    elapsedMs: Date.now() - startedAt
  };

  // 14) SSE : stream final
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
      contextLength: history.length,
      elapsedMs: result.elapsedMs
    });
    sse.end();
  }

  return { ...result, conversationId, isNewConversation: false };
}

// ================================================================================
// §4.8 — HANDLEACTIVEINTENT (annulation + expiration)
// ================================================================================

const CANCEL_WORDS = new Set([
  "annule", "annuler", "stop", "abandonne", "laisse tomber", "oublie", "cancel"
]);

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
          googleAccessToken, recipient: data.recipient,
          subject: data.subject, body, userId
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
// §4.9 — WHATSAPP BAILEYS MANAGER
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

    const savedCredentials = await loadWhatsAppCredentials(userId);
    if (savedCredentials) {
      try { fs.writeFileSync(path.join(authDir, "creds.json"), safeJsonStringify(savedCredentials)); }
      catch (e) { logger.error({ err: e.message }, "Erreur restauration WhatsApp"); }
    }

    let makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion;
    try {
      const baileys = require("@whiskeysockets/baileys");
      makeWASocket = baileys.default;
      useMultiFileAuthState = baileys.useMultiFileAuthState;
      DisconnectReason = baileys.DisconnectReason;
      fetchLatestBaileysVersion = baileys.fetchLatestBaileysVersion;
    } catch (e) {
      logger.error({ err: e.message }, "Baileys non installé");
      throw new Error("Baileys non installé");
    }

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    let version;
    try { version = (await fetchLatestBaileysVersion()).version; } catch {}

    const sock = makeWASocket({
      version, auth: state,
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      browser: ["Luba.ia", "Chrome", "16.0.0"]
    });

    const sessionData = { sock, qrCode: null, ready: false };
    this.sessions.set(userId, sessionData);

    sock.ev.on("creds.update", async () => {
      try {
        await saveCreds();
        const credsPath = path.join(authDir, "creds.json");
        if (fs.existsSync(credsPath)) {
          const creds = safeJsonParse(fs.readFileSync(credsPath, "utf8"), null);
          if (creds) await saveWhatsAppCredentials(userId, creds);
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
        const isLoggedOut = statusCode === DisconnectReason?.loggedOut;
        this.sessions.delete(userId);

        if (isLoggedOut) {
          try {
            const dir = path.join(CONFIG.PATHS.SESSIONS, userId);
            if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
            await deleteWhatsAppCredentials(userId);
          } catch {}
          logger.info({ userId }, "WhatsApp déconnecté (loggedOut)");
        } else {
          setTimeout(() => {
            this.initClient(userId).catch((e) => logger.error({ err: e.message }, "Reconnexion WhatsApp"));
          }, CONFIG.WHATSAPP.RETRY_DELAY_MS);
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
          if (!isWhatsAppAllowed(phoneNumber)) {
            logger.info({ phoneNumber }, "WhatsApp : expéditeur hors liste blanche");
            continue;
          }

          const waUserId = await this._resolveUser(phoneNumber);
          if (!waUserId) continue;

          const quota = await checkUserQuota(waUserId, "whatsapp");
          if (!quota.allowed) {
            await sock.sendMessage(remoteJid, { text: "Limite de messages atteinte pour aujourd'hui." });
            continue;
          }
          await incrementUserQuota(waUserId, "whatsapp");

          const conversationId = `whatsapp_${phoneNumber}`;
          const result = await handleChat({
            conversationId, userId: waUserId, firebaseUid: null,
            message: String(text).slice(0, CONFIG.LIMITS.MAX_MESSAGE_LENGTH),
            channel: "whatsapp", modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(remoteJid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (e) { logger.error({ err: e.message }, "Erreur message WhatsApp"); }
      }
    });

    return { connected: false, qrCode: null };
  }

  async _resolveUser(phoneNumber) {
    const placeholderUid = `wa_${phoneNumber}`;
    const existing = await dbGet(
      "SELECT id FROM users WHERE firebase_uid = ? OR id = ?",
      [placeholderUid, placeholderUid]
    );
    if (existing) return existing.id;
    try {
      await dbRun(
        `INSERT INTO users (id, firebase_uid, display_name, role, created_at, updated_at, whatsapp_connected, whatsapp_session_id)
         VALUES (?, ?, ?, 'FREE', ?, ?, 1, ?)`,
        [placeholderUid, placeholderUid, `WhatsApp ${phoneNumber}`, Date.now(), Date.now(), phoneNumber]
      );
      return placeholderUid;
    } catch (e) {
      logger.error({ err: e.message }, "resolveWhatsAppUser échec");
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
    const cleanNumber = String(to).replace(/[^\d]/g, "");
    if (!cleanNumber) {
      const e = new Error("Numéro destinataire invalide");
      e.code = "INVALID_RECIPIENT";
      throw e;
    }
    await session.sock.sendMessage(`${cleanNumber}@s.whatsapp.net`, { text: message });
    return { success: true, to: cleanNumber };
  }

  getQRCode(userId) {
    return this.sessions.get(userId)?.qrCode || null;
  }

  async destroyAll() {
    for (const [userId, session] of this.sessions) {
      try { session.sock.end(undefined); }
      catch (e) { logger.error({ err: e.message }, `Erreur fermeture WhatsApp (${userId})`); }
    }
  }
}

const baileysManager = new BaileysManager();

// Injection dans la Partie 3
if (typeof module.exports.setWhatsAppManager === "function") {
  module.exports.setWhatsAppManager(baileysManager);
}

// ================================================================================
// §4.10 — REMINDER SCHEDULER
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
    if (due.length === 0) return;

    for (const task of due) {
      try {
        const result = await dbRun(
          `UPDATE user_tasks SET notified_at = ? WHERE id = ? AND (notified_at IS NULL OR notified_at < due_at)`,
          [Date.now(), task.id]
        );
        if (result.changes === 0) continue;
        logger.info({ taskId: task.id, userId: task.user_id, title: task.title }, "🔔 Rappel échu");

        if (process.env.REMINDER_WHATSAPP_ENABLED === "true") {
          const user = await dbGet("SELECT whatsapp_session_id FROM users WHERE id = ?", [task.user_id]);
          if (user?.whatsapp_session_id) {
            sendWhatsAppSmart(
              task.user_id,
              user.whatsapp_session_id,
              `🔔 Rappel : ${task.title}${task.notes ? "\n" + task.notes : ""}`
            ).catch(() => {});
          }
        }
      } catch (e) { logger.error({ err: e.message, taskId: task.id }, "Erreur rappel"); }
    }
  } catch (e) { logger.error({ err: e.message }, "reminderTick erreur"); }
}

// ================================================================================
// §4.11 — EXPRESS APP + MIDDLEWARES
// ================================================================================

const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");

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
        "wss://*.firebaseio.com",
        "https://*.supabase.co"
      ],
      fontSrc: ["'self'", "https://fonts.gstatic.com", "https://cdnjs.cloudflare.com"],
      objectSrc: ["'none'"],
      frameSrc: [
        "https://*.firebaseapp.com", "https://*.web.app",
        "https://www.youtube.com", "https://youtube.com"
      ],
      workerSrc: ["'self'", "blob:"],
      mediaSrc: ["'self'", "blob:", "data:"],
      upgradeInsecureRequests: CONFIG.ENV === "production" ? [] : null
    }
  },
  hsts: CONFIG.ENV === "production"
    ? { maxAge: 63072000, includeSubDomains: true, preload: true }
    : false,
  referrerPolicy: { policy: "strict-origin-when-cross-origin" },
  crossOriginOpenerPolicy: { policy: "same-origin-allow-popups" },
  noSniff: true,
  frameguard: { action: "deny" }
}));

// ---------- HTTPS forcé ----------
app.use((req, res, next) => {
  if (CONFIG.ENV === "production"
      && req.headers["x-forwarded-proto"]
      && req.headers["x-forwarded-proto"] !== "https") {
    return res.redirect(301, "https://" + req.headers.host + req.originalUrl);
  }
  next();
});

// ---------- Body parsers ----------
const jsonBodySmall = express.json({ limit: "1mb" });
const jsonBodyLarge = express.json({ limit: "20mb" });
const urlEncoded   = express.urlencoded({ extended: true, limit: "1mb" });

app.use((req, res, next) => {
  const ct = req.headers["content-type"] || "";
  if (ct.startsWith("multipart/form-data")) return next();
  if (req.path === "/api/import/conversations") return jsonBodyLarge(req, res, next);
  return jsonBodySmall(req, res, next);
});
app.use(urlEncoded);

// ---------- Request ID + métriques ----------
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  res.setHeader("X-Request-Id", requestId);

  res.on("finish", () => {
    const duration = Date.now() - start;
    logger.info({
      requestId, method: req.method, path: req.path,
      status: res.statusCode, duration
    }, "requête");

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
    else cb(new Error(`Type non supporté. Autorisés: ${CONFIG.IMAGES.ALLOWED_TYPES.join(", ")}`));
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
    logger.info("✅ Rate-limit Redis activé");
  }
} catch {}

function makeLimiter({ windowMs, max, code, message, keyGenerator }) {
  return rateLimit({
    windowMs, max,
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
const strictLimiter = makeLimiter({ windowMs: 60 * 60 * 1000, max: 50,  code: "RATE_LIMIT_STRICT", message: "Limite de requêtes atteinte." });
const chatLimiter   = makeLimiter({ windowMs: 60 * 1000,       max: 30,  code: "RATE_LIMIT_CHAT",   message: "Trop de messages. Patientez un instant." });
const authLimiter   = makeLimiter({ windowMs: 15 * 60 * 1000, max: 60,  code: "RATE_LIMIT_AUTH",   message: "Trop de tentatives d'authentification." });
const toolLimiter   = makeLimiter({ windowMs: 60 * 1000,       max: 40,  code: "RATE_LIMIT_TOOL",   message: "Trop d'appels d'outils." });

// ---------- Auth middleware ----------
const tokenCache = new LRUCache({
  max: 5000,
  ttl: CONFIG.AUTH.TOKEN_CACHE_TTL_MS,
  updateAgeOnGet: false
});

function cacheGetToken(token) { return tokenCache.get(sha256(token)) || null; }
function cacheSetToken(token, user) { tokenCache.set(sha256(token), user); }

async function verifyFirebaseToken(token, { checkRevoked = CONFIG.AUTH.CHECK_REVOKED } = {}) {
  const cached = cacheGetToken(token);
  if (cached) return cached;

  if (firebaseApp() && firebaseAdmin) {
    try {
      const decoded = await firebaseAdmin.auth(firebaseApp()).verifyIdToken(token, checkRevoked);
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

async function isIPBlocked(ipAddress) {
  const row = await dbGet(
    `SELECT 1 FROM blocked_ips WHERE ip_address = ? AND blocked_until > ? LIMIT 1`,
    [ipAddress, Date.now()]
  );
  return Boolean(row);
}

async function recordLoginAttempt(ipAddress, userId, success, errorMessage = null, { countFailure = true } = {}) {
  if (!success && !countFailure) return;
  try {
    await dbRun(
      `INSERT INTO login_attempts (user_id, ip_address, success, error_message, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      [userId, ipAddress, success ? 1 : 0, errorMessage ? String(errorMessage).slice(0, 500) : null, Date.now()]
    );
  } catch {}
}

async function checkLoginAttempts(ipAddress) {
  const cutoff = Date.now() - CONFIG.AUTH.LOGIN_BLOCK_MS;
  const row = await dbGet(
    `SELECT COUNT(*) AS count FROM login_attempts WHERE ip_address = ? AND success = 0 AND created_at > ?`,
    [ipAddress, cutoff]
  );

  if ((row?.count || 0) >= CONFIG.AUTH.MAX_LOGIN_ATTEMPTS) {
    const existing = await dbGet(`SELECT strike_count FROM blocked_ips WHERE ip_address = ?`, [ipAddress]);
    const strikeCount = (existing?.strike_count || 0) + 1;
    const escalated = Math.min(CONFIG.AUTH.LOGIN_BLOCK_MS * Math.pow(2, strikeCount - 1), 24 * 60 * 60 * 1000);

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
    } catch {}
  });
}

async function fastUpsertUser(uid, user, userRole) {
  const hasAdmin = Boolean(firebaseApp() && firebaseAdmin);
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
      const newRole = hasAdmin ? userRole : (existing.role || "FREE");
      await dbRun(
        `UPDATE users SET last_seen_at = ?, email = COALESCE(?, email),
          display_name = COALESCE(?, display_name), role = ?, email_verified = ?, firebase_uid = ?
         WHERE id = ?`,
        [Date.now(), user.email, user.displayName, newRole, user.emailVerified ? 1 : 0, uid, uid]
      );
    }

    // Dual-write Firestore + Supabase
    if (firestoreDb) {
      fsSet("users", uid, {
        id: uid, firebase_uid: uid, email: user.email || null,
        display_name: user.displayName || null, role: userRole,
        last_seen_at: Date.now()
      }, { merge: true }).catch(() => {});
    }
    if (supabase) {
      supabaseWriteSafe({
        table: "users", op: "upsert",
        payload: {
          id: uid, firebase_uid: uid,
          email: user.email || null,
          display_name: user.displayName || null,
          last_seen_at: new Date().toISOString()
        },
        matchColumn: "firebase_uid",
        idempotencyKey: `user:${uid}`
      }).catch(() => {});
    }
  } catch (e) { logger.error({ err: e.message }, "Erreur fastUpsertUser"); }
}

function authenticateUser(req, res, next) {
  (async () => {
    const ip = req.ip;
    const ua = req.headers["user-agent"];

    if (await isIPBlocked(ip)) {
      return res.status(403).json({ success: false, error: true, reply: "Accès refusé.", code: "IP_BLOCKED" });
    }

    const authHeader = req.headers.authorization || req.headers.Authorization || "";
    const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    if (!bearerToken) {
      return res.status(401).json({ success: false, error: true, reply: "Authentification requise.", code: "MISSING_TOKEN" });
    }

    let user;
    try {
      user = await verifyFirebaseToken(bearerToken);
    } catch (error) {
      const isExpired = error?.code === "auth/id-token-expired"
        || error?.errorInfo?.code === "auth/id-token-expired";
      const isRevoked = error?.code === "auth/id-token-revoked";

      const countAsFailure = !isExpired && !isRevoked;
      await recordLoginAttempt(ip, null, false, error.message, { countFailure: countAsFailure });

      if (countAsFailure) {
        const check = await checkLoginAttempts(ip);
        if (check.blocked) {
          return res.status(403).json({ success: false, error: true, reply: check.message, code: "IP_BLOCKED" });
        }
      }
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

    setImmediate(() => {
      Promise.allSettled([
        recordLoginAttempt(ip, user.uid, true),
        logSecurityEvent(user.uid, "LOGIN_SUCCESS", { email: user.email }, ip, ua),
        detectAndLogNewDeviceAsync(user.uid, ip, ua),
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

function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!firebaseApp() && allowedRoles.includes("ADMIN")) {
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
// §4.12 — ROUTES API
// ================================================================================

// ---------- Info publique ----------
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
  const publicReport = {
    success: dbOk,
    error: !dbOk,
    reply: dbOk ? `Serveur ${CONFIG.AGENT_NAME} en bonne santé` : "Serveur en maintenance",
    data: {
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      version: CONFIG.VERSION,
      database: dbOk ? "ok" : "erreur",
      features: {
        vision: FEATURES.gemini || FEATURES.groq,
        quotas: true, sse: true,
        live: Boolean(process.env.LUBA_LIVE_ENABLED !== "false")
      }
    }
  };
  if (!full) return res.json(publicReport);

  return res.json({
    ...publicReport,
    data: {
      ...publicReport.data,
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
      firestore: Boolean(firestoreDb),
      supabase: Boolean(supabase),
      redis: Boolean(redisClient()),
      firebaseAuth: firebaseApp() ? "admin_sdk" : "api_rest",
      providers: {
        groq: LLM_PROVIDERS.GROQ.keyPool.length,
        openrouter: LLM_PROVIDERS.OPENROUTER.keyPool.length,
        cerebras: LLM_PROVIDERS.CEREBRAS.keyPool.length,
        gemini: geminiClient ? "actif" : "inactif"
      },
      circuits: getAllCircuitStates(),
      features: FEATURES
    }
  });
});

// ---------- /ready (Kubernetes / Docker health) ----------
app.get("/ready", async (req, res) => {
  try {
    await dbGet("SELECT 1");
    return res.json({ ready: true });
  } catch {
    return res.status(503).json({ ready: false });
  }
});

// ---------- /api/metrics (Prometheus) ----------
app.get("/api/metrics", async (req, res) => {
  const m = metrics();
  if (!m) return res.status(503).json({ error: "Metrics indisponibles" });
  // Optionnel : protéger par token
  const token = process.env.METRICS_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  try {
    res.setHeader("Content-Type", m.register.contentType);
    res.end(await m.register.metrics());
  } catch {
    res.status(500).end();
  }
});

// ---------- /api/user/whoami ----------
app.get("/api/user/whoami", authLimiter, authenticateUser, (req, res) => {
  res.status(200).json({
    success: true, error: false,
    userId: req.userId,
    role: req.userRole
  });
});

// ---------- /api/session/bootstrap ----------
app.get("/api/session/bootstrap", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const today = todayKeyMs();

    let conversations = [];

    // Firestore en priorité
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
      } catch (e) { logger.warn({ err: e.message }, "bootstrap Firestore échec"); }
    }

    // Fallback SQLite
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

    const [quotaRow, tasksResult, userRow, longTermMemory, facts] = await Promise.all([
      dbGet(`SELECT * FROM user_quotas WHERE user_id = ? AND date = ?`, [userId, today]).catch(() => null),
      listTasks(userId, { status: "pending" }).catch(() => ({ tasks: [] })),
      dbGet(`SELECT whatsapp_connected, display_name FROM users WHERE id = ?`, [userId]).catch(() => null),
      getUserMemory(userId).catch(() => ""),
      getAllFacts(userId).catch(() => ({ grouped: {}, total: 0 }))
    ]);

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
      memoryFacts: facts.grouped || {},
      quotas: quotaRow || { messages_count: 0, images_count: 0, whatsapp_count: 0, emails_count: 0 },
      limits: USER_QUOTAS[req.userRole] || USER_QUOTAS.FREE,
      whatsappConnected: Boolean(userRow?.whatsapp_connected),
      hasMemory: Boolean(longTermMemory),
      ads: getAllAdSlots()
    });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur bootstrap");
    return res.status(500).json({ success: false, error: true, code: "BOOTSTRAP_ERROR" });
  }
});

// ---------- /api/chat (SSE opt-in + JSON) ----------
function wantsStreaming(req) {
  const accept = String(req.headers.accept || "").toLowerCase();
  const bodyStream = req.body && req.body.stream === true;
  const queryStream = req.query.stream === "true";
  return accept.includes("text/event-stream") || bodyStream || queryStream;
}

function sseShortError(res, message, code) {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
  res.write(`event: error\ndata: ${safeJsonStringify({ reply: message, code })}\n\n`);
  res.write(`event: done\ndata: ${safeJsonStringify({ error: true })}\n\n`);
  res.end();
}

app.post(
  "/api/chat",
  chatLimiter,
  authenticateUser,
  upload.array("images", CONFIG.LIMITS.MAX_IMAGES_PER_REQUEST),
  async (req, res) => {
    const isStream = wantsStreaming(req) && !(req.files && req.files.length > 0);

    try {
      const rawMessage = req.body?.message;
      let conversationId = req.body?.conversationId || req.body?.conversation_id;
      const modelTier = req.body?.modelTier === "v250" ? "v250" : "v100";

      if (!rawMessage || typeof rawMessage !== "string") {
        if (isStream) return sseShortError(res, "Le message est obligatoire.", "MISSING_MESSAGE");
        return res.status(400).json({ success: false, error: true, reply: "Le paramètre 'message' est obligatoire.", code: "MISSING_MESSAGE" });
      }

      const sanitizedMessage = sanitizeForLLM(rawMessage);
      if (!sanitizedMessage) {
        if (isStream) return sseShortError(res, "Message vide.", "INVALID_MESSAGE");
        return res.status(400).json({ success: false, error: true, reply: "Message vide après nettoyage.", code: "INVALID_MESSAGE" });
      }

      // Détection prompt injection
      const injection = detectPromptInjection(sanitizedMessage);
      if (injection.detected) {
        logger.warn({ userId: req.userId, pattern: injection.pattern }, "🚫 Tentative de prompt injection");
        await logSecurityEvent(req.userId, "PROMPT_INJECTION_BLOCKED", { pattern: injection.pattern }, req.ip, req.headers["user-agent"]);
        if (isStream) return sseShortError(res, "Requête bloquée par sécurité.", "PROMPT_INJECTION");
        return res.status(400).json({ success: false, error: true, reply: "Requête bloquée par sécurité.", code: "PROMPT_INJECTION" });
      }

      // Modération
      const moderation = await moderateWithGroq(sanitizedMessage);
      if (!moderation.safe) {
        logger.warn({ userId: req.userId, category: moderation.category }, "🚫 Contenu bloqué");
        await logSecurityEvent(req.userId, "CONTENT_BLOCKED", { category: moderation.category }, req.ip, req.headers["user-agent"]);
        if (isStream) return sseShortError(res, "Contenu non autorisé.", "CONTENT_BLOCKED");
        return res.status(400).json({ success: false, error: true, reply: "Contenu non autorisé.", code: "CONTENT_BLOCKED" });
      }

      // Vérification signature HMAC si activée
      if (CONFIG.HMAC.ENABLED) {
        const hmacCheck = verifyHmacSignature(req);
        if (!hmacCheck.valid && !hmacCheck.skipped) {
          return res.status(401).json({ success: false, error: true, reply: "Signature invalide.", code: "INVALID_SIGNATURE" });
        }
      }

      // Conversation ID
      if (conversationId && !/^[a-zA-Z0-9_-]{6,80}$/.test(conversationId)) {
        if (isStream) return sseShortError(res, "Identifiant de conversation invalide.", "INVALID_CONVERSATION_ID");
        return res.status(400).json({ success: false, error: true, reply: "Identifiant invalide.", code: "INVALID_CONVERSATION_ID" });
      }
      if (!conversationId) conversationId = generateConversationId();
      const isNewConversation = !req.body?.conversationId && !req.body?.conversation_id;

      try { await assertConversationOwnership(conversationId, req.userId); }
      catch (e) {
        if (isStream) return sseShortError(res, e.message, "CONVERSATION_OWNERSHIP");
        return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" });
      }

      // Quota
      const quota = await checkUserQuota(req.userId, "message", req.userRole);
      if (!quota.allowed) {
        if (isStream) return sseShortError(res, quota.message, "QUOTA_EXCEEDED");
        return res.status(429).json({ success: false, error: true, reply: quota.message, code: "QUOTA_EXCEEDED" });
      }
      await incrementUserQuota(req.userId, "message");

      // Images
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

      // -------- SSE --------
      if (isStream) {
        const sse = new SSEWriter(res);
        req.on("close", () => { sse.closed = true; });

        sse.status("accepted", { conversationId, isNewConversation });

        try {
          await handleChat({
            conversationId, userId: req.userId, firebaseUid: req.firebaseUid,
            message: sanitizedMessage, googleAccessToken,
            channel: "web-sse", modelTier, images, sse
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

      // -------- JSON --------
      const result = await handleChat({
        conversationId, userId: req.userId, firebaseUid: req.firebaseUid,
        message: sanitizedMessage, googleAccessToken,
        channel: "web", modelTier, images, sse: null
      });

      return res.status(200).json({ ...result, conversationId, isNewConversation });
    } catch (e) {
      logger.error({ err: e.message, stack: e.stack }, "Erreur API /api/chat");
      if (res.headersSent) return;
      return res.status(500).json({ success: false, error: true, reply: "Une erreur est survenue.", code: "CHAT_ERROR" });
    }
  }
);

// ---------- /api/conversations ----------
app.get("/api/conversations", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? ORDER BY updated_at DESC LIMIT 50`,
      [req.userId]
    );
    const enriched = await Promise.all(rows.map(async (conv) => {
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
    return res.json({ success: true, error: false, conversations: enriched });
  } catch {
    return res.status(500).json({ success: false, error: true, conversations: [] });
  }
});

// ---------- /api/conversation/:id/messages ----------
app.get("/api/conversation/:conversationId/messages", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { conversationId } = req.params;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    const wantsFull = req.query.full === "true";
    const reqLimit = parseInt(req.query.limit, 10);
    const limit = wantsFull ? 500 : (Number.isFinite(reqLimit) && reqLimit > 0 ? Math.min(reqLimit, 200) : CONFIG.LIMITS.MAX_HISTORY_LENGTH);

    const messages = await getFullHistory(conversationId, req.userId, limit);
    return res.json({ success: true, error: false, conversationId, messages, count: messages.length });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "HISTORY_FETCH_ERROR" });
  }
});

// ---------- /api/user/stats ----------
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
        userId: req.userId
      }
    });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "STATS_ERROR" });
  }
});

// ---------- /api/tools ----------
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

    const googleAccessToken = req.headers["x-google-access-token"] || null;
    const { result, sourceKeys } = await executeToolNative(toolName, params, {
      userId: req.userId, googleAccessToken, agentMode: false
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
    logger.error({ err: e.message }, "Erreur /api/tools");
    return res.status(500).json({ success: false, error: true, code: "TOOL_EXECUTION_ERROR" });
  }
});

// ---------- /api/tasks ----------
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
      title, notes,
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

// ---------- /api/memory/facts (long terme) ----------
app.get("/api/memory/facts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const facts = await getAllFacts(req.userId);
    return res.json({ success: true, error: false, ...facts });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "MEMORY_FETCH_ERROR" });
  }
});

app.delete("/api/memory/facts/:factId", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const result = await deleteFact(req.userId, parseInt(req.params.factId, 10));
    return res.json({ success: result.success, error: !result.success });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

app.delete("/api/memory/facts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    await clearAllFacts(req.userId);
    await dbRun("DELETE FROM user_memory WHERE user_id = ?", [req.userId]);
    return res.json({ success: true, error: false, message: "Mémoire long terme effacée." });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/memory/recall ----------
app.post("/api/memory/recall", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { query } = req.body || {};
    if (!query) return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    const result = await recallMemory(req.userId, query, 5);
    return res.json({ success: true, error: false, ...result });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/ads ----------
app.get("/api/ads", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const slot = req.query.slot || "sidebar";
    const allowTest = req.query.allowTest !== "false";
    const ad = await getAd({ slot, userId: req.userId, allowTest });
    return res.json({ success: true, error: false, ad });
  } catch (e) {
    return res.status(500).json({ success: false, error: true, code: "ADS_ERROR" });
  }
});

app.get("/api/ads/slots", (req, res) => {
  return res.json({ success: true, error: false, slots: getAllAdSlots() });
});

// ---------- /api/youtube/search ----------
app.get("/api/youtube/search", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const q = req.query.q;
    if (!q || typeof q !== "string") return res.status(400).json({ success: false, error: true, code: "MISSING_QUERY" });
    const entity = extractEntity(q) || q;
    const result = await searchYouTube(entity);
    return res.json({ success: true, error: false, videos: result.videos || [], provider: result.provider });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "YOUTUBE_SEARCH_ERROR" });
  }
});

// ---------- /api/voice/transcribe ----------
app.post("/api/voice/transcribe", apiLimiter, authenticateUser, uploadAudio.single("audio"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: true, reply: "Aucun fichier audio.", code: "MISSING_AUDIO" });

    const quota = await checkUserQuota(req.userId, "message", req.userRole);
    if (!quota.allowed) return res.status(429).json({ success: false, error: true, reply: quota.message, code: "QUOTA_EXCEEDED" });

    const result = await transcribeAudioGroq(req.file.buffer, req.file.originalname, req.file.mimetype);
    if (!result.success) return res.status(502).json({ success: false, error: true, reply: "Transcription indisponible.", code: "TRANSCRIPTION_FAILED" });

    return res.json({ success: true, error: false, text: sanitizeForLLM(result.text, 5000), provider: result.provider });
  } catch (e) {
    logger.error({ err: e.message }, "Erreur transcription");
    return res.status(500).json({ success: false, error: true, code: "VOICE_TRANSCRIBE_ERROR" });
  }
});

// ---------- /api/voice/tts ----------
app.post("/api/voice/tts", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { text, voice } = req.body || {};
    if (!text) return res.status(400).json({ success: false, error: true, code: "MISSING_TEXT" });
    const result = await synthesizeSpeech(text, { voice: voice || "af_bella" });
    if (!result.success) return res.status(503).json({ success: false, error: true, reply: result.error });
    res.setHeader("Content-Type", result.format === "wav" ? "audio/wav" : "audio/mpeg");
    return res.send(result.audio);
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/whatsapp/connect ----------
app.post("/api/whatsapp/connect", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const result = await baileysManager.initClient(req.userId);
    if (result.connected) {
      return res.json({ success: true, error: false, message: "WhatsApp déjà connecté.", data: { qrCode: null } });
    }

    let qr = null;
    const start = Date.now();
    while (!qr && Date.now() - start < CONFIG.WHATSAPP.QR_TIMEOUT_MS) {
      await sleep(500);
      qr = baileysManager.getQRCode(req.userId);
    }
    if (qr) return res.json({ success: true, error: false, message: "Connexion initiée", data: { qrCode: qr } });
    return res.status(408).json({ success: false, error: true, message: "Délai dépassé.", code: "QR_TIMEOUT" });
  } catch (e) {
    logger.error({ err: e.message }, "WhatsApp connect échec");
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_CONNECT_ERROR" });
  }
});

// ---------- /api/whatsapp/send ----------
app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const { to, message } = req.body || {};
    if (!to || !message) return res.status(400).json({ success: false, error: true, code: "MISSING_PARAMS" });

    const quota = await checkUserQuota(req.userId, "whatsapp", req.userRole);
    if (!quota.allowed) return res.status(429).json({ success: false, error: true, reply: quota.message, code: "WHATSAPP_QUOTA_EXCEEDED" });

    const cleanTo = String(to).replace(/[^\d]/g, "");
    if (!PHONE_REGEX.test(cleanTo)) return res.status(400).json({ success: false, error: true, code: "INVALID_PHONE" });

    const result = await baileysManager.sendMessage(req.userId, cleanTo, sanitizeStrict(message, 2000));
    await incrementUserQuota(req.userId, "whatsapp");
    return res.json({ success: true, error: false, data: result });
  } catch (e) {
    if (e.code === "WHATSAPP_NOT_CONNECTED") {
      return res.status(409).json({ success: false, error: true, reply: "WhatsApp non connecté.", code: "WHATSAPP_NOT_CONNECTED" });
    }
    return res.status(500).json({ success: false, error: true, code: "WHATSAPP_SEND_ERROR" });
  }
});

// ---------- /api/intent/init ----------
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

// ---------- /api/memory/clear ----------
app.post("/api/memory/clear", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const conversationId = req.body?.conversationId || req.body?.conversation_id;
    if (!conversationId) return res.status(400).json({ success: false, error: true, code: "MISSING_CONVERSATION_ID" });

    try { await assertConversationOwnership(conversationId, req.userId); }
    catch (e) { return res.status(403).json({ success: false, error: true, reply: e.message, code: "CONVERSATION_OWNERSHIP" }); }

    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);
    await clearActiveIntent(conversationId);

    if (supabase) {
      supabaseWriteSafe({
        table: "messages", op: "delete", payload: {},
        matchColumn: "session_id", matchValue: conversationId,
        idempotencyKey: `messages_clear:${conversationId}:${Date.now()}`
      }).catch(() => {});
    }
    return res.json({ success: true, error: false, reply: "Mémoire effacée." });
  } catch {
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/admin/set-role ----------
app.post("/api/admin/set-role", strictLimiter, authenticateUser, requireRole(["ADMIN"]), async (req, res) => {
  try {
    const { uid, role } = req.body || {};
    if (!uid || !["FREE", "PREMIUM", "ADMIN"].includes(role)) {
      return res.status(400).json({ success: false, error: true, code: "INVALID_PARAMS" });
    }
    if (firebaseApp() && firebaseAdmin) {
      await firebaseAdmin.auth(firebaseApp()).setCustomUserClaims(uid, { role });
    }
    await dbRun(`UPDATE users SET role = ?, updated_at = ? WHERE firebase_uid = ? OR id = ?`, [role, Date.now(), uid, uid]);
    await logSecurityEvent(req.userId, "ROLE_UPDATED", { targetUid: uid, newRole: role }, req.ip, req.headers["user-agent"]);
    return res.json({ success: true, error: false, data: { uid, role } });
  } catch {
    return res.status(500).json({ success: false, error: true, code: "ROLE_UPDATE_ERROR" });
  }
});

// ---------- /api/admin/backfill (SQLite → Firestore + Supabase) ----------
app.post("/api/admin/backfill", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const stats = { firestore: 0, supabase: 0 };

    const sessions = await dbAll(`SELECT * FROM sessions WHERE user_id = ?`, [userId]);
    for (const s of sessions) {
      if (firestoreDb) {
        await fsSet("sessions", s.session_id, {
          session_id: s.session_id, user_id: s.user_id,
          firebase_uid: s.firebase_uid || s.user_id,
          created_at: s.created_at, updated_at: s.updated_at
        }, { merge: true });
        stats.firestore++;
      }
      if (supabase) {
        const r = await supabaseWriteSafe({
          table: "sessions", op: "upsert",
          payload: {
            session_id: s.session_id, user_id: s.user_id,
            firebase_uid: s.firebase_uid || s.user_id,
            created_at: new Date(s.created_at).toISOString(),
            updated_at: new Date(s.updated_at).toISOString()
          },
          matchColumn: "session_id",
          idempotencyKey: `backfill_session:${s.session_id}`
        });
        if (r.success) stats.supabase++;
      }
    }

    const messages = await dbAll(`SELECT * FROM messages WHERE user_id = ? ORDER BY created_at ASC`, [userId]);
    for (const m of messages) {
      if (firestoreDb) {
        await fsSet("messages", `bf_${m.id}`, {
          session_id: m.session_id, user_id: userId,
          role: m.role, content: m.content,
          created_at: m.created_at
        }).catch(() => {});
        stats.firestore++;
      }
      if (supabase) {
        await supabaseWriteSafe({
          table: "messages", op: "insert",
          payload: {
            session_id: m.session_id, firebase_uid: userId, user_id: userId,
            role: m.role, content: m.content,
            created_at: new Date(m.created_at).toISOString()
          },
          idempotencyKey: `backfill_msg:${m.session_id}:${m.created_at}:${sha256(m.content).slice(0, 8)}`
        }).catch(() => {});
        stats.supabase++;
      }
    }

    return res.json({ success: true, error: false, stats });
  } catch (e) {
    logger.error({ err: e.message }, "backfill échec");
    return res.status(500).json({ success: false, error: true });
  }
});

// ---------- /api/account (RGPD delete) ----------
app.delete("/api/account", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const userId = req.userId;
    const firebaseUid = req.firebaseUid;

    // WhatsApp cleanup
    try {
      const waSession = baileysManager.sessions.get(userId);
      if (waSession?.sock) waSession.sock.end(undefined);
      baileysManager.sessions.delete(userId);
      const authDir = path.join(CONFIG.PATHS.SESSIONS, userId);
      if (fs.existsSync(authDir)) fs.rmSync(authDir, { recursive: true, force: true });
    } catch {}

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
      await dbRun("DELETE FROM user_memory_facts WHERE user_id = ?", [userId]);
      await dbRun("DELETE FROM users WHERE id = ?", [userId]);
    });

    // Firestore + Supabase purge
    if (firestoreDb) {
      const collections = ["messages", "sessions", "user_tasks", "user_memory", "user_memory_facts"];
      for (const c of collections) {
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
          matchColumn: "user_id", matchValue: userId,
          idempotencyKey: `account_delete:${t}:${userId}`
        }).catch(() => {});
      }
    }

    // Firebase Auth delete
    let firebaseDeleted = false;
    if (firebaseApp() && firebaseAdmin) {
      try {
        await firebaseAdmin.auth(firebaseApp()).deleteUser(firebaseUid);
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

// ---------- 404 ----------
app.use((req, res) => {
  res.status(404).json({ success: false, error: true, reply: "Route non trouvée", code: "NOT_FOUND" });
});

// ---------- Gestion erreur globale ----------
app.use((error, req, res, next) => {
  logger.error({ err: error.message, stack: error.stack, path: req.path }, "Erreur non gérée");
  if (res.headersSent) return next(error);

  if (error.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({ success: false, error: true, reply: `Fichier trop volumineux (max ${CONFIG.LIMITS.MAX_IMAGE_SIZE_MB} Mo).`, code: "FILE_TOO_LARGE" });
  }
  if (error.code === "LIMIT_FILE_COUNT") {
    return res.status(413).json({ success: false, error: true, reply: "Trop de fichiers.", code: "TOO_MANY_FILES" });
  }

  return res.status(500).json({ success: false, error: true, reply: "Une erreur interne est survenue.", code: "INTERNAL_ERROR" });
});

// ================================================================================
// §4.13 — LUBA LIVE WEBSOCKET (protocole binaire custom)
// ================================================================================

// Frame types (1 octet) :
//   0x01 HELLO      client → serveur
//   0x02 AUDIO_IN   client → serveur (PCM 16kHz mono)
//   0x03 TEXT_IN    client → serveur (UTF-8)
//   0x04 BARGE_IN   client → serveur (interruption)
//   0x05 END_TURN   client → serveur (fin d'utterance)
//   0x11 TRANSCRIPT serveur → client (JSON)
//   0x12 TOKEN      serveur → client (UTF-8, tokens LLM)
//   0x13 AUDIO_OUT  serveur → client (PCM/MP3 TTS)
//   0x14 STATUS     serveur → client (JSON)
//   0x15 ERROR      serveur → client (JSON)
//   0x16 DONE       serveur → client (fin de tour)

const WS_FRAME = Object.freeze({
  HELLO: 0x01,
  AUDIO_IN: 0x02,
  TEXT_IN: 0x03,
  BARGE_IN: 0x04,
  END_TURN: 0x05,
  TRANSCRIPT: 0x11,
  TOKEN: 0x12,
  AUDIO_OUT: 0x13,
  STATUS: 0x14,
  ERROR: 0x15,
  DONE: 0x16
});

function encodeFrame(type, payload) {
  const payloadBuf = Buffer.isBuffer(payload)
    ? payload
    : typeof payload === "string"
      ? Buffer.from(payload, "utf8")
      : Buffer.from(safeJsonStringify(payload ?? {}), "utf8");

  const frame = Buffer.allocUnsafe(5 + payloadBuf.length);
  frame.writeUInt8(type, 0);
  frame.writeUInt32BE(payloadBuf.length, 1);
  payloadBuf.copy(frame, 5);
  return frame;
}

function decodeFrame(buffer) {
  if (buffer.length < 5) return null;
  const type = buffer.readUInt8(0);
  const length = buffer.readUInt32BE(1);
  if (buffer.length < 5 + length) return null;
  const payload = buffer.subarray(5, 5 + length);
  return { type, payload };
}

let WebSocketServer = null;
try { WebSocketServer = require("ws").WebSocketServer || require("ws").Server; } catch {}

let wsServer = null;
const liveSessions = new Map();

function setupLubaLiveWebSocket(server) {
  if (!WebSocketServer) {
    logger.warn("⚠️  ws non installé — Luba Live WebSocket désactivé");
    return;
  }
  if (process.env.LUBA_LIVE_ENABLED === "false") {
    logger.info("ℹ️  Luba Live désactivé par env");
    return;
  }

  wsServer = new WebSocketServer({
    server,
    path: "/live",
    perMessageDeflate: false,
    maxPayload: 1024 * 1024 // 1 Mo
  });

  wsServer.on("connection", (ws, req) => {
    const sessionId = `live_${crypto.randomUUID()}`;
    const state = {
      sessionId,
      userId: null,
      conversationId: null,
      vad: new SimpleVAD(),
      stt: null,
      interrupted: false,
      authenticated: false,
      createdAt: Date.now()
    };
    liveSessions.set(sessionId, state);

    if (metrics?.activeWebSockets) metrics.activeWebSockets.labels("live").inc();

    logger.info({ sessionId, ip: req.socket.remoteAddress }, "🔌 Luba Live connecté");

    ws.on("message", (data) => {
      try {
        const frame = decodeFrame(data);
        if (!frame) return;
        handleLiveFrame(ws, state, frame);
      } catch (e) {
        logger.error({ err: e.message, sessionId }, "Erreur frame WS");
        ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Bad frame" }));
      }
    });

    ws.on("close", () => {
      liveSessions.delete(sessionId);
      if (metrics?.activeWebSockets) metrics.activeWebSockets.labels("live").dec();
      logger.info({ sessionId }, "🔌 Luba Live déconnecté");
    });

    ws.on("error", (e) => {
      logger.warn({ err: e.message, sessionId }, "WS erreur");
    });
  });

  logger.info("✅ Luba Live WebSocket initialisé sur /live");
}

async function handleLiveFrame(ws, state, frame) {
  const { type, payload } = frame;

  // HELLO : authentification + init
  if (type === WS_FRAME.HELLO) {
    const hello = safeJsonParse(payload.toString("utf8"), {});
    const token = hello.token;
    if (!token) {
      return ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Missing token" }));
    }
    try {
      const user = await verifyFirebaseToken(token);
      if (!user) throw new Error("Invalid user");
      state.userId = user.uid;
      state.conversationId = hello.conversationId || `live_${crypto.randomUUID()}`;
      state.authenticated = true;

      // Init STT
      state.stt = new StreamingSTT({
        onPartial: (text) => {
          if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text, partial: true }));
        },
        onFinal: (text) => {
          if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text, partial: false }));
        },
        onError: () => {}
      });

      ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "ready", sessionId: state.sessionId, conversationId: state.conversationId }));
    } catch (e) {
      ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Authentication failed" }));
    }
    return;
  }

  if (!state.authenticated) {
    return ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Not authenticated" }));
  }

  // BARGE_IN
  if (type === WS_FRAME.BARGE_IN) {
    state.interrupted = true;
    if (state.stt) state.stt.reset();
    ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "barge_in" }));
    return;
  }

  // AUDIO_IN
  if (type === WS_FRAME.AUDIO_IN) {
    if (state.interrupted) return;
    if (state.stt) {
      await state.stt.push(payload, { mimetype: "audio/webm" });
    }
    return;
  }

  // TEXT_IN
  if (type === WS_FRAME.TEXT_IN) {
    const text = payload.toString("utf8").slice(0, CONFIG.LIMITS.MAX_MESSAGE_LENGTH);
    await handleLiveTurn(ws, state, text);
    return;
  }

  // END_TURN (finalize STT + LLM + TTS)
  if (type === WS_FRAME.END_TURN) {
    if (!state.stt) return;
    const result = await state.stt.finalize("audio/webm");
    if (result.success && result.text) {
      ws.send(encodeFrame(WS_FRAME.TRANSCRIPT, { text: result.text, partial: false }));
      await handleLiveTurn(ws, state, result.text);
    }
    return;
  }
}

async function handleLiveTurn(ws, state, userText) {
  if (!userText) return;
  state.interrupted = false;

  ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "thinking" }));

  // Utilise le chunker pour TTS anticipé
  const ttsQueue = [];
  let ttsProcessing = false;

  const processTTS = async () => {
    if (ttsProcessing) return;
    ttsProcessing = true;
    while (ttsQueue.length > 0 && !state.interrupted && ws.readyState === 1) {
      const sentence = ttsQueue.shift();
      try {
        const tts = await synthesizeSpeech(sentence, { voice: "af_bella" });
        if (tts.success && !state.interrupted && ws.readyState === 1) {
          ws.send(encodeFrame(WS_FRAME.AUDIO_OUT, tts.audio));
        }
      } catch (e) { logger.warn({ err: e.message }, "TTS WS échec"); }
    }
    ttsProcessing = false;
  };

  const chunker = new SentenceChunker({
    onSentence: (s) => {
      if (state.interrupted) return;
      ttsQueue.push(s);
      processTTS().catch(() => {});
    }
  });

  try {
    // Appel handleChat en mode streaming-like : on envoie les tokens via WS
    const sseAdapter = {
      closed: false,
      status: (stage, extra) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage, ...extra }));
      },
      reasoning: () => {},
      codeBlock: () => {},
      token: (text) => {
        if (state.interrupted || ws.readyState !== 1) return;
        ws.send(encodeFrame(WS_FRAME.TOKEN, text));
        chunker.push(text);
      },
      images: (list) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "images", images: list }));
      },
      videos: (list) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "videos", videos: list }));
      },
      suggestions: (list) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "suggestions", suggestions: list }));
      },
      sources: (list) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.STATUS, { stage: "sources", sources: list }));
      },
      error: (payload) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.ERROR, payload));
      },
      done: (payload) => {
        if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.DONE, payload));
      },
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
      sse: sseAdapter
    });

    chunker.flush();
    await new Promise((r) => setTimeout(r, 300));

    if (ws.readyState === 1 && !state.interrupted) {
      ws.send(encodeFrame(WS_FRAME.DONE, { conversationId: state.conversationId }));
    }
  } catch (e) {
    logger.error({ err: e.message }, "Live turn échec");
    if (ws.readyState === 1) ws.send(encodeFrame(WS_FRAME.ERROR, { error: "Erreur de traitement" }));
  }
}

// ================================================================================
// §4.14 — BOOTSTRAP + GRACEFUL SHUTDOWN
// ================================================================================

let server = null;
let isShuttingDown = false;

async function bootstrap() {
  console.log("");
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log(`║  🚀 LUBA AI PRO v${CONFIG.VERSION} — HIKLON TECHNOLOGIES            ║`);
  console.log("╚══════════════════════════════════════════════════════════════╝");

  // 1) Bootstrap Partie 1
  await bootstrapPart1();

  // 2) HTTP server
  server = app.listen(CONFIG.PORT, CONFIG.HOST, () => {
    global.__luba_server = server;
    logger.info(`Serveur ${CONFIG.AGENT_NAME} v${CONFIG.VERSION} démarré sur ${CONFIG.HOST}:${CONFIG.PORT}`);
  });

  server.on("error", (err) => {
    logger.fatal({ err: err.message }, "Erreur du serveur HTTP");
    process.exit(1);
  });

  // 3) Luba Live WebSocket
  setupLubaLiveWebSocket(server);

  // 4) Schedulers
  setInterval(reminderTick, CONFIG.TIMEOUTS.REMINDER_TICK_MS).unref?.();
  setInterval(runSecurityHousekeeping, CONFIG.TIMEOUTS.HOUSEKEEPING_MS).unref?.();

  // 5) Banner
  console.log("");
  console.log("🌐 Domaine      : " + HOSTING_CONFIG.domain);
  console.log("🔐 Firebase     : " + (firebaseApp() ? "Admin SDK ✅" : "REST API ⚠️"));
  console.log("💾 Firestore    : " + (firestoreDb ? "✅" : "❌"));
  console.log("💾 Supabase     : " + (supabase ? "✅" : "❌"));
  console.log("💾 SQLite       : ✅");
  console.log("⚡ Redis        : " + (redisClient() ? "✅" : "❌ (fallback LRU)"));
  console.log("📧 Email        : " + (emailTransporter ? "SMTP ✅" : (process.env.RESEND_API_KEY ? "Resend ✅" : "❌")));
  console.log("📱 WhatsApp     : " + (CONFIG.WHATSAPP.ENCRYPTION_KEY ? "Chiffré ✅" : "⚠️"));
  console.log("🛡️  Rate limit  : " + (redisRateLimitStore ? "Redis ✅" : "Mémoire ⚠️"));
  console.log("🧪 Sandbox      : " + (CONFIG.SANDBOX.PROVIDER || "Non configuré ⚠️"));
  console.log("📡 SSE          : ✅ /api/chat");
  console.log("🎙️  Luba Live    : " + (wsServer ? "✅ /live (WebSocket)" : "❌"));
  console.log("📊 Metrics      : " + (metrics() ? "✅ /api/metrics" : "❌"));
  console.log("");
  console.log("🤖 PROVIDERS LLM :");
  console.log(`   ├─ Groq        : ${LLM_PROVIDERS.GROQ.keyPool.length} clé(s)`);
  console.log(`   ├─ OpenRouter  : ${LLM_PROVIDERS.OPENROUTER.keyPool.length} clé(s)`);
  console.log(`   ├─ Cerebras    : ${LLM_PROVIDERS.CEREBRAS.keyPool.length} clé(s)`);
  console.log(`   └─ Gemini      : ${geminiClient ? "Actif ✅" : "Inactif ⚠️"}`);
  console.log("");
  console.log("🎯 Luba AI Pro v16.0 — Prêt.");
  console.log("");
}

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info({ signal }, "Arrêt propre du serveur");

  try {
    if (server) await new Promise((resolve) => server.close(resolve));
  } catch {}

  try { await baileysManager.destroyAll(); } catch {}
  try { if (wsServer) wsServer.close(); } catch {}
  try { if (redisClient()) await redisClient().quit(); } catch {}
  try { await new Promise((resolve) => db ? db.close(() => resolve()) : resolve()); } catch {}

  console.log("✅ Arrêt propre terminé");
  process.exit(0);
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("uncaughtException", (error) => {
  logger.fatal({ err: error.message, stack: error.stack }, "uncaughtException");
  gracefulShutdown("uncaughtException");
});
process.on("unhandledRejection", (reason) => {
  logger.error({ reason: String(reason) }, "unhandledRejection");
});

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
    logger.info("🧹 Nettoyage périodique effectué");
  } catch (e) {
    logger.error({ err: e.message }, "Erreur housekeeping");
  }
}

// ================================================================================
// §4.15 — DOCKER / DOCKER-COMPOSE / NGINX / CI-CD (fichiers annexes)
// ================================================================================
// Ces fichiers doivent être créés SÉPARÉMENT à la racine du projet.
// Contenu copiable ci-dessous.

const DEPLOYMENT_FILES = String.raw`
# =============================================================================
# Dockerfile — multi-stage, image < 200 Mo
# =============================================================================
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
RUN mkdir -p /app/data /app/sessions /app/uploads /app/logs && \
    chown -R node:node /app
USER node
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:3000/ready || exit 1
CMD ["node", "index.js"]

# =============================================================================
# docker-compose.yml
# =============================================================================
version: "3.9"
services:
  luba-backend:
    build: .
    container_name: luba-backend
    restart: unless-stopped
    ports:
      - "3000:3000"
    env_file:
      - .env
    environment:
      - NODE_ENV=production
      - REDIS_URL=redis://redis:6379
    volumes:
      - luba-data:/app/data
      - luba-sessions:/app/sessions
      - luba-uploads:/app/uploads
      - luba-logs:/app/logs
    depends_on:
      - redis
    networks:
      - luba-net

  redis:
    image: redis:7-alpine
    container_name: luba-redis
    restart: unless-stopped
    command: redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes:
      - luba-redis:/data
    networks:
      - luba-net

  nginx:
    image: nginx:alpine
    container_name: luba-nginx
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
    depends_on:
      - luba-backend
    networks:
      - luba-net

volumes:
  luba-data:
  luba-sessions:
  luba-uploads:
  luba-logs:
  luba-redis:

networks:
  luba-net:
    driver: bridge

# =============================================================================
# nginx.conf
# =============================================================================
events { worker_connections 1024; }

http {
  include       /etc/nginx/mime.types;
  default_type  application/octet-stream;
  sendfile      on;
  keepalive_timeout 65;
  client_max_body_size 25M;

  # Gzip
  gzip on;
  gzip_types text/plain text/css application/json application/javascript text/xml application/xml;

  # Rate limiting
  limit_req_zone $binary_remote_addr zone=api:10m rate=30r/s;
  limit_req_zone $binary_remote_addr zone=chat:10m rate=2r/s;

  upstream luba_backend {
    server luba-backend:3000;
    keepalive 32;
  }

  server {
    listen 80;
    server_name _;
    return 301 https://$host$request_uri;
  }

  server {
    listen 443 ssl http2;
    server_name luba.web.app;

    ssl_certificate     /etc/nginx/certs/fullchain.pem;
    ssl_certificate_key /etc/nginx/certs/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # SSE + WebSocket
    location /api/chat {
      limit_req zone=chat burst=5 nodelay;
      proxy_pass http://luba_backend;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_set_header Host $host;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto $scheme;
      proxy_buffering off;
      proxy_cache off;
      proxy_read_timeout 120s;
    }

    location /live {
      proxy_pass http://luba_backend;
      proxy_http_version 1.1;
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection "upgrade";
      proxy_set_header Host $host;
      proxy_read_timeout 600s;
    }

    location / {
      limit_req zone=api burst=20 nodelay;
      proxy_pass http://luba_backend;
      proxy_http_version 1.1;
      proxy_set_header Host $host;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto $scheme;
    }
  }
}

# =============================================================================
# .github/workflows/deploy.yml
# =============================================================================
name: Deploy Luba Backend

on:
  push:
    branches: [main]
  workflow_dispatch:

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
      - run: npm test --if-present

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
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/${{ github.repository }}:latest,ghcr.io/${{ github.repository }}:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

  deploy:
    needs: build-and-push
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy via SSH
        uses: appleboy/ssh-action@v1
        with:
          host: ${{ secrets.DEPLOY_HOST }}
          username: ${{ secrets.DEPLOY_USER }}
          key: ${{ secrets.DEPLOY_SSH_KEY }}
          script: |
            cd /opt/luba
            docker compose pull
            docker compose up -d --remove-orphans
            docker compose ps

# =============================================================================
# scripts/deploy.sh
# =============================================================================
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "🚀 Deploy Luba AI Pro"
docker compose build --no-cache
docker compose up -d --remove-orphans
docker compose ps
echo "✅ Déploiement terminé"

# =============================================================================
# scripts/rollback.sh
# =============================================================================
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${1:-previous}"
echo "⏪ Rollback vers $TAG"
docker compose down
git checkout "$TAG"
docker compose up -d --build
echo "✅ Rollback terminé"
`;

function getDeploymentFiles() {
  return DEPLOYMENT_FILES;
}

// ================================================================================
// §4.16 — EXPORTS FINAUX
// ================================================================================

Object.assign(module.exports, {
  // App
  app,
  get server() { return server; },
  get wsServer() { return wsServer; },

  // SSE
  SSEWriter, streamTextAsTokens,

  // Prompt
  LUBA_SYSTEM_PROMPT,

  // Sessions
  getSession, setActiveIntent, getActiveIntent, clearActiveIntent,
  getFullHistory, assertConversationOwnership, saveMessageWithUser,
  ACTIVE_INTENT_TTL_MS,

  // Mémoire long terme (résumé)
  getUserMemory, saveUserMemory, incrementUserMemoryCounter,
  runMemorySummaryImpl, maybeUpdateUserMemoryAsync,

  // Suggestions
  extractSuggestions, generateSuggestions,

  // Enrichissement + chat
  enrichContextWithIntent, toolCacheKey,
  handleChat, handleActiveIntent, isCancelMessage,

  // WhatsApp
  baileysManager, isWhatsAppAllowed,

  // Scheduler
  reminderTick,

  // WebSocket
  WS_FRAME, encodeFrame, decodeFrame, setupLubaLiveWebSocket,
  handleLiveFrame, handleLiveTurn,
  liveSessions: () => liveSessions,

  // Déploiement
  DEPLOYMENT_FILES, getDeploymentFiles,

  // Lifecycle
  bootstrap, gracefulShutdown, runSecurityHousekeeping
});

// ================================================================================
// AUTO-START (démarre si ce fichier est exécuté directement)
// ================================================================================

if (require.main === module) {
  bootstrap().catch((e) => {
    logger.fatal({ err: e.message, stack: e.stack }, "Bootstrap échoué");
    process.exit(1);
  });
}

// ================================================================================
// ==================== FIN PARTIE 4/4 — FIN DU FICHIER index.js ================
// ================================================================================
// ✅ Backend Luba AI Pro v16.0.0 — Production ready.
//
// Pour lancer :
//   1. npm install express cors helmet express-rate-limit axios qrcode nodemailer \
//      sqlite3 pino multer @supabase/supabase-js ws rss-parser form-data cheerio \
//      lru-cache dotenv prom-client mathjs ioredis @whiskeysockets/baileys \
//      firebase-admin @google-cloud/firestore @google/genai youtubei.js \
//      duck-duck-scrape rate-limit-redis
//   2. Copier les 4 fichiers de déploiement (Dockerfile, docker-compose.yml,
//      nginx.conf, .github/workflows/deploy.yml) via getDeploymentFiles()
//   3. node index.js
//
// Ou avec Docker :
//   docker compose up -d
//
// URLs :
//   http://localhost:3000/                — Info
//   http://localhost:3000/api/health      — Health
//   http://localhost:3000/ready           — Readiness
//   http://localhost:3000/api/metrics     — Prometheus
//   POST http://localhost:3000/api/chat   — Chat (JSON ou SSE)
//   ws://localhost:3000/live              — Luba Live (WebSocket binaire)
// ================================================================================
