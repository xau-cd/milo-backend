// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 9.3.0 Enterprise
// Architecture : Modulaire, Microservices-ready, Haute Disponibilité
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

// ==================== IMPORTS OPTIONNELS ====================
let firebaseAdmin = null;
try {
  firebaseAdmin = require("firebase-admin");
} catch (e) {
  console.warn("⚠️ firebase-admin non installé - authentification Firebase désactivée");
}

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
  VERSION: "9.3.0",
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
  
  // Chemins
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  UPLOADS_PATH: path.join(__dirname, "uploads"),
  
  // Modèles Vision
  VISION_MODEL_GROQ: process.env.VISION_MODEL_GROQ || "openai/gpt-4o-mini",
  VISION_MODEL_OPENROUTER: process.env.VISION_MODEL_OPENROUTER || "qwen/qwen-2.5-vl-72b-instruct:free",
  
  // Types MIME autorisés
  ALLOWED_IMAGE_TYPES: ["image/jpeg", "image/png", "image/gif", "image/webp"],
  
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/9.3.0 (contact@luba.ia)"
};

// ==================== VALIDATION ENVIRONNEMENT ====================
const requiredEnvVars = ["GROQ_API_KEY", "OPENROUTER_API_KEY"];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingEnvVars.length > 0) {
  console.error("=".repeat(60));
  console.error("❌ VARIABLES D'ENVIRONNEMENT OBLIGATOIRES MANQUANTES :");
  missingEnvVars.forEach((varName) => console.error(`   - ${varName}`));
  console.error("=".repeat(60));
}

// ==================== CRÉATION DES DOSSIERS ====================
for (const dir of [CONFIG.DB_PATH.split(path.sep).slice(0, -1).join(path.sep), CONFIG.SESSIONS_PATH, CONFIG.UPLOADS_PATH]) {
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
  // Table users
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      firebase_uid TEXT UNIQUE,
      email TEXT UNIQUE,
      display_name TEXT,
      whatsapp_connected INTEGER DEFAULT 0,
      whatsapp_session_id TEXT,
      last_seen_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  
  // Table sessions
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
  
  // Table messages
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
  
  // Index pour les performances
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, updated_at DESC)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sessions_firebase ON sessions(firebase_uid)");
  db.run("CREATE INDEX IF NOT EXISTS idx_messages_session_role ON messages(session_id, role)");
  
  // Table email_logs
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
  
  // Table llm_audit_log
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
      credential: firebaseAdmin.credential.cert(serviceAccount)
    });
    logger.info("Firebase Admin initialisé");
  } catch (e) {
    logger.error({ err: e.message }, "Erreur initialisation Firebase Admin");
  }
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

// ==================== CIRCUIT BREAKER PATTERN ====================
class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || CONFIG.CIRCUIT_BREAKER_THRESHOLD;
    this.resetTimeout = options.resetTimeout || CONFIG.CIRCUIT_BREAKER_RESET_MS;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED"; // CLOSED, OPEN, HALF_OPEN
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
      // File d'attente en mémoire
      const inMemoryQueue = [];
      const processQueue = async () => {
        while (inMemoryQueue.length > 0) {
          const job = inMemoryQueue.shift();
          try {
            await processor(job);
          } catch (error) {
            logger.error({ err: error.message }, `Job ${name} échoué (mémoire)`);
          }
        }
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
  
  const knownProviders = [
    "openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/",
    "anthropic/", "google/", "mistralai/", "cohere/"
  ];
  
  const isOpenRouterModel = knownProviders.some(prefix => model.includes(prefix));
  
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
    const isTimeout = ["ECONNABORTED", "ETIMEDOUT", "ESOCKETTIMEDOUT"].includes(error.code) ||
                      /timeout/i.test(error.message || "");
    const isNetworkError = ["ENOTFOUND", "ECONNRESET", "ECONNREFUSED", "EAI_AGAIN"].includes(error.code);
    
    return retryableStatuses.includes(status) || isTimeout || isNetworkError;
  }
  
  static getErrorCode(error) {
    const status = error.response?.status;
    if (status) return "HTTP_" + status;
    if (error.code === "ECONNABORTED") return "TIMEOUT";
    if (error.code === "ENOTFOUND") return "DNS_ERROR";
    if (error.code === "ECONNREFUSED") return "CONNECTION_REFUSED";
    if (error.code === "MISSING_API_KEY") return "MISSING_API_KEY";
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
    enableCircuitBreaker = true
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
        
        logger.info({ provider, model, attempt: attempt + 1 }, "Succès fournisseur");
        
        return {
          success: true,
          ...providerResult,
          providerChain: providerResults
        };
        
      } catch (error) {
        lastError = error;
        const errorCode = LLMErrorInterceptor.getErrorCode(error);
        
        logger.error({ provider, model, attempt: attempt + 1, errorCode }, "Échec fournisseur");
        
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
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        } else if (!LLMErrorInterceptor.isRetryableError(error)) {
          break;
        }
      }
    }
  }
  
  logger.error({ errorCode: LLMErrorInterceptor.getErrorCode(lastError) }, "Tous les fournisseurs ont échoué");
  
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
      
      formattedMessages = [...messages.slice(0, lastMessageIndex), {
        role: "user",
        content: contentParts
      }];
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
    headers["HTTP-Referer"] = "https://luba-ia.web.app";
    headers["X-Title"] = "Luba.ia Assistant";
  }
  
  const response = await axios.post(
    cfg.baseURL + "/chat/completions",
    payload,
    { headers, timeout: timeout || cfg.timeout }
  );
  
  const choice = response.data?.choices?.[0];
  const content = choice?.message?.content;
  
  if (choice?.finish_reason === "length") {
    logger.warn({ provider, model }, "Réponse tronquée par max_tokens");
  }
  
  if (!content) {
    throw new Error("Réponse " + provider + " vide");
  }
  
  return jsonMode ? JSON.parse(content) : content;
}

// ==================== CONTEXT MANAGER DYNAMIQUE ====================
class DynamicContextManager {
  constructor() {
    this.domainPatterns = [
      {
        domain: "mathematics",
        keywords: ["math", "calcul", "équation", "equation", "algèbre", "algebra", "géométrie", "geometry",
                   "calculus", "intégrale", "integrale", "dérivée", "derivative", "théorème", "theorem",
                   "nombre", "number", "fonction", "function", "limite", "limit", "matrice", "matrix",
                   "probabilité", "probability", "statistique", "statistics", "trigonométrie", "trigonometry"],
        systemPrompt: "Tu es un expert en mathématiques. Utilise LaTeX ($ pour inline, $$ pour display) pour toutes les formules. Détaille chaque étape du raisonnement. Sois rigoureux et pédagogique."
      },
      {
        domain: "cybersecurity",
        keywords: ["sécurité", "security", "cyber", "hack", "vulnérabilité", "vulnerability", "exploit",
                   "pentest", "cryptographie", "cryptography", "chiffrement", "encryption", "pare-feu",
                   "firewall", "malware", "virus", "phishing", "authentification", "token", "jwt",
                   "sql injection", "xss", "csrf", "ddos", "ransomware"],
        systemPrompt: "Tu es un expert en cybersécurité. Adopte une approche défensive et éthique. Explique les vulnérabilités, les vecteurs d'attaque et les contre-mesures. Utilise des blocs de code pour les exemples techniques."
      },
      {
        domain: "development",
        keywords: ["code", "coder", "programmation", "programming", "développement", "development",
                   "javascript", "python", "java", "c++", "rust", "go", "typescript", "react", "vue",
                   "angular", "node", "express", "api", "database", "sql", "nosql", "backend", "frontend",
                   "bug", "debug", "fonction", "function", "classe", "class", "objet", "object", "algorithme",
                   "framework", "library", "package", "npm", "git", "docker", "kubernetes", "ci/cd",
                   "microservice", "architecture", "design pattern", "oop", "functional programming"],
        systemPrompt: "Tu es un expert en développement logiciel. Fournis du code de production complet et fonctionnel dans des blocs Markdown avec triple backticks. Explique l'architecture, les choix techniques et les bonnes pratiques."
      },
      {
        domain: "data_science",
        keywords: ["data", "données", "machine learning", "deep learning", "neural network", "réseau de neurones",
                   "pandas", "numpy", "tensorflow", "pytorch", "scikit", "regression", "classification",
                   "clustering", "nlp", "computer vision", "dataset", "model training", "feature engineering"],
        systemPrompt: "Tu es un expert en data science et machine learning. Explique les concepts, les algorithmes et les implémentations pratiques. Utilise des blocs de code pour les exemples et LaTeX pour les formules mathématiques."
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
      "- TOUT code doit être encadré dans des blocs Markdown avec triple backticks",
      "- TOUTE formule mathématique doit être encadrée en LaTeX ($ pour inline, $$ pour display)",
      "- AUCUN caractère technique non formaté dans le texte brut",
      "- Les noms de variables, fonctions et fichiers doivent être en backticks simples",
      "- Les résultats de commandes doivent être dans des blocs de code"
    ].join("\n");
    
    return {
      role: "system",
      content: basePrompt + "\n\nDOMAINE D'EXPERTISE DÉTECTÉ : " + domain.domain.toUpperCase() + "\n" +
               domain.systemPrompt + "\n\n" + formattingRules
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
const ALLOWED_ORIGINS = [
  "https://luba-ia.web.app",
  "https://luba-ia.firebaseapp.com",
  "https://milo-ead21.web.app",
  "http://localhost:3000",
  "http://localhost:8080",
  "http://localhost:5173",
  "http://localhost:4200"
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) {
      callback(null, true);
    } else {
      logger.warn({ origin }, "Origine CORS refusée");
      callback(new Error("Origine non autorisée"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token"],
  credentials: true,
  maxAge: 86400
}));

// ==================== SECURITY MIDDLEWARE ====================
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  contentSecurityPolicy: false
}));

// ==================== BODY PARSERS ====================
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
  handler: (req, res) => {
    logger.warn({ ip: req.ip }, "Rate limit atteint");
    res.status(200).json({ reply: "Trop de requêtes. Réessaie dans 15 minutes.", error: true });
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    res.status(200).json({ reply: "Limite de requêtes atteinte.", error: true });
  }
});

// ==================== LOGGING MIDDLEWARE ====================
app.use((req, res, next) => {
  const requestId = generateRequestId();
  const start = Date.now();
  req.requestId = requestId;
  
  logger.info({
    requestId,
    method: req.method,
    url: req.url,
    ip: req.ip
  }, "Requête entrante");
  
  res.on("finish", () => {
    logger.info({
      requestId,
      status: res.statusCode,
      duration: Date.now() - start
    }, "Réponse envoyée");
  });
  
  next();
});

// ==================== AUTHENTIFICATION ====================
const authenticateUser = async (req, res, next) => {
  try {
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
        logger.warn({ err: error.message }, "Token Firebase invalide");
        return res.status(200).json({
          reply: "Session invalide ou expirée. Reconnecte-toi.",
          error: true,
          code: "INVALID_TOKEN"
        });
      }
    }
    
    const userId = verifiedUserId || (typeof providedUserId === "string" ? providedUserId.trim() : null);
    
    if (!userId) {
      return res.status(200).json({
        reply: "Authentification requise. Fournis un userId ou un token valide.",
        error: true
      });
    }
    
    req.userId = userId;
    req.firebaseUid = firebaseUid || userId;
    req.verifiedIdentity = Boolean(verifiedUserId);
    
    // Synchronisation utilisateur
    try {
      if (supabase && firebaseUid) {
        await syncUserWithSupabase(firebaseUid, verifiedEmail, verifiedName);
      }
      
      const user = await dbGet("SELECT * FROM users WHERE id = ?", [userId]);
      if (!user) {
        await dbRun(
          "INSERT INTO users (id, firebase_uid, email, display_name, last_seen_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
          [userId, firebaseUid, verifiedEmail, verifiedName || userId]
        );
      } else {
        await dbRun(
          "UPDATE users SET last_seen_at = CURRENT_TIMESTAMP, firebase_uid = COALESCE(?, firebase_uid), email = COALESCE(?, email), display_name = COALESCE(?, display_name) WHERE id = ?",
          [firebaseUid, verifiedEmail, verifiedName, userId]
        );
      }
    } catch (err) {
      logger.error({ err: err.message }, "Erreur synchronisation utilisateur");
    }
    
    next();
  } catch (error) {
    logger.error({ err: error.message }, "Erreur authentification");
    return res.status(200).json({ reply: "Erreur interne.", error: true });
  }
};

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
      logger.error({ err: fetchError.message }, "Erreur Supabase fetch user");
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
      
      if (insertError) logger.error({ err: insertError.message }, "Erreur Supabase insert user");
    } else {
      const { error: updateError } = await supabase
        .from("users")
        .update({ last_seen_at: new Date().toISOString() })
        .eq("firebase_uid", firebaseUid);
      
      if (updateError) logger.error({ err: updateError.message }, "Erreur Supabase update user");
    }
  } catch (error) {
    logger.error({ err: error.message }, "Erreur sync Supabase");
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
      
      if (insertError) logger.error({ err: insertError.message }, "Erreur Supabase insert session");
    } else {
      const { error: updateError } = await supabase
        .from("sessions")
        .update({ updated_at: new Date().toISOString() })
        .eq("session_id", sessionId);
      
      if (updateError) logger.error({ err: updateError.message }, "Erreur Supabase update session");
    }
  } catch (error) {
    logger.error({ err: error.message }, "Erreur sync session Supabase");
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
    
    if (insertError) logger.error({ err: insertError.message }, "Erreur Supabase insert message");
  } catch (error) {
    logger.error({ err: error.message }, "Erreur sync message Supabase");
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
        await dbRun(
          "INSERT OR IGNORE INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)",
          [conversationId, supabaseSession.user_id || userId, supabaseSession.firebase_uid]
        );
        return { session_id: conversationId, user_id: supabaseSession.user_id || userId, firebase_uid: supabaseSession.firebase_uid };
      }
    } catch (error) {
      logger.error({ err: error.message }, "Erreur Supabase getSession");
    }
  }
  
  await dbRun(
    "INSERT INTO sessions (session_id, user_id, firebase_uid) VALUES (?, ?, ?)",
    [conversationId, userId, firebaseUid]
  );
  
  await syncSessionWithSupabase(conversationId, firebaseUid, userId);
  
  return { session_id: conversationId, user_id: userId, firebase_uid: firebaseUid };
}

async function getHistory(conversationId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  const localRows = await dbAll(
    "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
    [conversationId, limit]
  );
  
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
      logger.error({ err: error.message }, "Erreur Supabase getHistory");
    }
  }
  
  return [];
}

async function saveMessage(conversationId, role, content, firebaseUid = null) {
  await dbRun(
    "INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)",
    [conversationId, role, content]
  );
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
  await syncMessageWithSupabase(conversationId, role, content, firebaseUid);
}

// ==================== GESTION DES INTENTIONS ====================
async function setActiveIntent(conversationId, intentType, intentData = {}) {
  await dbRun(
    "UPDATE sessions SET active_intent = ?, intent_data = ? WHERE session_id = ?",
    [intentType, JSON.stringify(intentData), conversationId]
  );
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

// ==================== CALL LLM V100 ====================
async function callLLM_v100(messages, images = null) {
  logger.info("Démarrage du routage Mwamba (v100)");
  
  const lastUserMessage = [...messages].reverse().find(m => m.role === "user");
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
  
  throw new Error("Échec complet du tier v100: " + result.errorCode);
}

// ==================== CALL LLM V250 ====================
async function callLLM_v250(messages, userMessage, images = null) {
  logger.info("Démarrage du pipeline Ngandu (v250)");
  
  const tier = MODEL_TIERS.v250;
  const providerChain = [];
  
  const dynamicSystemPrompt = dynamicContextManager.buildSystemPrompt(userMessage, LUBA_BASE_SYSTEM_PROMPT);
  
  // Étape 1 : Raisonnement
  const reasoningMessages = [
    {
      role: "system",
      content: dynamicSystemPrompt.content + "\n\nAnalyse ce problème complexe en profondeur. Effectue les démonstrations nécessaires, isole les edge cases et rédige le pseudo-code/l'architecture. Sois complet et rigoureux."
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
      }
    }
  );
  
  if (!reasoningResult.success || !reasoningResult.response || reasoningResult.response.trim().length < 40) {
    logger.error("Échec de l'étape de raisonnement - dégradation vers v100");
    return await degradedFallbackToV100(messages, "reasoning_failed", images);
  }
  
  const reasoningAnalysis = reasoningResult.response;
  providerChain.push("R1:" + reasoningResult.providerUsed + "/" + reasoningResult.modelUsed);
  
  // Étape 2 : Génération de code
  const formattingDirective = [
    "Tu DOIS répondre au format JSON strict :",
    "{",
    '  "replyText": "réponse complète en Markdown avec le code dans des blocs triple backticks",',
    '  "toolCalls": [],',
    '  "suggestions": ["question 1 ?", "question 2 ?", "question 3 ?"]',
    "}",
    "",
    "FORMATAGE STRICT :",
    "- Code dans des blocs triple backticks",
    "- Formules en LaTeX ($ ou $$)",
    "- Aucun caractère technique non formaté"
  ].join("\n");
  
  const codeMessages = [
    {
      role: "system",
      content: "Génère le code de production complet, typé, sécurisé et documenté en te basant strictement sur le plan ci-dessous.\n\nPLAN / ANALYSE (étape 1) :\n" + reasoningAnalysis + "\n\n" + formattingDirective
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
      }
    }
  );
  
  if (!codeResult.success || !codeResult.response) {
    logger.error("Échec de l'étape de génération - dégradation vers v100");
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
async function callVisionModel(messages, images) {
  logger.info("Démarrage du pipeline Vision");
  
  const result = await executeWithRetryAndFallback(
    MODEL_TIERS.vision.providers,
    { messages: messages, images: images },
    {
      maxRetriesPerProvider: 2,
      onProviderFail: (failInfo) => {
        logger.warn({ failInfo }, "Failover Vision");
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
  
  logger.warn("Échec des modèles vision - fallback vers v100 textuel");
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
    logger.error("Échec total de la dégradation : " + fallbackError.message);
    
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
      degradationReason: reason + "_and_v100_failed",
      error: true
    };
  }
}

// ==================== HANDLE CHAT PRINCIPAL ====================
async function handleChat({ conversationId, userId, firebaseUid, message, googleAccessToken = null, channel = "web", modelTier = "v100", images = null }) {
  logger.info({
    conversationId,
    userId,
    tier: modelTier,
    images: images ? images.length : 0,
    channel
  }, "Démarrage conversation");
  
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
          logger.error("Erreur LLM v100 : " + error.message);
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
              logger.error("Erreur outil " + toolCall.name + " : " + toolError.message);
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
      sources: Array.from(usedSources).map((key) => OPEN_SOURCES[key]).filter(Boolean)
    };
    
  } catch (error) {
    logger.error("Erreur critique : " + error.message);
    
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
        const result = await dispatchSendEmail({ googleAccessToken, recipient: data.recipient, subject: data.subject, body: userMessage });
        await clearActiveIntent(conversationId);
        if (result.success) return { reply: "Email envoyé à " + data.recipient + " !", error: false };
        return { reply: "Erreur : " + result.error, error: true };
      }
      break;
    }
  }
  
  await clearActiveIntent(conversationId);
  return { reply: "Je ne comprends plus l'action. Recommençons.", error: true };
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
      result = { success: false, error: "Outil inconnu : " + toolName };
  }
  
  return { result, sourceKeys };
}

// ==================== ROUTES ====================
app.get("/", (req, res) => {
  res.json({
    reply: "Serveur " + CONFIG.AGENT_NAME + " opérationnel",
    error: false,
    version: CONFIG.VERSION,
    company: CONFIG.COMPANY
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
      reply: "Serveur " + CONFIG.AGENT_NAME + " en bonne santé",
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
          retryMechanism: CONFIG.MAX_RETRY_ATTEMPTS + " tentatives max",
          circuitBreaker: "activé"
        }
      }
    });
  } catch (error) {
    logger.error("Erreur health check : " + error.message);
    res.status(500).json({ error: true, reply: "Erreur interne" });
  }
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post(
  "/api/chat",
  apiLimiter,
  authenticateUser,
  upload.array("images", CONFIG.MAX_IMAGES_PER_REQUEST),
  async (req, res) => {
    try {
      const message = req.body.message;
      let conversationId = req.body.conversationId || req.body.conversation_id;
      let isNewConversation = false;
      const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";
      
      if (!message || typeof message !== "string" || message.trim().length === 0) {
        return res.status(200).json({ reply: "Le paramètre 'message' est obligatoire.", error: true });
      }
      
      if (message.length > CONFIG.MAX_MESSAGE_LENGTH) {
        return res.status(200).json({
          reply: "Message trop long (max " + CONFIG.MAX_MESSAGE_LENGTH + " caractères).",
          error: true
        });
      }
      
      if (!conversationId || typeof conversationId !== "string") {
        conversationId = generateConversationId();
        isNewConversation = true;
      }
      
      try {
        await assertConversationOwnership(conversationId, req.userId);
      } catch (error) {
        return res.status(200).json({ reply: error.message, error: true });
      }
      
      if (modelTier === "v250") {
        req.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
        res.setTimeout(CONFIG.V250_ROUTE_TIMEOUT);
      }
      
      const googleAccessToken = req.headers["x-google-access-token"] || null;
      
      let images = null;
      if (req.files && req.files.length > 0) {
        images = req.files.map(file => convertImageToBase64(file.buffer, file.mimetype));
        logger.info("Images reçues : " + images.length);
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
      
      return res.status(200).json({ ...result, conversationId, isNewConversation });
      
    } catch (error) {
      logger.error("Erreur /api/chat : " + error.message);
      return res.status(200).json({
        reply: "Une erreur est survenue. Veuillez réessayer.",
        error: true,
        conversationId: req.body.conversationId || null,
        modelTier: req.body.modelTier || "v100"
      });
    }
  }
);

// ==================== ROUTE CONVERSATIONS ====================
app.get(
  "/api/conversations",
  apiLimiter,
  authenticateUser,
  async (req, res) => {
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
          logger.error("Erreur Supabase conversations : " + error.message);
        }
      }
      
      if (conversations.length === 0) {
        const rows = await dbAll(
          "SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? OR firebase_uid = ? ORDER BY updated_at DESC LIMIT 50",
          [req.userId, req.firebaseUid]
        );
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
              // Ignore
            }
          }
          
          if (!lastMessage) {
            lastMessage = await dbGet(
              "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1",
              [conv.session_id]
            );
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
      
    } catch (error) {
      logger.error("Erreur /api/conversations : " + error.message);
      return res.status(200).json({ reply: "Erreur interne.", error: true, conversations: [] });
    }
  }
);

// ==================== ROUTE OUTILS ====================
app.post(
  "/api/tools",
  apiLimiter,
  authenticateUser,
  async (req, res) => {
    try {
      const toolName = req.body.toolName || req.body.action;
      const params = req.body.params || req.body.arguments || {};
      
      if (!toolName || typeof toolName !== "string") {
        return res.status(200).json({ success: false, error: true, reply: "Le paramètre 'toolName' est obligatoire." });
      }
      
      const googleAccessToken = req.headers["x-google-access-token"] || null;
      
      const { result, sourceKeys } = await executeTool(toolName, params, { userId: req.userId, googleAccessToken });
      const sources = sourceKeys.map((k) => OPEN_SOURCES[k]).filter(Boolean);
      
      return res.status(200).json({ success: true, error: false, toolName, result, sources });
      
    } catch (error) {
      logger.error("Erreur /api/tools : " + error.message);
      return res.status(200).json({ success: false, error: true, reply: error.message });
    }
  }
);

// ==================== 404 ====================
app.use((req, res) => {
  res.status(404).json({
    reply: "Route non trouvée : " + req.method + " " + req.originalUrl,
    error: true,
    availableRoutes: [
      "GET /",
      "GET /api/health",
      "GET /api/conversations",
      "POST /api/chat",
      "POST /api/tools",
      "POST /api/whatsapp/connect",
      "POST /api/whatsapp/send",
      "POST /api/intent/init",
      "POST /api/memory/clear"
    ]
  });
});

// ==================== ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  if (err?.type === "entity.parse.failed") {
    return res.status(200).json({ reply: "Corps de requête JSON invalide.", error: true });
  }
  if (err?.message === "Origine non autorisée") {
    return res.status(200).json({ reply: "Origine non autorisée.", error: true });
  }
  if (err?.message?.includes("Type de fichier non supporté")) {
    return res.status(200).json({ reply: err.message, error: true });
  }
  
  logger.error("Erreur non gérée : " + (err?.message || err));
  return res.status(200).json({ reply: "Une erreur interne est survenue.", error: true });
});

// ==================== DÉMARRAGE ====================
const PORT = CONFIG.PORT;
const server = app.listen(PORT, () => {
  logger.info("=".repeat(60));
  logger.info("SERVEUR " + CONFIG.AGENT_NAME.toUpperCase() + " DÉMARRÉ (v" + CONFIG.VERSION + ")");
  logger.info("=".repeat(60));
  logger.info("Port : " + PORT);
  logger.info("Firebase Auth : " + (firebaseApp ? "actif" : "inactif"));
  logger.info("Supabase : " + (supabase ? "connecté" : "non configuré"));
  logger.info("Payload max : " + CONFIG.MAX_MESSAGE_LENGTH + " caractères");
  logger.info("Retry max : " + CONFIG.MAX_RETRY_ATTEMPTS + " tentatives");
  logger.info("Vision : activé");
  logger.info("Multi-appareils : " + (supabase ? "synchronisé" : "local"));
  logger.info("=".repeat(60));
});

server.timeout = CONFIG.V250_ROUTE_TIMEOUT;
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

// ==================== ARRÊT GRACIEUX ====================
let shuttingDown = false;
async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info("Arrêt gracieux (" + signal + ")...");
  
  const forceExitTimer = setTimeout(() => {
    logger.error("Arrêt forcé après délai dépassé");
    process.exit(1);
  }, 10000);
  
  try {
    await queueManager.close();
    await new Promise((resolve) => db.close(() => resolve()));
    server.close(() => {
      clearTimeout(forceExitTimer);
      logger.info("Arrêt terminé");
      process.exit(0);
    });
  } catch (err) {
    logger.error("Erreur pendant l'arrêt : " + err.message);
    clearTimeout(forceExitTimer);
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (error) => logger.error("Erreur non capturée : " + error.message));
process.on("unhandledRejection", (reason) => logger.error("Promesse rejetée : " + reason));

// ==================== EXPORT ====================
module.exports = app;
