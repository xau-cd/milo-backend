// ==================== INDEX.JS - CERVEAU LUBA (HIKLON TECHNOLOGIES) ====================
// Version : 9.1.0
// Technologies : Express, SQLite, @whiskeysockets/baileys (WhatsApp, sans Chrome),
//                axios, Groq + OpenRouter (routage multi-tier v100/v250),
//                Firebase Admin (vérification d'identité, optionnelle mais recommandée),
//                sources ouvertes sans clé API (actualités, sport, science, social, météo),
//                BullMQ/Redis (file d'attente, optionnel), Gmail API/Resend/SMTP (email)
//
// ⚠️ CHANGEMENTS IMPORTANTS DE CETTE VERSION 9.1.0 :
// 1) ARCHITECTURE LLM ENTERPRISE : Matrice de repli en cascade avec 4 niveaux de fallback
//    pour v100 et 3 niveaux par étape pour v250. Suffixe :free automatique sur OpenRouter.
// 2) GARDE-FOU : Dégradation gracieuse automatique v250 → v100 en cas d'échec complet.
//    L'utilisateur ne voit JAMAIS d'erreur technique (402, 404, 429, 500, timeout).
// 3) INTERCEPTEUR D'ERREURS GLOBAL : executeWithRetryAndFallback avec backoff exponentiel,
//    circuit breaker implicite et logging détaillé de chaque tentative.
// 4) GESTION DE CONTEXTE OPTIMISÉE : ConversationContextManager avec estimation des tokens
//    et troncature intelligente de l'historique pour respecter les fenêtres de contexte.
// 5) MONITORING ENRICHÉ : /api/health retourne l'état des providers LLM avec latence.

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
  VERSION: "9.1.0",
  AGENT_NAME: "Luba",
  MAX_MESSAGE_LENGTH: 2000,
  MAX_HISTORY_LENGTH: 15,
  IMAGE_SEARCH_LIMIT: 6,
  HTTP_USER_AGENT: process.env.HTTP_USER_AGENT || "LubaAI-App/1.0 (contact@luba.ia)",
  WHATSAPP_QR_TIMEOUT: 30000,
  WHATSAPP_RETRY_DELAY: 3000,
  V250_STEP_TIMEOUT: 55000,
  V250_ROUTE_TIMEOUT: 130000,
  DB_PATH: path.join(__dirname, "data", "luba.db"),
  SESSIONS_PATH: path.join(__dirname, "sessions"),
  MAX_CONTEXT_TOKENS: 6000
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

if (!process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  console.warn(
    "⚠️ SÉCURITÉ : FIREBASE_SERVICE_ACCOUNT_JSON non configurée — les userId ne sont PAS vérifiés cryptographiquement. " +
      "N'importe qui peut actuellement se faire passer pour n'importe quel utilisateur. À configurer avant une vraie mise en production."
  );
}
if (!process.env.WHATSAPP_WEBHOOK_SECRET) {
  console.warn("⚠️ WHATSAPP_WEBHOOK_SECRET non configuré (passerelle externe optionnelle uniquement).");
}
if (!process.env.REDIS_URL) {
  console.warn("⚠️ REDIS_URL non configurée — file d'attente WhatsApp en mémoire (non persistante).");
}

// ==================== CRÉATION DES DOSSIERS ====================
const dataDir = path.join(__dirname, "data");
const sessionsDir = path.join(__dirname, "sessions");
for (const dir of [dataDir, sessionsDir]) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Dossier créé: ${dir}`);
  }
}

// ==================== INITIALISATION SQLITE ====================
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
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    last_seen_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`ALTER TABLE users ADD COLUMN last_seen_at DATETIME`, (err) => {
    if (err && !/duplicate column/i.test(err.message)) {
      console.error("⚠️ Erreur migration last_seen_at:", err.message);
    }
  });

  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
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

  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

console.log("✅ Base de données SQLite initialisée");

// ==================== FIREBASE ADMIN (vérification d'identité, optionnelle) ====================
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
      throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON invalide (ni JSON direct, ni base64 valide)");
    }
  }
}

if (firebaseAdmin && process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  try {
    const serviceAccount = parseFirebaseServiceAccount(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    firebaseApp = firebaseAdmin.initializeApp({ credential: firebaseAdmin.credential.cert(serviceAccount) });
    console.log("✅ Firebase Admin initialisé — les tokens utilisateur sont maintenant vérifiés cryptographiquement");
  } catch (e) {
    console.error("❌ Erreur initialisation Firebase Admin:", e.message);
  }
} else if (!firebaseAdmin && process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_JSON est définie mais le paquet firebase-admin n'est pas installé (npm install firebase-admin).");
}

// ==================== CONFIGURATION NODEMAILER (dernier recours) ====================
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
    if (err) console.error("⚠️ SMTP non joignable:", err.message);
    else console.log("✅ SMTP prêt (dernier recours email)");
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

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || ALLOWED_ORIGINS.includes(origin)) callback(null, true);
      else {
        console.warn(`⚠️ Origine CORS refusée: ${origin}`);
        callback(new Error("Origine non autorisée"));
      }
    },
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-user-id", "X-Google-Access-Token"],
    credentials: true,
    maxAge: 86400
  })
);

app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));
app.use(
  express.json({
    limit: "10mb",
    verify: (req, res, buf) => {
      req.rawBody = buf;
    }
  })
);
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITERS ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(200).json({ reply: "⚠️ Trop de requêtes. Réessaie dans 15 minutes.", error: true })
});
const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(200).json({ reply: "⚠️ Limite de requêtes atteinte.", error: true })
});
const webhookLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: true, reply: "Trop de requêtes webhook." })
});

// ==================== LOGGING ====================
app.use((req, res, next) => {
  const requestId = crypto.randomUUID();
  const start = Date.now();
  req.requestId = requestId;
  console.log(`\n📥 [${requestId}] ${req.method} ${req.url}`);
  if (req.body && Object.keys(req.body).length > 0) console.log(`   Body: ${JSON.stringify(req.body).slice(0, 300)}`);
  res.on("finish", () => console.log(`📤 [${requestId}] ${res.statusCode} - ${Date.now() - start}ms`));
  next();
});

const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

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

// ==================== AUTHENTIFICATION (avec vérification Firebase optionnelle) ====================
const authenticateUser = asyncHandler(async (req, res, next) => {
  const providedUserId = req.body.userId || req.query.userId || req.headers["x-user-id"];
  const authHeader = req.headers.authorization || req.headers.Authorization;
  const bearerToken = authHeader && authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;

  let verifiedUserId = null;
  let verifiedEmail = null;
  let verifiedName = null;

  if (firebaseApp && bearerToken) {
    try {
      const decoded = await firebaseAdmin.auth(firebaseApp).verifyIdToken(bearerToken);
      verifiedUserId = decoded.uid;
      verifiedEmail = decoded.email || null;
      verifiedName = decoded.name || null;
    } catch (error) {
      console.warn("⚠️ Token Firebase invalide/expiré:", error.message);
      return res.status(200).json({ reply: "⚠️ Session invalide ou expirée. Reconnecte-toi.", error: true, code: "INVALID_TOKEN" });
    }
  }

  const userId = verifiedUserId || (typeof providedUserId === "string" ? providedUserId.trim() : null);

  if (!userId) {
    return res.status(200).json({ reply: "⚠️ Authentification requise. Fournis un userId ou un token valide.", error: true });
  }

  if (firebaseApp && !verifiedUserId) {
    console.warn(`⚠️ Requête NON vérifiée cryptographiquement pour userId=${userId} (Firebase configuré mais aucun Bearer token fourni)`);
  }

  req.userId = userId;
  req.verifiedIdentity = Boolean(verifiedUserId);

  try {
    const user = await dbGet("SELECT * FROM users WHERE id = ?", [userId]);
    const safeDisplayName =
      verifiedName || (typeof req.body.displayName === "string" ? req.body.displayName.slice(0, 120) : userId);

    if (!user) {
      await dbRun("INSERT INTO users (id, email, display_name, last_seen_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", [
        userId,
        verifiedEmail,
        safeDisplayName
      ]);
    } else {
      await dbRun(
        "UPDATE users SET last_seen_at = CURRENT_TIMESTAMP, email = COALESCE(?, email), display_name = COALESCE(?, display_name) WHERE id = ?",
        [verifiedEmail, verifiedName, userId]
      );
    }
    next();
  } catch (err) {
    console.error("❌ Erreur authentification:", err.message);
    return res.status(200).json({ reply: "⚠️ Erreur interne.", error: true });
  }
});

function extractGoogleAccessToken(req) {
  if (req.headers["x-google-access-token"]) return String(req.headers["x-google-access-token"]).trim();
  return null;
}

// ==================== SOURCES OUVERTES (sans clé API) ====================
const OPEN_SOURCES = {
  wikipedia: { name: "Wikipédia", url: "https://fr.wikipedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikipedia.org" },
  wikimediacommons: { name: "Wikimedia Commons", url: "https://commons.wikimedia.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=wikimedia.org" },
  googlenews: { name: "Google News", url: "https://news.google.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=news.google.com" },
  thesportsdb: { name: "TheSportsDB", url: "https://www.thesportsdb.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=thesportsdb.com" },
  arxiv: { name: "arXiv", url: "https://arxiv.org", logo: "https://www.google.com/s2/favicons?sz=64&domain=arxiv.org" },
  reddit: { name: "Reddit", url: "https://reddit.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=reddit.com" },
  openmeteo: { name: "Open-Meteo", url: "https://open-meteo.com", logo: "https://www.google.com/s2/favicons?sz=64&domain=open-meteo.com" }
};

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

// ---- Images (Wikimedia Commons) ----
async function searchWikimediaImages(query, limit = CONFIG.IMAGE_SEARCH_LIMIT) {
  if (!query || typeof query !== "string") return { images: [] };
  try {
    console.log(`🖼️ Recherche d'images: "${query}"`);
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
    console.error("❌ Erreur recherche images:", error.message);
    return { images: [], error: error.message };
  }
}

// ---- Résumé encyclopédique (Wikipédia) ----
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

// ---- Actualités (Google News RSS) ----
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
    console.error("❌ Erreur recherche actualités:", error.message);
    return { articles: [], error: error.message };
  }
}

// ---- Recherche générale (agrégation Wikipédia + actualités, sans clé API) ----
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

// ---- Scores sportifs (TheSportsDB, clé publique de test "3") ----
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
    console.error("❌ Erreur recherche scores sportifs:", error.message);
    return { events: [], error: error.message };
  }
}

// ---- Science (arXiv) ----
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
    console.error("❌ Erreur recherche scientifique (arXiv):", error.message);
    return { papers: [], error: error.message };
  }
}

// ---- Réseaux sociaux (Reddit, lecture publique) ----
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
    console.error("❌ Erreur recherche réseaux sociaux (Reddit):", error.message);
    return { posts: [], error: error.message };
  }
}

// ---- Météo (Open-Meteo, géocodage + prévisions, sans clé) ----
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
    console.error("❌ Erreur météo:", error.message);
    return { error: error.message };
  }
}

// ==================== ENVOI D'EMAIL : GMAIL API (OAuth) → RESEND → SMTP ====================
async function sendEmailViaGmail(accessToken, recipient, subject, body) {
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
    console.error("❌ Erreur envoi email via Resend:", apiError);
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
    console.error("❌ Erreur envoi email SMTP:", error.message);
    return { success: false, error: error.message };
  }
}

async function dispatchSendEmail({ googleAccessToken, recipient, subject, body }) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!recipient || !emailRegex.test(String(recipient).trim())) {
    return { success: false, error: "Adresse email destinataire invalide" };
  }

  let result;
  if (googleAccessToken) {
    try {
      result = await sendEmailViaGmail(googleAccessToken, recipient, subject, body);
    } catch (error) {
      const apiError = error.response?.data?.error?.message || error.message;
      console.error("❌ Erreur envoi email via Gmail API:", apiError);
      result = { success: false, error: `Gmail API: ${apiError}` };
    }
  } else if (process.env.RESEND_API_KEY) {
    result = await sendEmailViaResend(recipient, subject, body);
  } else {
    result = await sendEmailViaSMTP(recipient, subject, body);
  }

  try {
    await dbRun("INSERT INTO email_logs (to_email, subject, status) VALUES (?, ?, ?)", [
      recipient,
      subject || null,
      result.success ? "sent" : "failed"
    ]);
  } catch (logErr) {
    console.error("⚠️ Impossible de journaliser l'email:", logErr.message);
  }

  return result;
}

// ==================== PASSERELLE WHATSAPP EXTERNE OPTIONNELLE (Evolution API) ====================
const WHATSAPP_GATEWAY = {
  baseURL: (process.env.WHATSAPP_GATEWAY_URL || "").replace(/\/+$/, ""),
  apiKey: process.env.WHATSAPP_GATEWAY_API_KEY || "",
  instance: process.env.WHATSAPP_INSTANCE_NAME || "luba"
};

const whatsappGateway = {
  configured() {
    return Boolean(WHATSAPP_GATEWAY.baseURL && WHATSAPP_GATEWAY.apiKey);
  },
  async getQRCode() {
    if (!this.configured()) throw new Error("Passerelle WhatsApp externe non configurée");
    const response = await axios.get(`${WHATSAPP_GATEWAY.baseURL}/instance/connect/${WHATSAPP_GATEWAY.instance}`, {
      headers: { apikey: WHATSAPP_GATEWAY.apiKey },
      timeout: 15000
    });
    return response.data;
  },
  async getStatus() {
    if (!this.configured()) throw new Error("Passerelle WhatsApp externe non configurée");
    const response = await axios.get(`${WHATSAPP_GATEWAY.baseURL}/instance/connectionState/${WHATSAPP_GATEWAY.instance}`, {
      headers: { apikey: WHATSAPP_GATEWAY.apiKey },
      timeout: 10000
    });
    return response.data;
  },
  async sendMessage(phoneNumber, message) {
    if (!this.configured()) throw new Error("Passerelle WhatsApp externe non configurée");
    const cleanNumber = String(phoneNumber).replace(/[^\d]/g, "");
    if (!cleanNumber) throw new Error("Numéro de téléphone invalide");
    const response = await axios.post(
      `${WHATSAPP_GATEWAY.baseURL}/message/sendText/${WHATSAPP_GATEWAY.instance}`,
      { number: cleanNumber, text: message },
      { headers: { apikey: WHATSAPP_GATEWAY.apiKey, "Content-Type": "application/json" }, timeout: 15000 }
    );
    return response.data;
  }
};

function verifyWhatsappWebhookSignature(req) {
  const secret = process.env.WHATSAPP_WEBHOOK_SECRET;
  if (!secret) return true;
  const providedApiKey = req.headers["apikey"];
  if (providedApiKey && providedApiKey === secret) return true;
  const signatureHeader = req.headers["x-webhook-signature"] || req.headers["x-hub-signature-256"];
  if (signatureHeader && req.rawBody) {
    const expected = crypto.createHmac("sha256", secret).update(req.rawBody).digest("hex");
    const provided = String(signatureHeader).replace(/^sha256=/, "");
    try {
      return crypto.timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(provided, "hex"));
    } catch (e) {
      return false;
    }
  }
  return false;
}

// ==================== FILE D'ATTENTE RÉSILIENTE (BullMQ+Redis, ou repli mémoire) ====================
let BullMQ = null;
let IORedis = null;
try {
  BullMQ = require("bullmq");
  IORedis = require("ioredis");
} catch (e) {
  BullMQ = null;
  IORedis = null;
}

const useRedisQueue = Boolean(process.env.REDIS_URL) && Boolean(BullMQ) && Boolean(IORedis);
let whatsappQueue = null;
let whatsappWorker = null;

class InMemoryRetryQueue {
  constructor(processFn, { concurrency = 2, maxAttempts = 5, baseDelayMs = 2000 } = {}) {
    this.processFn = processFn;
    this.concurrency = concurrency;
    this.maxAttempts = maxAttempts;
    this.baseDelayMs = baseDelayMs;
    this.pending = [];
    this.active = 0;
  }
  push(data) {
    this.pending.push({ data, attempts: 0 });
    this._drain();
  }
  _drain() {
    while (this.active < this.concurrency && this.pending.length > 0) {
      const job = this.pending.shift();
      this.active++;
      this._process(job).finally(() => {
        this.active--;
        this._drain();
      });
    }
  }
  async _process(job) {
    try {
      await this.processFn(job.data);
    } catch (err) {
      job.attempts++;
      console.error(`⚠️ Échec job WhatsApp (tentative ${job.attempts}/${this.maxAttempts}):`, err.message);
      if (job.attempts < this.maxAttempts) {
        const delay = this.baseDelayMs * Math.pow(2, job.attempts - 1);
        setTimeout(() => {
          this.pending.push(job);
          this._drain();
        }, delay);
      } else {
        console.error(`❌ Abandon définitif du job WhatsApp après ${this.maxAttempts} tentatives:`, JSON.stringify(job.data));
      }
    }
  }
}

let inMemoryWhatsappQueue = null;

async function processWhatsAppSendJob({ userId, phoneNumber, message }) {
  if (whatsappManager.sessions.get(userId)?.ready) {
    await whatsappManager.sendMessage(userId, phoneNumber, message);
  } else if (whatsappGateway.configured()) {
    await whatsappGateway.sendMessage(phoneNumber, message);
  } else {
    throw new Error("Aucun canal WhatsApp disponible pour l'envoi");
  }
}

if (useRedisQueue) {
  const connection = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
  whatsappQueue = new BullMQ.Queue("whatsapp-outbound", { connection });
  whatsappWorker = new BullMQ.Worker("whatsapp-outbound", async (job) => processWhatsAppSendJob(job.data), {
    connection,
    concurrency: 3,
    limiter: { max: 10, duration: 1000 }
  });
  whatsappWorker.on("failed", (job, err) => console.error(`❌ Job WhatsApp BullMQ échoué (${job?.id}):`, err.message));
  console.log("✅ File d'attente WhatsApp : BullMQ + Redis");
} else {
  inMemoryWhatsappQueue = new InMemoryRetryQueue(processWhatsAppSendJob, { concurrency: 2, maxAttempts: 5, baseDelayMs: 2000 });
  console.log("⚠️ File d'attente WhatsApp : en mémoire (repli)");
}

async function enqueueWhatsAppSend(userId, phoneNumber, message) {
  if (useRedisQueue) {
    await whatsappQueue.add(
      "send",
      { userId, phoneNumber, message },
      { attempts: 5, backoff: { type: "exponential", delay: 2000 }, removeOnComplete: 100, removeOnFail: 500 }
    );
  } else {
    inMemoryWhatsappQueue.push({ userId, phoneNumber, message });
  }
}

async function sendWhatsAppSmart(userId, phoneNumber, message) {
  await enqueueWhatsAppSend(userId, phoneNumber, message);
  return { success: true, queued: true, provider: "queue" };
}

// ==================== GESTION WHATSAPP — BAILEYS (WebSocket natif, sans Chrome) ====================
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

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          sessionData.qrCode = await qrcode.toDataURL(qr, { width: 600, margin: 2 });
          console.log(`📱 QR Code (Baileys) généré pour ${userId}`);
        } catch (e) {
          console.error("❌ Erreur génération QR (Baileys):", e.message);
        }
      }

      if (connection === "open") {
        sessionData.ready = true;
        sessionData.qrCode = null;
        db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
        console.log(`✅ WhatsApp (Baileys) connecté pour ${userId}`);
      }

      if (connection === "close") {
        sessionData.ready = false;
        db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        console.warn(`⚠️ WhatsApp (Baileys) déconnecté pour ${userId} (code ${statusCode}). Reconnexion auto: ${shouldReconnect}`);
        this.sessions.delete(userId);
        if (shouldReconnect) {
          setTimeout(() => {
            this.initClient(userId).catch((e) => console.error("❌ Reconnexion Baileys échouée:", e.message));
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
            message: text.slice(0, CONFIG.MAX_MESSAGE_LENGTH),
            channel: "whatsapp",
            modelTier: "v100"
          });

          if (result?.reply) {
            await sock.sendMessage(remoteJid, { text: toPlainWhatsAppText(result.reply) || "🙂" });
          }
        } catch (err) {
          console.error("❌ Erreur traitement message entrant WhatsApp (Baileys):", err.message);
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
        console.error(`⚠️ Erreur fermeture socket WhatsApp (${userId}):`, e.message);
      }
    }
  }
}

const whatsappManager = new BaileysManager();

// ==================== SYSTEM PROMPT LUBA ====================
const LUBA_SYSTEM_PROMPT = {
  role: "system",
  content: `Tu es LUBA (Luba.ia), une intelligence artificielle créée par HIKLON Technology, une startup basée à Kinshasa, fondée en 2026.

IDENTITÉ (à respecter strictement) :
- Tu t'appelles Luba (ou Luba.ia). Tu ne t'appelles JAMAIS Milo, Milou, ou tout autre nom.
- Si on te demande qui t'a créée : IA développée par HIKLON Technology, startup à Kinshasa, fondée en 2026.
- Ton ton est chaleureux, intelligent et proactif.

RÈGLE SUR LES DONNÉES (OBLIGATOIRE, PRODUCTION — AUCUNE SIMULATION) :
- Tu ne dois JAMAIS inventer un score sportif, une actualité, un résultat de recherche, une donnée météo ou toute information factuelle changeante. Utilise TOUJOURS l'outil approprié pour obtenir une donnée réelle.
- Si un outil échoue ou ne retourne rien, dis-le honnêtement à l'utilisateur. N'invente jamais un résultat de remplacement.
- Ne mentionne JAMAIS toi-même de sources dans replyText : le backend les ajoute automatiquement à partir des outils réellement exécutés.

RÈGLE STRICTE SUR LES IMAGES (OBLIGATOIRE) :
- Dès que tu décris une personnalité, un lieu, un objet, un concept scientifique ou un événement, utilise TOUJOURS search_images.
- Dès qu'une recherche ou une information est demandée, ajoute TOUJOURS un appel à search_images en complément.

RÈGLE SUR LES SUGGESTIONS (OBLIGATOIRE) :
- Le champ "suggestions" doit TOUJOURS contenir 3 à 4 questions de suivi courtes et cliquables, directement liées à ta réponse. Jamais dans replyText — uniquement dans ce champ dédié.

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
- send_whatsapp_message : Envoyer un message WhatsApp réel (arguments: { phone_number, message })`
};

// ==================== ARCHITECTURE LLM ENTERPRISE - ROUTAGE MULTI-TIER ====================
const LLM_PROVIDERS = {
  GROQ: { 
    baseURL: "https://api.groq.com/openai/v1", 
    apiKey: process.env.GROQ_API_KEY || "", 
    timeout: 30000, 
    maxTokens: 1500, 
    temperature: 0.7 
  },
  OPENROUTER: { 
    baseURL: "https://openrouter.ai/api/v1", 
    apiKey: process.env.OPENROUTER_API_KEY || "", 
    timeout: 45000, 
    maxTokens: 1500, 
    temperature: 0.7 
  }
};

// ==================== MATRICE DE REPLI EN CASCADE ====================
const MODEL_TIERS = {
  v100: {
    name: "Mwamba",
    description: "Rapide - Réponses instantanées",
    providers: [
      {
        provider: "groq",
        model: process.env.GROQ_MODEL_V100 || "openai/gpt-oss-120b",
        maxTokens: 1500,
        timeout: 30000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 0
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_1 || "qwen/qwen-2.5-coder-32b-instruct:free",
        maxTokens: 1500,
        timeout: 45000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 1
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free",
        maxTokens: 1500,
        timeout: 45000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 2
      },
      {
        provider: "openrouter",
        model: process.env.OPENROUTER_MODEL_V100_FALLBACK_3 || "microsoft/phi-4:free",
        maxTokens: 1500,
        timeout: 45000,
        temperature: 0.7,
        jsonMode: true,
        failoverPriority: 3
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
          timeout: 55000,
          temperature: 0.3,
          jsonMode: false,
          failoverPriority: 0
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_REASONING_FALLBACK || "deepseek/deepseek-r1-distill-llama-70b:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_REASONING_MAX_TOKENS || "8000", 10),
          timeout: 55000,
          temperature: 0.3,
          jsonMode: false,
          failoverPriority: 1
        },
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_REASONING_FALLBACK || "openai/gpt-oss-120b",
          maxTokens: parseInt(process.env.GROQ_V250_REASONING_MAX_TOKENS || "4000", 10),
          timeout: 30000,
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
          timeout: 55000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 0
        },
        {
          provider: "groq",
          model: process.env.GROQ_MODEL_V250_CODE_FALLBACK || "openai/gpt-oss-120b",
          maxTokens: parseInt(process.env.GROQ_V250_CODE_MAX_TOKENS || "8000", 10),
          timeout: 30000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 1
        },
        {
          provider: "openrouter",
          model: process.env.OPENROUTER_MODEL_V250_CODE_FALLBACK_2 || "meta-llama/llama-3.3-70b-instruct:free",
          maxTokens: parseInt(process.env.OPENROUTER_V250_CODE_MAX_TOKENS || "8000", 10),
          timeout: 45000,
          temperature: 0.5,
          jsonMode: true,
          failoverPriority: 2
        }
      ]
    },
    maxRetries: 2,
    degradedMode: true
  }
};

// ==================== VALIDATION & FILTRE OPENROUTER ====================
function validateAndSanitizeOpenRouterModel(model) {
  if (!model || typeof model !== "string") {
    return null;
  }
  
  const knownProviders = [
    "openai/", "qwen/", "meta-llama/", "deepseek/", "microsoft/", 
    "anthropic/", "google/", "mistralai/", "cohere/"
  ];
  
  const isOpenRouterModel = knownProviders.some(prefix => model.includes(prefix));
  
  if (isOpenRouterModel) {
    if (!model.includes(":free") && !model.includes(":paid") && !model.includes(":beta")) {
      return `${model}:free`;
    }
  }
  
  return model;
}

// ==================== INTERCEPTEUR D'ERREURS GLOBAL ====================
class LLMErrorInterceptor {
  static isRetryableError(error) {
    const status = error.response?.status;
    const retryableStatuses = [402, 404, 408, 429, 500, 502, 503, 504];
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

// ==================== WRAPPER EXECUTE WITH RETRY AND FALLBACK ====================
async function executeWithRetryAndFallback(providerList, promptParams, options = {}) {
  const {
    maxRetriesPerProvider = 1,
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
    
    console.log(`[LLM-FALLBACK] Tentative ${i + 1}/${sortedProviders.length} : ${provider}/${model}`);
    
    for (let attempt = 0; attempt <= maxRetriesPerProvider; attempt++) {
      try {
        const timeout = providerConfig.timeout * (attempt > 0 ? timeoutMultiplier : 1);
        
        const result = await callProviderRaw({
          provider,
          model,
          messages: promptParams.messages,
          jsonMode: providerConfig.jsonMode,
          timeout,
          maxTokens: providerConfig.maxTokens,
          temperature: providerConfig.temperature
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
        
        console.log(`[LLM-FALLBACK] ✅ Succès avec ${provider}/${model} (tentative ${attempt + 1})`);
        
        return {
          success: true,
          ...providerResult,
          providerChain: providerResults
        };
        
      } catch (error) {
        lastError = error;
        const errorCode = LLMErrorInterceptor.getErrorCode(error);
        
        console.error(`[LLM-FALLBACK] ❌ Échec ${provider}/${model} (tentative ${attempt + 1}): ${errorCode} - ${error.message}`);
        
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
        
        if (!LLMErrorInterceptor.isRetryableError(error) && attempt === maxRetriesPerProvider) {
          break;
        }
        
        if (attempt < maxRetriesPerProvider) {
          const retryDelay = Math.min(1000 * Math.pow(2, attempt), 5000);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        }
      }
    }
  }
  
  console.error(`[LLM-FALLBACK] ❌ Tous les fournisseurs ont échoué. Dernière erreur: ${lastError?.message}`);
  
  return {
    success: false,
    error: lastError,
    providerChain: providerResults,
    errorCode: LLMErrorInterceptor.getErrorCode(lastError)
  };
}

// ==================== APPEL PROVIDER BRUT ====================
async function callProviderRaw({ provider, model, messages, jsonMode = false, timeout, maxTokens, temperature = 0.7 }) {
  const cfg = provider === "groq" ? LLM_PROVIDERS.GROQ : LLM_PROVIDERS.OPENROUTER;
  
  if (!cfg.apiKey) {
    const err = new Error(`Clé API manquante pour le fournisseur ${provider}`);
    err.code = "MISSING_API_KEY";
    throw err;
  }
  
  const payload = { 
    model, 
    messages, 
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
async function callLLM_v100(messages) {
  console.log("[LLM-v100] Démarrage du routage Mwamba");
  
  const providers = MODEL_TIERS.v100.providers;
  
  const result = await executeWithRetryAndFallback(
    providers,
    { messages: [LUBA_SYSTEM_PROMPT, ...messages] },
    {
      maxRetriesPerProvider: 1,
      timeoutMultiplier: 1.5,
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
async function callLLM_v250(messages, userMessage) {
  console.log("[LLM-v250] Démarrage du pipeline Ngandu");
  
  const tier = MODEL_TIERS.v250;
  let providerChain = [];
  
  // ==================== ÉTAPE 1 : RAISONNEMENT ====================
  console.log("[LLM-v250] Étape 1: Raisonnement");
  
  const reasoningMessages = [
    {
      role: "system",
      content: "Analyse ce problème complexe. Effectue les démonstrations mathématiques nécessaires, isole les edge cases et rédige le pseudo-code/l'architecture. Sois complet et rigoureux : cette analyse servira directement à générer le code final."
    },
    ...messages
  ];
  
  const reasoningResult = await executeWithRetryAndFallback(
    tier.reasoning.providers,
    { messages: reasoningMessages },
    {
      maxRetriesPerProvider: 1,
      timeoutMultiplier: 1.2,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-v250-R1] Failover raisonnement: ${failInfo.provider}/${failInfo.model}`);
      }
    }
  );
  
  if (!reasoningResult.success || !reasoningResult.response || reasoningResult.response.trim().length < 40) {
    console.error("[LLM-v250] Échec de l'étape de raisonnement - dégradation vers v100");
    return await degradedFallbackToV100(messages, "reasoning_failed");
  }
  
  const reasoningAnalysis = reasoningResult.response;
  providerChain.push(`R1:${reasoningResult.providerUsed}/${reasoningResult.modelUsed}`);
  
  console.log(`[LLM-v250] ✅ Raisonnement terminé via ${reasoningResult.providerUsed}/${reasoningResult.modelUsed}`);
  
  // ==================== ÉTAPE 2 : GÉNÉRATION DE CODE ====================
  console.log("[LLM-v250] Étape 2: Génération de code");
  
  const codeMessages = [
    {
      role: "system",
      content: `Génère le code de production complet, typé, sécurisé et documenté en te basant strictement sur le plan ci-dessous.

PLAN / ANALYSE (étape 1 — raisonnement) :
${reasoningAnalysis}

Tu DOIS répondre au format JSON strict : { "replyText": "réponse complète en Markdown avec le code", "toolCalls": [], "suggestions": ["question de suivi 1 ?", "question de suivi 2 ?", "question de suivi 3 ?"] }.`
    },
    { role: "user", content: userMessage }
  ];
  
  const codeResult = await executeWithRetryAndFallback(
    tier.code.providers,
    { messages: codeMessages },
    {
      maxRetriesPerProvider: 1,
      timeoutMultiplier: 1.2,
      onProviderFail: (failInfo) => {
        console.warn(`[LLM-v250-R2] Failover code: ${failInfo.provider}/${failInfo.model}`);
      }
    }
  );
  
  if (!codeResult.success || !codeResult.response) {
    console.error("[LLM-v250] Échec de l'étape de génération de code - dégradation vers v100");
    return await degradedFallbackToV100(messages, "code_generation_failed");
  }
  
  providerChain.push(`R2:${codeResult.providerUsed}/${codeResult.modelUsed}`);
  
  console.log(`[LLM-v250] ✅ Code généré via ${codeResult.providerUsed}/${codeResult.modelUsed}`);
  
  return {
    ...codeResult.response,
    providerUsed: "pipeline_v250",
    modelUsed: providerChain.join(" -> "),
    degraded: false,
    providerChain,
    reasoningProviderUsed: reasoningResult.providerUsed
  };
}

// ==================== GARDE-FOU : DÉGRADATION GRACIEUSE ====================
async function degradedFallbackToV100(messages, reason) {
  console.warn(`[LLM-GARDE-FOU] Dégradation gracieuse vers v100 (${reason})`);
  
  try {
    const fallbackResult = await callLLM_v100(messages);
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
async function getSession(conversationId, userId) {
  const session = await dbGet("SELECT * FROM sessions WHERE session_id = ?", [conversationId]);
  if (session) {
    await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
    return session;
  }
  await dbRun("INSERT INTO sessions (session_id, user_id) VALUES (?, ?)", [conversationId, userId]);
  return { session_id: conversationId, user_id: userId, active_intent: null, intent_data: null };
}

async function getHistory(conversationId, limit = CONFIG.MAX_HISTORY_LENGTH) {
  const rows = await dbAll("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?", [conversationId, limit]);
  return rows.reverse().map((row) => ({ role: row.role, content: row.content }));
}

async function saveMessage(conversationId, role, content) {
  await dbRun("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", [conversationId, role, content]);
  await dbRun("UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE session_id = ?", [conversationId]);
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
    console.error("❌ intent_data corrompu, réinitialisation:", e.message);
    return null;
  }
}

async function clearActiveIntent(conversationId) {
  await dbRun("UPDATE sessions SET active_intent = NULL, intent_data = NULL WHERE session_id = ?", [conversationId]);
}

async function assertConversationOwnership(conversationId, userId) {
  const existing = await dbGet("SELECT user_id FROM sessions WHERE session_id = ?", [conversationId]);
  if (existing && existing.user_id && existing.user_id !== userId) {
    const err = new Error("Cette conversation n'appartient pas à cet utilisateur.");
    err.code = "CONVERSATION_OWNERSHIP";
    throw err;
  }
}

// ==================== DISPATCHER D'OUTILS UNIFIÉ (boucle ReAct + /api/tools) ====================
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

// ==================== BOUCLE REACT (Tier v100) / PIPELINE (Tier v250) ====================
async function handleChat({ conversationId, userId, message, googleAccessToken = null, channel = "web", modelTier = "v100" }) {
  console.log(`[CHAT] Démarrage conversation ${conversationId} - Tier: ${modelTier} - Channel: ${channel}`);
  
  await getSession(conversationId, userId);

  const activeIntent = await getActiveIntent(conversationId);
  if (activeIntent) {
    return await handleActiveIntent(conversationId, activeIntent, message, { userId, googleAccessToken });
  }

  await saveMessage(conversationId, "user", message);

  const context = await contextManager.getCleanContext(conversationId, message);
  const messages = context.messages;

  console.log(`[CHAT] Contexte préparé - ${context.historyLength} messages d'historique, ~${context.estimatedTokens} tokens estimés`);

  let finalResponse = null;
  let imageUrls = [];
  let providerUsed = "unknown";
  let suggestions = [];
  const usedSources = new Set();
  let degraded = false;

  try {
    if (modelTier === "v250") {
      const result = await callLLM_v250(messages, message);
      finalResponse = result.replyText || "Je n'ai pas pu générer une réponse.";
      suggestions = Array.isArray(result.suggestions) ? result.suggestions.slice(0, 4) : [];
      providerUsed = result.providerUsed || "pipeline_v250";
      degraded = result.degraded || false;
      
      console.log(`[CHAT] Réponse v250 générée via ${providerUsed}${degraded ? ' (dégradé)' : ''}`);
      
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

    if (imageUrls.length > 0) {
      const imageMarkdown = imageUrls.map((url, index) => `![Image ${index + 1}](${url})`).join("\n\n");
      finalResponse += `\n\n---\n\n📷 **Illustrations :**\n\n${imageMarkdown}`;
      usedSources.add("wikimediacommons");
    }

    if (usedSources.size > 0) {
      const sourceLines = Array.from(usedSources)
        .map((key) => OPEN_SOURCES[key])
        .filter(Boolean)
        .map((src) => `[![${src.name}](${src.logo})](${src.url}) ${src.name}`);
      if (sourceLines.length > 0) finalResponse += `\n\n---\n\n**Sources :** ${sourceLines.join(" · ")}`;
    }

    await saveMessage(conversationId, "assistant", finalResponse);

    console.log(`[CHAT] ✅ Réponse finale générée (${finalResponse.length} caractères)`);

    return {
      reply: finalResponse,
      images: imageUrls,
      error: providerUsed.startsWith("error"),
      providerUsed,
      modelTier,
      degraded,
      suggestions,
      sources: Array.from(usedSources).map((key) => OPEN_SOURCES[key]).filter(Boolean)
    };

  } catch (error) {
    console.error("[CHAT] Erreur critique:", error.message);

    const fallbackResponse = {
      reply: "Je suis momentanément indisponible. Nos équipes techniques travaillent à résoudre le problème. Veuillez réessayer dans quelques instants.",
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

    await saveMessage(conversationId, "assistant", fallbackResponse.reply);

    return fallbackResponse;
  }
}

// ==================== GESTION DES INTENTIONS GUIDÉES (multi-tour) ====================
async function handleActiveIntent(conversationId, activeIntent, userMessage, context = {}) {
  const { userId, googleAccessToken } = context;

  switch (activeIntent.type) {
    case "WHATSAPP": {
      const data = activeIntent.data;
      if (data.step === "NEED_NUMBER") {
        const phoneRegex = /^(\+?\d{1,3}[-.\s]?)?\d{9,15}$/;
        if (phoneRegex.test(userMessage.trim())) {
          await setActiveIntent(conversationId, "WHATSAPP", { step: "NEED_MESSAGE", recipient: userMessage.trim() });
          await saveMessage(conversationId, "user", userMessage);
          await saveMessage(conversationId, "assistant", `✅ Numéro enregistré. Quel message voulez-vous envoyer ?`);
          return { reply: `✅ Numéro enregistré. Quel message voulez-vous envoyer à ${userMessage.trim()} ?`, error: false };
        }
        return { reply: "⚠️ Numéro invalide. Veuillez fournir un numéro valide (ex: +33612345678).", error: true };
      }
      if (data.step === "NEED_MESSAGE") {
        try {
          await sendWhatsAppSmart(userId, data.recipient, userMessage);
          await clearActiveIntent(conversationId);
          await saveMessage(conversationId, "user", userMessage);
          await saveMessage(conversationId, "assistant", `✅ Message mis en file d'envoi vers ${data.recipient}`);
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
        if (result.success) return { reply: `✅ Email envoyé à ${data.recipient} (via ${result.provider}) !`, error: false };
        return { reply: `⚠️ Erreur: ${result.error}`, error: true };
      }
      break;
    }
  }

  await clearActiveIntent(conversationId);
  return { reply: "Je ne comprends plus l'action. Recommençons.", error: true };
}

// ==================== ROUTES ====================
app.get("/", (req, res) => {
  res.json({ reply: `✅ Serveur ${CONFIG.AGENT_NAME} opérationnel`, error: false, version: CONFIG.VERSION });
});

// ==================== HEALTH CHECK AMÉLIORÉ ====================
app.get("/api/health", asyncHandler(async (req, res) => {
  let dbOk = true;
  try {
    await dbGet("SELECT 1", []);
  } catch (e) {
    dbOk = false;
  }

  const llmHealth = {
    groq: {
      configured: Boolean(LLM_PROVIDERS.GROQ.apiKey),
      status: "unknown",
      latency: null
    },
    openrouter: {
      configured: Boolean(LLM_PROVIDERS.OPENROUTER.apiKey),
      status: "unknown",
      latency: null
    }
  };

  if (LLM_PROVIDERS.GROQ.apiKey) {
    const startTime = Date.now();
    try {
      await axios.get("https://api.groq.com/openai/v1/models", {
        headers: { Authorization: `Bearer ${LLM_PROVIDERS.GROQ.apiKey}` },
        timeout: 5000
      });
      llmHealth.groq.status = "healthy";
      llmHealth.groq.latency = Date.now() - startTime;
    } catch (e) {
      llmHealth.groq.status = "degraded";
      llmHealth.groq.latency = Date.now() - startTime;
    }
  }

  if (LLM_PROVIDERS.OPENROUTER.apiKey) {
    const startTime = Date.now();
    try {
      await axios.get("https://openrouter.ai/api/v1/models", {
        headers: { Authorization: `Bearer ${LLM_PROVIDERS.OPENROUTER.apiKey}` },
        timeout: 5000
      });
      llmHealth.openrouter.status = "healthy";
      llmHealth.openrouter.latency = Date.now() - startTime;
    } catch (e) {
      llmHealth.openrouter.status = "degraded";
      llmHealth.openrouter.latency = Date.now() - startTime;
    }
  }

  res.json({
    reply: `✅ Serveur ${CONFIG.AGENT_NAME} en bonne santé`,
    error: !dbOk,
    data: {
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      memory: Math.round(process.memoryUsage().rss / 1024 / 1024) + "MB",
      database: dbOk ? "ok" : "erreur",
      security: { firebaseAuthConfigured: Boolean(firebaseApp) },
      whatsapp: {
        baileysSessionsActives: whatsappManager.sessions.size,
        gatewayExterneConfiguree: whatsappGateway.configured(),
        queue: useRedisQueue ? "bullmq+redis" : "memoire (repli)"
      },
      llmTiers: {
        v100: {
          name: "Mwamba",
          providers: MODEL_TIERS.v100.providers.map(p => ({
            provider: p.provider,
            model: p.model,
            priority: p.failoverPriority
          }))
        },
        v250: {
          name: "Ngandu",
          reasoning: MODEL_TIERS.v250.reasoning.providers.map(p => ({
            provider: p.provider,
            model: p.model,
            priority: p.failoverPriority
          })),
          code: MODEL_TIERS.v250.code.providers.map(p => ({
            provider: p.provider,
            model: p.model,
            priority: p.failoverPriority
          })),
          maxRetries: MODEL_TIERS.v250.maxRetries,
          degradedMode: MODEL_TIERS.v250.degradedMode
        }
      },
      llmHealth,
      llmProviders: { 
        groq: Boolean(LLM_PROVIDERS.GROQ.apiKey), 
        openrouter: Boolean(LLM_PROVIDERS.OPENROUTER.apiKey) 
      },
      openSources: Object.keys(OPEN_SOURCES),
      email: { 
        gmailOAuth: "à la demande", 
        resend: Boolean(process.env.RESEND_API_KEY), 
        smtp: Boolean(emailTransporter) 
      }
    }
  });
}));

app.get(
  "/api/conversations",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const rows = await dbAll("SELECT session_id, created_at, updated_at FROM sessions WHERE user_id = ? ORDER BY updated_at DESC LIMIT 50", [req.userId]);
    const conversations = await Promise.all(
      rows.map(async (row) => {
        const lastMessage = await dbGet("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 1", [row.session_id]);
        return {
          conversationId: row.session_id,
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          lastMessageRole: lastMessage?.role || null,
          lastMessagePreview: lastMessage?.content ? lastMessage.content.slice(0, 140) : null
        };
      })
    );
    return res.status(200).json({ reply: "Conversations récupérées.", error: false, conversations });
  })
);

app.get(
  "/api/conversations/:conversationId/messages",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const { conversationId } = req.params;
    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true, messages: [] });
    }
    const rows = await dbAll("SELECT role, content, created_at FROM messages WHERE session_id = ? ORDER BY id ASC LIMIT 200", [conversationId]);
    return res.status(200).json({
      reply: "Historique récupéré.",
      error: false,
      conversationId,
      messages: rows.map((r) => ({ role: r.role, content: r.content, createdAt: r.created_at }))
    });
  })
);

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post(
  "/api/chat",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const message = req.body.message;
    let conversationId = req.body.conversationId || req.body.conversation_id;
    let isNewConversation = false;
    const modelTier = req.body.modelTier === "v250" ? "v250" : "v100";

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'message' est obligatoire.", error: true });
    }
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

    try {
      const result = await handleChat({ conversationId, userId: req.userId, message: message.trim(), googleAccessToken, channel: "web", modelTier });
      return res.status(200).json({ ...result, conversationId, isNewConversation });
    } catch (error) {
      console.error("❌ Erreur /api/chat:", error.message);
      return res.status(200).json({ reply: "⚠️ Une erreur est survenue. Veuillez réessayer.", error: true, conversationId, modelTier });
    }
  })
);

// ==================== ROUTE OUTILS DIRECTS (hors boucle conversationnelle) ====================
app.post(
  "/api/tools",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const toolName = req.body.toolName || req.body.action;
    const params = req.body.params || req.body.arguments || {};

    if (!toolName || typeof toolName !== "string") {
      return res.status(200).json({ success: false, error: true, reply: "⚠️ Le paramètre 'toolName' (ou 'action') est obligatoire." });
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

// ==================== ROUTES WHATSAPP — BAILEYS ====================
app.post(
  "/api/whatsapp/connect",
  strictLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    try {
      const result = await whatsappManager.initClient(req.userId);
      if (result.connected) return res.status(200).json({ reply: "✅ WhatsApp est déjà connecté.", error: false });

      let qrCode = null;
      const startTime = Date.now();
      while (!qrCode && Date.now() - startTime < CONFIG.WHATSAPP_QR_TIMEOUT) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        qrCode = whatsappManager.getQRCode(req.userId);
      }

      if (qrCode) return res.status(200).json({ reply: "📱 Scannez ce QR Code avec WhatsApp :", qrCode, error: false });
      return res.status(200).json({ reply: "⚠️ Délai dépassé en attendant le QR Code. Veuillez réessayer.", error: true });
    } catch (error) {
      console.error("❌ Erreur WhatsApp connect (Baileys):", error.message);
      return res.status(200).json({ reply: "⚠️ Erreur lors de la connexion WhatsApp.", error: true });
    }
  })
);

app.post(
  "/api/whatsapp/send",
  strictLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    if (!req.body.to || !req.body.message) {
      return res.status(200).json({ reply: "⚠️ Les paramètres 'to' et 'message' sont obligatoires.", error: true });
    }
    try {
      const result = await whatsappManager.sendMessage(req.userId, req.body.to, req.body.message);
      return res.status(200).json({ reply: `✅ Message envoyé à ${req.body.to}`, error: false, data: result });
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ Erreur: ${error.message}`, error: true });
    }
  })
);

// ==================== ROUTES WHATSAPP — PASSERELLE EXTERNE OPTIONNELLE ====================
app.get(
  "/api/whatsapp/gateway/qr",
  strictLimiter,
  asyncHandler(async (req, res) => {
    if (process.env.ADMIN_API_KEY && req.headers["x-admin-key"] !== process.env.ADMIN_API_KEY) {
      return res.status(401).json({ error: true, reply: "⚠️ Non autorisé." });
    }
    try {
      const data = await whatsappGateway.getQRCode();
      return res.status(200).json({ reply: "📱 QR Code généré.", error: false, data });
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
  })
);

app.get(
  "/api/whatsapp/gateway/status",
  strictLimiter,
  asyncHandler(async (req, res) => {
    try {
      const data = await whatsappGateway.getStatus();
      return res.status(200).json({ reply: "Statut récupéré.", error: false, data });
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
  })
);

app.post(
  "/api/whatsapp/webhook",
  webhookLimiter,
  asyncHandler(async (req, res) => {
    if (!verifyWhatsappWebhookSignature(req)) {
      console.warn("⚠️ Webhook WhatsApp: signature invalide ou manquante");
      return res.status(401).json({ error: true, reply: "Signature invalide." });
    }
    res.status(200).json({ received: true });

    try {
      const payload = req.body;
      const remoteJid = payload?.data?.key?.remoteJid || payload?.data?.from;
      const fromMe = payload?.data?.key?.fromMe;
      const text = payload?.data?.message?.conversation || payload?.data?.message?.extendedTextMessage?.text || payload?.data?.body;
      if (!remoteJid || fromMe || !text) return;

      const phoneNumber = String(remoteJid).replace(/@.*$/, "");
      const conversationId = `whatsapp_${phoneNumber}`;

      const result = await handleChat({
        conversationId,
        userId: conversationId,
        message: String(text).slice(0, CONFIG.MAX_MESSAGE_LENGTH),
        channel: "whatsapp",
        modelTier: "v100"
      });

      if (result?.reply) await whatsappGateway.sendMessage(phoneNumber, toPlainWhatsAppText(result.reply) || "🙂");
    } catch (error) {
      console.error("❌ Erreur traitement webhook WhatsApp (gateway externe):", error.message);
    }
  })
);

// ==================== ROUTE INTENTION ====================
app.post(
  "/api/intent/init",
  apiLimiter,
  authenticateUser,
  asyncHandler(async (req, res) => {
    const { intentType, conversationId, conversation_id: conversationIdSnake } = req.body;
    const convId = conversationId || conversationIdSnake;
    if (!convId || typeof convId !== "string") {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'conversationId' est obligatoire.", error: true });
    }
    try {
      await assertConversationOwnership(convId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
    await getSession(convId, req.userId);

    if (intentType === "WHATSAPP") {
      await setActiveIntent(convId, "WHATSAPP", { step: "NEED_NUMBER" });
      return res.status(200).json({ reply: "📱 Envoi WhatsApp initié. Quel est le numéro du destinataire ?", error: false });
    }
    if (intentType === "EMAIL") {
      await setActiveIntent(convId, "EMAIL", { step: "NEED_RECIPIENT" });
      return res.status(200).json({ reply: "📧 Envoi d'email initié. Quelle est l'adresse du destinataire ?", error: false });
    }
    return res.status(200).json({ reply: "⚠️ Type d'intention inconnu.", error: true });
  })
);

app.post(
  "/api/memory/clear",
  authenticateUser,
  asyncHandler(async (req, res) => {
    const conversationId = req.body.conversationId || req.body.conversation_id;
    if (!conversationId || typeof conversationId !== "string") {
      return res.status(200).json({ reply: "⚠️ Le paramètre 'conversationId' est obligatoire.", error: true });
    }
    try {
      await assertConversationOwnership(conversationId, req.userId);
    } catch (error) {
      return res.status(200).json({ reply: `⚠️ ${error.message}`, error: true });
    }
    await dbRun("DELETE FROM messages WHERE session_id = ?", [conversationId]);
    await clearActiveIntent(conversationId);
    return res.status(200).json({ reply: "✅ Mémoire de cette conversation effacée.", error: false });
  })
);

// ==================== 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ 404 — Route non trouvée: ${req.method} ${req.originalUrl}`);
  res.status(404).json({
    reply: `⚠️ Route non trouvée: ${req.method} ${req.originalUrl}`,
    error: true,
    availableRoutes: [
      "GET /",
      "GET /api/health",
      "GET /api/conversations",
      "GET /api/conversations/:conversationId/messages",
      "POST /api/chat",
      "POST /api/tools",
      "POST /api/whatsapp/connect",
      "POST /api/whatsapp/send",
      "GET /api/whatsapp/gateway/qr (optionnel)",
      "GET /api/whatsapp/gateway/status (optionnel)",
      "POST /api/whatsapp/webhook (optionnel)",
      "POST /api/intent/init",
      "POST /api/memory/clear"
    ]
  });
});

// ==================== ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  if (err?.type === "entity.parse.failed") return res.status(200).json({ reply: "⚠️ Corps de requête JSON invalide.", error: true });
  if (err?.message === "Origine non autorisée") return res.status(200).json({ reply: "⚠️ Origine non autorisée.", error: true });
  console.error(`❌ [${req.requestId || "unknown"}] Erreur non gérée:`, err?.message || err);
  return res.status(200).json({ reply: "⚠️ Une erreur interne est survenue. Veuillez réessayer.", error: true });
});

// ==================== DÉMARRAGE ====================
const PORT = CONFIG.PORT;
const server = app.listen(PORT, () => {
  console.log("\n" + "=".repeat(60));
  console.log(`🚀 SERVEUR ${CONFIG.AGENT_NAME.toUpperCase()} DÉMARRÉ (Production Ready)`);
  console.log("=".repeat(60));
  console.log(`🔢 Version : ${CONFIG.VERSION}`);
  console.log(`🔌 Port : ${PORT}`);
  console.log(`🔐 Sécurité utilisateur : ${firebaseApp ? "Firebase Admin actif (tokens vérifiés)" : "⚠️ NON vérifiée cryptographiquement"}`);
  console.log("");
  console.log("🎚️ ARCHITECTURE LLM ENTERPRISE :");
  console.log("=".repeat(60));
  
  console.log("\n📦 TIER v100 (Mwamba - Rapide) :");
  MODEL_TIERS.v100.providers.forEach((p, index) => {
    const priority = index === 0 ? "PRIMAIRE" : `FALLBACK ${index}`;
    console.log(`   ${priority.padEnd(15)}: ${p.provider}/${p.model} (max ${p.maxTokens} tokens)`);
  });
  
  console.log("\n📦 TIER v250 (Ngandu - Raisonnement & Code) :");
  console.log("   Étape 1 - Raisonnement :");
  MODEL_TIERS.v250.reasoning.providers.forEach((p, index) => {
    const priority = index === 0 ? "PRIMAIRE" : `FALLBACK ${index}`;
    console.log(`     ${priority.padEnd(13)}: ${p.provider}/${p.model} (max ${p.maxTokens} tokens)`);
  });
  console.log("   Étape 2 - Code :");
  MODEL_TIERS.v250.code.providers.forEach((p, index) => {
    const priority = index === 0 ? "PRIMAIRE" : `FALLBACK ${index}`;
    console.log(`     ${priority.padEnd(13)}: ${p.provider}/${p.model} (max ${p.maxTokens} tokens)`);
  });
  
  console.log("\n🛡️ GARDE-FOU : Dégradation gracieuse active");
  console.log("📧 Email : Gmail OAuth + Resend + SMTP");
  console.log("📱 WhatsApp : Baileys (sans Chrome)");
  console.log(`🌍 Sources ouvertes : ${Object.keys(OPEN_SOURCES).join(", ")}`);
  console.log(`📬 File d'attente WhatsApp : ${useRedisQueue ? "BullMQ + Redis" : "en mémoire (repli)"}`);
  console.log("=".repeat(60) + "\n");
});

server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

let shuttingDown = false;
async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n🛑 Arrêt gracieux (${signal})...`);
  const forceExitTimer = setTimeout(() => {
    console.error("⏱️ Arrêt forcé après délai dépassé");
    process.exit(1);
  }, 10000);

  try {
    await whatsappManager.destroyAll();
    if (whatsappWorker) await whatsappWorker.close();
    if (whatsappQueue) await whatsappQueue.close();
    if (emailTransporter) emailTransporter.close();
    await new Promise((resolve) => db.close((err) => { if (err) console.error("⚠️ Erreur fermeture DB:", err.message); resolve(); }));
    server.close(() => {
      clearTimeout(forceExitTimer);
      console.log("✅ Arrêt terminé");
      process.exit(0);
    });
  } catch (err) {
    console.error("❌ Erreur pendant l'arrêt gracieux:", err.message);
    clearTimeout(forceExitTimer);
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (error) => console.error("❌ Erreur non capturée:", error.message));
process.on("unhandledRejection", (reason) => console.error("❌ Promesse rejetée non gérée:", reason));

module.exports = app;
