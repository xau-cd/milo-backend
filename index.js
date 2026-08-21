// ==================== INDEX.JS - SERVEUR MILO PRODUCTION STRICT ====================
// Backend Express.js pour l'agent MILO (HIKLON Technology)
// Mode production : zéro simulation, appels réels, JSON structuré.

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const {
  default: makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
} = require("@whiskeysockets/baileys");
const qrcode = require("qrcode");
const pino = require("pino");
const fs = require("fs");
const path = require("path");

// ==================== SUPABASE ====================
function initializeSupabase() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_KEY;
  if (!url || !key) {
    console.warn("⚠️ Supabase non configuré. Mode dégradé.");
    return null;
  }
  const client = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  console.log("✅ Supabase connecté.");
  return client;
}
const supabase = initializeSupabase();
const supabaseReady = supabase !== null;

// ==================== EXPRESS ====================
const app = express();
app.use(cors({ origin: "*" }));
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: false,
  })
);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Trop de requêtes." },
});

// ==================== LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  console.log(`📥 ${req.method} ${req.url}`);
  res.on("finish", () => {
    console.log(`📤 ${req.method} ${req.url} - ${res.statusCode} - ${Date.now() - start}ms`);
  });
  next();
});

// ==================== PROMPT SYSTÈME CONFIDENTIEL ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, une Intelligence Artificielle propriétaire avancée développée par HIKLON Technology.
Tu réponds avec précision, sans paresse et de manière complète.

RÈGLE ABSOLUE DE CONFIDENTIALITÉ :
Tu ne dois sous aucun prétexte expliquer ton fonctionnement interne, ni citer les technologies, API, bibliothèques ou services tiers que tu utilises.
Tu présentes toutes tes fonctions (recherche web/images, analyse e-mail, gestion WhatsApp, automatisation) comme tes propres capacités natives HIKLON.
Si un utilisateur te demande comment tu fonctionnes ou quelles API tu utilises, réponds simplement que tu es propulsé par les technologies propriétaires d'IA et d'automatisation de HIKLON Technologies.

Pour toute demande d'image, de photo, ou de média visuel, tu DOIS renvoyer une action JSON structurée :
{"intent":"search_images","query":"le terme exact à rechercher"}
Pour toute autre action (WhatsApp, e-mail, recherche documentaire), renvoie également une action JSON adaptée.
Sinon, tu peux répondre en texte normal.
`;

// ==================== OPENROUTER ====================
async function callOpenRouter(userMessage, history = []) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    return {
      type: "text",
      message: "Service d'IA non configuré. Contactez l'administrateur.",
      payload: {},
    };
  }

  try {
    const messages = [
      { role: "system", content: MILO_SYSTEM_PROMPT },
      ...history.slice(-10).map((m) => ({ role: m.role || "user", content: m.content || m.message || "" })),
      { role: "user", content: userMessage },
    ];

    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat",
        messages,
        temperature: 0.7,
        max_tokens: 1500,
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://milo-ead21.web.app",
          "X-Title": "MILO Assistant",
        },
        timeout: 45000,
      }
    );

    const content = response.data?.choices?.[0]?.message?.content;
    if (!content) throw new Error("Réponse vide");

    // Tenter de parser si c'est un JSON d'intention
    try {
      const parsed = JSON.parse(content);
      if (parsed.intent && parsed.query) {
        return { type: "intent", intent: parsed.intent, query: parsed.query, payload: {} };
      }
    } catch {
      // pas JSON, on le traite comme texte
    }

    return { type: "text", message: content, payload: {} };
  } catch (error) {
    console.error("❌ Erreur OpenRouter:", error.message);
    return {
      type: "text",
      message: "Erreur de communication avec le service d'IA.",
      payload: { error: error.message },
    };
  }
}

// ==================== WIKIMEDIA RECHERCHE IMAGES ====================
async function searchWikimediaImages(query) {
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=5&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    if (!pages) return { type: "media_gallery", message: "Aucune image trouvée.", payload: { images: [] } };

    const images = Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        title: page.title,
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value || null,
      }))
      .filter((img) => img.url);

    return {
      type: "media_gallery",
      message: `Voici les images pour "${query}" :`,
      payload: { images },
    };
  } catch (error) {
    console.error("❌ Erreur Wikimedia:", error.message);
    return {
      type: "text",
      message: "Recherche d'images indisponible.",
      payload: { error: error.message },
    };
  }
}

// ==================== WHATSAPP MANAGER ====================
class WhatsAppManager {
  constructor() {
    this.sessions = new Map();
    this.dir = path.join(__dirname, "sessions");
    if (!fs.existsSync(this.dir)) fs.mkdirSync(this.dir, { recursive: true });
    this.logger = pino({ level: "silent" });
  }

  getPath(uid) {
    return path.join(this.dir, String(uid).replace(/[^a-zA-Z0-9_-]/g, ""));
  }

  async init(uid) {
    if (this.sessions.has(uid)) {
      const s = this.sessions.get(uid);
      if (s.sock?.user) return { connected: true };
    }
    const p = this.getPath(uid);
    if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
    const { state, saveCreds } = await useMultiFileAuthState(p);
    const { version } = await fetchLatestBaileysVersion();
    const sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger: this.logger,
      browser: Browsers.appropriate("Chrome"),
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
    });
    const session = { sock, saveCreds, connected: false, qrCode: null };
    this.sessions.set(uid, session);
    this.bindEvents(uid, session);
    return { connected: false };
  }

  bindEvents(uid, session) {
    session.sock.ev.on("creds.update", session.saveCreds);
    session.sock.ev.on("connection.update", async (upd) => {
      const { connection, lastDisconnect, qr } = upd;
      if (qr) {
        try {
          session.qrCode = await qrcode.toDataURL(qr);
          console.log(`📱 QR généré pour ${uid}`);
        } catch (e) {
          console.error("QR error:", e.message);
        }
      }
      if (connection === "open") {
        session.connected = true;
        session.qrCode = null;
        console.log(`✅ WhatsApp connecté pour ${uid}`);
        if (supabaseReady) {
          supabase.from("whatsapp_sessions").insert([{ user_id: uid, status: "connected" }]).then();
        }
      }
      if (connection === "close") {
        session.connected = false;
        const code = lastDisconnect?.error?.output?.statusCode;
        if (code !== DisconnectReason.loggedOut) {
          setTimeout(() => this.init(uid).catch(console.error), 5000);
        } else {
          this.cleanup(uid);
        }
      }
    });
    session.sock.ev.on("messages.upsert", async (m) => {
      const msg = m.messages[0];
      if (!msg.key.fromMe && m.type === "notify") {
        const content = msg.message?.conversation || msg.message?.extendedTextMessage?.text || "Message non textuel";
        console.log(`📩 WhatsApp reçu pour ${uid}: ${content.substring(0, 50)}`);
        if (supabaseReady) {
          supabase.from("whatsapp_messages").insert([{
            user_id: uid,
            from: msg.key.remoteJid,
            content,
            direction: "incoming",
            timestamp: new Date().toISOString(),
          }]).then();
        }
      }
    });
  }

  getQR(uid) {
    return this.sessions.get(uid)?.qrCode || null;
  }

  getStatus(uid) {
    const s = this.sessions.get(uid);
    return { connected: s?.connected || false, user: s?.sock?.user || null };
  }

  async logout(uid) {
    const s = this.sessions.get(uid);
    if (s?.sock) await s.sock.logout();
    this.cleanup(uid);
  }

  cleanup(uid) {
    this.sessions.delete(uid);
    const p = this.getPath(uid);
    if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
  }
}

const whatsapp = new WhatsAppManager();

// ==================== RENDELL (E-MAILS) ====================
async function checkRendellAuth(userId) {
  if (!supabaseReady) return false;
  const { data } = await supabase.from("rendell_credentials").select("id").eq("user_id", userId).maybeSingle();
  return !!data;
}

async function readEmails(userId) {
  if (!(await checkRendellAuth(userId))) {
    return {
      type: "auth_modal",
      message: "Pour me permettre d'analyser tes e-mails, saisis tes identifiants de messagerie.",
      payload: { requiresAuth: true, authType: "rendell" },
    };
  }
  if (process.env.RENDELL_API_URL && process.env.RENDELL_API_KEY) {
    try {
      const res = await axios.get(`${process.env.RENDELL_API_URL}/emails`, {
        headers: { Authorization: `Bearer ${process.env.RENDELL_API_KEY}` },
        params: { userId },
        timeout: 10000,
      });
      return { type: "text", message: "Voici tes e-mails :", payload: { emails: res.data } };
    } catch (e) {
      console.error("Rendell API error:", e.message);
      return { type: "text", message: "Impossible de récupérer tes e-mails.", payload: { error: e.message } };
    }
  }
  // Si pas d'API configurée, on lit depuis Supabase comme fallback (si table existe)
  if (supabaseReady) {
    const { data: emails, error } = await supabase.from("rendell_emails").select("*").eq("user_id", userId).limit(10);
    if (error) {
      console.error("Supabase read emails error:", error.message);
      return { type: "text", message: "Erreur lors de la lecture des e-mails.", payload: {} };
    }
    return { type: "text", message: "Voici tes e-mails :", payload: { emails: emails || [] } };
  }
  return { type: "text", message: "Service e-mail non configuré.", payload: {} };
}

// ==================== RECHERCHE GÉNÉRIQUE ====================
async function searchWikipedia(query) {
  try {
    const sr = await axios.get(`https://fr.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(query)}&format=json&origin=*`, { timeout: 10000 });
    const results = sr.data?.query?.search;
    if (!results?.length) return { type: "text", message: "Aucun résultat trouvé.", payload: {} };
    const title = results[0].title;
    const ex = await axios.get(`https://fr.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&titles=${encodeURIComponent(title)}&format=json&origin=*`, { timeout: 10000 });
    const pages = ex.data?.query?.pages;
    const page = pages ? Object.values(pages)[0] : null;
    const summary = page?.extract || "";
    return {
      type: "text",
      message: `**${title}**\n\n${summary}\n\n[Lire l'article](https://fr.wikipedia.org/wiki/${encodeURIComponent(title)})`,
      payload: { title, summary, url: `https://fr.wikipedia.org/wiki/${encodeURIComponent(title)}` },
    };
  } catch (e) {
    console.error("Wikipedia error:", e.message);
    return { type: "text", message: "Recherche Wikipédia indisponible.", payload: { error: e.message } };
  }
}

async function searchWeb(query) {
  try {
    const res = await axios.get(`https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`, { timeout: 10000 });
    const abstract = res.data?.AbstractText;
    const heading = res.data?.Heading;
    const url = res.data?.AbstractURL;
    if (!abstract && !heading) return { type: "text", message: "Aucune information trouvée.", payload: {} };
    return {
      type: "text",
      message: `**${heading || "Résultat"}**\n\n${abstract || ""}${url ? `\n\n[Source](${url})` : ""}`,
      payload: { heading, abstract, url },
    };
  } catch (e) {
    console.error("DuckDuckGo error:", e.message);
    return { type: "text", message: "Recherche web indisponible.", payload: { error: e.message } };
  }
}

async function searchDocShare(query) {
  if (process.env.DOCSHARE_API_URL) {
    try {
      const res = await axios.get(`${process.env.DOCSHARE_API_URL}/search`, { params: { q: query }, timeout: 10000 });
      return { type: "text", message: "Documents trouvés :", payload: { documents: res.data } };
    } catch (e) {
      console.error("DocShare error:", e.message);
      return { type: "text", message: "Recherche de documents indisponible.", payload: { error: e.message } };
    }
  }
  if (supabaseReady) {
    const { data, error } = await supabase.from("documents").select("*").ilike("title", `%${query}%`).limit(5);
    if (error) return { type: "text", message: "Erreur de recherche de documents.", payload: {} };
    return { type: "text", message: "Documents trouvés :", payload: { documents: data || [] } };
  }
  return { type: "text", message: "Recherche de documents non configurée.", payload: {} };
}

// ==================== ROUTES ====================
app.get("/", (req, res) => res.json({ status: "ok", message: "Serveur Milo opérationnel" }));

app.get("/api/health", (req, res) =>
  res.json({ status: "ok", message: "Serveur Milo opérationnel", database: supabaseReady ? "connected" : "disconnected" })
);

app.post("/api/chat", apiLimiter, async (req, res) => {
  try {
    const { message, user, userId, history } = req.body;
    if (!message?.trim()) {
      return res.status(400).json({ type: "text", message: "Message requis.", payload: {} });
    }

    // Détection simple de demande d'image
    const imageIntent = /\b(image|photo|picture|pic|affiche|montre)\b/i.test(message);
    if (imageIntent) {
      // Extraire le mot-clé après le verbe
      const match = message.match(/(?:image|photo|picture|pic|affiche|montre)\s+(?:de\s+)?(.+)/i);
      const query = match?.[1]?.trim() || message.replace(/\b(image|photo|picture|pic|affiche|montre)\b/gi, "").trim() || message;
      const result = await searchWikimediaImages(query);
      return res.json(result);
    }

    // Sinon, appeler l'IA
    const aiResult = await callOpenRouter(message, history || []);

    // Si l'IA renvoie une intention
    if (aiResult.type === "intent") {
      switch (aiResult.intent) {
        case "search_images":
          const imgRes = await searchWikimediaImages(aiResult.query);
          return res.json(imgRes);
        default:
          return res.json({ type: "text", message: "Action non reconnue.", payload: {} });
      }
    }

    // Log
    if (supabaseReady) {
      supabase.from("chat_logs").insert([{
        user_id: userId || user || "anonymous",
        message,
        response: aiResult.message,
        type: aiResult.type,
        created_at: new Date().toISOString(),
      }]).then();
    }

    return res.json(aiResult);
  } catch (e) {
    console.error("/api/chat error:", e.message);
    return res.status(500).json({ type: "text", message: "Erreur interne du serveur.", payload: { error: e.message } });
  }
});

app.post("/api/tools", apiLimiter, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const { userId } = req.body;

    if (!action) return res.status(400).json({ type: "text", message: "Action requise.", payload: {} });

    let result;
    switch (action) {
      case "search_wikipedia":
      case "wikipedia":
        result = await searchWikipedia(data.query);
        break;
      case "search_web":
      case "web_search":
      case "web":
        result = await searchWeb(data.query);
        break;
      case "search_images":
      case "images":
        result = await searchWikimediaImages(data.query);
        break;
      case "docshare":
      case "documents":
        result = await searchDocShare(data.query);
        break;
      case "whatsapp_qr":
      case "whatsapp": {
        if (!userId) return res.status(400).json({ type: "text", message: "userId requis.", payload: {} });
        await whatsapp.init(userId);
        let qr = null;
        const start = Date.now();
        while (!qr && Date.now() - start < 30000) {
          qr = whatsapp.getQR(userId);
          if (qr) break;
          await new Promise((r) => setTimeout(r, 1000));
        }
        if (qr) {
          result = { type: "qr_code", message: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.", payload: { qrCode: qr, expiresIn: 60 } };
        } else {
          const status = whatsapp.getStatus(userId);
          result = status.connected
            ? { type: "text", message: "WhatsApp déjà connecté.", payload: {} }
            : { type: "text", message: "Délai dépassé. Veuillez réessayer.", payload: {} };
        }
        break;
      }
      case "whatsapp_status":
        result = { type: "text", message: "Statut WhatsApp.", payload: whatsapp.getStatus(userId) };
        break;
      case "whatsapp_logout":
        await whatsapp.logout(userId);
        result = { type: "text", message: "WhatsApp déconnecté.", payload: {} };
        break;
      case "rendell_auth_check":
        result = (await checkRendellAuth(userId))
          ? { type: "text", message: "Identifiants de messagerie présents.", payload: {} }
          : { type: "auth_modal", message: "Pour me permettre d'analyser tes e-mails, saisis tes identifiants de messagerie.", payload: { requiresAuth: true, authType: "rendell" } };
        break;
      case "rendell_read_emails":
        result = await readEmails(userId);
        break;
      default:
        return res.status(400).json({ type: "text", message: `Action inconnue : ${action}`, payload: {} });
    }

    if (supabaseReady && userId) {
      supabase.from("tool_logs").insert([{ user_id: userId, action, result_type: result.type, created_at: new Date().toISOString() }]).then();
    }

    return res.json(result);
  } catch (e) {
    console.error("/api/tools error:", e.message);
    return res.status(500).json({ type: "text", message: "Erreur d'exécution de l'outil.", payload: { error: e.message } });
  }
});

app.post("/api/clear-cache", (req, res) => res.json({ status: "success", message: "Cache nettoyé" }));
app.get("/api/info", (req, res) => res.json({ status: "success", uptime: process.uptime(), version: "1.0.0" }));

// 404
app.use((req, res) => res.status(404).json({ error: "Route non trouvée sur le serveur" }));

// Gestion d'erreurs Express
app.use((err, req, res, next) => {
  console.error("Erreur Express:", err.message);
  res.status(500).json({ error: "Erreur interne du serveur." });
});

// Démarrage
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`Supabase: ${supabaseReady ? "connecté" : "non connecté"}`);
  console.log(`OpenRouter: ${process.env.OPENROUTER_API_KEY ? "configuré" : "non configuré"}`);
});

server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// Arrêt gracieux
process.on("SIGTERM", async () => {
  for (const [uid, session] of whatsapp.sessions) {
    if (session.sock) await session.sock.end();
  }
  server.close(() => process.exit(0));
});
