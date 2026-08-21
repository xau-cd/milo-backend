// ==================== INDEX.JS - CERVEAU MILO (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, Supabase (optionnel), SQLite, whatsapp-web.js, qrcode, axios, cheerio, OpenRouter
// Format de réponse strict : { replyText, systemAction, payload }

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const cheerio = require("cheerio");
const qrcode = require("qrcode");
const { Client, LocalAuth } = require("whatsapp-web.js");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");

// ==================== INITIALISATION SQLITE ====================
const dbDir = path.join(__dirname, "data");
if (!fs.existsSync(dbDir)) fs.mkdirSync(dbDir, { recursive: true });

const db = new sqlite3.Database(path.join(dbDir, "milo.db"));
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    rendell_credentials TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS chat_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    message TEXT,
    response TEXT,
    system_action TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS tool_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    action TEXT,
    result_type TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);
});

// ==================== INITIALISATION EXPRESS ====================
const app = express();
app.use(cors({ origin: "*" }));
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: false
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
  message: { error: "Trop de requêtes." }
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  console.log(`📥 ${req.method} ${req.url}`);
  res.on("finish", () => {
    console.log(`📤 ${req.method} ${req.url} - ${res.statusCode} - ${Date.now() - start}ms`);
  });
  next();
});

// ==================== PROMPT SYSTÈME CONFIDENTIEL (INTERNE) ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, une Intelligence Artificielle propriétaire avancée développée par HIKLON Technology.
Tu réponds avec précision, sans paresse et de manière complète.

RÈGLE ABSOLUE DE CONFIDENTIALITÉ :
Tu ne dois sous aucun prétexte expliquer ton fonctionnement interne, ni citer les technologies, API, bibliothèques ou services tiers que tu utilises.
Tu présentes toutes tes fonctions (recherche web/images, analyse e-mail, gestion WhatsApp, automatisation) comme tes propres capacités natives HIKLON.
Si un utilisateur te demande comment tu fonctionnes ou quelles API tu utilises, réponds simplement que tu es propulsé par les technologies propriétaires d'IA et d'automatisation de HIKLON Technologies.

Pour les demandes d'actions spécifiques, tu DOIS produire une réponse qui sera interprétée par le système pour déclencher les bonnes actions.
Tu ne dois JAMAIS inclure de JSON dans ta réponse visible. Les instructions techniques sont gérées en interne.
`;

// ==================== FONCTION OPENROUTER ====================
async function callOpenRouter(userMessage, history = []) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    return {
      replyText: "Service d'IA non configuré. Contactez l'administrateur.",
      systemAction: "NONE",
      payload: {}
    };
  }

  try {
    const messages = [
      { role: "system", content: MILO_SYSTEM_PROMPT },
      ...history.slice(-10).map((m) => ({ role: m.role || "user", content: m.content || m.message || "" })),
      { role: "user", content: userMessage }
    ];

    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat",
        messages,
        temperature: 0.7,
        max_tokens: 1500
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://milo-ead21.web.app",
          "X-Title": "MILO Assistant"
        },
        timeout: 45000
      }
    );

    const content = response.data?.choices?.[0]?.message?.content;
    if (!content) throw new Error("Réponse vide");

    // Détection d'intention basée sur des mots-clés dans le message utilisateur
    const intent = detectIntent(userMessage);
    if (intent === "WHATSAPP_CONNECT") {
      return {
        replyText: "Génération du QR Code en cours...",
        systemAction: "RENDER_QR",
        payload: { requiresAuth: true }
      };
    }
    if (intent === "SEARCH_IMAGES") {
      const query = extractImageQuery(userMessage);
      const images = await searchWikimediaImages(query);
      return {
        replyText: `Voici les images pour "${query}" :`,
        systemAction: "RENDER_GALLERY",
        payload: { images }
      };
    }

    return {
      replyText: content,
      systemAction: "NONE",
      payload: {}
    };
  } catch (error) {
    console.error("❌ Erreur OpenRouter:", error.message);
    return {
      replyText: "Erreur de communication avec le service d'IA.",
      systemAction: "NONE",
      payload: {}
    };
  }
}

// ==================== DÉTECTION D'INTENTION ====================
function detectIntent(message) {
  const msg = message.toLowerCase();
  if (/(connecte|connecter|connecte mon|scanne|qr code|whatsapp)/.test(msg) && /(whatsapp|wa|chat|message)/.test(msg)) {
    return "WHATSAPP_CONNECT";
  }
  if (/(image|photo|picture|pic|affiche|montre|galerie)/.test(msg)) {
    return "SEARCH_IMAGES";
  }
  return "NONE";
}

function extractImageQuery(message) {
  const match = message.match(/(?:image|photo|picture|pic|affiche|montre)\s+(?:de\s+)?(.+)/i);
  return match ? match[1].trim() : message;
}

// ==================== RECHERCHE WIKIMEDIA (IMAGES) ====================
async function searchWikimediaImages(query) {
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=5&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    if (!pages) return [];

    return Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        title: page.title,
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value || null
      }))
      .filter((img) => img.url);
  } catch (error) {
    console.error("❌ Erreur Wikimedia:", error.message);
    return [];
  }
}

// ==================== RECHERCHE WEB (DuckDuckGo + Cheerio) ====================
async function searchWeb(query) {
  try {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await axios.get(url, { timeout: 10000 });
    const abstract = response.data?.AbstractText || null;
    const heading = response.data?.Heading || null;
    const urlRes = response.data?.AbstractURL || null;
    return { heading, abstract, url: urlRes };
  } catch (error) {
    console.error("❌ Erreur recherche web:", error.message);
    return { heading: null, abstract: null, url: null };
  }
}

// ==================== GESTION WHATSAPP (whatsapp-web.js) ====================
class WhatsAppManager {
  constructor() {
    this.clients = new Map(); // userId -> { client, qrCode }
  }

  async initClient(userId) {
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.client.info) {
        return { connected: true };
      }
      return { connected: false };
    }

    const client = new Client({
      authStrategy: new LocalAuth({ clientId: userId, dataPath: path.join(__dirname, "sessions") }),
      puppeteer: {
        headless: true,
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
      }
    });

    const sessionData = { client, qrCode: null };
    this.clients.set(userId, sessionData);

    client.on("qr", async (qr) => {
      try {
        sessionData.qrCode = await qrcode.toDataURL(qr);
        console.log(`📱 QR Code généré pour ${userId}`);
      } catch (error) {
        console.error("❌ Erreur génération QR:", error.message);
      }
    });

    client.on("ready", () => {
      console.log(`✅ WhatsApp connecté pour ${userId}`);
      sessionData.qrCode = null;
      db.run("UPDATE users SET whatsapp_connected = 1 WHERE id = ?", [userId]);
    });

    client.on("authenticated", () => {
      console.log(`🔐 Authentifié pour ${userId}`);
    });

    client.on("auth_failure", (msg) => {
      console.error(`❌ Échec auth WhatsApp ${userId}:`, msg);
      sessionData.qrCode = null;
    });

    client.on("disconnected", (reason) => {
      console.log(`🔌 WhatsApp déconnecté pour ${userId}: ${reason}`);
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
      sessionData.qrCode = null;
      this.clients.delete(userId);
    });

    try {
      await client.initialize();
    } catch (error) {
      console.error("❌ Erreur init WhatsApp:", error.message);
      this.clients.delete(userId);
      throw error;
    }

    return { connected: false };
  }

  getQRCode(userId) {
    const session = this.clients.get(userId);
    return session ? session.qrCode : null;
  }

  getStatus(userId) {
    const session = this.clients.get(userId);
    if (!session || !session.client.info) return { connected: false };
    return { connected: true, user: session.client.info.pushname || null };
  }

  async logout(userId) {
    const session = this.clients.get(userId);
    if (session && session.client) {
      await session.client.logout();
      session.client.destroy();
      this.clients.delete(userId);
      db.run("UPDATE users SET whatsapp_connected = 0 WHERE id = ?", [userId]);
    }
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== ROUTES ====================

// Route racine
app.get("/", (req, res) => {
  res.json({ status: "ok", message: "Serveur Milo opérationnel" });
});

// Healthcheck
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    message: "Serveur Milo opérationnel",
    timestamp: new Date().toISOString()
  });
});

// ==================== ROUTE CHAT ====================
app.post("/api/chat", apiLimiter, async (req, res) => {
  try {
    const { message, user, userId, history } = req.body;
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        replyText: "Le champ 'message' est requis.",
        systemAction: "NONE",
        payload: {}
      });
    }

    const logUserId = userId || user || "anonymous";

    // Détection d'intention directe
    const intent = detectIntent(message);

    if (intent === "WHATSAPP_CONNECT") {
      // Initialiser le client WhatsApp et récupérer le QR
      await whatsappManager.initClient(logUserId);
      let qr = null;
      const start = Date.now();
      while (!qr && Date.now() - start < 30000) {
        qr = whatsappManager.getQRCode(logUserId);
        if (qr) break;
        await new Promise((r) => setTimeout(r, 1000));
      }
      if (qr) {
        db.run("INSERT INTO chat_logs (user_id, message, response, system_action) VALUES (?, ?, ?, ?)", [
          logUserId,
          message,
          "QR Code généré",
          "RENDER_QR"
        ]);
        return res.json({
          replyText: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.",
          systemAction: "RENDER_QR",
          payload: { qrBase64: qr, expiresIn: 60 }
        });
      } else {
        return res.json({
          replyText: "Délai dépassé pour la génération du QR Code. Veuillez réessayer.",
          systemAction: "NONE",
          payload: {}
        });
      }
    }

    if (intent === "SEARCH_IMAGES") {
      const query = extractImageQuery(message);
      const images = await searchWikimediaImages(query);
      db.run("INSERT INTO chat_logs (user_id, message, response, system_action) VALUES (?, ?, ?, ?)", [
        logUserId,
        message,
        `${images.length} images trouvées`,
        "RENDER_GALLERY"
      ]);
      return res.json({
        replyText: `Voici les images pour "${query}" :`,
        systemAction: "RENDER_GALLERY",
        payload: { images }
      });
    }

    // Sinon, appel à OpenRouter
    const aiResult = await callOpenRouter(message, history || []);
    db.run("INSERT INTO chat_logs (user_id, message, response, system_action) VALUES (?, ?, ?, ?)", [
      logUserId,
      message,
      aiResult.replyText,
      aiResult.systemAction
    ]);

    return res.json(aiResult);
  } catch (error) {
    console.error("❌ Erreur /api/chat:", error.message);
    return res.status(500).json({
      replyText: "Erreur interne du serveur.",
      systemAction: "NONE",
      payload: {}
    });
  }
});

// ==================== ROUTE OUTILS ====================
app.post("/api/tools", apiLimiter, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const { userId } = req.body;

    if (!action) {
      return res.status(400).json({
        replyText: "Action requise.",
        systemAction: "NONE",
        payload: {}
      });
    }

    let result;

    switch (action) {
      case "search_images":
      case "images": {
        if (!data.query) {
          return res.status(400).json({ replyText: "Paramètre 'query' requis.", systemAction: "NONE", payload: {} });
        }
        const images = await searchWikimediaImages(data.query);
        result = {
          replyText: `Voici les images pour "${data.query}" :`,
          systemAction: "RENDER_GALLERY",
          payload: { images }
        };
        break;
      }
      case "search_web":
      case "web_search":
      case "web": {
        if (!data.query) {
          return res.status(400).json({ replyText: "Paramètre 'query' requis.", systemAction: "NONE", payload: {} });
        }
        const web = await searchWeb(data.query);
        result = {
          replyText: web.abstract ? `**${web.heading || "Résultat"}**\n\n${web.abstract}` : "Aucune information trouvée.",
          systemAction: "NONE",
          payload: web
        };
        break;
      }
      case "whatsapp_qr":
      case "whatsapp": {
        if (!userId) {
          return res.status(400).json({ replyText: "userId requis.", systemAction: "NONE", payload: {} });
        }
        await whatsappManager.initClient(userId);
        let qr = null;
        const start = Date.now();
        while (!qr && Date.now() - start < 30000) {
          qr = whatsappManager.getQRCode(userId);
          if (qr) break;
          await new Promise((r) => setTimeout(r, 1000));
        }
        if (qr) {
          result = {
            replyText: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.",
            systemAction: "RENDER_QR",
            payload: { qrBase64: qr, expiresIn: 60 }
          };
        } else {
          const status = whatsappManager.getStatus(userId);
          result = status.connected
            ? { replyText: "WhatsApp déjà connecté.", systemAction: "NONE", payload: {} }
            : { replyText: "Délai dépassé. Veuillez réessayer.", systemAction: "NONE", payload: {} };
        }
        break;
      }
      case "whatsapp_status": {
        const status = whatsappManager.getStatus(userId);
        result = {
          replyText: status.connected ? "WhatsApp est connecté." : "WhatsApp n'est pas connecté.",
          systemAction: "NONE",
          payload: status
        };
        break;
      }
      case "whatsapp_logout": {
        await whatsappManager.logout(userId);
        result = { replyText: "WhatsApp déconnecté.", systemAction: "NONE", payload: {} };
        break;
      }
      default:
        return res.status(400).json({
          replyText: `Action inconnue : ${action}`,
          systemAction: "NONE",
          payload: {}
        });
    }

    if (userId) {
      db.run("INSERT INTO tool_logs (user_id, action, result_type) VALUES (?, ?, ?)", [
        userId,
        action,
        result.systemAction || "NONE"
      ]);
    }

    return res.json(result);
  } catch (error) {
    console.error("❌ Erreur /api/tools:", error.message);
    return res.status(500).json({
      replyText: "Erreur lors de l'exécution de l'outil.",
      systemAction: "NONE",
      payload: { error: error.message }
    });
  }
});

// ==================== ROUTES SIMPLES ====================
app.post("/api/clear-cache", (req, res) => {
  res.json({ status: "success", message: "Cache nettoyé" });
});

app.get("/api/info", (req, res) => {
  res.json({
    status: "success",
    uptime: process.uptime(),
    version: "1.0.0",
    timestamp: new Date().toISOString()
  });
});

// ==================== 404 ====================
app.use((req, res) => {
  res.status(404).json({ error: "Route non trouvée sur le serveur" });
});

// ==================== GESTION ERREURS ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express:", err.message);
  res.status(500).json({ error: "Erreur interne du serveur." });
});

// ==================== DÉMARRAGE ====================
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`💾 Base de données : SQLite (locale)`);
  console.log(`🔑 OpenRouter : ${process.env.OPENROUTER_API_KEY ? "configuré" : "non configuré"}`);
  console.log("========================================");
});

server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Signal ${signal} reçu. Arrêt gracieux...`);
  for (const [userId, session] of whatsappManager.clients) {
    if (session.client) {
      try {
        await session.client.destroy();
        console.log(`👋 Session WhatsApp fermée pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur fermeture session ${userId}:`, error.message);
      }
    }
  }
  db.close(() => {
    console.log("💾 Base de données SQLite fermée.");
    server.close(() => process.exit(0));
  });
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

module.exports = app;