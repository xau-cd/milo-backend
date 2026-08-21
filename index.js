// ==================== INDEX.JS - SERVEUR MILO ====================
// Backend Express.js pour l'agent MILO (HIKLON Technology)
// Communication Frontend Firebase via CORS ouvert
// Intégration : Supabase, OpenRouter, Baileys (WhatsApp), Rendell (E-mails), Recherche (Wikimedia, Wikipédia, DuckDuckGo)

// ==================== CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ====================
require("dotenv").config();

// ==================== IMPORTS DES MODULES ====================
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const { default: makeWASocket, DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = require("@whiskeysockets/baileys");
const qrcode = require("qrcode");
const pino = require("pino");
const fs = require("fs");
const path = require("path");

// ==================== INITIALISATION SUPABASE ====================
function initializeSupabase() {
  try {
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_KEY;

    if (!supabaseUrl || !supabaseKey) {
      console.warn("⚠️ SUPABASE_URL ou SUPABASE_KEY non défini.");
      console.warn("⚠️ Le serveur démarre en mode dégradé (sans base de données).");
      return null;
    }

    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false, autoRefreshToken: false }
    });

    console.log("✅ Supabase initialisé avec succès.");
    console.log(`📁 URL : ${supabaseUrl}`);
    return supabase;
  } catch (error) {
    console.error("❌ Erreur d'initialisation Supabase :", error.message);
    return null;
  }
}

const supabase = initializeSupabase();
const supabaseReady = supabase !== null;

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// ==================== CONFIGURATION CORS OUVERT ====================
app.use(cors({ origin: "*" }));

// ==================== CONFIGURATION HELMET ====================
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: false
  })
);

// ==================== MIDDLEWARE JSON ====================
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Trop de requêtes. Veuillez réessayer dans 15 minutes." }
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  console.log(`📥 ${req.method} ${req.url} - ${new Date().toISOString()}`);
  res.on("finish", () => {
    const duration = Date.now() - start;
    console.log(`📤 ${req.method} ${req.url} - ${res.statusCode} - ${duration}ms`);
  });
  next();
});

// ==================== SYSTÈME DE LOGS (SUPABASE) ====================
async function saveLog(collection, data) {
  try {
    if (!supabaseReady || !supabase) return null;
    const { data: inserted, error } = await supabase.from(collection).insert([data]);
    if (error) {
      console.error(`❌ Erreur Supabase (${collection}) :`, error.message);
      return null;
    }
    return inserted;
  } catch (error) {
    console.error(`❌ Erreur saveLog (${collection}) :`, error.message);
    return null;
  }
}

// ==================== SYSTÈME PROMPT CONFIDENTIEL ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, une Intelligence Artificielle propriétaire avancée développée par HIKLON Technology.
Tu réponds avec précision, sans paresse et de manière complète.

RÈGLE ABSOLUE DE CONFIDENTIALITÉ :
Tu ne dois sous aucun prétexte expliquer ton fonctionnement interne, ni citer les technologies, API, bibliothèques ou services tiers que tu utilises.
Tu présentes toutes tes fonctions (recherche web/images, analyse e-mail, gestion WhatsApp, automatisation) comme tes propres capacités natives HIKLON.

Si un utilisateur te demande comment tu fonctionnes ou quelles API tu utilises, réponds simplement que tu es propulsé par les technologies propriétaires d'IA et d'automatisation de HIKLON Technologies.

TES CAPACITÉS (à ne mentionner que comme des capacités intégrées) :
- Recherche d'informations et de documents (textes, images).
- Lecture, analyse et automatisation des e-mails.
- Connexion et automatisation WhatsApp.
- Exécution de tâches diverses.

Pour les actions privées, tu dois demander les autorisations nécessaires de manière fluide :
- WhatsApp : "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé." (sans mentionner d'outil sous-jacent)
- E-mails : "Pour me permettre d'analyser tes e-mails, saisis tes identifiants de messagerie." (sans mentionner le service)
`;

// ==================== FONCTION OPENROUTER ====================
async function callOpenRouter(userMessage, history = []) {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;
    if (!apiKey) {
      console.warn("⚠️ OPENROUTER_API_KEY non configurée.");
      return "Je suis MILO, votre assistant IA. Veuillez configurer la clé API OpenRouter.";
    }

    const messages = [
      { role: "system", content: MILO_SYSTEM_PROMPT },
      ...history.slice(-10).map((msg) => ({ role: msg.role || "user", content: msg.content || msg.message || "" })),
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
    if (!content) throw new Error("Réponse OpenRouter vide");
    return content;
  } catch (error) {
    console.error("❌ Erreur OpenRouter :", error.message);
    return "Désolé, je rencontre une difficulté technique. Veuillez réessayer plus tard.";
  }
}

// ==================== MODULE WHATSAPP (BAILEYS) ====================
class WhatsAppManager {
  constructor() {
    this.sessions = new Map(); // userId -> session data
    this.sessionsDir = path.join(__dirname, "sessions");
    if (!fs.existsSync(this.sessionsDir)) {
      fs.mkdirSync(this.sessionsDir, { recursive: true });
    }
    this.logger = pino({ level: "silent" }); // Logger silencieux pour Baileys
  }

  getSessionPath(userId) {
    const safeUid = String(userId).replace(/[^a-zA-Z0-9_-]/g, "");
    return path.join(this.sessionsDir, safeUid);
  }

  async initSession(userId) {
    try {
      if (this.sessions.has(userId)) {
        const existing = this.sessions.get(userId);
        if (existing.sock && existing.sock.user) {
          return { connected: true, sock: existing.sock };
        }
      }

      const sessionPath = this.getSessionPath(userId);
      if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      const sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger: this.logger,
        browser: Browsers.appropriate("Chrome"),
        syncFullHistory: false,
        markOnlineOnConnect: true,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000
      });

      const sessionData = { sock, saveCreds, connected: false, qrCode: null };
      this.sessions.set(userId, sessionData);
      this.setupSocketEvents(userId, sessionData);

      return { connected: false, sock };
    } catch (error) {
      console.error(`❌ Erreur initSession WhatsApp (${userId}) :`, error.message);
      throw error;
    }
  }

  setupSocketEvents(userId, sessionData) {
    const { sock, saveCreds } = sessionData;
    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          const qrDataUrl = await qrcode.toDataURL(qr);
          sessionData.qrCode = qrDataUrl;
          console.log(`📱 QR Code généré pour ${userId}`);
        } catch (qrError) {
          console.error(`❌ Erreur génération QR Code (${userId}) :`, qrError.message);
        }
      }

      if (connection === "open") {
        sessionData.connected = true;
        sessionData.qrCode = null;
        console.log(`✅ WhatsApp connecté pour ${userId}`);
        await saveLog("whatsapp_sessions", { user_id: userId, status: "connected", timestamp: new Date().toISOString() });
      }

      if (connection === "close") {
        sessionData.connected = false;
        console.log(`🔌 WhatsApp déconnecté pour ${userId}`);
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        if (statusCode !== DisconnectReason.loggedOut) {
          console.log(`🔄 Reconnexion automatique pour ${userId}...`);
          setTimeout(() => {
            this.initSession(userId).catch((err) => console.error(`❌ Échec reconnexion (${userId}) :`, err.message));
          }, 5000);
        } else {
          this.cleanupSession(userId);
        }
      }
    });

    sock.ev.on("messages.upsert", async (m) => {
      const message = m.messages[0];
      if (!message.key.fromMe && m.type === "notify") {
        console.log(`📩 Message WhatsApp reçu pour ${userId}`);
        await saveLog("whatsapp_messages", {
          user_id: userId,
          from: message.key.remoteJid,
          content: message.message?.conversation || message.message?.extendedTextMessage?.text || "Message non textuel",
          timestamp: new Date().toISOString(),
          direction: "incoming"
        });
      }
    });
  }

  async getQRCode(userId) {
    const session = this.sessions.get(userId);
    return session ? session.qrCode : null;
  }

  async getStatus(userId) {
    const session = this.sessions.get(userId);
    if (!session || !session.sock) return { connected: false };
    return { connected: session.connected, user: session.sock.user || null };
  }

  async logout(userId) {
    try {
      const session = this.sessions.get(userId);
      if (session && session.sock) {
        await session.sock.logout();
      }
      this.cleanupSession(userId);
      return true;
    } catch (error) {
      console.error(`❌ Erreur logout WhatsApp (${userId}) :`, error.message);
      return false;
    }
  }

  cleanupSession(userId) {
    this.sessions.delete(userId);
    const sessionPath = this.getSessionPath(userId);
    if (fs.existsSync(sessionPath)) {
      try {
        fs.rmSync(sessionPath, { recursive: true, force: true });
        console.log(`🧹 Session WhatsApp nettoyée pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur nettoyage session (${userId}) :`, error.message);
      }
    }
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== MODULE RENDELL (E-MAILS) ====================
// Service de messagerie intégré (simulation d'API propriétaire)
async function checkRendellAuth(userId) {
  // Vérifier en base Supabase si les identifiants existent pour cet utilisateur
  if (!supabaseReady) return false;
  try {
    const { data, error } = await supabase
      .from("rendell_credentials")
      .select("id")
      .eq("user_id", userId)
      .maybeSingle();
    if (error) {
      console.error("❌ Erreur checkRendellAuth :", error.message);
      return false;
    }
    return !!data;
  } catch (error) {
    console.error("❌ Erreur checkRendellAuth :", error.message);
    return false;
  }
}

async function readEmails(userId) {
  // Simuler la lecture des e-mails (à remplacer par l'appel réel au service Rendell)
  if (!(await checkRendellAuth(userId))) {
    return { authenticated: false, message: "Identifiants de messagerie requis." };
  }
  // Logique de récupération des e-mails (simulation)
  const emails = [
    { id: 1, subject: "Rapport hebdomadaire", from: "direction@hiklon.com", date: new Date().toISOString() },
    { id: 2, subject: "Réunion de planification", from: "equipe@hiklon.com", date: new Date().toISOString() }
  ];
  return { authenticated: true, emails };
}

// ==================== MODULE DE RECHERCHE (Wikimedia, Wikipédia, DuckDuckGo, DocShare) ====================
async function searchWikimedia(query) {
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=5&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    if (!pages) return { success: true, images: [] };
    const images = Object.values(pages)
      .map((page) => ({
        title: page.title,
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value || null
      }))
      .filter((img) => img.url);
    return { success: true, images };
  } catch (error) {
    console.error("❌ Erreur searchWikimedia :", error.message);
    return { success: false, error: "Recherche d'images indisponible." };
  }
}

async function searchWikipedia(query) {
  try {
    const searchUrl = `https://fr.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(
      query
    )}&format=json&origin=*`;
    const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
    const results = searchResponse.data?.query?.search;
    if (!results || results.length === 0) return { success: true, title: null, summary: null, url: null };
    const title = results[0].title;
    const summaryUrl = `https://fr.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&titles=${encodeURIComponent(
      title
    )}&format=json&origin=*`;
    const summaryResponse = await axios.get(summaryUrl, { timeout: 10000 });
    const pages = summaryResponse.data?.query?.pages;
    const page = pages ? Object.values(pages)[0] : null;
    const summary = page?.extract || null;
    return {
      success: true,
      title,
      summary: summary ? summary.substring(0, 2000) : null,
      url: `https://fr.wikipedia.org/wiki/${encodeURIComponent(title)}`
    };
  } catch (error) {
    console.error("❌ Erreur searchWikipedia :", error.message);
    return { success: false, error: "Recherche Wikipédia indisponible." };
  }
}

async function searchDuckDuckGo(query) {
  try {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await axios.get(url, { timeout: 10000 });
    return {
      success: true,
      abstract: response.data?.AbstractText || null,
      heading: response.data?.Heading || null,
      url: response.data?.AbstractURL || null
    };
  } catch (error) {
    console.error("❌ Erreur searchDuckDuckGo :", error.message);
    return { success: false, error: "Recherche web indisponible." };
  }
}

async function searchDocShare(query) {
  // Service de documents internes (simulation ou appel réel)
  try {
    // Exemple : appel à une API de partage de documents (peut être remplacé)
    const url = `https://docshare.example.com/api/search?q=${encodeURIComponent(query)}`;
    // Pour éviter les erreurs réseau, on simule un retour
    // const response = await axios.get(url, { timeout: 10000 });
    // return response.data;
    return { success: true, documents: [{ title: "Document interne", url: "https://docshare.example.com/doc/1" }] };
  } catch (error) {
    console.error("❌ Erreur searchDocShare :", error.message);
    return { success: false, error: "Recherche de documents indisponible." };
  }
}

// ==================== ROUTES ====================

// ==================== ROUTE RACINE (HEALTHCHECK) ====================
app.get("/", (req, res) => {
  res.status(200).json({ status: "ok", message: "Serveur Milo opérationnel" });
});

// ==================== ROUTE HEALTHCHECK API ====================
app.get("/api/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    message: "Serveur Milo opérationnel",
    timestamp: new Date().toISOString(),
    database: supabaseReady ? "connected" : "disconnected"
  });
});

// ==================== ROUTE /api/chat ====================
app.post("/api/chat", apiLimiter, async (req, res) => {
  try {
    const { message, user, userId, history } = req.body;
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ status: "error", message: "Le champ 'message' est requis." });
    }

    const aiResponse = await callOpenRouter(message, history || []);
    const logUserId = userId || user || "anonymous";
    await saveLog("chat_logs", {
      user_id: logUserId,
      message,
      response: aiResponse,
      created_at: new Date().toISOString()
    });

    return res.status(200).json({ status: "success", message: aiResponse });
  } catch (error) {
    console.error("❌ Erreur /api/chat :", error.message);
    return res.status(500).json({ status: "error", message: "Erreur interne du serveur." });
  }
});

// ==================== ROUTE /api/tools ====================
app.post("/api/tools", apiLimiter, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const { userId } = req.body;

    if (!action || typeof action !== "string") {
      return res.status(400).json({ error: "Le champ 'action' ou 'actionType' est requis." });
    }

    let result;

    switch (action) {
      case "search_wikipedia":
      case "wikipedia": {
        if (!data.query) return res.status(400).json({ error: "Paramètre 'query' requis." });
        result = await searchWikipedia(data.query);
        break;
      }
      case "search_web":
      case "web_search":
      case "web": {
        if (!data.query) return res.status(400).json({ error: "Paramètre 'query' requis." });
        result = await searchDuckDuckGo(data.query);
        break;
      }
      case "search_images":
      case "images": {
        if (!data.query) return res.status(400).json({ error: "Paramètre 'query' requis." });
        result = await searchWikimedia(data.query);
        break;
      }
      case "docshare":
      case "documents": {
        if (!data.query) return res.status(400).json({ error: "Paramètre 'query' requis." });
        result = await searchDocShare(data.query);
        break;
      }
      case "whatsapp_qr":
      case "whatsapp": {
        // Gestion de l'authentification WhatsApp
        if (!userId) return res.status(400).json({ error: "userId requis pour WhatsApp." });
        await whatsappManager.initSession(userId);
        // Attendre un court instant pour que le QR soit généré
        let qrCode = null;
        const start = Date.now();
        while (!qrCode && Date.now() - start < 30000) {
          qrCode = await whatsappManager.getQRCode(userId);
          if (qrCode) break;
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        if (qrCode) {
          result = {
            success: true,
            message: "QR Code WhatsApp généré. Scannez pour connecter.",
            qrCodeBase64: qrCode,
            expiresIn: 60
          };
        } else {
          const status = await whatsappManager.getStatus(userId);
          if (status.connected) {
            result = { success: true, message: "WhatsApp déjà connecté." };
          } else {
            result = { success: false, error: "Délai dépassé pour la génération du QR Code." };
          }
        }
        break;
      }
      case "whatsapp_status": {
        if (!userId) return res.status(400).json({ error: "userId requis." });
        const status = await whatsappManager.getStatus(userId);
        result = { success: true, connected: status.connected, user: status.user };
        break;
      }
      case "whatsapp_logout": {
        if (!userId) return res.status(400).json({ error: "userId requis." });
        const logoutResult = await whatsappManager.logout(userId);
        result = { success: logoutResult, message: logoutResult ? "WhatsApp déconnecté." : "Erreur lors de la déconnexion." };
        break;
      }
      case "rendell_auth_check": {
        if (!userId) return res.status(400).json({ error: "userId requis." });
        const isAuthenticated = await checkRendellAuth(userId);
        result = { success: true, authenticated: isAuthenticated, message: isAuthenticated ? "Identifiants de messagerie présents." : "Identifiants de messagerie requis." };
        break;
      }
      case "rendell_read_emails": {
        if (!userId) return res.status(400).json({ error: "userId requis." });
        const emailResult = await readEmails(userId);
        result = emailResult;
        break;
      }
      default:
        return res.status(400).json({ error: `Action inconnue : ${action}` });
    }

    return res.status(200).json(result);
  } catch (error) {
    console.error("❌ Erreur /api/tools :", error.message);
    return res.status(500).json({ error: "Erreur lors de l'exécution de l'outil." });
  }
});

// ==================== ROUTE /api/clear-cache ====================
app.post("/api/clear-cache", (req, res) => {
  console.log("🧹 Demande de nettoyage du cache");
  // Ici, on pourrait vider un cache en mémoire (ex: Map)
  // Pour l'exemple, on renvoie simplement un succès
  res.status(200).json({ status: "success", message: "Cache nettoyé avec succès" });
});

// ==================== ROUTE /api/info ====================
app.get("/api/info", (req, res) => {
  res.status(200).json({
    status: "success",
    uptime: process.uptime(),
    version: "1.0.0",
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || "development"
  });
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée : ${req.method} ${req.url}`);
  res.status(404).json({ error: "Route non trouvée sur le serveur" });
});

// ==================== MIDDLEWARE DE GESTION DES ERREURS ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express :", err.message);
  console.error(err.stack);
  if (res.headersSent) return next(err);
  res.status(err.status || 500).json({ error: "Erreur interne du serveur." });
});

// ==================== GESTION DES ERREURS GLOBALES ====================
process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception :", error.message);
  console.error(error.stack);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection :", reason);
});

// ==================== DÉMARRAGE DU SERVEUR ====================
const PORT = process.env.PORT || 3000;

const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`🌍 Environnement : ${process.env.NODE_ENV || "development"}`);
  console.log(`💾 Supabase : ${supabaseReady ? "connecté" : "non connecté"}`);
  console.log(`🔑 OpenRouter : ${process.env.OPENROUTER_API_KEY ? "configuré" : "non configuré"}`);
  console.log("========================================");
});

// Configuration des timeouts
server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Signal ${signal} reçu. Arrêt gracieux...`);
  // Fermer toutes les sessions WhatsApp
  for (const [userId, session] of whatsappManager.sessions) {
    if (session.sock) {
      try {
        await session.sock.end();
        console.log(`👋 Session WhatsApp fermée pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur fermeture session ${userId} :`, error.message);
      }
    }
  }
  server.close(() => {
    console.log("✅ Serveur arrêté proprement.");
    process.exit(0);
  });
  setTimeout(() => {
    console.error("⏰ Arrêt forcé après délai.");
    process.exit(1);
  }, 10000);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

module.exports = app;
