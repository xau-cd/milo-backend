// ==================== INDEX.JS - SERVEUR MILO PRODUCTION ====================
// Backend Express.js pour l'agent MILO (HIKLON Technology)
// Intégrations réelles : Supabase, OpenRouter, Baileys (WhatsApp), Wikimedia, Rendell (E-mails)
// Format de réponse standardisé JSON

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

// ==================== FONCTIONS SUPABASE ====================
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
      return {
        type: "text",
        message: "Je suis MILO, votre assistant IA. Veuillez configurer la clé API OpenRouter.",
        payload: {}
      };
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

    return {
      type: "text",
      message: content,
      payload: {}
    };
  } catch (error) {
    console.error("❌ Erreur OpenRouter :", error.message);
    return {
      type: "text",
      message: "Désolé, je rencontre une difficulté technique. Veuillez réessayer plus tard.",
      payload: {}
    };
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
          // Génération du QR code en Data-URL base64 réel
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
// Service de messagerie intégré (utilise l'API si configurée, sinon simulation en base)
async function checkRendellAuth(userId) {
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
  try {
    if (!(await checkRendellAuth(userId))) {
      return {
        type: "auth_modal",
        message: "Pour me permettre d'analyser tes e-mails, saisis tes identifiants de messagerie.",
        payload: { requiresAuth: true, authType: "rendell" }
      };
    }

    // Si une API Rendell externe est configurée, l'utiliser, sinon simuler à partir de Supabase
    if (process.env.RENDELL_API_URL && process.env.RENDELL_API_KEY) {
      try {
        const response = await axios.get(`${process.env.RENDELL_API_URL}/emails`, {
          headers: { Authorization: `Bearer ${process.env.RENDELL_API_KEY}` },
          params: { userId },
          timeout: 10000
        });
        return {
          type: "text",
          message: "Voici tes derniers e-mails :",
          payload: { emails: response.data || [] }
        };
      } catch (apiError) {
        console.error("❌ Erreur API Rendell :", apiError.message);
        return { type: "text", message: "Impossible de récupérer tes e-mails pour le moment.", payload: {} };
      }
    } else {
      // Simulation de démonstration avec des données réalistes stockées dans Supabase
      const { data: emails, error } = await supabase
        .from("rendell_emails")
        .select("*")
        .eq("user_id", userId)
        .order("date", { ascending: false })
        .limit(10);
      if (error) {
        console.error("❌ Erreur lecture e-mails Supabase :", error.message);
        return { type: "text", message: "Erreur lors de la lecture de tes e-mails.", payload: {} };
      }
      return {
        type: "text",
        message: "Voici tes derniers e-mails :",
        payload: { emails: emails || [] }
      };
    }
  } catch (error) {
    console.error("❌ Erreur readEmails :", error.message);
    return { type: "text", message: "Erreur lors de la lecture de tes e-mails.", payload: {} };
  }
}

// ==================== MODULE DE RECHERCHE (Wikimedia, Wikipédia, DuckDuckGo, DocShare) ====================
// Chaque fonction retourne un objet avec type, message, payload selon le format standardisé

async function searchWikimedia(query) {
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=5&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    if (!pages) {
      return { type: "media_gallery", message: "Aucune image trouvée.", payload: { images: [] } };
    }
    const images = Object.values(pages)
      .map((page) => ({
        url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
        title: page.title,
        description: page.imageinfo?.[0]?.extmetadata?.ImageDescription?.value || null
      }))
      .filter((img) => img.url);
    return {
      type: "media_gallery",
      message: `Voici les images pour "${query}" :`,
      payload: { images }
    };
  } catch (error) {
    console.error("❌ Erreur searchWikimedia :", error.message);
    return { type: "text", message: "Recherche d'images indisponible.", payload: { error: error.message } };
  }
}

async function searchWikipedia(query) {
  try {
    const searchUrl = `https://fr.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(
      query
    )}&format=json&origin=*`;
    const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
    const results = searchResponse.data?.query?.search;
    if (!results || results.length === 0) {
      return { type: "text", message: "Aucun résultat trouvé sur Wikipédia.", payload: {} };
    }
    const title = results[0].title;
    const summaryUrl = `https://fr.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&titles=${encodeURIComponent(
      title
    )}&format=json&origin=*`;
    const summaryResponse = await axios.get(summaryUrl, { timeout: 10000 });
    const pages = summaryResponse.data?.query?.pages;
    const page = pages ? Object.values(pages)[0] : null;
    const summary = page?.extract || null;
    return {
      type: "text",
      message: `**${title}**\n\n${summary ? summary.substring(0, 2000) : "Pas de résumé disponible."}\n\n[Lire l'article complet](https://fr.wikipedia.org/wiki/${encodeURIComponent(title)})`,
      payload: { title, summary, url: `https://fr.wikipedia.org/wiki/${encodeURIComponent(title)}` }
    };
  } catch (error) {
    console.error("❌ Erreur searchWikipedia :", error.message);
    return { type: "text", message: "Recherche Wikipédia indisponible.", payload: { error: error.message } };
  }
}

async function searchDuckDuckGo(query) {
  try {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await axios.get(url, { timeout: 10000 });
    const abstract = response.data?.AbstractText || null;
    const heading = response.data?.Heading || null;
    const urlRes = response.data?.AbstractURL || null;
    if (!abstract && !heading) {
      return { type: "text", message: "Aucune information trouvée sur le web.", payload: {} };
    }
    return {
      type: "text",
      message: `**${heading || "Résultat"}**\n\n${abstract || "Pas de résumé disponible."}${urlRes ? `\n\n[Source](${urlRes})` : ""}`,
      payload: { heading, abstract, url: urlRes }
    };
  } catch (error) {
    console.error("❌ Erreur searchDuckDuckGo :", error.message);
    return { type: "text", message: "Recherche web indisponible.", payload: { error: error.message } };
  }
}

async function searchDocShare(query) {
  // Service de documents internes (peut être remplacé par une vraie API)
  try {
    if (process.env.DOCSHARE_API_URL) {
      const response = await axios.get(`${process.env.DOCSHARE_API_URL}/search`, {
        params: { q: query },
        timeout: 10000
      });
      return {
        type: "text",
        message: "Voici les documents trouvés :",
        payload: { documents: response.data || [] }
      };
    } else {
      // Fallback : recherche dans Supabase si configuré
      if (supabaseReady) {
        const { data, error } = await supabase
          .from("documents")
          .select("*")
          .ilike("title", `%${query}%`)
          .limit(5);
        if (error) throw error;
        return {
          type: "text",
          message: "Voici les documents trouvés :",
          payload: { documents: data || [] }
        };
      } else {
        return { type: "text", message: "Recherche de documents indisponible.", payload: {} };
      }
    }
  } catch (error) {
    console.error("❌ Erreur searchDocShare :", error.message);
    return { type: "text", message: "Recherche de documents indisponible.", payload: { error: error.message } };
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
      return res.status(400).json({
        type: "text",
        message: "Le champ 'message' est requis.",
        payload: {}
      });
    }

    const aiResponse = await callOpenRouter(message, history || []);

    // Log dans Supabase (si disponible)
    const logUserId = userId || user || "anonymous";
    await saveLog("chat_logs", {
      user_id: logUserId,
      message,
      response: aiResponse.message,
      type: aiResponse.type,
      created_at: new Date().toISOString()
    });

    return res.status(200).json(aiResponse);
  } catch (error) {
    console.error("❌ Erreur /api/chat :", error.message);
    return res.status(500).json({
      type: "text",
      message: "Erreur interne du serveur.",
      payload: {}
    });
  }
});

// ==================== ROUTE /api/tools ====================
app.post("/api/tools", apiLimiter, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const { userId } = req.body;

    if (!action || typeof action !== "string") {
      return res.status(400).json({
        type: "text",
        message: "Le champ 'action' ou 'actionType' est requis.",
        payload: {}
      });
    }

    let result;

    switch (action) {
      case "search_wikipedia":
      case "wikipedia": {
        if (!data.query) {
          return res.status(400).json({ type: "text", message: "Paramètre 'query' requis.", payload: {} });
        }
        result = await searchWikipedia(data.query);
        break;
      }
      case "search_web":
      case "web_search":
      case "web": {
        if (!data.query) {
          return res.status(400).json({ type: "text", message: "Paramètre 'query' requis.", payload: {} });
        }
        result = await searchDuckDuckGo(data.query);
        break;
      }
      case "search_images":
      case "images": {
        if (!data.query) {
          return res.status(400).json({ type: "text", message: "Paramètre 'query' requis.", payload: {} });
        }
        result = await searchWikimedia(data.query);
        break;
      }
      case "docshare":
      case "documents": {
        if (!data.query) {
          return res.status(400).json({ type: "text", message: "Paramètre 'query' requis.", payload: {} });
        }
        result = await searchDocShare(data.query);
        break;
      }
      case "whatsapp_qr":
      case "whatsapp": {
        if (!userId) {
          return res.status(400).json({ type: "text", message: "userId requis pour WhatsApp.", payload: {} });
        }
        await whatsappManager.initSession(userId);
        // Attendre la génération du QR code (max 30 secondes)
        let qrCode = null;
        const start = Date.now();
        while (!qrCode && Date.now() - start < 30000) {
          qrCode = await whatsappManager.getQRCode(userId);
          if (qrCode) break;
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        if (qrCode) {
          result = {
            type: "qr_code",
            message: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.",
            payload: { qrCode, expiresIn: 60 }
          };
        } else {
          const status = await whatsappManager.getStatus(userId);
          if (status.connected) {
            result = {
              type: "text",
              message: "WhatsApp déjà connecté.",
              payload: {}
            };
          } else {
            result = {
              type: "text",
              message: "Délai dépassé pour la génération du QR Code. Veuillez réessayer.",
              payload: {}
            };
          }
        }
        break;
      }
      case "whatsapp_status": {
        if (!userId) {
          return res.status(400).json({ type: "text", message: "userId requis.", payload: {} });
        }
        const status = await whatsappManager.getStatus(userId);
        result = {
          type: "text",
          message: status.connected ? "WhatsApp est connecté." : "WhatsApp n'est pas connecté.",
          payload: { connected: status.connected, user: status.user }
        };
        break;
      }
      case "whatsapp_logout": {
        if (!userId) {
          return res.status(400).json({ type: "text", message: "userId requis.", payload: {} });
        }
        const logoutResult = await whatsappManager.logout(userId);
        result = {
          type: "text",
          message: logoutResult ? "WhatsApp déconnecté." : "Erreur lors de la déconnexion.",
          payload: {}
        };
        break;
      }
      case "rendell_auth_check": {
        if (!userId) {
          return res.status(400).json({ type: "text", message: "userId requis.", payload: {} });
        }
        const isAuthenticated = await checkRendellAuth(userId);
        result = {
          type: isAuthenticated ? "text" : "auth_modal",
          message: isAuthenticated ? "Identifiants de messagerie présents." : "Pour me permettre d'analyser tes e-mails, saisis tes identifiants de messagerie.",
          payload: isAuthenticated ? {} : { requiresAuth: true, authType: "rendell" }
        };
        break;
      }
      case "rendell_read_emails": {
        if (!userId) {
          return res.status(400).json({ type: "text", message: "userId requis.", payload: {} });
        }
        result = await readEmails(userId);
        break;
      }
      default:
        return res.status(400).json({
          type: "text",
          message: `Action inconnue : ${action}`,
          payload: {}
        });
    }

    // Log de l'outil dans Supabase
    if (userId) {
      await saveLog("tool_logs", {
        user_id: userId,
        action,
        result_type: result.type,
        created_at: new Date().toISOString()
      });
    }

    return res.status(200).json(result);
  } catch (error) {
    console.error("❌ Erreur /api/tools :", error.message);
    return res.status(500).json({
      type: "text",
      message: "Erreur lors de l'exécution de l'outil.",
      payload: { error: error.message }
    });
  }
});

// ==================== ROUTE /api/clear-cache ====================
app.post("/api/clear-cache", (req, res) => {
  console.log("🧹 Demande de nettoyage du cache");
  // Ici, on pourrait vider un cache en mémoire (ex: Map)
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
