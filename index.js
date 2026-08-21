// ==================== INDEX.JS - SERVEUR MILO ====================
// Backend Express.js pour l'agent MILO
// Communication avec Frontend Firebase via CORS ouvert
// Intégration : Supabase + OpenRouter

// ==================== CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ====================
require("dotenv").config();

// ==================== IMPORTS DES MODULES ====================
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

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
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
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
    contentSecurityPolicy: false,
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
  message: {
    error: "Trop de requêtes. Veuillez réessayer dans 15 minutes.",
  },
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

// ==================== FONCTION OPENROUTER ====================
async function callOpenRouter(userMessage) {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;

    if (!apiKey) {
      console.warn("⚠️ OPENROUTER_API_KEY non configurée.");
      return "Je suis MILO, votre assistant IA. Veuillez configurer la clé API OpenRouter.";
    }

    console.log("🤖 Appel OpenRouter...");

    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat",
        messages: [
          {
            role: "system",
            content:
              "Tu es MILO, une intelligence artificielle créée par HIKLON Technologie. Réponds de manière claire, concise et utile.",
          },
          {
            role: "user",
            content: userMessage,
          },
        ],
        temperature: 0.7,
        max_tokens: 1000,
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

    if (!content) {
      throw new Error("Réponse OpenRouter vide");
    }

    return content;
  } catch (error) {
    console.error("❌ Erreur OpenRouter :", error.message);
    if (error.response) {
      console.error("Détails :", JSON.stringify(error.response.data).substring(0, 500));
    }
    return "Désolé, je rencontre une difficulté technique. Veuillez réessayer plus tard.";
  }
}

// ==================== FONCTION SUPABASE ====================
async function saveChatLog(userId, message, response) {
  try {
    if (!supabaseReady || !supabase) {
      return null;
    }

    const { data, error } = await supabase.from("chat_logs").insert([
      {
        user_id: userId || "anonymous",
        message: message,
        response: response,
        created_at: new Date().toISOString(),
      },
    ]);

    if (error) {
      console.error("❌ Erreur Supabase (saveChatLog) :", error.message);
      return null;
    }

    console.log("✅ Log de chat sauvegardé.");
    return data;
  } catch (error) {
    console.error("❌ Erreur saveChatLog :", error.message);
    return null;
  }
}

// ==================== ROUTES ====================

// ==================== ROUTE RACINE (HEALTHCHECK) ====================
app.get("/", (req, res) => {
  console.log("✅ Healthcheck demandé");
  res.status(200).json({
    status: "ok",
    message: "Serveur Milo opérationnel",
  });
});

// ==================== ROUTE HEALTHCHECK API ====================
app.get("/api/health", (req, res) => {
  console.log("🔍 Healthcheck API demandé");
  res.status(200).json({
    status: "ok",
    message: "Serveur Milo opérationnel",
    timestamp: new Date().toISOString(),
    database: supabaseReady ? "connected" : "disconnected",
  });
});

// ==================== ROUTE PRINCIPALE DE CHAT ====================
app.post("/api/chat", apiLimiter, async (req, res) => {
  try {
    const { message, user, userId } = req.body;

    console.log(
      `💬 Message reçu de ${userId || user || "anonymous"} : "${
        message ? message.substring(0, 100) : "vide"
      }"`
    );

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        error: "Le champ 'message' est requis et doit être une chaîne non vide.",
      });
    }

    const aiResponse = await callOpenRouter(message);

    // Utiliser userId pour les logs (fallback sur user ou "anonymous")
    await saveChatLog(userId || user || "anonymous", message, aiResponse);

    // CORRECTION : Réponse formatée pour correspondre au frontend
    return res.status(200).json({
      status: "success",
      message: aiResponse,
    });
  } catch (error) {
    console.error("❌ Erreur route /api/chat :", error.message);
    return res.status(500).json({
      status: "error",
      message: "Erreur lors du traitement de votre message.",
    });
  }
});

// ==================== ROUTE DES OUTILS ====================
app.post("/api/tools", apiLimiter, async (req, res) => {
  try {
    // CORRECTION : accepter action ou actionType
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};

    console.log(`🔧 Outil demandé : ${action || "inconnu"}`);

    if (!action || typeof action !== "string") {
      return res.status(400).json({
        error: "Le champ 'action' ou 'actionType' est requis.",
      });
    }

    let result;

    switch (action) {
      case "search_wikipedia":
      case "wikipedia": {
        if (!data || !data.query) {
          return res.status(400).json({
            error: "Le paramètre 'query' est requis pour la recherche Wikipédia.",
          });
        }
        try {
          const searchUrl = `https://fr.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(
            data.query
          )}&format=json&origin=*`;
          const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
          const results = searchResponse.data?.query?.search;
          result = {
            success: true,
            results: results
              ? results.slice(0, 5).map((r) => ({ title: r.title, snippet: r.snippet }))
              : [],
          };
        } catch (wikiError) {
          result = { success: false, error: wikiError.message };
        }
        break;
      }

      case "search_web":
      case "web_search":
      case "web": {
        if (!data || !data.query) {
          return res.status(400).json({
            error: "Le paramètre 'query' est requis pour la recherche web.",
          });
        }
        try {
          const webUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(
            data.query
          )}&format=json&no_html=1&skip_disambig=1`;
          const webResponse = await axios.get(webUrl, { timeout: 10000 });
          result = {
            success: true,
            abstract: webResponse.data?.AbstractText || null,
            heading: webResponse.data?.Heading || null,
            url: webResponse.data?.AbstractURL || null,
          };
        } catch (webError) {
          result = { success: false, error: webError.message };
        }
        break;
      }

      case "search_images":
      case "images": {
        if (!data || !data.query) {
          return res.status(400).json({
            error: "Le paramètre 'query' est requis pour la recherche d'images.",
          });
        }
        try {
          const imageUrl = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
            data.query
          )}&gsrlimit=5&prop=imageinfo&iiprop=url&iiurlwidth=800&format=json&origin=*`;
          const imageResponse = await axios.get(imageUrl, { timeout: 10000 });
          const pages = imageResponse.data?.query?.pages;
          const images = pages
            ? Object.values(pages)
                .map((page) => ({
                  title: page.title,
                  url: page.imageinfo?.[0]?.thumburl || page.imageinfo?.[0]?.url || null,
                }))
                .filter((img) => img.url)
            : [];
          result = { success: true, images };
        } catch (imageError) {
          result = { success: false, error: imageError.message };
        }
        break;
      }

      case "whatsapp_qr":
      case "whatsapp": {
        // CORRECTION : gérer le cas 'whatsapp' envoyé par le client
        result = {
          success: true,
          message: "QR Code WhatsApp demandé.",
          qrCodeBase64: "BASE64_QR_CODE_PLACEHOLDER",
          expiresIn: 60,
        };
        break;
      }

      case "voice_siri":
      case "siri": {
        result = {
          success: true,
          message: "Synthèse vocale demandée.",
          audioText: data?.text || "Je vous écoute",
          locale: data?.locale || "fr-FR",
        };
        break;
      }

      default:
        return res.status(400).json({
          error: `Action inconnue : ${action}`,
        });
    }

    return res.status(200).json(result);
  } catch (error) {
    console.error("❌ Erreur route /api/tools :", error.message);
    return res.status(500).json({
      error: "Erreur lors de l'exécution de l'outil.",
    });
  }
});

// ==================== ROUTE CLEAR-CACHE ====================
app.post("/api/clear-cache", (req, res) => {
  console.log("🧹 Demande de nettoyage du cache");
  // Ici vous pouvez implémenter un vrai nettoyage (ex: vider un cache en mémoire)
  // Pour l'instant, simple réponse de succès
  res.status(200).json({
    status: "success",
    message: "Cache nettoyé avec succès",
  });
});

// ==================== ROUTE INFO ====================
app.get("/api/info", (req, res) => {
  console.log("ℹ️ Demande d'informations");
  res.status(200).json({
    status: "success",
    uptime: process.uptime(),
    version: "1.0.0",
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || "development",
  });
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée : ${req.method} ${req.url}`);
  res.status(404).json({
    error: "Route non trouvée sur le serveur",
  });
});

// ==================== MIDDLEWARE DE GESTION DES ERREURS ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express :", err.message);
  console.error(err.stack);

  if (res.headersSent) {
    return next(err);
  }

  res.status(err.status || 500).json({
    error: "Erreur interne du serveur.",
  });
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

  if (supabaseReady) {
    console.log("✅ Supabase connecté");
  } else {
    console.log("⚠️ Supabase non connecté");
  }

  if (process.env.OPENROUTER_API_KEY) {
    console.log("✅ OpenRouter configuré");
  } else {
    console.log("⚠️ OpenRouter non configuré");
  }

  console.log("========================================");
});

// Configuration des timeouts
server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Signal ${signal} reçu. Arrêt gracieux...`);

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
