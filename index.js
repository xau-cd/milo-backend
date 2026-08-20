// ==================== SERVER.JS - CERVEAU ORCHESTRATEUR MILO ====================
// Backend événementiel ultra-sécurisé pour l'agent MILO
// Architecture : Supabase + OpenRouter Function Calling + Sécurité Zero Trust
// REFACTORISÉ : Supabase remplace Firebase Admin SDK

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
// CORRECTION : Utilisation de Supabase au lieu de Firebase Admin
function initializeSupabase() {
  try {
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_KEY;
    
    if (!supabaseUrl || !supabaseKey) {
      console.warn("⚠️ SUPABASE_URL ou SUPABASE_KEY non défini.");
      console.warn("⚠️ Le serveur démarre en mode dégradé (sans base de données).");
      console.warn("⚠️ Ajoutez SUPABASE_URL et SUPABASE_KEY dans les variables d'environnement Render.");
      return null;
    }
    
    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false
      }
    });
    
    console.log("✅ Supabase initialisé avec succès.");
    console.log(`📁 URL : ${supabaseUrl}`);
    
    return supabase;
  } catch (error) {
    console.error("❌ Erreur d'initialisation Supabase :", error.message);
    console.error("❌ Le serveur démarre en mode dégradé (sans base de données).");
    return null;
  }
}

const supabase = initializeSupabase();
const supabaseReady = supabase !== null;

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// ==================== CONFIGURATION HELMET (SÉCURITÉ) ====================
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  crossOriginEmbedderPolicy: false,
  contentSecurityPolicy: false,
  hsts: {
    maxAge: 31536000,
    includeSubDomains: true,
    preload: true
  },
  noSniff: true,
  xssFilter: true,
  referrerPolicy: { policy: "strict-origin-when-cross-origin" }
}));

// ==================== CONFIGURATION CORS STRICTE (ZERO TRUST) ====================
const ALLOWED_ORIGINS = [
  "https://milo-ead21.web.app",
  "https://milo-ead21.firebaseapp.com",
  "http://localhost:3000",
  "http://localhost:5000",
  "http://localhost:5173",
  "http://localhost:8080"
];

app.use(cors({
  origin: function(origin, callback) {
    if (!origin) {
      return callback(null, true);
    }
    
    if (ALLOWED_ORIGINS.includes(origin)) {
      console.log(`✅ CORS autorisé : ${origin}`);
      callback(null, true);
    } else {
      console.warn(`⛔ CORS rejeté : ${origin}`);
      callback(null, false);
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
  allowedHeaders: ["Authorization", "Content-Type", "X-Requested-With", "Accept", "Origin", "x-api-key"],
  exposedHeaders: ["X-RateLimit-Remaining", "X-RateLimit-Limit", "X-Action-Trigger"],
  credentials: true,
  maxAge: 86400
}));

app.options("*", (req, res) => {
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type, x-api-key");
  res.setHeader("Access-Control-Expose-Headers", "X-RateLimit-Remaining, X-Action-Trigger");
  res.status(204).end();
});

// ==================== PARSERS JSON ====================
app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true, limit: "5mb" }));

// ==================== RATE LIMITER ANTI-DDOS ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    status: "error",
    message: "Trop de requêtes. Veuillez réessayer dans 15 minutes.",
    actionTrigger: "NONE",
    actionData: null
  },
  handler: (req, res) => {
    console.warn(`⛔ Rate limit dépassé pour IP : ${req.ip}`);
    res.status(429).json({
      status: "error",
      message: "Trop de requêtes. Veuillez réessayer dans 15 minutes.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

const chatLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    status: "error",
    message: "Limite de messages IA atteinte. Veuillez patienter.",
    actionTrigger: "NONE",
    actionData: null
  },
  handler: (req, res) => {
    console.warn(`⛔ Rate limit chat dépassé pour IP : ${req.ip}`);
    res.status(429).json({
      status: "error",
      message: "Limite de messages IA atteinte. Veuillez patienter.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
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

// ==================== MIDDLEWARE DE SÉCURITÉ PAR CLÉ API ====================
function verifyApiKey(req, res, next) {
  try {
    if (!process.env.API_KEY) {
      console.error("❌ ERREUR CRITIQUE : API_KEY non définie.");
      return res.status(503).json({
        status: "error",
        message: "Service de sécurité indisponible. API_KEY non configurée.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    let providedKey = null;
    
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      providedKey = authHeader.split("Bearer ")[1];
    }
    
    if (!providedKey && req.headers["x-api-key"]) {
      providedKey = req.headers["x-api-key"];
    }
    
    if (!providedKey || providedKey.trim().length === 0) {
      console.warn("⛔ Tentative d'accès sans clé API.");
      return res.status(401).json({
        status: "error",
        message: "Accès refusé. Clé API manquante.",
        actionTrigger: "AUTH_EXPIRED",
        actionData: { redirect: "/login" }
      });
    }
    
    const expectedKey = process.env.API_KEY;
    const providedKeyBuffer = Buffer.from(providedKey);
    const expectedKeyBuffer = Buffer.from(expectedKey);
    
    if (providedKeyBuffer.length !== expectedKeyBuffer.length) {
      console.warn("⛔ Tentative d'accès avec clé API invalide (longueur différente).");
      return res.status(401).json({
        status: "error",
        message: "Accès refusé. Clé API invalide.",
        actionTrigger: "AUTH_EXPIRED",
        actionData: { redirect: "/login" }
      });
    }
    
    let isValid = true;
    for (let i = 0; i < providedKeyBuffer.length; i++) {
      if (providedKeyBuffer[i] !== expectedKeyBuffer[i]) {
        isValid = false;
        break;
      }
    }
    
    if (!isValid) {
      console.warn("⛔ Tentative d'accès avec clé API invalide.");
      return res.status(401).json({
        status: "error",
        message: "Accès refusé. Clé API invalide.",
        actionTrigger: "AUTH_EXPIRED",
        actionData: { redirect: "/login" }
      });
    }
    
    console.log("✅ Clé API valide. Accès autorisé.");
    req.apiKeyAuthenticated = true;
    next();
    
  } catch (error) {
    console.error("❌ Erreur middleware de sécurité :", error.message);
    return res.status(500).json({
      status: "error",
      message: "Erreur lors de la vérification de la clé API.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
}

// ==================== FILTRE DE SÉCURITÉ - SANITIZATION ====================
const SENSITIVE_PATTERNS = [
  { pattern: /password/i, replacement: "[DONNÉE_SENSIBLE]" },
  { pattern: /passwd/i, replacement: "[DONNÉE_SENSIBLE]" },
  { pattern: /secret/i, replacement: "[DONNÉE_SENSIBLE]" },
  { pattern: /api[_\s-]?key/i, replacement: "[CLÉ_API_MASQUÉE]" },
  { pattern: /token/i, replacement: "[JETON_MASQUÉ]" },
  { pattern: /\b\d{16}\b/g, replacement: "[NUMÉRO_CARTE_MASQUÉ]" },
  { pattern: /\b\d{3}-\d{2}-\d{4}\b/g, replacement: "[SSN_MASQUÉ]" },
  { pattern: /sk-[a-zA-Z0-9]{20,}/g, replacement: "[CLÉ_OPENAI_MASQUÉE]" },
  { pattern: /AKIA[0-9A-Z]{16}/g, replacement: "[CLÉ_AWS_MASQUÉE]" },
  { pattern: /ghp_[a-zA-Z0-9]{36}/g, replacement: "[CLÉ_GITHUB_MASQUÉE]" }
];

function sanitizeMessage(message) {
  let sanitized = message;
  let sensitiveFound = false;
  
  for (const pattern of SENSITIVE_PATTERNS) {
    if (pattern.pattern.test(sanitized)) {
      sanitized = sanitized.replace(pattern.pattern, pattern.replacement);
      sensitiveFound = true;
    }
  }
  
  return { sanitized, sensitiveFound };
}

function sanitizeMiddleware(req, res, next) {
  if (req.body && req.body.message) {
    const result = sanitizeMessage(req.body.message);
    req.body.originalMessage = req.body.message;
    req.body.message = result.sanitized;
    
    if (result.sensitiveFound) {
      console.warn("⚠️ Données sensibles détectées et masquées.");
    }
  }
  next();
}

// ==================== FONCTIONS SUPABASE (BASE DE DONNÉES) ====================

/**
 * Sauvegarde un log de chat dans Supabase
 */
async function saveChatLog(userId, message, response, actionTrigger) {
  try {
    if (!supabaseReady) {
      console.warn("⚠️ Supabase non disponible. Log non sauvegardé.");
      return null;
    }
    
    const { data, error } = await supabase
      .from("chat_logs")
      .insert([
        {
          user_id: userId || "anonymous",
          message: message,
          response: response,
          action_trigger: actionTrigger || "NONE",
          created_at: new Date().toISOString()
        }
      ]);
    
    if (error) {
      console.error("❌ Erreur Supabase (saveChatLog) :", error.message);
      return null;
    }
    
    console.log("✅ Log de chat sauvegardé dans Supabase.");
    return data;
  } catch (error) {
    console.error("❌ Erreur saveChatLog :", error.message);
    return null;
  }
}

/**
 * Récupère l'historique de chat d'un utilisateur
 */
async function getChatHistory(userId, limit = 10) {
  try {
    if (!supabaseReady) {
      console.warn("⚠️ Supabase non disponible. Historique non récupéré.");
      return [];
    }
    
    const { data, error } = await supabase
      .from("chat_logs")
      .select("*")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit);
    
    if (error) {
      console.error("❌ Erreur Supabase (getChatHistory) :", error.message);
      return [];
    }
    
    return data || [];
  } catch (error) {
    console.error("❌ Erreur getChatHistory :", error.message);
    return [];
  }
}

/**
 * Sauvegarde un utilisateur dans Supabase
 */
async function saveUser(userData) {
  try {
    if (!supabaseReady) {
      console.warn("⚠️ Supabase non disponible. Utilisateur non sauvegardé.");
      return null;
    }
    
    const { data, error } = await supabase
      .from("users")
      .upsert([
        {
          id: userData.id || userData.uid,
          email: userData.email,
          display_name: userData.displayName || userData.email?.split("@")[0],
          whatsapp_connected: false,
          created_at: new Date().toISOString(),
          last_login: new Date().toISOString()
        }
      ], { onConflict: "id" });
    
    if (error) {
      console.error("❌ Erreur Supabase (saveUser) :", error.message);
      return null;
    }
    
    console.log("✅ Utilisateur sauvegardé dans Supabase.");
    return data;
  } catch (error) {
    console.error("❌ Erreur saveUser :", error.message);
    return null;
  }
}

// ==================== DÉFINITION DES TOOLS (FUNCTION CALLING) ====================
const MILO_TOOLS = [
  {
    type: "function",
    function: {
      name: "trigger_whatsapp_auth",
      description: "Déclenche l'authentification WhatsApp. À utiliser quand l'utilisateur veut connecter WhatsApp, scanner un QR code, ou envoyer un message WhatsApp.",
      parameters: {
        type: "object",
        properties: {
          action: {
            type: "string",
            enum: ["connect", "scan_qr", "send_message"],
            description: "L'action WhatsApp à effectuer"
          },
          message: {
            type: "string",
            description: "Le message à envoyer (si action=send_message)"
          },
          phoneNumber: {
            type: "string",
            description: "Le numéro de téléphone du destinataire (si action=send_message)"
          }
        },
        required: ["action"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "trigger_voice_siri",
      description: "Déclenche la synthèse vocale ou l'assistant Siri.",
      parameters: {
        type: "object",
        properties: {
          text: {
            type: "string",
            description: "Le texte à prononcer à voix haute"
          },
          locale: {
            type: "string",
            enum: ["fr-FR", "en-US", "es-ES", "de-DE"],
            description: "La langue de la synthèse vocale"
          }
        },
        required: ["text"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "search_wikipedia",
      description: "Recherche des informations sur Wikipédia.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Le terme à rechercher sur Wikipédia"
          },
          language: {
            type: "string",
            enum: ["fr", "en", "es", "de"],
            description: "La langue de recherche"
          }
        },
        required: ["query"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "search_web",
      description: "Effectue une recherche web générale.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "La requête de recherche"
          }
        },
        required: ["query"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "search_images",
      description: "Recherche des images sur Wikimedia Commons.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Le terme de recherche d'images"
          },
          limit: {
            type: "integer",
            description: "Nombre maximum d'images à retourner (1-10)"
          }
        },
        required: ["query"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "manage_productivity",
      description: "Gère les tâches de productivité : lire les emails, créer des rappels, ajouter des événements au calendrier.",
      parameters: {
        type: "object",
        properties: {
          action: {
            type: "string",
            enum: ["read_emails", "create_reminder", "add_event"],
            description: "L'action de productivité à effectuer"
          },
          details: {
            type: "string",
            description: "Les détails de l'action"
          },
          dateTime: {
            type: "string",
            description: "La date et l'heure (format ISO 8601)"
          }
        },
        required: ["action"]
      }
    }
  }
];

// ==================== EXÉCUTION DES TOOLS ====================

async function executeWikipediaSearch(query, language = "fr") {
  try {
    console.log(`📚 Recherche Wikipédia : ${query} (${language})`);
    
    const searchUrl = `https://${language}.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(query)}&format=json&origin=*`;
    const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
    const results = searchResponse.data?.query?.search;
    
    if (!results || results.length === 0) {
      return { success: true, title: null, summary: null, url: null };
    }
    
    const title = results[0].title;
    
    const summaryUrl = `https://${language}.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&titles=${encodeURIComponent(title)}&format=json&origin=*`;
    const summaryResponse = await axios.get(summaryUrl, { timeout: 10000 });
    const pages = summaryResponse.data?.query?.pages;
    const page = pages ? Object.values(pages)[0] : null;
    const summary = page?.extract || null;
    
    return {
      success: true,
      title,
      summary: summary ? summary.substring(0, 2000) : null,
      url: `https://${language}.wikipedia.org/wiki/${encodeURIComponent(title)}`
    };
  } catch (error) {
    console.error("❌ Erreur recherche Wikipédia :", error.message);
    return { success: false, error: error.message };
  }
}

async function executeWebSearch(query) {
  try {
    console.log(`🔍 Recherche web : ${query}`);
    
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await axios.get(url, { timeout: 10000 });
    
    return {
      success: true,
      abstract: response.data?.AbstractText || null,
      heading: response.data?.Heading || null,
      relatedTopics: (response.data?.RelatedTopics || []).slice(0, 5).map(topic => ({
        text: topic.Text || null,
        url: topic.FirstURL || null
      }))
    };
  } catch (error) {
    console.error("❌ Erreur recherche web :", error.message);
    return { success: false, error: error.message };
  }
}

async function executeImageSearch(query, limit = 5) {
  try {
    console.log(`🖼️ Recherche d'images : ${query}`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(query)}&gsrlimit=${Math.min(limit, 10)}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    
    if (!pages) {
      return { success: true, images: [] };
    }
    
    const images = Object.values(pages).map(page => {
      const imageInfo = page.imageinfo?.[0];
      return {
        title: page.title,
        url: imageInfo?.url || null,
        thumburl: imageInfo?.thumburl || null,
        description: imageInfo?.extmetadata?.ImageDescription?.value || null
      };
    }).filter(img => img.url);
    
    return { success: true, images };
  } catch (error) {
    console.error("❌ Erreur recherche d'images :", error.message);
    return { success: false, error: error.message, images: [] };
  }
}

async function executeProductivityAction(action, details = "", dateTime = null) {
  try {
    console.log(`📋 Action productivité : ${action}`);
    
    switch (action) {
      case "read_emails":
        return {
          success: true,
          action: "read_emails",
          emails: [
            { id: 1, subject: "Réunion demain", from: "equipe@hiklon.com", date: "2024-01-15" },
            { id: 2, subject: "Rapport mensuel", from: "direction@hiklon.com", date: "2024-01-14" }
          ]
        };
      case "create_reminder":
        return {
          success: true,
          action: "create_reminder",
          reminder: {
            content: details || "Rappel",
            dateTime: dateTime || new Date().toISOString(),
            status: "created"
          }
        };
      case "add_event":
        return {
          success: true,
          action: "add_event",
          event: {
            title: details || "Événement",
            dateTime: dateTime || new Date().toISOString(),
            status: "added"
          }
        };
      default:
        return { success: false, error: "Action inconnue" };
    }
  } catch (error) {
    console.error("❌ Erreur action productivité :", error.message);
    return { success: false, error: error.message };
  }
}

// ==================== APPEL OPENROUTER AVEC FUNCTION CALLING ====================

async function callOpenRouterWithTools(userMessage, history = [], userId = null) {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;
    
    if (!apiKey) {
      throw new Error("Clé OpenRouter non configurée");
    }
    
    console.log("🤖 Appel OpenRouter avec Function Calling...");
    
    const messages = [
      {
        role: "system",
        content: "Tu es MILO, une intelligence artificielle créée par HIKLON Technologie. Tu es un assistant personnel intelligent et proactif. Analyse l'intention de l'utilisateur et choisis la fonction appropriée."
      },
      ...history.slice(-10).map(msg => ({
        role: msg.role || "user",
        content: msg.content || msg.message || ""
      })),
      {
        role: "user",
        content: userMessage
      }
    ];
    
    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: process.env.OPENROUTER_MODEL || "anthropic/claude-3.5-sonnet",
        messages: messages,
        tools: MILO_TOOLS,
        tool_choice: "auto",
        temperature: 0.7,
        max_tokens: 2000
      },
      {
        headers: {
          "Authorization": `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://milo-ead21.web.app",
          "X-Title": "MILO Orchestrator"
        },
        timeout: 55000
      }
    );
    
    const responseMessage = response.data?.choices?.[0]?.message;
    
    if (!responseMessage) {
      throw new Error("Réponse OpenRouter vide");
    }
    
    if (responseMessage.tool_calls && responseMessage.tool_calls.length > 0) {
      const toolCall = responseMessage.tool_calls[0];
      const functionName = toolCall.function.name;
      const functionArgs = JSON.parse(toolCall.function.arguments || "{}");
      
      console.log(`🔧 Fonction appelée : ${functionName}`);
      console.log(`📝 Arguments :`, functionArgs);
      
      let toolResult = null;
      let actionTrigger = "NONE";
      let actionData = null;
      
      switch (functionName) {
        case "trigger_whatsapp_auth":
          actionTrigger = "WHATSAPP_QR";
          actionData = {
            action: functionArgs.action || "connect",
            message: functionArgs.message || null,
            phoneNumber: functionArgs.phoneNumber || null
          };
          toolResult = {
            message: "Authentification WhatsApp requise. Veuillez scanner le QR code.",
            qrRequired: true
          };
          break;
        case "trigger_voice_siri":
          actionTrigger = "VOICE_SIRI";
          actionData = {
            audioText: functionArgs.text || "Je vous écoute",
            locale: functionArgs.locale || "fr-FR"
          };
          toolResult = {
            message: "Activation de la synthèse vocale.",
            audioText: functionArgs.text || "Je vous écoute"
          };
          break;
        case "search_wikipedia":
          actionTrigger = "WIKI_MODAL";
          const wikiResult = await executeWikipediaSearch(functionArgs.query, functionArgs.language || "fr");
          actionData = wikiResult;
          toolResult = wikiResult;
          break;
        case "search_web":
          actionTrigger = "WIKI_MODAL";
          const webResult = await executeWebSearch(functionArgs.query);
          actionData = webResult;
          toolResult = webResult;
          break;
        case "search_images":
          actionTrigger = "WIKI_MODAL";
          const imageResult = await executeImageSearch(functionArgs.query, functionArgs.limit || 5);
          actionData = imageResult;
          toolResult = imageResult;
          break;
        case "manage_productivity":
          actionTrigger = "NONE";
          const productivityResult = await executeProductivityAction(functionArgs.action, functionArgs.details || "", functionArgs.dateTime || null);
          actionData = productivityResult;
          toolResult = productivityResult;
          break;
        default:
          actionTrigger = "NONE";
          toolResult = { message: "Fonction inconnue" };
      }
      
      // Sauvegarder le log dans Supabase
      await saveChatLog(userId, userMessage, toolResult.message || "Action exécutée", actionTrigger);
      
      return {
        status: "success",
        message: toolResult.message || responseMessage.content || "Action exécutée.",
        actionTrigger: actionTrigger,
        actionData: actionData
      };
    }
    
    const content = responseMessage.content;
    
    if (!content) {
      throw new Error("Réponse OpenRouter vide");
    }
    
    // Sauvegarder le log dans Supabase
    await saveChatLog(userId, userMessage, content, "NONE");
    
    return {
      status: "success",
      message: content,
      actionTrigger: "NONE",
      actionData: null
    };
    
  } catch (error) {
    console.error("❌ Erreur OpenRouter :", error.message);
    if (error.response) {
      console.error("Détails :", JSON.stringify(error.response.data).substring(0, 500));
    }
    throw error;
  }
}

// ==================== ROUTES ====================

// ==================== ROUTE HEALTHCHECK OBLIGATOIRE ====================
app.get("/api/health", async (req, res) => {
  try {
    console.log("🔍 Healthcheck demandé");
    
    let databaseStatus = "disconnected";
    
    if (supabaseReady && supabase) {
      try {
        // Requête test rapide sur Supabase
        const { data, error } = await supabase
          .from("health_check")
          .select("*")
          .limit(1);
        
        if (error) {
          // La table n'existe peut-être pas encore, essayons une requête générique
          const { error: fallbackError } = await supabase
            .from("users")
            .select("id")
            .limit(1);
          
          if (!fallbackError) {
            databaseStatus = "connected";
          } else {
            console.warn("⚠️ Erreur Supabase healthcheck :", fallbackError.message);
          }
        } else {
          databaseStatus = "connected";
        }
      } catch (supabaseError) {
        console.error("❌ Erreur Supabase healthcheck :", supabaseError.message);
      }
    }
    
    res.status(200).json({
      status: "ok",
      database: databaseStatus,
      timestamp: new Date().toISOString(),
      uptime: process.uptime()
    });
  } catch (error) {
    console.error("❌ Erreur healthcheck :", error.message);
    res.status(500).json({
      status: "error",
      database: "disconnected",
      timestamp: new Date().toISOString()
    });
  }
});

// ==================== ROUTE RACINE ====================
app.get("/", (req, res) => {
  res.status(200).json({
    status: "success",
    message: "✅ API MILO active et opérationnelle",
    timestamp: new Date().toISOString(),
    version: "1.0.0",
    database: supabaseReady ? "Supabase connecté" : "Supabase non connecté"
  });
});

// ==================== ROUTE PRINCIPALE DE CHAT ORCHESTRÉ ====================
app.post("/api/chat", verifyApiKey, sanitizeMiddleware, chatLimiter, async (req, res) => {
  try {
    const { message, history, userId } = req.body;
    
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        status: "error",
        message: "Le champ 'message' est requis.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    console.log(`💬 Message reçu : "${message.substring(0, 100)}"`);
    
    // Récupérer l'historique depuis Supabase si non fourni
    let chatHistory = history || [];
    if (!chatHistory.length && userId && supabaseReady) {
      const dbHistory = await getChatHistory(userId, 10);
      chatHistory = dbHistory.map(log => ({
        role: "user",
        content: log.message
      })).reverse();
    }
    
    const result = await callOpenRouterWithTools(
      message,
      chatHistory,
      userId || "anonymous"
    );
    
    res.setHeader("X-Action-Trigger", result.actionTrigger);
    
    return res.status(200).json(result);
    
  } catch (error) {
    console.error("❌ Erreur route /api/chat :", error.message);
    
    return res.status(500).json({
      status: "error",
      message: "Erreur lors du traitement de votre demande.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

// ==================== ROUTE D'INSCRIPTION UTILISATEUR ====================
app.post("/api/auth/register", verifyApiKey, async (req, res) => {
  try {
    const { email, displayName, userId } = req.body;
    
    if (!email) {
      return res.status(400).json({
        status: "error",
        message: "Email requis.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    // Sauvegarder l'utilisateur dans Supabase
    const savedUser = await saveUser({
      id: userId || `user_${Date.now()}`,
      email: email,
      displayName: displayName || email.split("@")[0]
    });
    
    if (!savedUser) {
      return res.status(500).json({
        status: "error",
        message: "Erreur lors de l'enregistrement de l'utilisateur.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    res.status(201).json({
      status: "success",
      message: "Utilisateur enregistré avec succès.",
      actionTrigger: "NONE",
      actionData: null
    });
  } catch (error) {
    console.error("❌ Erreur inscription :", error.message);
    res.status(500).json({
      status: "error",
      message: "Erreur lors de l'inscription.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

// ==================== ROUTES SPÉCIALISÉES /api/tools/* ====================

app.get("/api/tools/whatsapp/qr", verifyApiKey, async (req, res) => {
  try {
    res.status(200).json({
      status: "success",
      message: "QR Code WhatsApp généré.",
      actionTrigger: "WHATSAPP_QR",
      actionData: {
        qrCodeBase64: "BASE64_QR_CODE_PLACEHOLDER",
        expiresIn: 60
      }
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: "Erreur lors de la génération du QR code.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

app.get("/api/tools/wiki/search", verifyApiKey, async (req, res) => {
  try {
    const query = req.query.q;
    
    if (!query) {
      return res.status(400).json({
        status: "error",
        message: "Paramètre 'q' requis.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    const result = await executeWikipediaSearch(query);
    
    res.status(200).json({
      status: "success",
      message: "Résultats Wikipédia.",
      actionTrigger: "WIKI_MODAL",
      actionData: result
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: "Erreur lors de la recherche Wikipédia.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

app.get("/api/tools/images/search", verifyApiKey, async (req, res) => {
  try {
    const query = req.query.q;
    const limit = parseInt(req.query.limit) || 5;
    
    if (!query) {
      return res.status(400).json({
        status: "error",
        message: "Paramètre 'q' requis.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    const result = await executeImageSearch(query, limit);
    
    res.status(200).json({
      status: "success",
      message: "Résultats d'images.",
      actionTrigger: "WIKI_MODAL",
      actionData: result
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: "Erreur lors de la recherche d'images.",
      actionTrigger: "NONE",
      actionData: null
    });
  }
});

// ==================== MIDDLEWARE 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée : ${req.method} ${req.url}`);
  res.status(404).json({
    status: "error",
    message: "Route non trouvée.",
    actionTrigger: "NONE",
    actionData: null
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
    status: "error",
    message: "Erreur interne du serveur.",
    actionTrigger: "NONE",
    actionData: null
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
  console.log(`🚀 Cerveau Orchestrateur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`🌍 Environnement : ${process.env.NODE_ENV || "development"}`);
  
  if (supabaseReady) {
    console.log("✅ Supabase connecté avec succès");
  } else {
    console.log("⚠️ Supabase non connecté");
    console.log("⚠️ Ajoutez SUPABASE_URL et SUPABASE_KEY dans Render");
  }
  
  if (process.env.API_KEY) {
    console.log("✅ Sécurité API Key configurée");
  } else {
    console.log("⚠️ API_KEY non configurée. Les routes protégées seront indisponibles.");
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
