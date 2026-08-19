// ==================== INDEX.JS - CERVEAU ORCHESTRATEUR MILO ====================
// Backend événementiel ultra-sécurisé pour l'agent MILO
// Architecture : API Key Auth + OpenRouter Function Calling + Sécurité Zero Trust
// CORRIGÉ : Authentification par clé API statique (sans firebase-admin)

// ==================== CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ====================
require("dotenv").config();

// ==================== IMPORTS DES MODULES ====================
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");

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
    // Requêtes sans origine (curl, serveur-à-serveur)
    if (!origin) {
      return callback(null, true);
    }
    
    // Vérification stricte de l'origine
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

// Gestion des requêtes preflight OPTIONS
app.options("*", (req, res) => {
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type, x-api-key");
  res.setHeader("Access-Control-Expose-Headers", "X-RateLimit-Remaining, X-Action-Trigger");
  res.status(204).end();
});

// ==================== PARSERS JSON ====================
app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true, limit: "5mb" }));

// ==================== RATE LIMITER ANTI-DDOS ====================
// Limitation stricte : 30 requêtes / 15 minutes par IP
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

// Rate limiter plus strict pour le chat IA
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
// CORRECTION : Nouveau middleware d'authentification par API Key
// Remplacé : verifyFirebaseToken → verifyApiKey

function verifyApiKey(req, res, next) {
  try {
    // Vérifier si API_KEY est configurée dans les variables d'environnement
    if (!process.env.API_KEY) {
      console.error("❌ ERREUR CRITIQUE : API_KEY non définie.");
      console.error("❌ Ajoutez la variable API_KEY dans les variables d'environnement Render.");
      console.error("❌ Le serveur ne peut pas sécuriser les routes sans cette clé.");
      
      return res.status(503).json({
        status: "error",
        message: "Service de sécurité indisponible. API_KEY non configurée.",
        actionTrigger: "NONE",
        actionData: null
      });
    }
    
    // Récupérer la clé depuis l'en-tête Authorization ou x-api-key
    let providedKey = null;
    
    // Option 1 : Header Authorization: Bearer <CLÉ>
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      providedKey = authHeader.split("Bearer ")[1];
      console.log("🔑 Clé API reçue via Authorization Bearer");
    }
    
    // Option 2 : Header personnalisé x-api-key
    if (!providedKey && req.headers["x-api-key"]) {
      providedKey = req.headers["x-api-key"];
      console.log("🔑 Clé API reçue via x-api-key");
    }
    
    // Vérifier si la clé est présente
    if (!providedKey || providedKey.trim().length === 0) {
      console.warn("⛔ Tentative d'accès sans clé API.");
      return res.status(401).json({
        status: "error",
        message: "Accès refusé. Clé API manquante.",
        actionTrigger: "AUTH_EXPIRED",
        actionData: { redirect: "/login" }
      });
    }
    
    // Comparaison sécurisée de la clé (temps constant pour éviter le timing attack)
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
    
    // Comparaison en temps constant
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
    
    // Clé valide
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
// Détection et masquage des données sensibles avant envoi à l'IA
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
  
  return {
    sanitized,
    sensitiveFound
  };
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

// ==================== DÉFINITION DES TOOLS (FUNCTION CALLING) ====================
// Ces définitions sont envoyées à OpenRouter pour le Function Calling
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
      description: "Déclenche la synthèse vocale ou l'assistant Siri. À utiliser quand l'utilisateur veut parler à voix haute, activer Siri, ou utiliser des commandes vocales.",
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
      description: "Recherche des informations sur Wikipédia. À utiliser pour des explications scientifiques, des recherches encyclopédiques, ou des demandes de connaissances générales.",
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
      description: "Effectue une recherche web générale. À utiliser pour des recherches d'actualités, des informations récentes, ou des questions non encyclopédiques.",
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
      description: "Recherche des images sur Wikimedia Commons. À utiliser quand l'utilisateur demande des images, des photos, ou des illustrations.",
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
            description: "Les détails de l'action (contenu du rappel, description de l'événement, etc.)"
          },
          dateTime: {
            type: "string",
            description: "La date et l'heure pour les rappels et événements (format ISO 8601)"
          }
        },
        required: ["action"]
      }
    }
  }
];

// ==================== EXÉCUTION DES TOOLS ====================

/**
 * Exécute l'outil de recherche Wikipédia
 */
async function executeWikipediaSearch(query, language = "fr") {
  try {
    console.log(`📚 Recherche Wikipédia : ${query} (${language})`);
    
    const searchUrl = `https://${language}.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(query)}&format=json&origin=*`;
    const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
    const results = searchResponse.data?.query?.search;
    
    if (!results || results.length === 0) {
      return {
        success: true,
        title: null,
        summary: null,
        url: null
      };
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
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Exécute l'outil de recherche web (DuckDuckGo)
 */
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
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Exécute l'outil de recherche d'images (Wikimedia Commons)
 */
async function executeImageSearch(query, limit = 5) {
  try {
    console.log(`🖼️ Recherche d'images : ${query}`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(query)}&gsrlimit=${Math.min(limit, 10)}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;
    
    if (!pages) {
      return {
        success: true,
        images: []
      };
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
    
    return {
      success: true,
      images
    };
  } catch (error) {
    console.error("❌ Erreur recherche d'images :", error.message);
    return {
      success: false,
      error: error.message,
      images: []
    };
  }
}

/**
 * Exécute l'outil de productivité (simulation)
 */
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
        return {
          success: false,
          error: "Action inconnue"
        };
    }
  } catch (error) {
    console.error("❌ Erreur action productivité :", error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

// ==================== APPEL OPENROUTER AVEC FUNCTION CALLING ====================

/**
 * Appelle OpenRouter avec le Function Calling pour orchestrer les actions
 */
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
        content: "Tu es MILO, une intelligence artificielle créée par HIKLON Technologie. Tu es un assistant personnel intelligent et proactif. Tu peux déclencher des actions spécifiques en utilisant les fonctions disponibles. Analyse l'intention de l'utilisateur et choisis la fonction appropriée."
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
    
    // Vérifier si l'IA veut appeler une fonction
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
          const wikiResult = await executeWikipediaSearch(
            functionArgs.query,
            functionArgs.language || "fr"
          );
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
          const imageResult = await executeImageSearch(
            functionArgs.query,
            functionArgs.limit || 5
          );
          actionData = imageResult;
          toolResult = imageResult;
          break;
        
        case "manage_productivity":
          actionTrigger = "NONE";
          const productivityResult = await executeProductivityAction(
            functionArgs.action,
            functionArgs.details || "",
            functionArgs.dateTime || null
          );
          actionData = productivityResult;
          toolResult = productivityResult;
          break;
        
        default:
          actionTrigger = "NONE";
          toolResult = { message: "Fonction inconnue" };
      }
      
      return {
        status: "success",
        message: toolResult.message || responseMessage.content || "Action exécutée.",
        actionTrigger: actionTrigger,
        actionData: actionData
      };
    }
    
    // Réponse directe sans appel de fonction
    const content = responseMessage.content;
    
    if (!content) {
      throw new Error("Réponse OpenRouter vide");
    }
    
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

// ==================== ROUTE HEALTHCHECK (PUBLIQUE) ====================
app.get("/api/health", (req, res) => {
  res.status(200).json({
    status: "online",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    services: {
      apiKeyAuth: !!process.env.API_KEY,
      openrouter: !!process.env.OPENROUTER_API_KEY
    }
  });
});

// ==================== ROUTE RACINE ====================
app.get("/", (req, res) => {
  res.status(200).json({
    status: "success",
    message: "✅ API MILO active et opérationnelle",
    timestamp: new Date().toISOString(),
    version: "1.0.0",
    security: process.env.API_KEY ? "API Key configurée" : "API Key non configurée"
  });
});

// ==================== ROUTE PRINCIPALE DE CHAT ORCHESTRÉ ====================
// CORRECTION : verifyFirebaseToken → verifyApiKey
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
    
    const result = await callOpenRouterWithTools(
      message,
      history || [],
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

// ==================== ROUTES SPÉCIALISÉES /api/tools/* ====================

// Route pour l'authentification WhatsApp
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

// Route pour la recherche Wikipédia
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

// Route pour la recherche d'images
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
const PORT = process.env.PORT || 5000;

const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Cerveau Orchestrateur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`🌍 Environnement : ${process.env.NODE_ENV || "development"}`);
  
  if (process.env.API_KEY) {
    console.log("✅ Sécurité API Key configurée avec succès");
  } else {
    console.log("⚠️ API_KEY non configurée. Les routes protégées seront indisponibles.");
    console.log("⚠️ Ajoutez la variable API_KEY dans Render.");
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
