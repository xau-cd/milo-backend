// ==================== INDEX.JS - CERVEAU MILO (HIKLON TECHNOLOGIES) ====================
// Architecture : Backend (Cerveau) → Frontend (Corps)
// Technologies : Express, Supabase (optionnel), SQLite, whatsapp-web.js, qrcode, axios, cheerio, OpenRouter, Nodemailer
// Format de réponse strict : { replyText, systemAction, payload }

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const cheerio = require("cheerio");
const qrcode = require("qrcode");
const nodemailer = require("nodemailer");
const { Client, LocalAuth } = require("whatsapp-web.js");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

// ==================== CONFIGURATION ET VALIDATION DES VARIABLES D'ENVIRONNEMENT ====================
const requiredEnvVars = [
  "OPENROUTER_API_KEY",
  "SMTP_HOST",
  "SMTP_PORT",
  "SMTP_USER",
  "SMTP_PASS",
  "EMAIL_FROM"
];

const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.warn(`⚠️ Variables d'environnement manquantes: ${missingEnvVars.join(", ")}`);
  console.warn("⚠️ Certaines fonctionnalités seront désactivées jusqu'à leur configuration.");
}

// ==================== INITIALISATION SQLITE ====================
const dbDir = path.join(__dirname, "data");
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
  console.log(`📁 Dossier de données créé: ${dbDir}`);
}

const db = new sqlite3.Database(path.join(dbDir, "milo.db"));
db.serialize(() => {
  // Table des utilisateurs
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE,
    display_name TEXT,
    whatsapp_connected INTEGER DEFAULT 0,
    whatsapp_session_id TEXT,
    rendell_credentials TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  // Table des logs de chat
  db.run(`CREATE TABLE IF NOT EXISTS chat_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    message TEXT,
    response TEXT,
    system_action TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  // Table des logs d'outils
  db.run(`CREATE TABLE IF NOT EXISTS tool_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    action TEXT,
    result_type TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  // Table des sessions WhatsApp
  db.run(`CREATE TABLE IF NOT EXISTS whatsapp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    session_id TEXT UNIQUE,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);

  // Table des emails envoyés
  db.run(`CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
  )`);
});

// ==================== CONFIGURATION NODEMAILER ====================
const emailTransporter = process.env.SMTP_HOST ? nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || "587"),
  secure: process.env.SMTP_PORT === "465",
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  },
  tls: {
    rejectUnauthorized: false
  }
}) : null;

// Vérification de la connexion email au démarrage
if (emailTransporter) {
  emailTransporter.verify((error, success) => {
    if (error) {
      console.error("❌ Erreur de connexion SMTP:", error.message);
    } else {
      console.log("✅ Serveur SMTP connecté et prêt à envoyer des emails");
    }
  });
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// Configuration CORS
const corsOptions = {
  origin: function (origin, callback) {
    const allowedOrigins = [
      "https://milo-ead21.web.app",
      "http://localhost:3000",
      "http://localhost:8080",
      "http://localhost:5173"
    ];
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Origine non autorisée par CORS"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  credentials: true,
  maxAge: 86400
};

app.use(cors(corsOptions));
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:", "https:"],
        scriptSrc: ["'self'"]
      }
    }
  })
);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER ====================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limite de 100 requêtes par fenêtre
  standardHeaders: true,
  legacyHeaders: false,
  message: { 
    error: "Trop de requêtes. Veuillez réessayer dans 15 minutes.",
    systemAction: "NONE",
    payload: {}
  }
});

const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 heure
  max: 30, // limite de 30 requêtes par heure pour les actions sensibles
  standardHeaders: true,
  legacyHeaders: false,
  message: { 
    error: "Limite de requêtes atteinte pour cette action.",
    systemAction: "NONE",
    payload: {}
  }
});

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();
  req.requestId = requestId;
  
  console.log(`📥 [${requestId}] ${req.method} ${req.url}`);
  console.log(`   Headers: Content-Type=${req.headers["content-type"] || "none"}, Authorization=${req.headers.authorization ? "present" : "absent"}`);
  
  res.on("finish", () => {
    const duration = Date.now() - start;
    console.log(`📤 [${requestId}] ${req.method} ${req.url} - ${res.statusCode} - ${duration}ms`);
  });
  
  next();
});

// ==================== MIDDLEWARE D'AUTHENTIFICATION ====================
// Simplifié pour le moment - à remplacer par Firebase Admin SDK en production
const authenticateUser = (req, res, next) => {
  try {
    const userId = req.body.userId || req.query.userId || req.headers["x-user-id"];
    if (!userId) {
      return res.status(401).json({
        error: "Authentification requise",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    // Vérifier que l'utilisateur existe dans la base de données
    db.get("SELECT * FROM users WHERE id = ?", [userId], (err, user) => {
      if (err) {
        console.error("❌ Erreur DB lors de l'authentification:", err.message);
        return res.status(500).json({
          error: "Erreur interne du serveur",
          systemAction: "NONE",
          payload: {}
        });
      }
      
      if (!user) {
        // Créer l'utilisateur s'il n'existe pas
        db.run(
          "INSERT INTO users (id, display_name, created_at, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
          [userId, req.body.displayName || userId],
          (insertErr) => {
            if (insertErr) {
              console.error("❌ Erreur création utilisateur:", insertErr.message);
              return res.status(500).json({
                error: "Erreur lors de la création de l'utilisateur",
                systemAction: "NONE",
                payload: {}
              });
            }
            req.user = { id: userId, displayName: req.body.displayName || userId };
            next();
          }
        );
      } else {
        req.user = user;
        next();
      }
    });
  } catch (error) {
    console.error("❌ Erreur d'authentification:", error.message);
    return res.status(500).json({
      error: "Erreur d'authentification",
      systemAction: "NONE",
      payload: {}
    });
  }
};

// ==================== PROMPT SYSTÈME CONFIDENTIEL (INTERNE) ====================
const MILO_SYSTEM_PROMPT = `
Tu es MILO, une Intelligence Artificielle propriétaire avancée développée par HIKLON Technology.
Tu réponds avec précision, sans paresse et de manière complète.

RÈGLE ABSOLUE DE CONFIDENTIALITÉ :
Tu ne dois sous aucun prétexte expliquer ton fonctionnement interne, ni citer les technologies, API, bibliothèques ou services tiers que tu utilises.
Tu présentes toutes tes fonctions (recherche web/images, analyse e-mail, gestion WhatsApp, automatisation) comme tes propres capacités natives HIKLON.
Si un utilisateur te demande comment tu fonctionnes ou quelles API tu utilises, réponds simplement que tu es propulsé par les technologies propriétaires d'IA et d'automatisation de HIKLON Technologies.

FORMAT DE RÉPONSE OBLIGATOIRE :
Tu DOIS TOUJOURS répondre au format JSON strict suivant, sans aucun texte avant ou après le JSON :
{
  "replyText": "Ta réponse visible à l'utilisateur",
  "systemAction": "ACTION_TYPE",
  "payload": {}
}

ACTIONS DISPONIBLES :
- "NONE" : Réponse normale sans action particulière
- "RENDER_QR" : Générer un QR code WhatsApp (payload: { qrBase64 })
- "SEND_EMAIL" : Envoyer un email (payload: { to, subject, body })
- "SEND_WHATSAPP" : Envoyer un message WhatsApp (payload: { to, message })
- "SEARCH_WEB" : Rechercher sur le web (payload: { query })
- "SEARCH_IMAGES" : Rechercher des images (payload: { query })
- "RENDER_GALLERY" : Afficher une galerie d'images (payload: { images: [] })

EXEMPLES :
1. Utilisateur: "Envoie un email à jean@email.com"
   Réponse: {"replyText":"Je vais envoyer l'email. Quel est le sujet et le contenu ?","systemAction":"NONE","payload":{}}

2. Utilisateur: "Envoie un message WhatsApp à Jean"
   Réponse: {"replyText":"Je vais envoyer le message. Quel message souhaitez-vous envoyer ?","systemAction":"NONE","payload":{}}

3. Utilisateur: "Montre-moi des images de chats"
   Réponse: {"replyText":"Je recherche des images de chats...","systemAction":"SEARCH_IMAGES","payload":{"query":"chats"}}
`;

// ==================== FONCTION OPENROUTER AVEC JSON NATIF ====================
async function callOpenRouter(userMessage, history = [], userId = null) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    console.error("❌ OPENROUTER_API_KEY non configurée");
    return {
      replyText: "Service d'IA non configuré. Contactez l'administrateur.",
      systemAction: "NONE",
      payload: {},
      error: true
    };
  }

  try {
    const messages = [
      { role: "system", content: MILO_SYSTEM_PROMPT },
      ...history.slice(-10).map((m) => ({ 
        role: m.role || "user", 
        content: typeof m.content === "string" ? m.content : m.message || JSON.stringify(m.content || "")
      })),
      { role: "user", content: userMessage }
    ];

    console.log(`🤖 Appel OpenRouter avec ${messages.length} messages (userId: ${userId || "anonymous"})`);

    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat",
        messages,
        temperature: 0.7,
        max_tokens: 2000,
        response_format: { type: "json_object" }
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
    if (!content) {
      console.error("❌ Réponse OpenRouter vide");
      return {
        replyText: "Je n'ai pas pu générer une réponse. Veuillez réessayer.",
        systemAction: "NONE",
        payload: {},
        error: true
      };
    }

    // Parser la réponse JSON
    try {
      const parsedResponse = JSON.parse(content);
      
      // Valider les champs requis
      if (!parsedResponse.replyText || typeof parsedResponse.replyText !== "string") {
        throw new Error("Champ 'replyText' manquant ou invalide");
      }
      
      const systemAction = parsedResponse.systemAction || "NONE";
      const payload = parsedResponse.payload || {};
      
      console.log(`✅ Action détectée: ${systemAction}`);
      
      return {
        replyText: parsedResponse.replyText,
        systemAction,
        payload,
        error: false
      };
    } catch (parseError) {
      console.error("❌ Erreur parsing JSON OpenRouter:", parseError.message);
      console.error("   Contenu brut:", content);
      
      // Fallback: retourner le contenu brut comme texte
      return {
        replyText: content.replace(/```json\n?|\n?```/g, "").trim(),
        systemAction: "NONE",
        payload: {},
        error: false
      };
    }
  } catch (error) {
    console.error("❌ Erreur OpenRouter:", error.message);
    if (error.response) {
      console.error("   Status:", error.response.status);
      console.error("   Data:", JSON.stringify(error.response.data).slice(0, 500));
    }
    
    return {
      replyText: "Je rencontre des difficultés techniques. Veuillez réessayer dans un instant.",
      systemAction: "NONE",
      payload: {},
      error: true
    };
  }
}

// ==================== FONCTIONS DE RECHERCHE AVANCÉES ====================
async function searchWebAdvanced(query) {
  try {
    console.log(`🔍 Recherche web avancée: "${query}"`);
    
    // Essayer DuckDuckGo Instant Answer
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const ddgResponse = await axios.get(ddgUrl, { timeout: 10000 });
    
    const results = [];
    
    if (ddgResponse.data?.AbstractText) {
      results.push({
        title: ddgResponse.data.Heading || "Résultat",
        snippet: ddgResponse.data.AbstractText,
        url: ddgResponse.data.AbstractURL || null,
        source: "DuckDuckGo"
      });
    }
    
    // Recherche complémentaire avec Bing (scraping)
    try {
      const bingUrl = `https://www.bing.com/search?q=${encodeURIComponent(query)}`;
      const bingResponse = await axios.get(bingUrl, {
        timeout: 10000,
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
      });
      
      const $ = cheerio.load(bingResponse.data);
      
      $(".b_algo").each((index, element) => {
        if (results.length >= 5) return;
        
        const title = $(element).find("h2").text().trim();
        const snippet = $(element).find(".b_caption p").text().trim();
        const url = $(element).find("h2 a").attr("href");
        
        if (title && snippet && url) {
          results.push({
            title,
            snippet: snippet.slice(0, 300),
            url,
            source: "Bing"
          });
        }
      });
    } catch (bingError) {
      console.warn("⚠️ Recherche Bing échouée:", bingError.message);
    }
    
    // Recherche avec Google (scraping simplifié)
    if (results.length < 3) {
      try {
        const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&hl=fr`;
        const googleResponse = await axios.get(googleUrl, {
          timeout: 10000,
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
          }
        });
        
        const $ = cheerio.load(googleResponse.data);
        
        $("div.g").each((index, element) => {
          if (results.length >= 5) return;
          
          const title = $(element).find("h3").text().trim();
          const snippet = $(element).find(".VwiC3b").text().trim();
          const url = $(element).find("a").attr("href");
          
          if (title && snippet && url && url.startsWith("http")) {
            results.push({
              title,
              snippet: snippet.slice(0, 300),
              url,
              source: "Google"
            });
          }
        });
      } catch (googleError) {
        console.warn("⚠️ Recherche Google échouée:", googleError.message);
      }
    }
    
    return {
      query,
      results: results.slice(0, 5),
      totalResults: results.length,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("❌ Erreur recherche web avancée:", error.message);
    return {
      query,
      results: [],
      totalResults: 0,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

async function searchWikimediaImages(query, limit = 10) {
  try {
    console.log(`🖼️ Recherche d'images Wikimedia: "${query}" (limite: ${limit})`);
    
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(
      query
    )}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&format=json&origin=*`;
    
    const response = await axios.get(url, { timeout: 15000 });
    const pages = response.data?.query?.pages;
    
    if (!pages) {
      console.log("   Aucune image trouvée");
      return [];
    }
    
    const images = Object.values(pages)
      .map((page) => {
        const imageInfo = page.imageinfo?.[0];
        const extmetadata = imageInfo?.extmetadata || {};
        
        return {
          url: imageInfo?.thumburl || imageInfo?.url || null,
          thumbnail: imageInfo?.thumburl || null,
          title: page.title || "Sans titre",
          description: extmetadata.ImageDescription?.value?.replace(/<[^>]*>/g, "") || null,
          author: extmetadata.Artist?.value?.replace(/<[^>]*>/g, "") || null,
          license: extmetadata.LicenseShortName?.value || null,
          width: imageInfo?.thumbwidth || null,
          height: imageInfo?.thumbheight || null,
          pageUrl: imageInfo?.descriptionurl || null
        };
      })
      .filter((img) => img.url);
    
    console.log(`   ${images.length} images trouvées`);
    return images;
  } catch (error) {
    console.error("❌ Erreur Wikimedia:", error.message);
    return [];
  }
}

// ==================== GESTION EMAIL ====================
async function sendEmail(to, subject, body, userId = null) {
  try {
    console.log(`📧 Préparation email à ${to}`);
    
    // Validation des entrées
    if (!to || !subject || !body) {
      throw new Error("Paramètres email incomplets: to, subject et body sont requis");
    }
    
    // Validation du format email
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      throw new Error(`Format d'email invalide: ${to}`);
    }
    
    if (!emailTransporter) {
      throw new Error("Serveur SMTP non configuré");
    }
    
    const mailOptions = {
      from: process.env.EMAIL_FROM || `"MILO Assistant" <${process.env.SMTP_USER}>`,
      to,
      subject,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
          <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px 10px 0 0;">
            <h1 style="color: white; margin: 0;">📧 Message de MILO</h1>
          </div>
          <div style="background: #f8f9fa; padding: 20px; border-radius: 0 0 10px 10px;">
            <p style="color: #333; font-size: 14px; line-height: 1.6;">${body.replace(/\n/g, "<br>")}</p>
          </div>
          <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px;">
            <p>Envoyé par MILO - Assistant IA de HIKLON Technologies</p>
          </div>
        </div>
      `,
      text: body
    };
    
    const info = await emailTransporter.sendMail(mailOptions);
    console.log(`✅ Email envoyé à ${to} (Message ID: ${info.messageId})`);
    
    // Logger l'envoi
    if (userId) {
      db.run(
        "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, ?)",
        [userId, to, subject, "sent"],
        (err) => {
          if (err) console.error("❌ Erreur logging email:", err.message);
        }
      );
    }
    
    return {
      success: true,
      messageId: info.messageId,
      to,
      subject,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error("❌ Erreur envoi email:", error.message);
    
    // Logger l'échec
    if (userId) {
      db.run(
        "INSERT INTO email_logs (user_id, to_email, subject, status) VALUES (?, ?, ?, ?)",
        [userId, to || "inconnu", subject || "sans sujet", "failed"],
        (err) => {
          if (err) console.error("❌ Erreur logging email échoué:", err.message);
        }
      );
    }
    
    throw error;
  }
}

// ==================== GESTION WHATSAPP (whatsapp-web.js) ====================
class WhatsAppManager {
  constructor() {
    this.clients = new Map(); // userId -> { client, qrCode, status, ready }
    this.maxRetries = 3;
    this.retryDelay = 5000;
  }

  async initClient(userId, retryCount = 0) {
    console.log(`📱 Initialisation WhatsApp pour ${userId} (tentative ${retryCount + 1}/${this.maxRetries + 1})`);
    
    // Vérifier si le client existe déjà
    if (this.clients.has(userId)) {
      const existing = this.clients.get(userId);
      if (existing.status === "ready" && existing.client.info) {
        console.log(`   WhatsApp déjà connecté pour ${userId}`);
        return { 
          connected: true, 
          status: "ready",
          user: existing.client.info.pushname || null
        };
      }
      if (existing.status === "initializing") {
        console.log(`   Initialisation déjà en cours pour ${userId}`);
        return { connected: false, status: "initializing" };
      }
    }

    const client = new Client({
      authStrategy: new LocalAuth({ 
        clientId: userId, 
        dataPath: path.join(__dirname, "sessions") 
      }),
      puppeteer: {
        headless: true,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-gpu",
          "--disable-software-rasterizer",
          "--disable-extensions"
        ]
      }
    });

    const sessionData = { 
      client, 
      qrCode: null, 
      status: "initializing",
      ready: false,
      initAttempts: 0
    };
    
    this.clients.set(userId, sessionData);

    // Event: QR Code généré
    client.on("qr", async (qr) => {
      try {
        console.log(`📱 QR Code généré pour ${userId}`);
        sessionData.qrCode = await qrcode.toDataURL(qr, {
          width: 800,
          margin: 2,
          color: {
            dark: "#000000",
            light: "#FFFFFF"
          }
        });
        sessionData.status = "waiting_scan";
        
        // Mettre à jour la base de données
        db.run(
          "INSERT OR REPLACE INTO whatsapp_sessions (user_id, session_id, status, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
          [userId, userId, "waiting_scan"],
          (err) => {
            if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
          }
        );
      } catch (error) {
        console.error("❌ Erreur génération QR:", error.message);
      }
    });

    // Event: WhatsApp prêt
    client.on("ready", () => {
      console.log(`✅ WhatsApp connecté pour ${userId}`);
      sessionData.status = "ready";
      sessionData.ready = true;
      sessionData.qrCode = null;
      
      db.run("UPDATE users SET whatsapp_connected = 1, whatsapp_session_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
        [userId, userId],
        (err) => {
          if (err) console.error("❌ Erreur DB mise à jour WhatsApp:", err.message);
        }
      );
      
      db.run(
        "INSERT OR REPLACE INTO whatsapp_sessions (user_id, session_id, status, updated_at) VALUES (?, ?, 'connected', CURRENT_TIMESTAMP)",
        [userId, userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
    });

    // Event: Authentifié
    client.on("authenticated", () => {
      console.log(`🔐 WhatsApp authentifié pour ${userId}`);
      sessionData.status = "authenticated";
    });

    // Event: Échec d'authentification
    client.on("auth_failure", (msg) => {
      console.error(`❌ Échec auth WhatsApp ${userId}:`, msg);
      sessionData.qrCode = null;
      sessionData.status = "failed";
      sessionData.ready = false;
    });

    // Event: Déconnexion
    client.on("disconnected", (reason) => {
      console.log(`🔌 WhatsApp déconnecté pour ${userId}: ${reason}`);
      sessionData.status = "disconnected";
      sessionData.ready = false;
      sessionData.qrCode = null;
      
      db.run("UPDATE users SET whatsapp_connected = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB déconnexion WhatsApp:", err.message);
        }
      );
      
      db.run(
        "UPDATE whatsapp_sessions SET status = 'disconnected', updated_at = CURRENT_TIMESTAMP WHERE session_id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
      
      // Ne pas supprimer immédiatement pour permettre la reconnexion
      setTimeout(() => {
        if (this.clients.has(userId) && this.clients.get(userId).status === "disconnected") {
          this.clients.delete(userId);
          console.log(`🗑️ Session WhatsApp supprimée pour ${userId}`);
        }
      }, 60000);
    });

    // Initialiser le client
    try {
      await client.initialize();
      console.log(`✅ Initialisation lancée pour ${userId}`);
      return { connected: false, status: "initializing" };
    } catch (error) {
      console.error(`❌ Erreur init WhatsApp pour ${userId}:`, error.message);
      
      if (retryCount < this.maxRetries) {
        console.log(`🔄 Retry ${retryCount + 1}/${this.maxRetries} dans ${this.retryDelay}ms`);
        this.clients.delete(userId);
        await new Promise(resolve => setTimeout(resolve, this.retryDelay));
        return this.initClient(userId, retryCount + 1);
      }
      
      this.clients.delete(userId);
      throw error;
    }
  }

  async sendMessage(userId, to, message) {
    console.log(`📤 Envoi WhatsApp de ${userId} à ${to}`);
    
    const session = this.clients.get(userId);
    if (!session || !session.client || !session.ready) {
      const error = new Error(`WhatsApp non connecté pour ${userId}`);
      error.code = "WHATSAPP_NOT_CONNECTED";
      throw error;
    }
    
    try {
      // Formater le numéro de téléphone
      let formattedTo = to.replace(/[^\d]/g, "");
      if (!formattedTo.startsWith("55")) {
        formattedTo = "55" + formattedTo;
      }
      formattedTo = formattedTo + "@c.us";
      
      console.log(`   Numéro formaté: ${formattedTo}`);
      
      // Vérifier que le chat existe
      const chat = await session.client.getChatById(formattedTo);
      
      // Envoyer le message
      const result = await session.client.sendMessage(chat.id._serialized, message);
      
      console.log(`✅ Message WhatsApp envoyé à ${to}`);
      
      // Logger l'envoi
      db.run(
        "INSERT INTO tool_logs (user_id, action, result_type, created_at) VALUES (?, ?, 'whatsapp_sent', CURRENT_TIMESTAMP)",
        [userId, `send_whatsapp_${to}`],
        (err) => {
          if (err) console.error("❌ Erreur logging WhatsApp:", err.message);
        }
      );
      
      return {
        success: true,
        to,
        messageId: result.id?._serialized || null,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error(`❌ Erreur envoi WhatsApp:`, error.message);
      throw error;
    }
  }

  getQRCode(userId) {
    const session = this.clients.get(userId);
    return session ? session.qrCode : null;
  }

  getStatus(userId) {
    const session = this.clients.get(userId);
    if (!session) {
      return { connected: false, status: "not_initialized" };
    }
    
    const status = {
      connected: session.ready,
      status: session.status,
      user: session.ready && session.client.info ? session.client.info.pushname || null : null,
      phone: session.ready && session.client.info ? session.client.info.wid?.user || null : null
    };
    
    return status;
  }

  async logout(userId) {
    console.log(`🔌 Déconnexion WhatsApp pour ${userId}`);
    
    const session = this.clients.get(userId);
    if (session && session.client) {
      try {
        await session.client.logout();
        await session.client.destroy();
        console.log(`✅ WhatsApp déconnecté pour ${userId}`);
      } catch (error) {
        console.error(`❌ Erreur déconnexion WhatsApp:`, error.message);
      }
      this.clients.delete(userId);
      
      db.run("UPDATE users SET whatsapp_connected = 0, whatsapp_session_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB logout WhatsApp:", err.message);
        }
      );
      
      db.run(
        "UPDATE whatsapp_sessions SET status = 'logged_out', updated_at = CURRENT_TIMESTAMP WHERE session_id = ?",
        [userId],
        (err) => {
          if (err) console.error("❌ Erreur DB session WhatsApp:", err.message);
        }
      );
      
      return { success: true };
    }
    
    return { success: false, error: "Session non trouvée" };
  }
}

const whatsappManager = new WhatsAppManager();

// ==================== ROUTES ====================

// Route racine
app.get("/", (req, res) => {
  res.json({ 
    status: "ok", 
    message: "Serveur MILO opérationnel",
    version: "1.0.0",
    timestamp: new Date().toISOString()
  });
});

// Healthcheck
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    message: "Serveur MILO opérationnel",
    timestamp: new Date().toISOString(),
    services: {
      database: "connected",
      email: emailTransporter ? "configured" : "not_configured",
      openrouter: process.env.OPENROUTER_API_KEY ? "configured" : "not_configured",
      whatsapp: "available"
    },
    uptime: process.uptime()
  });
});

// ==================== ROUTE CHAT PRINCIPALE ====================
app.post("/api/chat", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const { message, history } = req.body;
    const userId = req.user.id;
    
    // Validation du message
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        replyText: "Le champ 'message' est requis.",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    if (message.length > 2000) {
      return res.status(400).json({
        replyText: "Message trop long (maximum 2000 caractères).",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    console.log(`💬 Chat reçu de ${userId}: "${message.slice(0, 100)}${message.length > 100 ? "..." : ""}"`);
    
    // Appel à OpenRouter avec JSON natif
    const aiResult = await callOpenRouter(message, history || [], userId);
    
    // Exécuter l'action si nécessaire
    let finalResult = aiResult;
    
    if (aiResult.systemAction && aiResult.systemAction !== "NONE") {
      try {
        const actionResult = await executeAction(aiResult.systemAction, aiResult.payload, userId);
        
        // Fusionner le résultat de l'action avec la réponse IA
        finalResult = {
          ...aiResult,
          ...actionResult,
          replyText: actionResult.replyText || aiResult.replyText,
          payload: actionResult.payload || aiResult.payload
        };
      } catch (actionError) {
        console.error(`❌ Erreur exécution action ${aiResult.systemAction}:`, actionError.message);
        finalResult = {
          ...aiResult,
          replyText: `Je n'ai pas pu exécuter l'action demandée: ${actionError.message}`,
          systemAction: "ERROR",
          payload: { error: actionError.message }
        };
      }
    }
    
    // Logger le chat
    db.run(
      "INSERT INTO chat_logs (user_id, message, response, system_action, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
      [userId, message, finalResult.replyText, finalResult.systemAction],
      (err) => {
        if (err) console.error("❌ Erreur logging chat:", err.message);
      }
    );
    
    return res.json(finalResult);
  } catch (error) {
    console.error("❌ Erreur /api/chat:", error.message);
    return res.status(500).json({
      replyText: "Erreur interne du serveur. Veuillez réessayer.",
      systemAction: "NONE",
      payload: {}
    });
  }
});

// ==================== FONCTION D'EXÉCUTION DES ACTIONS ====================
async function executeAction(action, payload, userId) {
  console.log(`⚡ Exécution action: ${action} pour ${userId}`);
  
  switch (action) {
    case "SEARCH_WEB": {
      const query = payload.query;
      if (!query) {
        throw new Error("Paramètre 'query' requis pour la recherche web");
      }
      
      const results = await searchWebAdvanced(query);
      return {
        replyText: `Voici les résultats de recherche pour "${query}":`,
        systemAction: "RENDER_SEARCH_RESULTS",
        payload: { results }
      };
    }
    
    case "SEARCH_IMAGES":
    case "RENDER_GALLERY": {
      const query = payload.query;
      if (!query) {
        throw new Error("Paramètre 'query' requis pour la recherche d'images");
      }
      
      const images = await searchWikimediaImages(query, payload.limit || 10);
      if (images.length === 0) {
        return {
          replyText: `Aucune image trouvée pour "${query}".`,
          systemAction: "NONE",
          payload: {}
        };
      }
      
      return {
        replyText: `Voici les images pour "${query}":`,
        systemAction: "RENDER_GALLERY",
        payload: { images, query }
      };
    }
    
    case "SEND_EMAIL": {
      const { to, subject, body } = payload;
      if (!to || !subject || !body) {
        throw new Error("Paramètres email incomplets");
      }
      
      const emailResult = await sendEmail(to, subject, body, userId);
      return {
        replyText: `✅ Email envoyé avec succès à ${to}`,
        systemAction: "NONE",
        payload: emailResult
      };
    }
    
    case "SEND_WHATSAPP": {
      const { to, message } = payload;
      if (!to || !message) {
        throw new Error("Paramètres WhatsApp incomplets");
      }
      
      const whatsappResult = await whatsappManager.sendMessage(userId, to, message);
      return {
        replyText: `✅ Message WhatsApp envoyé avec succès à ${to}`,
        systemAction: "NONE",
        payload: whatsappResult
      };
    }
    
    case "RENDER_QR": {
      // Vérifier si le QR code existe déjà
      let qr = whatsappManager.getQRCode(userId);
      
      if (!qr) {
        // Initialiser le client WhatsApp
        await whatsappManager.initClient(userId);
        
        // Attendre le QR code de manière asynchrone (maximum 5 secondes)
        const startTime = Date.now();
        while (!qr && Date.now() - startTime < 5000) {
          await new Promise(resolve => setTimeout(resolve, 500));
          qr = whatsappManager.getQRCode(userId);
        }
      }
      
      if (qr) {
        return {
          replyText: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.",
          systemAction: "RENDER_QR",
          payload: { qrBase64: qr, expiresIn: 60 }
        };
      } else {
        return {
          replyText: "La génération du QR Code est en cours. Veuillez patienter quelques secondes et réessayer.",
          systemAction: "NONE",
          payload: {}
        };
      }
    }
    
    default: {
      throw new Error(`Action inconnue: ${action}`);
    }
  }
}

// ==================== ROUTES OUTILS ====================
app.post("/api/tools", apiLimiter, authenticateUser, async (req, res) => {
  try {
    const action = req.body.action || req.body.actionType;
    const data = req.body.data || {};
    const userId = req.user.id;
    
    if (!action) {
      return res.status(400).json({
        replyText: "Action requise.",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    console.log(`🛠️ Action outil: ${action} (userId: ${userId})`);
    
    let result;
    
    switch (action) {
      case "search_images":
      case "images": {
        if (!data.query) {
          return res.status(400).json({ 
            replyText: "Paramètre 'query' requis.", 
            systemAction: "NONE", 
            payload: {} 
          });
        }
        const images = await searchWikimediaImages(data.query, data.limit || 10);
        result = {
          replyText: `Voici les images pour "${data.query}":`,
          systemAction: "RENDER_GALLERY",
          payload: { images, query: data.query }
        };
        break;
      }
      
      case "search_web":
      case "web_search":
      case "web": {
        if (!data.query) {
          return res.status(400).json({ 
            replyText: "Paramètre 'query' requis.", 
            systemAction: "NONE", 
            payload: {} 
          });
        }
        const webResults = await searchWebAdvanced(data.query);
        result = {
          replyText: webResults.results.length > 0 
            ? `Voici les résultats pour "${data.query}":` 
            : `Aucun résultat trouvé pour "${data.query}".`,
          systemAction: "RENDER_SEARCH_RESULTS",
          payload: { results: webResults }
        };
        break;
      }
      
      case "send_email":
      case "email": {
        const { to, subject, body } = data;
        if (!to || !subject || !body) {
          return res.status(400).json({ 
            replyText: "Paramètres email incomplets (to, subject, body requis).", 
            systemAction: "NONE", 
            payload: {} 
          });
        }
        
        try {
          const emailResult = await sendEmail(to, subject, body, userId);
          result = {
            replyText: `✅ Email envoyé avec succès à ${to}`,
            systemAction: "NONE",
            payload: emailResult
          };
        } catch (emailError) {
          result = {
            replyText: `❌ Erreur lors de l'envoi de l'email: ${emailError.message}`,
            systemAction: "ERROR",
            payload: { error: emailError.message }
          };
        }
        break;
      }
      
      case "send_whatsapp":
      case "whatsapp_send": {
        const { to, message } = data;
        if (!to || !message) {
          return res.status(400).json({ 
            replyText: "Paramètres WhatsApp incomplets (to, message requis).", 
            systemAction: "NONE", 
            payload: {} 
          });
        }
        
        try {
          const whatsappResult = await whatsappManager.sendMessage(userId, to, message);
          result = {
            replyText: `✅ Message WhatsApp envoyé avec succès à ${to}`,
            systemAction: "NONE",
            payload: whatsappResult
          };
        } catch (whatsappError) {
          result = {
            replyText: `❌ Erreur lors de l'envoi WhatsApp: ${whatsappError.message}`,
            systemAction: "ERROR",
            payload: { 
              error: whatsappError.message,
              code: whatsappError.code || "WHATSAPP_ERROR"
            }
          };
        }
        break;
      }
      
      case "whatsapp_qr":
      case "whatsapp_connect": {
        // Initialiser le client WhatsApp de manière asynchrone
        const initResult = await whatsappManager.initClient(userId);
        
        if (initResult.connected) {
          result = {
            replyText: "WhatsApp est déjà connecté.",
            systemAction: "NONE",
            payload: { connected: true }
          };
        } else {
          // Attendre le QR code de manière asynchrone
          let qr = null;
          const startTime = Date.now();
          while (!qr && Date.now() - startTime < 30000) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            qr = whatsappManager.getQRCode(userId);
          }
          
          if (qr) {
            result = {
              replyText: "Pour connecter ton compte WhatsApp, scanne ce QR Code d'autorisation sécurisé.",
              systemAction: "RENDER_QR",
              payload: { qrBase64: qr, expiresIn: 60 }
            };
          } else {
            result = {
              replyText: "Délai dépassé pour la génération du QR Code. Veuillez réessayer.",
              systemAction: "NONE",
              payload: {}
            };
          }
        }
        break;
      }
      
      case "whatsapp_status": {
        const status = whatsappManager.getStatus(userId);
        result = {
          replyText: status.connected 
            ? `WhatsApp est connecté${status.user ? ` en tant que ${status.user}` : ""}.` 
            : "WhatsApp n'est pas connecté.",
          systemAction: "NONE",
          payload: status
        };
        break;
      }
      
      case "whatsapp_logout": {
        const logoutResult = await whatsappManager.logout(userId);
        result = {
          replyText: logoutResult.success ? "WhatsApp déconnecté." : "Erreur lors de la déconnexion.",
          systemAction: "NONE",
          payload: logoutResult
        };
        break;
      }
      
      default: {
        return res.status(400).json({
          replyText: `Action inconnue : ${action}`,
          systemAction: "NONE",
          payload: {}
        });
      }
    }
    
    // Logger l'action
    db.run(
      "INSERT INTO tool_logs (user_id, action, result_type, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
      [userId, action, result.systemAction || "NONE"],
      (err) => {
        if (err) console.error("❌ Erreur logging outil:", err.message);
      }
    );
    
    return res.json(result);
  } catch (error) {
    console.error("❌ Erreur /api/tools:", error.message);
    return res.status(500).json({
      replyText: "Erreur lors de l'exécution de l'outil.",
      systemAction: "ERROR",
      payload: { error: error.message }
    });
  }
});

// ==================== ROUTES EMAIL DÉDIÉES ====================
app.post("/api/email/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const { to, subject, body } = req.body;
    const userId = req.user.id;
    
    // Validation
    if (!to || !subject || !body) {
      return res.status(400).json({
        success: false,
        error: "Paramètres email incomplets",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    const emailResult = await sendEmail(to, subject, body, userId);
    
    return res.json({
      success: true,
      replyText: `✅ Email envoyé avec succès à ${to}`,
      systemAction: "NONE",
      payload: emailResult
    });
  } catch (error) {
    console.error("❌ Erreur /api/email/send:", error.message);
    return res.status(500).json({
      success: false,
      replyText: `❌ Erreur lors de l'envoi de l'email: ${error.message}`,
      systemAction: "ERROR",
      payload: { error: error.message }
    });
  }
});

// ==================== ROUTES WHATSAPP DÉDIÉES ====================
app.post("/api/whatsapp/send", strictLimiter, authenticateUser, async (req, res) => {
  try {
    const { to, message } = req.body;
    const userId = req.user.id;
    
    // Validation
    if (!to || !message) {
      return res.status(400).json({
        success: false,
        error: "Paramètres WhatsApp incomplets",
        systemAction: "NONE",
        payload: {}
      });
    }
    
    const whatsappResult = await whatsappManager.sendMessage(userId, to, message);
    
    return res.json({
      success: true,
      replyText: `✅ Message WhatsApp envoyé avec succès à ${to}`,
      systemAction: "NONE",
      payload: whatsappResult
    });
  } catch (error) {
    console.error("❌ Erreur /api/whatsapp/send:", error.message);
    
    let errorMessage = error.message;
    if (error.code === "WHATSAPP_NOT_CONNECTED") {
      return res.status(400).json({
        success: false,
        replyText: "WhatsApp n'est pas connecté. Veuillez d'abord scanner le QR Code.",
        systemAction: "RENDER_QR",
        payload: { requiresConnection: true }
      });
    }
    
    return res.status(500).json({
      success: false,
      replyText: `❌ Erreur lors de l'envoi WhatsApp: ${errorMessage}`,
      systemAction: "ERROR",
      payload: { error: errorMessage }
    });
  }
});

// ==================== ROUTES SIMPLES ====================
app.post("/api/clear-cache", authenticateUser, (req, res) => {
  console.log(`🗑️ Nettoyage du cache demandé par ${req.user.id}`);
  
  // Nettoyer les sessions WhatsApp inactives
  for (const [userId, session] of whatsappManager.clients) {
    if (session.status === "disconnected") {
      whatsappManager.clients.delete(userId);
      console.log(`   Session WhatsApp supprimée: ${userId}`);
    }
  }
  
  res.json({ 
    status: "success", 
    message: "Cache nettoyé",
    timestamp: new Date().toISOString()
  });
});

app.get("/api/info", (req, res) => {
  res.json({
    status: "success",
    uptime: process.uptime(),
    version: "1.0.0",
    timestamp: new Date().toISOString(),
    capabilities: [
      "chat",
      "email",
      "whatsapp",
      "web_search",
      "image_search"
    ]
  });
});

// ==================== GESTION DES ERREURS 404 ====================
app.use((req, res) => {
  res.status(404).json({ 
    error: "Route non trouvée sur le serveur",
    path: req.url,
    timestamp: new Date().toISOString()
  });
});

// ==================== GESTION DES ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
  console.error("❌ Erreur Express:", err.message);
  console.error("   Stack:", err.stack);
  
  // Gérer les erreurs de parsing JSON
  if (err instanceof SyntaxError && err.status === 400 && "body" in err) {
    return res.status(400).json({
      error: "JSON invalide dans la requête",
      systemAction: "NONE",
      payload: {}
    });
  }
  
  res.status(500).json({ 
    error: "Erreur interne du serveur.",
    systemAction: "NONE",
    payload: {}
  });
});

// ==================== DÉMARRAGE DU SERVEUR ====================
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`💾 Base de données : SQLite (locale)`);
  console.log(`🔑 OpenRouter : ${process.env.OPENROUTER_API_KEY ? "configuré" : "non configuré"}`);
  console.log(`📧 Email SMTP : ${emailTransporter ? "configuré" : "non configuré"}`);
  console.log(`💬 WhatsApp : disponible`);
  console.log("========================================");
});

// Configuration des timeouts
server.timeout = 60000;
server.keepAliveTimeout = 60000;
server.headersTimeout = 65000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Signal ${signal} reçu. Arrêt gracieux en cours...`);
  
  // Fermer toutes les sessions WhatsApp
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
  
  // Fermer la connexion email
  if (emailTransporter) {
    emailTransporter.close();
    console.log("📧 Connexion SMTP fermée");
  }
  
  // Fermer la base de données
  db.close((err) => {
    if (err) {
      console.error("❌ Erreur fermeture DB:", err.message);
    } else {
      console.log("💾 Base de données SQLite fermée proprement");
    }
    
    // Fermer le serveur HTTP
    server.close(() => {
      console.log("✅ Arrêt gracieux terminé");
      process.exit(0);
    });
    
    // Force exit après 10 secondes si le serveur ne se ferme pas
    setTimeout(() => {
      console.error("⚠️ Arrêt forcé après timeout");
      process.exit(1);
    }, 10000);
  });
}

// Gestion des signaux de terminaison
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGHUP", () => gracefulShutdown("SIGHUP"));

// Gestion des erreurs non capturées
process.on("uncaughtException", (error) => {
  console.error("❌ Erreur non capturée:", error);
  console.error("   Stack:", error.stack);
  // Ne pas quitter le processus pour les erreurs mineures
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Promesse rejetée non gérée:", reason);
  console.error("   Promise:", promise);
  // Ne pas quitter le processus pour les rejets mineurs
});

// ==================== EXPORT ====================
module.exports = app;
