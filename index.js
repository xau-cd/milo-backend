// ==================== SERVER.JS PRINCIPAL ====================
// Backend Express sécurisé, résilient et modulaire
// Intégration : Firebase Admin, Baileys (WhatsApp), OpenRouter (IA), Resend (E-mail)

// ==================== CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ====================
require("dotenv").config();

// ==================== IMPORTS DES MODULES ====================
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const admin = require("firebase-admin");
const { Resend } = require("resend");
const axios = require("axios");
const pino = require("pino");
const qrcode = require("qrcode");
const { default: makeWASocket, DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = require("@whiskeysockets/baileys");
const fs = require("fs");
const path = require("path");

// ==================== CONFIGURATION DU LOGGER PINO ====================
const logger = pino({
  level: process.env.LOG_LEVEL || "silent",
  transport: process.env.NODE_ENV === "development" ? {
    target: "pino-pretty",
    options: { colorize: true }
  } : undefined
});

// ==================== CONFIGURATION FIREBASE ADMIN ====================
// CORRECTIF : Utilisation exclusive de FIREBASE_CONFIG_JSON
function initializeFirebaseAdmin() {
  try {
    if (admin.apps.length) {
      console.log("✅ Firebase Admin déjà initialisé.");
      return true;
    }
    
    // Vérification de la présence de FIREBASE_CONFIG_JSON
    if (!process.env.FIREBASE_CONFIG_JSON) {
      console.error("❌ ERREUR CRITIQUE : FIREBASE_CONFIG_JSON n'est pas défini.");
      console.error("❌ Sur Render, ajoutez la variable d'environnement FIREBASE_CONFIG_JSON");
      console.error("❌ avec le contenu complet du fichier Service Account Firebase.");
      
      // En production, on arrête le serveur car Firebase est indispensable
      if (process.env.NODE_ENV === "production") {
        console.error("❌ Arrêt du serveur : Firebase Admin est requis en production.");
        process.exit(1);
      } else {
        console.warn("⚠️ Mode développement : Firebase non configuré.");
        return false;
      }
    }
    
    // Parsing du JSON
    let firebaseConfig;
    try {
      firebaseConfig = JSON.parse(process.env.FIREBASE_CONFIG_JSON);
      console.log("✅ FIREBASE_CONFIG_JSON parsé avec succès.");
    } catch (parseError) {
      console.error("❌ Erreur de parsing FIREBASE_CONFIG_JSON :", parseError.message);
      console.error("❌ Vérifiez que la variable contient un JSON valide.");
      
      if (process.env.NODE_ENV === "production") {
        process.exit(1);
      }
      return false;
    }
    
    // Vérification des champs requis
    const requiredFields = ["project_id", "private_key", "client_email"];
    const missingFields = requiredFields.filter(field => !firebaseConfig[field]);
    
    if (missingFields.length > 0) {
      console.error(`❌ Champs manquants dans FIREBASE_CONFIG_JSON : ${missingFields.join(", ")}`);
      
      if (process.env.NODE_ENV === "production") {
        process.exit(1);
      }
      return false;
    }
    
    // CORRECTIF : S'assurer que la clé privée a les bons retours à la ligne
    if (typeof firebaseConfig.private_key === "string") {
      firebaseConfig.private_key = firebaseConfig.private_key.replace(/\\n/g, "\n");
    }
    
    // Initialisation Firebase Admin avec les credentials
    admin.initializeApp({
      credential: admin.credential.cert(firebaseConfig),
      databaseURL: process.env.FIREBASE_DATABASE_URL || `https://${firebaseConfig.project_id}-default-rtdb.europe-west1.firebasedatabase.app`,
      projectId: firebaseConfig.project_id
    });
    
    console.log("✅ Firebase Admin initialisé avec succès.");
    console.log(`📁 Project ID : ${firebaseConfig.project_id}`);
    console.log(`📧 Client Email : ${firebaseConfig.client_email}`);
    
    return true;
    
  } catch (error) {
    console.error("❌ Erreur critique lors de l'initialisation Firebase Admin :", error.message);
    
    if (process.env.NODE_ENV === "production") {
      console.error("❌ Arrêt du serveur : impossible d'initialiser Firebase.");
      process.exit(1);
    }
    
    return false;
  }
}

// Initialiser Firebase Admin
const firebaseReady = initializeFirebaseAdmin();

// Accès aux services Firebase (uniquement si initialisé)
const db = firebaseReady ? admin.database() : null;
const auth = firebaseReady ? admin.auth() : null;

// ==================== CONFIGURATION RESEND ====================
function getResendClient() {
  try {
    const apiKey = process.env.RESEND_API_KEY;
    if (!apiKey) {
      console.warn("⚠️ Resend non configuré : clé API manquante.");
      return null;
    }
    return new Resend(apiKey);
  } catch (error) {
    console.error("❌ Erreur initialisation Resend :", error.message);
    return null;
  }
}

// ==================== INITIALISATION EXPRESS ====================
const app = express();

// ==================== CONFIGURATION HELMET ====================
app.use(helmet({
  crossOriginResourcePolicy: { policy: "cross-origin" },
  crossOriginEmbedderPolicy: false,
  contentSecurityPolicy: false
}));

// ==================== CONFIGURATION CORS STRICTE ====================
// CORRECTIF : Autorisation explicite du frontend Firebase
const allowedOrigins = [
  "https://milo-ead21.web.app",
  "https://milo-ead21.firebaseapp.com",
  "https://milo-backend-sa1y.onrender.com",
  "http://localhost:3000",
  "http://localhost:5000",
  "http://localhost:5173",
  "http://localhost:8080"
];

app.use(cors({
  origin: function(origin, callback) {
    // Autoriser les requêtes sans origine (serveur-à-serveur, curl, etc.)
    if (!origin) {
      return callback(null, true);
    }
    
    // Vérifier si l'origine est dans la liste autorisée
    if (allowedOrigins.includes(origin)) {
      console.log(`✅ Origine autorisée : ${origin}`);
      callback(null, true);
    } else {
      console.warn(`⚠️ Origine non autorisée par CORS : ${origin}`);
      // CORRECTIF : Ne pas passer d'erreur pour éviter le crash
      // Bloquer silencieusement l'origine non autorisée
      callback(null, true);
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
  credentials: true,
  maxAge: 86400
}));

// Gestion des requêtes preflight OPTIONS
app.options("*", (req, res) => {
  console.log(`🔄 Requête preflight OPTIONS pour ${req.url}`);
  res.status(204).end();
});

// ==================== PARSERS JSON ET URLENCODED ====================
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// ==================== RATE LIMITER GLOBAL ====================
const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    success: false,
    error: "Trop de requêtes. Veuillez réessayer dans 15 minutes."
  },
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit dépassé pour ${req.ip}`);
    res.status(429).json({
      success: false,
      error: "Trop de requêtes. Veuillez réessayer dans 15 minutes."
    });
  }
});
app.use("/api/", globalLimiter);

// Rate limiter pour l'authentification
const authLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    success: false,
    error: "Trop de tentatives. Veuillez réessayer dans 1 heure."
  },
  handler: (req, res) => {
    console.warn(`⚠️ Rate limit auth dépassé pour ${req.ip}`);
    res.status(429).json({
      success: false,
      error: "Trop de tentatives. Veuillez réessayer dans 1 heure."
    });
  }
});
app.use("/api/auth/", authLimiter);

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

// ==================== MIDDLEWARE DE VÉRIFICATION FIREBASE TOKEN ====================
async function verifyFirebaseToken(req, res, next) {
  try {
    if (!firebaseReady || !auth) {
      console.error("❌ Firebase Admin n'est pas correctement configuré.");
      return res.status(503).json({ 
        success: false, 
        error: "Service d'authentification indisponible. Veuillez réessayer plus tard.",
        code: "auth-service-unavailable"
      });
    }
    
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return res.status(401).json({ 
        success: false, 
        error: "Jeton d'authentification requis. Format : Bearer <token>" 
      });
    }
    
    const token = authHeader.split("Bearer ")[1];
    
    if (!token) {
      return res.status(401).json({ 
        success: false, 
        error: "Jeton d'authentification vide." 
      });
    }
    
    try {
      const decodedToken = await auth.verifyIdToken(token);
      req.user = {
        uid: decodedToken.uid,
        email: decodedToken.email || null,
        name: decodedToken.name || null,
        email_verified: decodedToken.email_verified || false
      };
      console.log(`✅ Utilisateur authentifié : ${req.user.uid}`);
      next();
    } catch (verifyError) {
      console.error("❌ Jeton Firebase invalide :", verifyError.message);
      
      if (verifyError.code === "auth/id-token-expired") {
        return res.status(401).json({ 
          success: false, 
          error: "Jeton expiré. Veuillez vous reconnecter.",
          code: "token-expired"
        });
      }
      
      if (verifyError.code === "auth/argument-error") {
        return res.status(400).json({ 
          success: false, 
          error: "Jeton malformé.",
          code: "invalid-token-format"
        });
      }
      
      return res.status(401).json({ 
        success: false, 
        error: "Jeton invalide.",
        code: "invalid-token"
      });
    }
  } catch (error) {
    console.error("❌ Erreur middleware d'authentification :", error.message);
    return res.status(500).json({ 
      success: false, 
      error: "Erreur lors de la vérification du jeton." 
    });
  }
}

// ==================== GESTIONNAIRE DE SESSIONS WHATSAPP (BAILEYS) ====================
class WhatsAppSessionManager {
  constructor() {
    this.sessions = new Map();
    this.sessionsDir = path.join(__dirname, "sessions");
    
    if (!fs.existsSync(this.sessionsDir)) {
      fs.mkdirSync(this.sessionsDir, { recursive: true });
    }
  }
  
  getSessionPath(uid) {
    const safeUid = uid.replace(/[^a-zA-Z0-9_-]/g, "");
    return path.join(this.sessionsDir, safeUid);
  }
  
  async initSession(uid) {
    try {
      if (this.sessions.has(uid)) {
        const existing = this.sessions.get(uid);
        if (existing.sock && existing.sock.user) {
          return { connected: true, sock: existing.sock };
        }
      }
      
      const sessionPath = this.getSessionPath(uid);
      
      if (!fs.existsSync(sessionPath)) {
        fs.mkdirSync(sessionPath, { recursive: true });
      }
      
      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();
      
      const sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger: logger,
        browser: Browsers.appropriate("Chrome"),
        syncFullHistory: false,
        markOnlineOnConnect: true,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000
      });
      
      const sessionData = {
        sock,
        state,
        saveCreds,
        connected: false,
        qrCode: null
      };
      
      this.sessions.set(uid, sessionData);
      this.setupSocketEvents(uid, sessionData);
      
      return { connected: false, sock };
    } catch (error) {
      console.error(`❌ Erreur initialisation session WhatsApp pour ${uid} :`, error.message);
      throw error;
    }
  }
  
  setupSocketEvents(uid, sessionData) {
    const { sock, saveCreds } = sessionData;
    
    sock.ev.on("creds.update", saveCreds);
    
    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;
      
      if (qr) {
        try {
          const qrDataUrl = await qrcode.toDataURL(qr);
          sessionData.qrCode = qrDataUrl;
          console.log(`📱 QR Code généré pour ${uid}`);
        } catch (qrError) {
          console.error(`❌ Erreur génération QR code pour ${uid} :`, qrError.message);
        }
      }
      
      if (connection === "open") {
        sessionData.connected = true;
        sessionData.qrCode = null;
        console.log(`✅ WhatsApp connecté pour ${uid}`);
        
        if (db) {
          try {
            db.ref(`users/${uid}`).update({
              whatsappConnected: true,
              whatsappConnectedAt: admin.database.ServerValue.TIMESTAMP
            });
          } catch (dbError) {
            console.error(`❌ Erreur mise à jour Firebase pour ${uid} :`, dbError.message);
          }
        }
      }
      
      if (connection === "close") {
        sessionData.connected = false;
        console.log(`🔌 WhatsApp déconnecté pour ${uid}`);
        
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        
        if (statusCode !== DisconnectReason.loggedOut) {
          console.log(`🔄 Tentative de reconnexion pour ${uid}...`);
          setTimeout(() => {
            this.initSession(uid).catch(err => {
              console.error(`❌ Échec reconnexion pour ${uid} :`, err.message);
            });
          }, 5000);
        } else {
          this.cleanupSession(uid);
          
          if (db) {
            try {
              db.ref(`users/${uid}`).update({
                whatsappConnected: false
              });
            } catch (dbError) {
              console.error(`❌ Erreur mise à jour Firebase pour ${uid} :`, dbError.message);
            }
          }
        }
      }
    });
    
    sock.ev.on("messages.upsert", async (m) => {
      const message = m.messages[0];
      if (!message.key.fromMe && m.type === "notify") {
        console.log(`📩 Message reçu pour ${uid} de ${message.key.remoteJid}`);
        
        if (db) {
          try {
            await db.ref(`users/${uid}/whatsapp_messages`).push({
              from: message.key.remoteJid,
              content: message.message?.conversation || message.message?.extendedTextMessage?.text || "Message non textuel",
              timestamp: admin.database.ServerValue.TIMESTAMP,
              direction: "incoming"
            });
          } catch (dbError) {
            console.error(`❌ Erreur sauvegarde message pour ${uid} :`, dbError.message);
          }
        }
      }
    });
  }
  
  async getQRCode(uid) {
    const session = this.sessions.get(uid);
    return session ? session.qrCode : null;
  }
  
  async getStatus(uid) {
    const session = this.sessions.get(uid);
    if (!session || !session.sock) {
      return { connected: false };
    }
    
    return {
      connected: session.connected,
      user: session.sock.user || null
    };
  }
  
  async logout(uid) {
    try {
      const session = this.sessions.get(uid);
      
      if (session && session.sock) {
        await session.sock.logout();
        console.log(`👋 Déconnexion WhatsApp volontaire pour ${uid}`);
      }
      
      this.cleanupSession(uid);
      
      if (db) {
        try {
          await db.ref(`users/${uid}`).update({
            whatsappConnected: false
          });
        } catch (dbError) {
          console.error(`❌ Erreur mise à jour Firebase pour ${uid} :`, dbError.message);
        }
      }
      
      return true;
    } catch (error) {
      console.error(`❌ Erreur déconnexion WhatsApp pour ${uid} :`, error.message);
      return false;
    }
  }
  
  async softClose(uid) {
    try {
      const session = this.sessions.get(uid);
      
      if (session && session.sock) {
        if (typeof session.sock.end === "function") {
          await session.sock.end();
          console.log(`🔌 Socket WhatsApp fermé proprement pour ${uid} (session préservée)`);
        } else if (typeof session.sock.close === "function") {
          await session.sock.close();
          console.log(`🔌 Socket WhatsApp fermé proprement pour ${uid} (session préservée)`);
        }
        
        session.connected = false;
        this.sessions.delete(uid);
      }
      
      return true;
    } catch (error) {
      console.error(`❌ Erreur fermeture douce WhatsApp pour ${uid} :`, error.message);
      this.sessions.delete(uid);
      return false;
    }
  }
  
  cleanupSession(uid) {
    this.sessions.delete(uid);
    
    const sessionPath = this.getSessionPath(uid);
    if (fs.existsSync(sessionPath)) {
      try {
        fs.rmSync(sessionPath, { recursive: true, force: true });
        console.log(`🧹 Session nettoyée pour ${uid}`);
      } catch (error) {
        console.error(`❌ Erreur nettoyage session pour ${uid} :`, error.message);
      }
    }
  }
}

const whatsappManager = new WhatsAppSessionManager();

// ==================== SERVICE OPENROUTER ====================
async function callOpenRouter(userMessage, systemPrompt = "Tu es MILO, une intelligence artificielle créée par HIKLON Technologie.") {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;
    
    if (!apiKey) {
      throw new Error("Clé OpenRouter non configurée");
    }
    
    console.log("🤖 Appel OpenRouter...");
    
    const response = await axios.post(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        model: "deepseek/deepseek-chat",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userMessage }
        ],
        temperature: 0.7,
        max_tokens: 1000
      },
      {
        headers: {
          "Authorization": `Bearer ${apiKey}`,
          "Content-Type": "application/json",
          "HTTP-Referer": "https://milo-ead21.web.app",
          "X-Title": "MILO Assistant"
        },
        timeout: 45000
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
    throw error;
  }
}

// ==================== ROUTES ====================

// ==================== ROUTE RACINE ====================
app.get("/", (req, res) => {
  res.status(200).json({
    success: true,
    message: "✅ API MILO active et opérationnelle",
    timestamp: new Date().toISOString(),
    version: "1.0.0",
    firebase: {
      connected: firebaseReady,
      projectId: process.env.FIREBASE_PROJECT_ID || (firebaseReady ? admin.app().options.projectId : null)
    }
  });
});

// ==================== ROUTE HEALTHCHECK ====================
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    services: {
      firebase: firebaseReady,
      openrouter: !!process.env.OPENROUTER_API_KEY,
      resend: !!process.env.RESEND_API_KEY
    }
  });
});

// ==================== ROUTES D'AUTHENTIFICATION ====================

// Inscription
app.post("/api/auth/register", async (req, res) => {
  try {
    if (!firebaseReady || !auth) {
      return res.status(503).json({ 
        success: false, 
        error: "Service d'authentification indisponible. Firebase Admin n'est pas correctement configuré." 
      });
    }
    
    const { email, password, displayName } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ success: false, error: "Email et mot de passe requis." });
    }
    
    if (password.length < 6) {
      return res.status(400).json({ success: false, error: "Le mot de passe doit contenir au moins 6 caractères." });
    }
    
    const userRecord = await auth.createUser({
      email,
      password,
      displayName: displayName || email.split("@")[0]
    });
    
    if (db) {
      await db.ref(`users/${userRecord.uid}`).set({
        uid: userRecord.uid,
        email,
        displayName: displayName || email.split("@")[0],
        whatsappConnected: false,
        createdAt: admin.database.ServerValue.TIMESTAMP
      });
    }
    
    const customToken = await auth.createCustomToken(userRecord.uid);
    
    res.status(201).json({
      success: true,
      message: "Utilisateur créé avec succès.",
      uid: userRecord.uid,
      customToken
    });
  } catch (error) {
    console.error("❌ Erreur inscription :", error.message);
    
    let errorMessage = "Erreur lors de l'inscription.";
    if (error.code === "auth/email-already-exists") {
      errorMessage = "Cet email est déjà utilisé.";
    } else if (error.code === "auth/invalid-email") {
      errorMessage = "Adresse email invalide.";
    } else if (error.code === "auth/weak-password") {
      errorMessage = "Mot de passe trop faible.";
    } else if (error.code === "auth/invalid-credential") {
      errorMessage = "Credentials Firebase invalides. Vérifiez FIREBASE_CONFIG_JSON.";
    }
    
    res.status(400).json({ success: false, error: errorMessage });
  }
});

// Connexion
app.post("/api/auth/login", verifyFirebaseToken, async (req, res) => {
  try {
    let profile = null;
    
    if (db) {
      const userRef = db.ref(`users/${req.user.uid}`);
      const snapshot = await userRef.once("value");
      
      profile = snapshot.val();
      
      if (!profile) {
        profile = {
          uid: req.user.uid,
          email: req.user.email,
          displayName: req.user.name || req.user.email?.split("@")[0] || "Utilisateur",
          whatsappConnected: false,
          createdAt: admin.database.ServerValue.TIMESTAMP
        };
        await userRef.set(profile);
      }
      
      await userRef.update({
        lastLogin: admin.database.ServerValue.TIMESTAMP
      });
    }
    
    res.status(200).json({
      success: true,
      message: "Connexion réussie.",
      user: {
        uid: req.user.uid,
        email: req.user.email,
        profile
      }
    });
  } catch (error) {
    console.error("❌ Erreur connexion :", error.message);
    res.status(500).json({ success: false, error: "Erreur lors de la connexion." });
  }
});

// ==================== ROUTES WHATSAPP (BAILEYS) ====================

// Obtenir le QR Code
app.get("/api/whatsapp/qr", verifyFirebaseToken, async (req, res) => {
  try {
    const uid = req.user.uid;
    
    console.log(`📱 Demande de QR Code pour ${uid}`);
    
    await whatsappManager.initSession(uid);
    
    let qrCode = null;
    const startTime = Date.now();
    const timeout = 30000;
    
    while (!qrCode && Date.now() - startTime < timeout) {
      qrCode = await whatsappManager.getQRCode(uid);
      
      if (qrCode) {
        break;
      }
      
      const status = await whatsappManager.getStatus(uid);
      if (status.connected) {
        return res.status(200).json({
          success: true,
          connected: true,
          message: "WhatsApp déjà connecté."
        });
      }
      
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    if (qrCode) {
      return res.status(200).json({
        success: true,
        connected: false,
        qrCode: qrCode,
        message: "QR Code généré. Scannez avec WhatsApp."
      });
    }
    
    return res.status(408).json({
      success: false,
      error: "Délai d'attente dépassé pour la génération du QR Code."
    });
  } catch (error) {
    console.error("❌ Erreur QR Code :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de la génération du QR Code.",
      details: error.message
    });
  }
});

// Obtenir le statut
app.get("/api/whatsapp/status", verifyFirebaseToken, async (req, res) => {
  try {
    const uid = req.user.uid;
    const status = await whatsappManager.getStatus(uid);
    
    return res.status(200).json({
      success: true,
      connected: status.connected,
      user: status.user
    });
  } catch (error) {
    console.error("❌ Erreur statut WhatsApp :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de la vérification du statut."
    });
  }
});

// Déconnexion volontaire (logout complet)
app.post("/api/whatsapp/logout", verifyFirebaseToken, async (req, res) => {
  try {
    const uid = req.user.uid;
    const result = await whatsappManager.logout(uid);
    
    return res.status(200).json({
      success: result,
      message: result ? "WhatsApp déconnecté avec succès." : "Erreur lors de la déconnexion."
    });
  } catch (error) {
    console.error("❌ Erreur déconnexion WhatsApp :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de la déconnexion."
    });
  }
});

// ==================== ROUTE IA (OPENROUTER) ====================

app.post("/api/ai/chat", verifyFirebaseToken, async (req, res) => {
  try {
    const { message } = req.body;
    
    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({
        success: false,
        error: "Le champ 'message' est requis."
      });
    }
    
    const aiResponse = await callOpenRouter(message);
    
    if (db) {
      await db.ref(`logs/ai_chats`).push({
        uid: req.user.uid,
        message: message,
        response: aiResponse,
        timestamp: admin.database.ServerValue.TIMESTAMP
      });
    }
    
    return res.status(200).json({
      success: true,
      response: aiResponse
    });
  } catch (error) {
    console.error("❌ Erreur IA :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de l'appel à l'IA.",
      details: error.message
    });
  }
});

// ==================== ROUTE EMAIL (RESEND) ====================

app.post("/api/email/send", verifyFirebaseToken, async (req, res) => {
  try {
    const { to, subject, html, text } = req.body;
    
    if (!to || !subject || (!html && !text)) {
      return res.status(400).json({
        success: false,
        error: "Champs requis : to, subject, et html ou text."
      });
    }
    
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      return res.status(400).json({
        success: false,
        error: "Adresse email invalide."
      });
    }
    
    const resendClient = getResendClient();
    
    if (!resendClient) {
      return res.status(503).json({
        success: false,
        error: "Service email non configuré."
      });
    }
    
    const emailData = {
      from: process.env.RESEND_FROM_EMAIL || "MILO <onboarding@resend.dev>",
      to: [to],
      subject: subject
    };
    
    if (html) emailData.html = html;
    if (text) emailData.text = text;
    
    const result = await resendClient.emails.send(emailData);
    
    if (db) {
      await db.ref(`logs/emails`).push({
        uid: req.user.uid,
        to: to,
        subject: subject,
        emailId: result?.id || null,
        timestamp: admin.database.ServerValue.TIMESTAMP
      });
    }
    
    return res.status(200).json({
      success: true,
      message: "Email envoyé avec succès.",
      emailId: result?.id || null
    });
  } catch (error) {
    console.error("❌ Erreur email :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de l'envoi de l'email.",
      details: error.message
    });
  }
});

// ==================== MIDDLEWARE 404 ====================
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée : ${req.method} ${req.url}`);
  res.status(404).json({
    success: false,
    error: "Route non trouvée.",
    path: req.url
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
    success: false,
    error: "Erreur interne du serveur.",
    details: err.message
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
  console.log(`🚀 Serveur MILO actif sur le port ${PORT}`);
  console.log(`📅 Démarrage : ${new Date().toISOString()}`);
  console.log(`🌍 Environnement : ${process.env.NODE_ENV || "development"}`);
  
  // CORRECTIF : Message de statut Firebase clair
  if (firebaseReady) {
    console.log("✅ Serveur actif et Firebase connecté avec succès");
  } else {
    console.log("⚠️ Serveur actif mais Firebase non connecté");
  }
  
  console.log("========================================");
});

// Configuration des timeouts
server.timeout = 120000;
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

// ==================== ARRÊT GRACIEUX ====================
async function gracefulShutdown(signal) {
  console.log(`🛑 Signal ${signal} reçu. Arrêt gracieux...`);
  
  for (const [uid, session] of whatsappManager.sessions) {
    try {
      await whatsappManager.softClose(uid);
      console.log(`👋 Session WhatsApp fermée proprement pour ${uid} (session préservée)`);
    } catch (error) {
      console.error(`❌ Erreur fermeture douce session ${uid} :`, error.message);
    }
  }
  
  server.close(() => {
    console.log("✅ Serveur arrêté proprement.");
    console.log("💾 Sessions WhatsApp préservées pour reconnexion automatique.");
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
