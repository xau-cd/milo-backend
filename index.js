// ==================== SERVER.JS PRINCIPAL ====================
// Backend Express sécurisé, résilient et modulaire
// Intégration : Firebase REST API, Baileys (WhatsApp), OpenRouter (IA), Resend (E-mail)

// ==================== CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ====================
require("dotenv").config();

// ==================== IMPORTS DES MODULES ====================
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
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

// ==================== CONFIGURATION FIREBASE REST API ====================
// CORRECTIF : Utilisation de FIREBASE_SECRET au lieu de FIREBASE_CONFIG_JSON
const FIREBASE_DB_URL = "https://milo-ead21-default-rtdb.europe-west1.firebasedatabase.app";
const FIREBASE_SECRET = process.env.FIREBASE_SECRET || "";

// Vérification de la configuration Firebase
const firebaseReady = FIREBASE_SECRET.length > 0;

if (!firebaseReady) {
  console.warn("⚠️ FIREBASE_SECRET n'est pas défini.");
  console.warn("⚠️ Les opérations Firebase échoueront.");
  console.warn("⚠️ Définissez FIREBASE_SECRET dans les variables d'environnement Render.");
} else {
  console.log("✅ FIREBASE_SECRET est configuré.");
  console.log(`📁 Base de données : ${FIREBASE_DB_URL}`);
}

// ==================== FONCTIONS FIREBASE REST API ====================

/**
 * Lecture de données depuis Firebase Realtime Database via REST API
 * @param {string} path - Chemin dans la base de données (ex: "users/uid123")
 * @returns {Promise<any>} - Données lues ou null
 */
async function firebaseRead(path) {
  try {
    if (!firebaseReady) {
      throw new Error("FIREBASE_SECRET non configuré");
    }
    
    const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    console.log(`📖 Firebase READ : ${path}`);
    
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json"
      }
    });
    
    if (!response.ok) {
      throw new Error(`Firebase READ échoué (${response.status}) : ${response.statusText}`);
    }
    
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`❌ Erreur Firebase READ (${path}) :`, error.message);
    return null;
  }
}

/**
 * Écriture de données dans Firebase Realtime Database via REST API
 * @param {string} path - Chemin dans la base de données
 * @param {any} data - Données à écrire
 * @returns {Promise<boolean>} - true si succès
 */
async function firebaseWrite(path, data) {
  try {
    if (!firebaseReady) {
      throw new Error("FIREBASE_SECRET non configuré");
    }
    
    const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    console.log(`📝 Firebase WRITE : ${path}`);
    
    const response = await fetch(url, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(data)
    });
    
    if (!response.ok) {
      throw new Error(`Firebase WRITE échoué (${response.status}) : ${response.statusText}`);
    }
    
    return true;
  } catch (error) {
    console.error(`❌ Erreur Firebase WRITE (${path}) :`, error.message);
    return false;
  }
}

/**
 * Ajout de données dans Firebase Realtime Database via REST API (POST pour générer un ID)
 * @param {string} path - Chemin dans la base de données
 * @param {any} data - Données à ajouter
 * @returns {Promise<string|null>} - ID généré ou null
 */
async function firebasePush(path, data) {
  try {
    if (!firebaseReady) {
      throw new Error("FIREBASE_SECRET non configuré");
    }
    
    const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    console.log(`📝 Firebase PUSH : ${path}`);
    
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(data)
    });
    
    if (!response.ok) {
      throw new Error(`Firebase PUSH échoué (${response.status}) : ${response.statusText}`);
    }
    
    const result = await response.json();
    return result.name || null; // Firebase retourne { "name": "-ID_unique" }
  } catch (error) {
    console.error(`❌ Erreur Firebase PUSH (${path}) :`, error.message);
    return null;
  }
}

/**
 * Mise à jour partielle dans Firebase Realtime Database via REST API
 * @param {string} path - Chemin dans la base de données
 * @param {any} updates - Données à mettre à jour
 * @returns {Promise<boolean>} - true si succès
 */
async function firebaseUpdate(path, updates) {
  try {
    if (!firebaseReady) {
      throw new Error("FIREBASE_SECRET non configuré");
    }
    
    const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    console.log(`📝 Firebase UPDATE : ${path}`);
    
    const response = await fetch(url, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(updates)
    });
    
    if (!response.ok) {
      throw new Error(`Firebase UPDATE échoué (${response.status}) : ${response.statusText}`);
    }
    
    return true;
  } catch (error) {
    console.error(`❌ Erreur Firebase UPDATE (${path}) :`, error.message);
    return false;
  }
}

/**
 * Suppression de données dans Firebase Realtime Database via REST API
 * @param {string} path - Chemin dans la base de données
 * @returns {Promise<boolean>} - true si succès
 */
async function firebaseDelete(path) {
  try {
    if (!firebaseReady) {
      throw new Error("FIREBASE_SECRET non configuré");
    }
    
    const url = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    console.log(`🗑️ Firebase DELETE : ${path}`);
    
    const response = await fetch(url, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json"
      }
    });
    
    if (!response.ok) {
      throw new Error(`Firebase DELETE échoué (${response.status}) : ${response.statusText}`);
    }
    
    return true;
  } catch (error) {
    console.error(`❌ Erreur Firebase DELETE (${path}) :`, error.message);
    return false;
  }
}

// ==================== SERVICE DE VÉRIFICATION DES TOKENS FIREBASE ====================
// CORRECTIF : Vérification des tokens via Firebase REST API
async function verifyFirebaseIdToken(idToken) {
  try {
    if (!idToken) {
      return null;
    }
    
    // Utiliser l'API REST de Firebase pour vérifier le token
    // Note : Cette méthode utilise l'API Identity Toolkit
    const response = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${FIREBASE_SECRET}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ idToken })
      }
    );
    
    if (!response.ok) {
      throw new Error(`Vérification token échouée (${response.status})`);
    }
    
    const data = await response.json();
    
    if (data.users && data.users.length > 0) {
      const user = data.users[0];
      return {
        uid: user.localId,
        email: user.email || null,
        name: user.displayName || null,
        email_verified: user.emailVerified || false
      };
    }
    
    return null;
  } catch (error) {
    console.error("❌ Erreur vérification token :", error.message);
    return null;
  }
}

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

// ==================== CONFIGURATION CORS ====================
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
    if (!origin) {
      return callback(null, true);
    }
    
    if (allowedOrigins.includes(origin)) {
      console.log(`✅ Origine autorisée : ${origin}`);
      callback(null, true);
    } else {
      console.warn(`⚠️ Origine non autorisée par CORS : ${origin}`);
      callback(null, true);
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
  credentials: true,
  maxAge: 86400
}));

app.options("*", (req, res) => {
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
    if (!firebaseReady) {
      console.error("❌ FIREBASE_SECRET non configuré.");
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
    
    const userData = await verifyFirebaseIdToken(token);
    
    if (!userData) {
      return res.status(401).json({ 
        success: false, 
        error: "Jeton invalide ou expiré.",
        code: "invalid-token"
      });
    }
    
    req.user = userData;
    console.log(`✅ Utilisateur authentifié : ${req.user.uid}`);
    next();
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
        
        await firebaseUpdate(`users/${uid}`, {
          whatsappConnected: true,
          whatsappConnectedAt: Date.now()
        });
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
          
          await firebaseUpdate(`users/${uid}`, {
            whatsappConnected: false
          });
        }
      }
    });
    
    sock.ev.on("messages.upsert", async (m) => {
      const message = m.messages[0];
      if (!message.key.fromMe && m.type === "notify") {
        console.log(`📩 Message reçu pour ${uid} de ${message.key.remoteJid}`);
        
        await firebasePush(`users/${uid}/whatsapp_messages`, {
          from: message.key.remoteJid,
          content: message.message?.conversation || message.message?.extendedTextMessage?.text || "Message non textuel",
          timestamp: Date.now(),
          direction: "incoming"
        });
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
      
      await firebaseUpdate(`users/${uid}`, {
        whatsappConnected: false
      });
      
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
      connected: firebaseReady
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

// Inscription (via Firebase REST API)
app.post("/api/auth/register", async (req, res) => {
  try {
    if (!firebaseReady) {
      return res.status(503).json({ 
        success: false, 
        error: "Service d'authentification indisponible. FIREBASE_SECRET non configuré." 
      });
    }
    
    const { email, password, displayName } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ success: false, error: "Email et mot de passe requis." });
    }
    
    if (password.length < 6) {
      return res.status(400).json({ success: false, error: "Le mot de passe doit contenir au moins 6 caractères." });
    }
    
    // Appel à l'API REST Firebase pour créer l'utilisateur
    const response = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=${FIREBASE_SECRET}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          email,
          password,
          returnSecureToken: true
        })
      }
    );
    
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error?.message || "Erreur lors de l'inscription");
    }
    
    const userData = await response.json();
    
    // Créer le profil utilisateur dans Firebase Realtime Database
    await firebaseWrite(`users/${userData.localId}`, {
      uid: userData.localId,
      email: email,
      displayName: displayName || email.split("@")[0],
      whatsappConnected: false,
      createdAt: Date.now()
    });
    
    await firebasePush("logs/inscriptions", {
      uid: userData.localId,
      email: email,
      displayName: displayName || email.split("@")[0],
      timestamp: Date.now()
    });
    
    res.status(201).json({
      success: true,
      message: "Utilisateur créé avec succès.",
      uid: userData.localId,
      idToken: userData.idToken,
      refreshToken: userData.refreshToken
    });
  } catch (error) {
    console.error("❌ Erreur inscription :", error.message);
    
    let errorMessage = "Erreur lors de l'inscription.";
    if (error.message.includes("EMAIL_EXISTS")) {
      errorMessage = "Cet email est déjà utilisé.";
    } else if (error.message.includes("INVALID_EMAIL")) {
      errorMessage = "Adresse email invalide.";
    } else if (error.message.includes("WEAK_PASSWORD")) {
      errorMessage = "Mot de passe trop faible.";
    }
    
    res.status(400).json({ success: false, error: errorMessage });
  }
});

// Connexion (via Firebase REST API)
app.post("/api/auth/login", async (req, res) => {
  try {
    if (!firebaseReady) {
      return res.status(503).json({ 
        success: false, 
        error: "Service d'authentification indisponible. FIREBASE_SECRET non configuré." 
      });
    }
    
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ success: false, error: "Email et mot de passe requis." });
    }
    
    // Appel à l'API REST Firebase pour la connexion
    const response = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${FIREBASE_SECRET}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          email,
          password,
          returnSecureToken: true
        })
      }
    );
    
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error?.message || "Erreur lors de la connexion");
    }
    
    const userData = await response.json();
    
    // Récupérer le profil utilisateur
    const profile = await firebaseRead(`users/${userData.localId}`);
    
    if (!profile) {
      // Créer le profil s'il n'existe pas
      await firebaseWrite(`users/${userData.localId}`, {
        uid: userData.localId,
        email: email,
        displayName: userData.displayName || email.split("@")[0],
        whatsappConnected: false,
        createdAt: Date.now(),
        lastLogin: Date.now()
      });
    } else {
      await firebaseUpdate(`users/${userData.localId}`, {
        lastLogin: Date.now()
      });
    }
    
    await firebasePush("logs/connexions", {
      uid: userData.localId,
      email: email,
      timestamp: Date.now()
    });
    
    res.status(200).json({
      success: true,
      message: "Connexion réussie.",
      uid: userData.localId,
      idToken: userData.idToken,
      refreshToken: userData.refreshToken,
      profile: profile || {
        uid: userData.localId,
        email: email,
        displayName: email.split("@")[0]
      }
    });
  } catch (error) {
    console.error("❌ Erreur connexion :", error.message);
    
    let errorMessage = "Erreur lors de la connexion.";
    if (error.message.includes("INVALID_LOGIN_CREDENTIALS")) {
      errorMessage = "Email ou mot de passe incorrect.";
    } else if (error.message.includes("USER_DISABLED")) {
      errorMessage = "Ce compte a été désactivé.";
    }
    
    res.status(401).json({ success: false, error: errorMessage });
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

// Déconnexion volontaire
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
    
    await firebasePush("logs/ai_chats", {
      uid: req.user.uid,
      message: message,
      response: aiResponse,
      timestamp: Date.now()
    });
    
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
    
    await firebasePush("logs/emails", {
      uid: req.user.uid,
      to: to,
      subject: subject,
      emailId: result?.id || null,
      timestamp: Date.now()
    });
    
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
  
  if (firebaseReady) {
    console.log("✅ Serveur actif et Firebase connecté avec succès");
  } else {
    console.log("⚠️ Serveur actif mais Firebase non connecté");
    console.log("⚠️ Définissez FIREBASE_SECRET dans les variables d'environnement");
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
