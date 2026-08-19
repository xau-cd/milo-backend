const admin = require("firebase-admin");
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Twilio } = require("twilio");
const { Resend } = require("resend");

// ==================== CONFIGURATION FIREBASE SIMPLIFIÉE ====================
if (!admin.apps.length) {
  admin.initializeApp({
    databaseURL: "https://milo-ead21-default-rtdb.europe-west1.firebasedatabase.app"
  });
}
const db = admin.database();
const auth = admin.auth();

// ==================== CONFIGURATION EXPRESS ====================
const app = express();

// ==================== CONFIGURATION CORS RENFORCÉE ====================
// Liste des origines autorisées (à adapter selon vos domaines)
const allowedOrigins = [
  "https://milo-ead21.web.app",
  "https://milo-ead21.firebaseapp.com",
  "http://localhost:3000",
  "http://localhost:5000",
  "http://localhost:5173",
  "http://localhost:8080",
  "https://milo-ead21-default-rtdb.europe-west1.firebasedatabase.app"
];

// Middleware CORS personnalisé pour un contrôle total
app.use((req, res, next) => {
  const origin = req.headers.origin;
  
  // Si l'origine est dans la liste autorisée ou si c'est une requête sans origine (comme curl)
  if (!origin || allowedOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader("Access-Control-Max-Age", "86400"); // 24 heures
    
    // Gérer les requêtes preflight OPTIONS
    if (req.method === "OPTIONS") {
      return res.status(204).end();
    }
  }
  
  next();
});

// Middleware CORS standard en secours
app.use(cors({
  origin: function(origin, callback) {
    // Autoriser les requêtes sans origine (comme les appels serveur-à-serveur)
    if (!origin) return callback(null, true);
    
    if (allowedOrigins.indexOf(origin) !== -1) {
      callback(null, true);
    } else {
      console.warn(`⚠️ Origine bloquée par CORS : ${origin}`);
      callback(null, true); // En production, vous pourriez bloquer avec callback(new Error('CORS'))
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"],
  credentials: true,
  maxAge: 86400
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ==================== MIDDLEWARE DE LOGGING ====================
app.use((req, res, next) => {
  const start = Date.now();
  console.log(`📥 ${req.method} ${req.url} - ${new Date().toISOString()}`);
  
  // Capturer la fin de la réponse pour logger le statut
  res.on("finish", () => {
    const duration = Date.now() - start;
    console.log(`📤 ${req.method} ${req.url} - ${res.statusCode} - ${duration}ms`);
  });
  
  next();
});

// ==================== MIDDLEWARE ANTI-TIMEOUT ====================
// Garde la connexion active pendant les longues réponses
app.use((req, res, next) => {
  // Définir un timeout plus long pour les requêtes
  req.setTimeout(120000, () => {
    console.error(`⏰ Timeout dépassé pour ${req.method} ${req.url}`);
    if (!res.headersSent) {
      res.status(408).json({ error: "Délai d'attente dépassé. Veuillez réessayer." });
    }
  });
  
  // Garder la connexion alive
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Keep-Alive", "timeout=120");
  
  next();
});

// ==================== CLIENTS DYNAMIQUES SÉCURISÉS ====================

/**
 * Retourne une instance Twilio uniquement si les variables d'environnement sont valides.
 * Ne crashe JAMAIS le processus si les clés manquent ou sont invalides.
 */
function getTwilioClient() {
  try {
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const fromNumber = process.env.TWILIO_WHATSAPP_NUMBER || process.env.TWILIO_PHONE_NUMBER;

    if (!accountSid || !authToken || !fromNumber) {
      console.warn("⚠️ Twilio non configuré : variables manquantes.");
      return null;
    }

    if (!accountSid.startsWith("AC")) {
      console.warn("⚠️ TWILIO_ACCOUNT_SID invalide : doit commencer par 'AC'.");
      return null;
    }

    return { client: new Twilio(accountSid, authToken), fromNumber };
  } catch (error) {
    console.error("❌ Erreur lors de l'initialisation Twilio :", error.message);
    return null;
  }
}

/**
 * Retourne une instance Resend uniquement si la clé API est présente.
 * Ne crashe JAMAIS le processus si la clé manque.
 */
function getResendClient() {
  try {
    const apiKey = process.env.RESEND_API_KEY;

    if (!apiKey) {
      console.warn("⚠️ Resend non configuré : clé API manquante.");
      return null;
    }

    return new Resend(apiKey);
  } catch (error) {
    console.error("❌ Erreur lors de l'initialisation Resend :", error.message);
    return null;
  }
}

// ==================== IDENTITÉ OFFICIELLE DE MILO ====================
const MILO_IDENTITY = "Je suis MILO, une intelligence artificielle créée par HIKLON Technologie.";
const MILO_SYSTEM_PROMPT = `${MILO_IDENTITY} Je suis un assistant personnel intelligent, serviable et professionnel. Je peux aider avec des recherches, des informations, et je peux envoyer des messages WhatsApp si l'utilisateur m'y autorise.`;

// ==================== FONCTIONS UTILITAIRES ====================

/**
 * Vérifie que les variables d'environnement essentielles sont présentes.
 */
function checkEnvironmentVariables() {
  const requiredVars = [
    "OPENROUTER_API_KEY",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
    "TWILIO_WHATSAPP_NUMBER",
    "RESEND_API_KEY"
  ];
  
  const missing = [];
  const present = [];
  
  requiredVars.forEach(varName => {
    if (process.env[varName]) {
      present.push(varName);
      console.log(`✅ ${varName} : configurée`);
    } else {
      missing.push(varName);
      console.warn(`⚠️ ${varName} : MANQUANTE`);
    }
  });
  
  return { missing, present };
}

// Vérification au démarrage
console.log("🔍 Vérification des variables d'environnement :");
const envStatus = checkEnvironmentVariables();
console.log(`📊 Variables présentes : ${envStatus.present.length}/${envStatus.present.length + envStatus.missing.length}`);

/**
 * Recherche des images et un résumé sur Wikipédia (français et anglais).
 */
async function fetchWikiMedia(query) {
  try {
    const searchUrl = `https://fr.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(query)}&format=json&origin=*`;
    const searchResponse = await axios.get(searchUrl, { timeout: 10000 });
    const results = searchResponse.data?.query?.search;

    if (!results || results.length === 0) {
      return { summary: null, image: null, title: null, url: null, categories: null, pageId: null };
    }

    const title = results[0].title;
    const pageId = results[0].pageid;

    // Récupération du résumé
    const summaryUrl = `https://fr.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&titles=${encodeURIComponent(title)}&format=json&origin=*`;
    const summaryResponse = await axios.get(summaryUrl, { timeout: 10000 });
    const pages = summaryResponse.data?.query?.pages;
    const page = pages ? Object.values(pages)[0] : null;
    const summary = page?.extract || null;

    // Récupération de l'image principale
    let image = null;
    try {
      const imageUrl = `https://fr.wikipedia.org/w/api.php?action=query&prop=pageimages&piprop=original&titles=${encodeURIComponent(title)}&format=json&origin=*`;
      const imageResponse = await axios.get(imageUrl, { timeout: 10000 });
      const imagePages = imageResponse.data?.query?.pages;
      const imagePage = imagePages ? Object.values(imagePages)[0] : null;
      image = imagePage?.original?.source || null;
    } catch (imageError) {
      console.warn("⚠️ Impossible de récupérer l'image Wikipédia :", imageError.message);
    }

    // Récupération des catégories
    let categories = [];
    try {
      const categoriesUrl = `https://fr.wikipedia.org/w/api.php?action=query&prop=categories&cllimit=10&titles=${encodeURIComponent(title)}&format=json&origin=*`;
      const categoriesResponse = await axios.get(categoriesUrl, { timeout: 10000 });
      const categoryPages = categoriesResponse.data?.query?.pages;
      const categoryPage = categoryPages ? Object.values(categoryPages)[0] : null;
      const rawCategories = categoryPage?.categories || [];
      categories = rawCategories.map(cat => cat.title.replace("Catégorie:", ""));
    } catch (categoryError) {
      console.warn("⚠️ Impossible de récupérer les catégories :", categoryError.message);
    }

    // URL de la page
    const url = `https://fr.wikipedia.org/wiki/${encodeURIComponent(title)}`;

    return { summary, image, title, url, categories, pageId };
  } catch (error) {
    console.error("❌ Erreur fetchWikiMedia :", error.message);
    return { summary: null, image: null, title: null, url: null, categories: null, pageId: null };
  }
}

/**
 * Recherche des images via l'API Wikimedia Commons.
 */
async function fetchWikimediaCommons(query, limit = 5) {
  try {
    const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(query)}&gsrlimit=${limit}&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=800&format=json&origin=*`;
    const response = await axios.get(url, { timeout: 10000 });
    const pages = response.data?.query?.pages;

    if (!pages) {
      return [];
    }

    const images = Object.values(pages).map(page => {
      const imageInfo = page.imageinfo?.[0];
      return {
        title: page.title,
        url: imageInfo?.url || null,
        thumburl: imageInfo?.thumburl || null,
        description: imageInfo?.extmetadata?.ImageDescription?.value || null,
        license: imageInfo?.extmetadata?.LicenseShortName?.value || null,
        artist: imageInfo?.extmetadata?.Artist?.value || null
      };
    });

    return images.filter(img => img.url);
  } catch (error) {
    console.error("❌ Erreur fetchWikimediaCommons :", error.message);
    return [];
  }
}

/**
 * Recherche sur DuckDuckGo (API Instant Answer).
 */
async function searchWeb(query) {
  try {
    const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await axios.get(url, { timeout: 10000 });

    const abstractText = response.data?.AbstractText || null;
    const image = response.data?.Image || null;
    const heading = response.data?.Heading || null;
    const relatedTopics = response.data?.RelatedTopics || [];

    let relatedResults = [];
    if (Array.isArray(relatedTopics)) {
      relatedResults = relatedTopics
        .filter(topic => topic && topic.Text)
        .slice(0, 3)
        .map(topic => ({
          title: topic.Text.split(" - ")[0] || null,
          description: topic.Text,
          url: topic.FirstURL || null
        }));
    }

    return {
      abstract: abstractText,
      image,
      heading,
      relatedResults
    };
  } catch (error) {
    console.error("❌ Erreur searchWeb :", error.message);
    return { abstract: null, image: null, heading: null, relatedResults: [] };
  }
}

/**
 * Appelle l'API OpenRouter avec le modèle DeepSeek.
 */
async function callOpenRouter(userMessage, systemPrompt = MILO_SYSTEM_PROMPT) {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY || "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";
    
    console.log("🤖 Appel OpenRouter avec le modèle deepseek/deepseek-chat");
    
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
        timeout: 45000 // 45 secondes pour éviter les timeouts
      }
    );

    const content = response.data?.choices?.[0]?.message?.content;
    if (!content) {
      throw new Error("Réponse OpenRouter vide ou invalide.");
    }

    return content;
  } catch (error) {
    console.error("❌ Erreur callOpenRouter :", error.message);
    if (error.response) {
      console.error("Détails OpenRouter :", JSON.stringify(error.response.data).substring(0, 500));
    }
    throw error;
  }
}

/**
 * Sauvegarde un log dans Firebase Realtime Database.
 */
async function saveLog(path, data) {
  try {
    const ref = db.ref(path);
    await ref.push({
      ...data,
      timestamp: admin.database.ServerValue.TIMESTAMP
    });
    return true;
  } catch (error) {
    console.error(`❌ Erreur saveLog (${path}) :`, error.message);
    return false;
  }
}

/**
 * Récupère ou crée un profil utilisateur dans Firebase.
 */
async function getOrCreateUserProfile(uid, data = {}) {
  try {
    const userRef = db.ref(`users/${uid}`);
    const snapshot = await userRef.once("value");
    
    if (snapshot.exists()) {
      return snapshot.val();
    }
    
    const profile = {
      uid: uid,
      email: data.email || null,
      displayName: data.displayName || "Utilisateur MILO",
      whatsappNumber: data.whatsappNumber || null,
      whatsappAuthorized: false,
      createdAt: admin.database.ServerValue.TIMESTAMP,
      lastLogin: admin.database.ServerValue.TIMESTAMP
    };
    
    await userRef.set(profile);
    return profile;
  } catch (error) {
    console.error("❌ Erreur getOrCreateUserProfile :", error.message);
    return null;
  }
}

/**
 * Met à jour le profil utilisateur.
 */
async function updateUserProfile(uid, updates) {
  try {
    const userRef = db.ref(`users/${uid}`);
    await userRef.update(updates);
    return true;
  } catch (error) {
    console.error("❌ Erreur updateUserProfile :", error.message);
    return false;
  }
}

/**
 * Vérifie le token Firebase et retourne l'utilisateur.
 */
async function verifyFirebaseToken(idToken) {
  try {
    const decodedToken = await auth.verifyIdToken(idToken);
    return decodedToken;
  } catch (error) {
    console.error("❌ Erreur verifyFirebaseToken :", error.message);
    return null;
  }
}

/**
 * Envoie un message WhatsApp via Twilio.
 */
async function sendWhatsAppMessage(to, message) {
  try {
    const twilioData = getTwilioClient();
    
    if (!twilioData) {
      throw new Error("Client Twilio non configuré");
    }
    
    const result = await twilioData.client.messages.create({
      from: `whatsapp:${twilioData.fromNumber}`,
      to: `whatsapp:${to}`,
      body: message
    });
    
    return result;
  } catch (error) {
    console.error("❌ Erreur sendWhatsAppMessage :", error.message);
    throw error;
  }
}

// ==================== ROUTES ====================

/**
 * Route racine pour vérifier que le serveur est actif.
 * Inclut un warm-up pour éviter les cold starts.
 */
app.get("/", (req, res) => {
  res.status(200).json({
    success: true,
    message: "✅ Cerveau MILO actif et opérationnel !",
    identity: MILO_IDENTITY,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    env: {
      openrouter: !!process.env.OPENROUTER_API_KEY,
      twilio: !!(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN),
      resend: !!process.env.RESEND_API_KEY,
      firebase: true
    }
  });
});

/**
 * Route de healthcheck pour les services externes.
 */
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage()
  });
});

/**
 * Route de warm-up pour réveiller le serveur.
 */
app.get("/warmup", (req, res) => {
  res.status(200).json({
    success: true,
    message: "Warm-up effectué",
    timestamp: new Date().toISOString()
  });
});

// ==================== ROUTES D'AUTHENTIFICATION ====================

/**
 * Route d'inscription utilisateur.
 */
app.post("/auth/register", async (req, res) => {
  try {
    const { email, password, displayName, whatsappNumber } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: "Email et mot de passe requis." });
    }

    if (password.length < 6) {
      return res.status(400).json({ error: "Le mot de passe doit contenir au moins 6 caractères." });
    }

    // Création de l'utilisateur dans Firebase Auth
    const userRecord = await auth.createUser({
      email: email,
      password: password,
      displayName: displayName || email.split("@")[0]
    });

    // Création du profil utilisateur dans la base de données
    await getOrCreateUserProfile(userRecord.uid, {
      email: email,
      displayName: displayName || email.split("@")[0],
      whatsappNumber: whatsappNumber || null,
      whatsappAuthorized: false
    });

    // Génération d'un token personnalisé
    const customToken = await auth.createCustomToken(userRecord.uid);

    await saveLog("logs_systeme/inscriptions", {
      uid: userRecord.uid,
      email: email,
      displayName: displayName || email.split("@")[0]
    });

    return res.status(201).json({
      success: true,
      message: "Utilisateur créé avec succès.",
      uid: userRecord.uid,
      customToken: customToken
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
    }
    
    return res.status(400).json({ error: errorMessage });
  }
});

/**
 * Route de connexion utilisateur.
 */
app.post("/auth/login", async (req, res) => {
  try {
    const { idToken } = req.body;

    if (!idToken) {
      return res.status(400).json({ error: "Token d'identification requis." });
    }

    // Vérification du token Firebase
    const decodedToken = await verifyFirebaseToken(idToken);
    
    if (!decodedToken) {
      return res.status(401).json({ error: "Token invalide ou expiré." });
    }

    // Récupération ou création du profil utilisateur
    const profile = await getOrCreateUserProfile(decodedToken.uid, {
      email: decodedToken.email || null,
      displayName: decodedToken.name || "Utilisateur MILO"
    });

    // Mise à jour de la date de dernière connexion
    await updateUserProfile(decodedToken.uid, {
      lastLogin: admin.database.ServerValue.TIMESTAMP
    });

    await saveLog("logs_systeme/connexions", {
      uid: decodedToken.uid,
      email: decodedToken.email || null
    });

    return res.status(200).json({
      success: true,
      message: "Connexion réussie.",
      user: {
        uid: decodedToken.uid,
        email: decodedToken.email,
        profile: profile
      }
    });
  } catch (error) {
    console.error("❌ Erreur connexion :", error.message);
    return res.status(500).json({ error: "Erreur lors de la connexion." });
  }
});

/**
 * Route pour mettre à jour le profil utilisateur.
 */
app.post("/auth/update-profile", async (req, res) => {
  try {
    const { idToken, whatsappNumber, displayName } = req.body;

    if (!idToken) {
      return res.status(400).json({ error: "Token d'identification requis." });
    }

    const decodedToken = await verifyFirebaseToken(idToken);
    
    if (!decodedToken) {
      return res.status(401).json({ error: "Token invalide ou expiré." });
    }

    const updates = {};
    if (whatsappNumber) {
      // Validation simple du numéro WhatsApp
      const cleanedNumber = whatsappNumber.replace(/[^\d+]/g, "");
      if (cleanedNumber.length < 10) {
        return res.status(400).json({ error: "Numéro WhatsApp invalide." });
      }
      updates.whatsappNumber = cleanedNumber;
    }
    if (displayName) {
      updates.displayName = displayName;
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: "Aucune donnée à mettre à jour." });
    }

    await updateUserProfile(decodedToken.uid, updates);

    await saveLog("logs_systeme/profils_mis_a_jour", {
      uid: decodedToken.uid,
      updates: updates
    });

    return res.status(200).json({
      success: true,
      message: "Profil mis à jour avec succès."
    });
  } catch (error) {
    console.error("❌ Erreur mise à jour profil :", error.message);
    return res.status(500).json({ error: "Erreur lors de la mise à jour du profil." });
  }
});

/**
 * Route pour autoriser l'envoi de messages WhatsApp.
 */
app.post("/auth/authorize-whatsapp", async (req, res) => {
  try {
    const { idToken, whatsappNumber, authorized } = req.body;

    if (!idToken) {
      return res.status(400).json({ error: "Token d'identification requis." });
    }

    const decodedToken = await verifyFirebaseToken(idToken);
    
    if (!decodedToken) {
      return res.status(401).json({ error: "Token invalide ou expiré." });
    }

    if (!whatsappNumber) {
      return res.status(400).json({ error: "Numéro WhatsApp requis." });
    }

    const cleanedNumber = whatsappNumber.replace(/[^\d+]/g, "");
    if (cleanedNumber.length < 10) {
      return res.status(400).json({ error: "Numéro WhatsApp invalide." });
    }

    await updateUserProfile(decodedToken.uid, {
      whatsappNumber: cleanedNumber,
      whatsappAuthorized: authorized === true
    });

    await saveLog("logs_systeme/autorisations_whatsapp", {
      uid: decodedToken.uid,
      whatsappNumber: cleanedNumber,
      authorized: authorized === true
    });

    return res.status(200).json({
      success: true,
      message: authorized ? "Autorisation WhatsApp accordée." : "Autorisation WhatsApp révoquée."
    });
  } catch (error) {
    console.error("❌ Erreur autorisation WhatsApp :", error.message);
    return res.status(500).json({ error: "Erreur lors de l'autorisation WhatsApp." });
  }
});

// ==================== ROUTES DE RECHERCHE ====================

/**
 * Route de recherche Wikipédia enrichie.
 */
app.get("/wiki/search", async (req, res) => {
  try {
    const query = req.query.q;

    if (!query || query.trim().length === 0) {
      return res.status(400).json({ error: "Le paramètre 'q' est requis." });
    }

    const wikiData = await fetchWikiMedia(query);
    const commonsImages = await fetchWikimediaCommons(query, 5);

    return res.status(200).json({
      success: true,
      wiki: wikiData,
      commons: commonsImages
    });
  } catch (error) {
    console.error("❌ Erreur route /wiki/search :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de la recherche Wikipédia.",
      details: error.message
    });
  }
});

/**
 * Route de recherche d'images Wikimedia Commons.
 */
app.get("/wiki/commons", async (req, res) => {
  try {
    const query = req.query.q;
    const limit = parseInt(req.query.limit) || 10;

    if (!query || query.trim().length === 0) {
      return res.status(400).json({ error: "Le paramètre 'q' est requis." });
    }

    const images = await fetchWikimediaCommons(query, Math.min(limit, 20));

    return res.status(200).json({
      success: true,
      query: query,
      images: images
    });
  } catch (error) {
    console.error("❌ Erreur route /wiki/commons :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de la recherche d'images.",
      details: error.message
    });
  }
});

// ==================== ROUTE DE CHAT ====================

/**
 * Route de chat frontend.
 */
app.post("/chat", async (req, res) => {
  try {
    const { message, user_id, idToken } = req.body;

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ error: "Le champ 'message' est requis et doit être une chaîne non vide." });
    }

    // Vérification de l'utilisateur si un token est fourni
    let userProfile = null;
    if (idToken) {
      const decodedToken = await verifyFirebaseToken(idToken);
      if (decodedToken) {
        userProfile = await getOrCreateUserProfile(decodedToken.uid, {
          email: decodedToken.email || null,
          displayName: decodedToken.name || "Utilisateur MILO"
        });
      }
    }

    // Recherche d'informations complémentaires
    const wikiData = await fetchWikiMedia(message);
    const commonsImages = await fetchWikimediaCommons(message, 3);
    const webData = await searchWeb(message);

    // Construction du contexte enrichi
    let contextPrompt = `Question de l'utilisateur : ${message}\n\n`;
    
    if (userProfile) {
      contextPrompt += `Utilisateur connecté : ${userProfile.displayName || "Anonyme"}\n`;
      if (userProfile.whatsappNumber && userProfile.whatsappAuthorized) {
        contextPrompt += `Numéro WhatsApp autorisé : ${userProfile.whatsappNumber}\n`;
      }
      contextPrompt += `\n`;
    }
    
    if (wikiData.summary) {
      contextPrompt += `Contexte Wikipédia (${wikiData.title}) :\n${wikiData.summary.substring(0, 1000)}\n\n`;
    }
    
    if (wikiData.categories && wikiData.categories.length > 0) {
      contextPrompt += `Catégories : ${wikiData.categories.join(", ")}\n\n`;
    }
    
    if (webData.abstract) {
      contextPrompt += `Contexte Web :\n${webData.abstract.substring(0, 1000)}\n\n`;
    }
    
    if (webData.relatedResults && webData.relatedResults.length > 0) {
      contextPrompt += "Sources supplémentaires :\n";
      webData.relatedResults.forEach((result, index) => {
        contextPrompt += `${index + 1}. ${result.description?.substring(0, 200)}\n`;
      });
    }

    // Appel à OpenRouter avec l'identité MILO
    const aiResponse = await callOpenRouter(contextPrompt, MILO_SYSTEM_PROMPT);

    // Sauvegarde du log
    const logData = {
      user_id: user_id || (userProfile ? userProfile.uid : "anonymous"),
      message: message,
      response: aiResponse,
      wiki_found: !!wikiData.summary,
      web_found: !!webData.abstract,
      commons_images_found: commonsImages.length
    };

    if (userProfile) {
      logData.authenticated = true;
      logData.whatsapp_authorized = userProfile.whatsappAuthorized || false;
    }

    await saveLog("logs_systeme/chat_frontend", logData);

    return res.status(200).json({
      success: true,
      response: aiResponse,
      identity: MILO_IDENTITY,
      wiki: {
        title: wikiData.title,
        summary: wikiData.summary,
        image: wikiData.image,
        url: wikiData.url,
        categories: wikiData.categories
      },
      commons: commonsImages,
      web: webData,
      user: userProfile ? {
        uid: userProfile.uid,
        displayName: userProfile.displayName,
        whatsappAuthorized: userProfile.whatsappAuthorized || false
      } : null
    });
  } catch (error) {
    console.error("❌ Erreur route /chat :", error.message);
    return res.status(500).json({
      success: false,
      error: "Une erreur est survenue lors du traitement de votre demande.",
      details: error.message
    });
  }
});

/**
 * Route pour que l'IA envoie un message WhatsApp à l'utilisateur.
 */
app.post("/chat/send-whatsapp", async (req, res) => {
  try {
    const { idToken, message } = req.body;

    if (!idToken) {
      return res.status(400).json({ error: "Token d'identification requis." });
    }

    if (!message || message.trim().length === 0) {
      return res.status(400).json({ error: "Message requis." });
    }

    const decodedToken = await verifyFirebaseToken(idToken);
    
    if (!decodedToken) {
      return res.status(401).json({ error: "Token invalide ou expiré." });
    }

    const userProfile = await getOrCreateUserProfile(decodedToken.uid);

    if (!userProfile || !userProfile.whatsappNumber) {
      return res.status(400).json({ 
        error: "Numéro WhatsApp non configuré.",
        requiresSetup: true
      });
    }

    if (!userProfile.whatsappAuthorized) {
      return res.status(403).json({ 
        error: "Autorisation WhatsApp requise.",
        requiresAuthorization: true
      });
    }

    // Envoi du message WhatsApp
    const result = await sendWhatsAppMessage(userProfile.whatsappNumber, message);

    await saveLog("logs_systeme/whatsapp_envoyes", {
      uid: decodedToken.uid,
      to: userProfile.whatsappNumber,
      message: message,
      twilio_sid: result.sid
    });

    return res.status(200).json({
      success: true,
      message: "Message WhatsApp envoyé avec succès.",
      twilio_sid: result.sid
    });
  } catch (error) {
    console.error("❌ Erreur envoi WhatsApp :", error.message);
    return res.status(500).json({
      success: false,
      error: "Erreur lors de l'envoi du message WhatsApp.",
      details: error.message
    });
  }
});

// ==================== ROUTE WEBHOOK WHATSAPP ====================

/**
 * Route webhook WhatsApp pour Twilio.
 */
app.post("/webhook/whatsapp", async (req, res) => {
  try {
    const incomingMessage = req.body.Body || req.body.body || "";
    const from = req.body.From || req.body.from || "unknown";
    const messageSid = req.body.MessageSid || req.body.messageSid || null;

    if (!incomingMessage || incomingMessage.trim().length === 0) {
      return res.status(400).json({ error: "Message vide non accepté." });
    }

    // Nettoyage du numéro de téléphone
    const cleanedFrom = from.replace("whatsapp:", "").replace("+", "");

    // Sauvegarde du message entrant
    await saveLog("logs_systeme/whatsapp_entrants", {
      from: cleanedFrom,
      message: incomingMessage,
      message_sid: messageSid,
      direction: "incoming"
    });

    // Recherche de l'utilisateur par numéro WhatsApp
    let userProfile = null;
    try {
      const usersRef = db.ref("users");
      const snapshot = await usersRef.once("value");
      const users = snapshot.val();
      
      if (users) {
        for (const uid in users) {
          if (users[uid].whatsappNumber === cleanedFrom || users[uid].whatsappNumber === `+${cleanedFrom}`) {
            userProfile = users[uid];
            userProfile.uid = uid;
            break;
          }
        }
      }
    } catch (userError) {
      console.warn("⚠️ Impossible de trouver l'utilisateur :", userError.message);
    }

    // Initialisation Twilio dynamique
    const twilioData = getTwilioClient();

    // Recherche d'informations complémentaires
    const wikiData = await fetchWikiMedia(incomingMessage);
    const webData = await searchWeb(incomingMessage);

    // Construction du contexte enrichi
    let contextPrompt = `Question de l'utilisateur WhatsApp : ${incomingMessage}\n\n`;
    
    if (userProfile) {
      contextPrompt += `Utilisateur identifié : ${userProfile.displayName || "Utilisateur"}\n`;
      if (userProfile.whatsappAuthorized) {
        contextPrompt += `Autorisation WhatsApp accordée.\n`;
      }
      contextPrompt += `\n`;
    }
    
    if (wikiData.summary) {
      contextPrompt += `Contexte Wikipédia :\n${wikiData.summary.substring(0, 800)}\n\n`;
    }
    
    if (webData.abstract) {
      contextPrompt += `Contexte Web :\n${webData.abstract.substring(0, 800)}\n\n`;
    }

    // Appel à OpenRouter avec l'identité MILO
    let aiResponse;
    try {
      aiResponse = await callOpenRouter(contextPrompt, `${MILO_SYSTEM_PROMPT} Réponds de manière concise pour WhatsApp.`);
    } catch (aiError) {
      console.error("❌ Erreur OpenRouter dans webhook :", aiError.message);
      aiResponse = "Désolé, je n'ai pas pu traiter votre demande pour le moment. Veuillez réessayer plus tard.";
    }

    // Sauvegarde du log de la réponse
    await saveLog("logs_systeme/whatsapp_entrants", {
      from: cleanedFrom,
      message: aiResponse,
      message_sid: messageSid,
      direction: "outgoing",
      user_identified: !!userProfile
    });

    // Réponse via Twilio si le client est valide
    if (twilioData) {
      try {
        await twilioData.client.messages.create({
          from: `whatsapp:${twilioData.fromNumber}`,
          to: from,
          body: aiResponse
        });
        console.log(`✅ Réponse WhatsApp envoyée à ${cleanedFrom}`);
      } catch (twilioError) {
        console.error("❌ Erreur envoi Twilio :", twilioError.message);
      }
    } else {
      console.warn("⚠️ Réponse WhatsApp non envoyée : client Twilio indisponible.");
    }

    // Réponse au webhook Twilio
    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?><Response><Message>${aiResponse.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")}</Message></Response>`;
    res.set("Content-Type", "text/xml");
    return res.status(200).send(twimlResponse);
  } catch (error) {
    console.error("❌ Erreur webhook WhatsApp :", error.message);
    return res.status(500).json({ error: "Erreur lors du traitement du webhook." });
  }
});

// ==================== ROUTE D'ENVOI D'E-MAILS ====================

/**
 * Route d'envoi d'e-mails via Resend.
 */
app.post("/action/send-email", async (req, res) => {
  try {
    const { to, subject, html, text, from } = req.body;

    if (!to || !subject || (!html && !text)) {
      return res.status(400).json({
        error: "Les champs 'to', 'subject' et au moins 'html' ou 'text' sont requis."
      });
    }

    // Validation simple de l'email
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      return res.status(400).json({ error: "Adresse e-mail invalide." });
    }

    // Initialisation Resend dynamique
    const resendClient = getResendClient();

    if (!resendClient) {
      await saveLog("logs_systeme/emails", {
        to: to,
        subject: subject,
        status: "failed",
        error: "Client Resend non configuré"
      });

      return res.status(503).json({
        success: false,
        error: "Service e-mail non configuré. Veuillez configurer RESEND_API_KEY."
      });
    }

    // Envoi de l'e-mail
    const emailData = {
      from: from || process.env.RESEND_FROM_EMAIL || "MILO <onboarding@resend.dev>",
      to: [to],
      subject: subject
    };

    if (html) emailData.html = html;
    if (text) emailData.text = text;

    const emailResult = await resendClient.emails.send(emailData);

    // Sauvegarde du log
    await saveLog("logs_systeme/emails", {
      to: to,
      subject: subject,
      status: "sent",
      email_id: emailResult?.id || null
    });

    return res.status(200).json({
      success: true,
      message: "E-mail envoyé avec succès.",
      email_id: emailResult?.id || null
    });
  } catch (error) {
    console.error("❌ Erreur envoi e-mail :", error.message);

    await saveLog("logs_systeme/emails", {
      to: req.body?.to || "unknown",
      subject: req.body?.subject || "unknown",
      status: "failed",
      error: error.message
    });

    return res.status(500).json({
      success: false,
      error: "Erreur lors de l'envoi de l'e-mail.",
      details: error.message
    });
  }
});

// ==================== GESTION DES ERREURS 404 ====================
// Middleware pour les routes non trouvées
app.use((req, res) => {
  console.warn(`⚠️ Route non trouvée : ${req.method} ${req.url}`);
  res.status(404).json({
    success: false,
    error: "Route non trouvée",
    path: req.url,
    method: req.method
  });
});

// ==================== GESTION DES ERREURS GLOBALES ====================

// Middleware de gestion des erreurs Express
app.use((error, req, res, next) => {
  console.error("❌ Erreur Express :", error.message);
  console.error(error.stack);
  
  if (res.headersSent) {
    return next(error);
  }
  
  res.status(500).json({
    success: false,
    error: "Erreur interne du serveur",
    details: error.message
  });
});

// Capture les erreurs non gérées pour éviter le crash du serveur
process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception :", error);
  console.error(error.stack);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection :", reason);
});

// ==================== DÉMARRAGE DU SERVEUR ====================
const PORT = process.env.PORT || 5000;

// Création du serveur avec configuration optimisée
const server = app.listen(PORT, () => {
  console.log("========================================");
  console.log(`🚀 Cerveau MILO actif sur le port ${PORT}`);
  console.log(MILO_IDENTITY);
  console.log(`🕐 Démarrage : ${new Date().toISOString()}`);
  console.log(`📊 Environnement : ${process.env.NODE_ENV || "development"}`);
  console.log("========================================");
});

// Configuration du serveur pour les timeouts
server.timeout = 120000; // 2 minutes
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

// Gestion de l'arrêt gracieux
process.on("SIGTERM", () => {
  console.log("🛑 Signal SIGTERM reçu. Arrêt gracieux...");
  server.close(() => {
    console.log("✅ Serveur arrêté proprement.");
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  console.log("🛑 Signal SIGINT reçu. Arrêt gracieux...");
  server.close(() => {
    console.log("✅ Serveur arrêté proprement.");
    process.exit(0);
  });
});
