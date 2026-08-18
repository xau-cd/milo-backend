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

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://milo-ead21-default-rtdb.europe-west1.firebasedatabase.app"
  });
}

const db = admin.database();

// ==================== CONFIGURATION EXPRESS ====================
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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

// ==================== FONCTIONS UTILITAIRES ====================

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
async function callOpenRouter(userMessage, systemPrompt = "Tu es MILO, un assistant IA serviable et intelligent.") {
  try {
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
          "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY || "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"}`,
          "Content-Type": "application/json"
        },
        timeout: 30000
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
      console.error("Détails OpenRouter :", error.response.data);
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

// ==================== ROUTES ====================

/**
 * Route racine pour vérifier que le serveur est actif.
 */
app.get("/", (req, res) => {
  res.status(200).send("✅ Cerveau MILO actif et opérationnel !");
});

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
 * Route de chat frontend.
 */
app.post("/chat", async (req, res) => {
  try {
    const { message, user_id } = req.body;

    if (!message || typeof message !== "string" || message.trim().length === 0) {
      return res.status(400).json({ error: "Le champ 'message' est requis et doit être une chaîne non vide." });
    }

    // Recherche d'informations complémentaires
    const wikiData = await fetchWikiMedia(message);
    const commonsImages = await fetchWikimediaCommons(message, 3);
    const webData = await searchWeb(message);

    // Construction du contexte enrichi
    let contextPrompt = `Question de l'utilisateur : ${message}\n\n`;
    
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

    // Appel à OpenRouter
    const aiResponse = await callOpenRouter(contextPrompt);

    // Sauvegarde du log
    await saveLog("logs_systeme/chat_frontend", {
      user_id: user_id || "anonymous",
      message: message,
      response: aiResponse,
      wiki_found: !!wikiData.summary,
      web_found: !!webData.abstract,
      commons_images_found: commonsImages.length
    });

    return res.status(200).json({
      success: true,
      response: aiResponse,
      wiki: {
        title: wikiData.title,
        summary: wikiData.summary,
        image: wikiData.image,
        url: wikiData.url,
        categories: wikiData.categories
      },
      commons: commonsImages,
      web: webData
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

    // Sauvegarde du message entrant
    await saveLog("logs_systeme/whatsapp_entrants", {
      from: from,
      message: incomingMessage,
      message_sid: messageSid,
      direction: "incoming"
    });

    // Initialisation Twilio dynamique
    const twilioData = getTwilioClient();

    // Recherche d'informations complémentaires
    const wikiData = await fetchWikiMedia(incomingMessage);
    const webData = await searchWeb(incomingMessage);

    // Construction du contexte enrichi
    let contextPrompt = `Question de l'utilisateur WhatsApp : ${incomingMessage}\n\n`;
    
    if (wikiData.summary) {
      contextPrompt += `Contexte Wikipédia :\n${wikiData.summary.substring(0, 800)}\n\n`;
    }
    
    if (webData.abstract) {
      contextPrompt += `Contexte Web :\n${webData.abstract.substring(0, 800)}\n\n`;
    }

    // Appel à OpenRouter
    let aiResponse;
    try {
      aiResponse = await callOpenRouter(contextPrompt, "Tu es MILO, un assistant WhatsApp serviable et concis. Réponds de manière claire et directe.");
    } catch (aiError) {
      console.error("❌ Erreur OpenRouter dans webhook :", aiError.message);
      aiResponse = "Désolé, je n'ai pas pu traiter votre demande pour le moment. Veuillez réessayer plus tard.";
    }

    // Sauvegarde du log de la réponse
    await saveLog("logs_systeme/whatsapp_entrants", {
      from: from,
      message: aiResponse,
      message_sid: messageSid,
      direction: "outgoing"
    });

    // Réponse via Twilio si le client est valide
    if (twilioData) {
      try {
        await twilioData.client.messages.create({
          from: `whatsapp:${twilioData.fromNumber}`,
          to: from,
          body: aiResponse
        });
        console.log(`✅ Réponse WhatsApp envoyée à ${from}`);
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

// ==================== GESTION DES ERREURS GLOBALES ====================

// Capture les erreurs non gérées pour éviter le crash du serveur
process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception :", error);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection :", reason);
});

// ==================== DÉMARRAGE DU SERVEUR ====================
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Cerveau MILO actif sur le port ${PORT}`);
});
