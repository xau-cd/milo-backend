const functions = require("firebase-functions");
const admin = require("firebase-admin");
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Twilio } = require("twilio");
const { Resend } = require("resend");

// 1. INITIALISATION FIREBASE SÉCURISÉE
admin.initializeApp({
  databaseURL: "https://milo-ead21-default-rtdb.europe-west1.firebasedatabase.app"
});
const db = admin.database();

// 2. CONFIGURATION DES CLÉS API
const OPENROUTER_API_KEY = "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";
const RESEND_API_KEY = "TA_CLE_RESEND"; // À remplacer
const TWILIO_SID = "TON_TWILIO_SID";     // À remplacer
const TWILIO_TOKEN = "TON_TWILIO_TOKEN"; // À remplacer

const resend = new Resend(RESEND_API_KEY);
const twilioClient = new Twilio(TWILIO_SID, TWILIO_TOKEN);

const app = express();
app.use(cors({ origin: true }));
app.use(express.json());
app.use(express.urlencoded({ extended: true })); // Requis pour lire les webhooks Twilio

// ============================================================================
// LES COMPÉTENCES DU CERVEAU (FONCTIONS D'AUTOMATISATION)
// ============================================================================

// Compétence 1 : Recherche WikiMedia (Images et Infos)
async function fetchWikiMedia(query) {
    try {
        const url = `https://fr.wikipedia.org/w/api.php?action=query&titles=${encodeURIComponent(query)}&prop=extracts|pageimages&format=json&exintro=1&pithumbsize=500`;
        const response = await axios.get(url);
        const pages = response.data.query.pages;
        const pageId = Object.keys(pages)[0];
        if (pageId === "-1") return { error: "Aucun résultat trouvé sur Wikimedia." };
        
        return {
            titre: pages[pageId].title,
            extrait: pages[pageId].extract.replace(/(<([^>]+)>)/gi, ""), // Nettoie le HTML
            image_url: pages[pageId].thumbnail ? pages[pageId].thumbnail.source : null
        };
    } catch (e) { return { error: "Erreur lors de la connexion à Wikimedia." }; }
}

// Compétence 2 : Recherche Web (DuckDuckGo via API tierce ou scrape simulé)
async function searchWeb(query) {
    // Note: DuckDuckGo n'a pas d'API REST officielle simple, on utilise généralement une API comme Serper 
    // ou on simule la recherche ici avec Axios pour l'exemple.
    try {
        const response = await axios.get(`https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json`);
        return response.data.AbstractText || "Aucun résumé direct trouvé, analyse des liens requise.";
    } catch (e) { return { error: "Erreur réseau DuckDuckGo." }; }
}

// ============================================================================
// ROUTE PRINCIPALE : LE COEUR DE L'IA (FRONTEND -> BACKEND)
// ============================================================================

app.post("/chat", async (req, res) => {
    try {
        const { message } = req.body;
        if (!message) return res.status(400).json({ error: "Message vide." });

        const payload = {
            model: "deepseek/deepseek-chat",
            messages: [
                { role: "system", content: "Tu es MILO, une IA avancée de E-clant technologie. Tu peux envoyer des emails, des messages WhatsApp, chercher des images et scrapper le web. Réponds de manière concise et professionnelle." },
                { role: "user", content: message }
            ]
            // Ici, dans une version "Tool Calling" absolue, on injecterait le tableau 'tools' 
            // pour que DeepSeek nous demande d'exécuter la recherche ou l'email.
        };

        const aiResponse = await axios.post("https://openrouter.ai/api/v1/chat/completions", payload, {
            headers: {
                "Authorization": `Bearer ${OPENROUTER_API_KEY}`,
                "Content-Type": "application/json",
                "HTTP-Referer": "https://milo-ead21.firebaseapp.com",
                "X-Title": "MILO Agent"
            }
        });

        const replyText = aiResponse.data.choices[0].message.content;

        // Historisation Firebase
        await db.ref("logs_systeme/chat_frontend").push({
            user: message,
            milo: replyText,
            timestamp: admin.database.ServerValue.TIMESTAMP
        });

        return res.status(200).json({ reply: replyText });

    } catch (error) {
        console.error("Crash critique /chat :", error);
        return res.status(500).json({ error: "Surcharge cognitive. Le cerveau MILO a rencontré une erreur." });
    }
});

// ============================================================================
// WEBHOOKS : LES YEUX ET LES OREILLES DU CERVEAU
// ============================================================================

// 1. Lire les messages WhatsApp entrants (Webhook Twilio)
app.post("/webhook/whatsapp", async (req, res) => {
    try {
        const incomingMsg = req.body.Body;
        const sender = req.body.From; // ex: "whatsapp:+243xxxxxxxxx"

        console.log(`WhatsApp reçu de ${sender} : ${incomingMsg}`);

        // Sauvegarde le message entrant
        await db.ref("logs_systeme/whatsapp_entrants").push({
            expediteur: sender,
            message: incomingMsg,
            timestamp: admin.database.ServerValue.TIMESTAMP
        });

        // MILO réfléchit et répond automatiquement
        const payload = {
            model: "deepseek/deepseek-chat",
            messages: [{ role: "user", content: incomingMsg }]
        };

        const aiResponse = await axios.post("https://openrouter.ai/api/v1/chat/completions", payload, {
            headers: { "Authorization": `Bearer ${OPENROUTER_API_KEY}` }
        });

        const replyText = aiResponse.data.choices[0].message.content;

        // Envoi de la réponse sur WhatsApp via Twilio
        await twilioClient.messages.create({
            body: replyText,
            from: "whatsapp:+14155238886", // Ton numéro Twilio
            to: sender
        });

        return res.status(200).send("<Response></Response>"); // Twilio exige cette réponse
    } catch (error) {
        console.error("Erreur Webhook WhatsApp :", error);
        return res.status(500).send("Erreur");
    }
});

// 2. Envoyer un Email automatique (Requête depuis ton système)
app.post("/action/send-email", async (req, res) => {
    try {
        const { to, subject, htmlContent } = req.body;

        const data = await resend.emails.send({
            from: 'MILO <onboarding@resend.dev>', // Ton domaine validé plus tard
            to: [to],
            subject: subject,
            html: htmlContent
        });

        await db.ref("logs_systeme/emails_envoyes").push({ to, subject, timestamp: admin.database.ServerValue.TIMESTAMP });

        return res.status(200).json({ success: true, data });
    } catch (error) {
        return res.status(500).json({ error: "Échec de l'envoi de l'email." });
    }
});

// Exporter l'application express dans Firebase
exports.miloBrain = functions.runWith({ timeoutSeconds: 60, memory: "1GB" }).https.onRequest(app);
