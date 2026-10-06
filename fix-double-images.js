#!/usr/bin/env node
/**
 * ============================================================================
 * fix-double-images.js — Luba AI v16.5.1
 * ============================================================================
 * CORRECTIF COMPLET (2 patches) :
 *
 *   Patch A : Supprime le bloc `shouldInlineImages` qui injectait
 *             jusqu'à 3 images dans `reply`.
 *
 *   Patch B : Ajoute un filtre `stripMarkdownImages` dans `formatFinalReply`
 *             qui retire TOUTES les images Markdown ![alt](url) du texte
 *             final, y compris celles écrites par le LLM lui-même
 *             (source du bug 4-5 images).
 *
 * USAGE :
 *   node fix-double-images.js
 *
 * EFFET :
 *   Modifie index.js en place + crée index.js.v1650.backup
 * ============================================================================
 */

"use strict";

const fs   = require("fs");
const path = require("path");

const ROOT   = process.cwd();
const TARGET = path.join(ROOT, "index.js");
const BACKUP = path.join(ROOT, "index.js.v1650.backup");

// ─── 0. Vérifications ───────────────────────────────────────────────────────
if (!fs.existsSync(TARGET)) {
  console.error("❌ index.js introuvable dans : " + ROOT);
  console.error("   → Lance ce script depuis la racine de ton projet.");
  process.exit(1);
}

// ─── 1. Backup ──────────────────────────────────────────────────────────────
if (!fs.existsSync(BACKUP)) {
  fs.copyFileSync(TARGET, BACKUP);
  console.log("✅ Backup créé : index.js.v1650.backup");
} else {
  console.log("ℹ️  Backup déjà présent : index.js.v1650.backup");
}

let src = fs.readFileSync(TARGET, "utf8");
const originalSrc = src;
let patchesApplied = 0;

// ═══════════════════════════════════════════════════════════════════════════
// PATCH A — Suppression du bloc `shouldInlineImages`
// ═══════════════════════════════════════════════════════════════════════════
const RE_A = /[ \t]*\/\/\s*Images EN HAUT[ \t]*\r?\n[ \t]*const\s+shouldInlineImages\s*=[\s\S]*?\n[ \t]*\}[ \t]*\r?\n/;

if (src.includes("shouldInlineImages")) {
  if (!RE_A.test(src)) {
    console.error("❌ [Patch A] Bloc `shouldInlineImages` détecté mais regex n'a pas matché.");
    console.error("   → Édite index.js manuellement (voir instructions du README).");
    process.exit(1);
  }
  src = src.replace(
    RE_A,
    "  // ✅ FIX v16.5.1 : Les images restent UNIQUEMENT dans `images` et `media.images`.\n" +
    "  //    Aucune injection Markdown automatique dans `reply`.\n"
  );
  patchesApplied++;
  console.log("✅ [Patch A] Bloc `shouldInlineImages` supprimé.");
} else {
  console.log("ℹ️  [Patch A] Déjà corrigé ou absent.");
}

// ═══════════════════════════════════════════════════════════════════════════
// PATCH B — Ajout du filtre `stripMarkdownImages` dans formatFinalReply
// ═══════════════════════════════════════════════════════════════════════════

// B.1 — Vérifier si le filtre existe déjà
if (src.includes("stripMarkdownImages")) {
  console.log("ℹ️  [Patch B] Filtre `stripMarkdownImages` déjà présent.");
} else {
  // B.2 — Localiser la fonction `formatFinalReply`
  const RE_FMT = /function formatFinalReply\(rawText\)\s*\{([\s\S]*?)\n\}/;

  if (!RE_FMT.test(src)) {
    console.error("❌ [Patch B] Fonction `formatFinalReply` introuvable.");
    console.error("   → Édite index.js manuellement.");
    process.exit(1);
  }

  // B.3 — Ajouter la fonction `stripMarkdownImages` AVANT `formatFinalReply`
  const STRIP_FN =
`/**
 * ✅ FIX v16.5.1 : Retire toutes les images Markdown ![alt](url) d'un texte.
 * Utilisé pour empêcher le LLM d'injecter des images dans la réponse finale
 * (les images sont affichées séparément par le frontend via `images`).
 *
 * Attention :
 *   - Ne touche PAS aux liens Markdown [texte](url)
 *   - Ne touche PAS aux liens-images d'ads [![alt](img)](link) — car les ads
 *     sont ajoutées APRÈS ce filtre.
 *   - Préserve les images dans les blocs de code (```...```)
 */
function stripMarkdownImages(text) {
  if (!text || typeof text !== "string") return text || "";

  // Découpage : préserve les blocs de code
  const parts = [];
  const codeRe = /\`\`\`[\\s\\S]*?\`\`\`|\`[^\`\\n]*\`/g;
  let lastIndex = 0;
  let m;
  while ((m = codeRe.exec(text)) !== null) {
    if (m.index > lastIndex) parts.push({ code: false, content: text.slice(lastIndex, m.index) });
    parts.push({ code: true, content: m[0] });
    lastIndex = m.index + m[0].length;
  }
  if (lastIndex < text.length) parts.push({ code: false, content: text.slice(lastIndex) });

  // Filtre dans les segments non-code uniquement
  const cleaned = parts.map((part) => {
    if (part.code) return part.content;
    return part.content.replace(/!\\[[^\\]]*\\]\\([^)]+\\)/g, "");
  }).join("");

  // Nettoie les lignes vides multiples laissées par la suppression
  return cleaned.replace(/\\n{3,}/g, "\\n\\n").trim();
}

`;

  // B.4 — Insérer la fonction AVANT formatFinalReply
  const fnMatch = src.match(RE_FMT);
  const insertPos = fnMatch.index;
  src = src.slice(0, insertPos) + STRIP_FN + src.slice(insertPos);

  // B.5 — Modifier formatFinalReply pour appeler stripMarkdownImages
  //      On cherche la ligne "return cleaned.trim();" dans formatFinalReply
  const RE_RETURN = /(function formatFinalReply\(rawText\)\s*\{[\s\S]*?)(  return\s+cleaned\.trim\(\);)/;

  if (!RE_RETURN.test(src)) {
    console.error("❌ [Patch B] Impossible de modifier `formatFinalReply` (return introuvable).");
    console.error("   → Édite index.js manuellement.");
    process.exit(1);
  }

  src = src.replace(
    RE_RETURN,
    "$1  cleaned = stripMarkdownImages(cleaned);\n$2"
  );

  patchesApplied++;
  console.log("✅ [Patch B] Filtre `stripMarkdownImages` ajouté et intégré dans `formatFinalReply`.");
}

// ═══════════════════════════════════════════════════════════════════════════
// VÉRIFICATIONS FINALES
// ═══════════════════════════════════════════════════════════════════════════

if (src === originalSrc) {
  console.log("");
  console.log("ℹ️  Aucun changement. Le correctif est probablement déjà appliqué.");
  process.exit(0);
}

// Vérifie qu'il ne reste plus de `shouldInlineImages` / `imagesMd`
if (src.includes("shouldInlineImages") || src.includes("imagesMd")) {
  console.error("❌ Des références à `shouldInlineImages` ou `imagesMd` persistent.");
  console.error("   → Restaure : cp index.js.v1650.backup index.js");
  process.exit(1);
}

// Vérifie que stripMarkdownImages est bien présent ET utilisé
if (!src.includes("function stripMarkdownImages")) {
  console.error("❌ `stripMarkdownImages` n'a pas été inséré.");
  process.exit(1);
}
if (!src.includes("cleaned = stripMarkdownImages(cleaned);")) {
  console.error("❌ `stripMarkdownImages` n'est pas appelé dans `formatFinalReply`.");
  process.exit(1);
}

// Vérifie que les champs structurés d'images sont toujours là
if (!src.includes("images: collectedImages")) {
  console.error("⚠️  Attention : `images: collectedImages` introuvable.");
  console.error("   → Le patch a peut-être cassé la structure de retour.");
  process.exit(1);
}

// ─── Écriture ───────────────────────────────────────────────────────────────
fs.writeFileSync(TARGET, src, "utf8");

console.log("");
console.log("╔═══════════════════════════════════════════════════════════════╗");
console.log("║  ✅ FIX v16.5.1 COMPLET appliqué (" + patchesApplied + " patch(s))              ║");
console.log("╚═══════════════════════════════════════════════════════════════╝");
console.log("");
console.log("Ce qui a été corrigé :");
console.log("  • Patch A : plus d'injection backend des images dans `reply` (max 3)");
console.log("  • Patch B : plus AUCUNE image Markdown du LLM dans `reply` (4-6+)");
console.log("");
console.log("Vérifie maintenant avec :");
console.log("  node --check index.js");
console.log("  grep -n \"shouldInlineImages\\|imagesMd\" index.js       → 0 résultat");
console.log("  grep -n \"stripMarkdownImages\" index.js               → 2-3 résultats");
console.log("  grep -n \"images: collectedImages\" index.js           → 2 résultats");
console.log("");
console.log("Pour restaurer :");
console.log("  cp index.js.v1650.backup index.js");
console.log("");