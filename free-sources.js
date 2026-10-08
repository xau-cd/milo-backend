// ================================================================================
// free-sources.js — Sources GRATUITES, SANS CLÉ API, pour Luba v17
// HIKLON TECHNOLOGIES · Module autonome (aucune dépendance obligatoire)
// ================================================================================
// 14 outils prêts à brancher sur le système d'outils de Luba :
//
//   ACTUALITÉS  search_google_news · search_hacker_news
//   SCIENCE     search_openalex · search_crossref · search_arxiv
//   LIVRES      search_openlibrary
//   CARTES      osm_geocode · osm_nearby_places
//   MÉTÉO       get_weather_open_meteo
//   DONNÉES     worldbank_indicator
//   VIDÉO       search_youtube_innertube   (paquet optionnel : youtubei.js)
//   MUSIQUE     search_deezer · search_itunes
//   IMAGES      search_loc_gov             (Library of Congress, domaine public)
//
// UTILISATION (dans index.js, déjà prévu dans la v17 : il suffit de poser ce fichier à côté) :
//   const free = require("./free-sources.js");
//   free.schemas()                       → schémas d'outils (format OpenAI / function calling)
//   free.has("search_arxiv")             → true
//   await free.execute(nom, args, ctx)   → { result, sourceKeys }   (ne lève JAMAIS d'exception)
//
// VARIABLES D'ENVIRONNEMENT (toutes facultatives)
//   FREE_SOURCES_TOOLS   liste d'outils à activer, séparés par des virgules (défaut : tous)
//                        → moins d'outils = prompt plus léger = réponses plus rapides
//   CONTACT_EMAIL        ajouté au User-Agent et aux « polite pools » (OpenAlex, Crossref)
//
// BONNES PRATIQUES INTÉGRÉES : timeouts, 1 retry sur 429/5xx, cache mémoire par outil,
// limitation de débit (Nominatim ≥ 1 s, Overpass ≥ 1 s, loc.gov), User-Agent identifié,
// résultats tronqués (le LLM n'a pas besoin de 50 Ko par réponse), erreurs en français.
// ================================================================================

"use strict";

// ---------------------------------------------------------------------------------
// §1 — Attribution (clés utilisées par le pied de réponse « Sources : »)
// ---------------------------------------------------------------------------------
const SOURCES = {
  googlenews: ["Google News", "https://news.google.com"],
  hackernews: ["Hacker News", "https://news.ycombinator.com"],
  openalex: ["OpenAlex", "https://openalex.org"],
  crossref: ["Crossref", "https://www.crossref.org"],
  arxiv: ["arXiv", "https://arxiv.org"],
  openlibrary: ["Open Library", "https://openlibrary.org"],
  openstreetmap: ["OpenStreetMap", "https://www.openstreetmap.org"],
  openmeteo: ["Open-Meteo", "https://open-meteo.com"],
  worldbank: ["Banque mondiale", "https://data.worldbank.org"],
  youtube: ["YouTube", "https://youtube.com"],
  deezer: ["Deezer", "https://www.deezer.com"],
  itunes: ["Apple Music / iTunes", "https://music.apple.com"],
  locgov: ["Library of Congress", "https://www.loc.gov"]
};

// ---------------------------------------------------------------------------------
// §2 — Utilitaires généraux
// ---------------------------------------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const UA = `LubaAI/17 (+HIKLON Technologies${process.env.CONTACT_EMAIL ? `; ${process.env.CONTACT_EMAIL}` : ""})`;
const CONTACT = process.env.CONTACT_EMAIL || "";

class UserError extends Error {}           // erreur « attendue » (paramètre manquant…) : message montré tel quel
const fail = (msg) => { throw new UserError(msg); };

const clampInt = (v, def, lo, hi) => { const n = parseInt(v, 10); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : def; };
const clampNum = (v, def, lo, hi) => { const n = parseFloat(v); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : def; };
const cut = (s, max = 400) => { const t = String(s ?? "").replace(/\s+/g, " ").trim(); return t.length > max ? `${t.slice(0, max - 1)}…` : t; };
const needText = (v, label = "requête", max = 200) => { const t = cut(v, max); if (!t) fail(`Paramètre « ${label} » manquant.`); return t; };
const oneOf = (v, list, def) => (list.includes(v) ? v : def);
const toIso = (s) => { const t = Date.parse(s); return Number.isFinite(t) ? new Date(t).toISOString() : null; };
const unaccent = (s) => String(s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
const httpsify = (u) => (typeof u === "string" ? (u.startsWith("//") ? `https:${u}` : u.replace(/^http:\/\//, "https://")) : null);

// Distance (m) entre deux points GPS
function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371000, rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad, dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  return Math.round(2 * R * Math.asin(Math.sqrt(a)));
}

// ---------------------------------------------------------------------------------
// §3 — Mini-parseur XML (RSS / Atom) — suffisant pour Google News et arXiv, sans dépendance
// ---------------------------------------------------------------------------------
function decodeXml(s) {
  return String(s ?? "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#x([0-9a-f]+);/gi, (_m, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_m, d) => String.fromCodePoint(parseInt(d, 10)))
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"").replace(/&apos;/g, "'").replace(/&amp;/g, "&");
}
const stripTags = (s) => decodeXml(s).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
const xmlBlocks = (xml, tag) => [...String(xml).matchAll(new RegExp(`<${tag}(?:\\s[^>]*)?>[\\s\\S]*?</${tag}>`, "g"))].map((m) => m[0]);
function xmlText(xml, tag) {
  const m = String(xml).match(new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`));
  return m ? decodeXml(m[1]).trim() : "";
}
function xmlAttr(xml, tag, attr) {
  const m = String(xml).match(new RegExp(`<${tag}\\b[^>]*\\b${attr}="([^"]*)"`));
  return m ? decodeXml(m[1]) : "";
}

// ---------------------------------------------------------------------------------
// §4 — Couche HTTP : timeout, retry, cache, limitation de débit
// ---------------------------------------------------------------------------------
let doFetch = (...a) => globalThis.fetch(...a);          // remplaçable (tests)

const cache = new Map();
const CACHE_MAX = 400;
function cacheGet(k) {
  const e = cache.get(k);
  if (!e) return undefined;
  if (e.exp <= Date.now()) { cache.delete(k); return undefined; }
  return e.value;
}
function cacheSet(k, value, ttlMs) {
  if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value);
  cache.set(k, { value, exp: Date.now() + ttlMs });
}

// File d'attente par hôte : garantit un écart minimal entre deux appels (Nominatim exige ≥ 1 s)
const lastCall = new Map(), chains = new Map();
function throttle(key, gapMs) {
  const prev = chains.get(key) || Promise.resolve();
  const run = prev.then(async () => {
    const wait = (lastCall.get(key) || 0) + gapMs - Date.now();
    if (wait > 0) await sleep(wait);
    lastCall.set(key, Date.now());
  });
  chains.set(key, run.catch(() => {}));
  return run;
}

async function http(url, { as = "json", method = "GET", headers = {}, body, ttlMs = 0, timeoutMs = 9000, gate = null, retries = 1 } = {}) {
  const ck = ttlMs ? `${method}|${url}|${body || ""}` : null;
  if (ck) { const hit = cacheGet(ck); if (hit !== undefined) return hit; }
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      if (gate) await throttle(gate.key, gate.gapMs);
      const res = await doFetch(url, {
        method, body, redirect: "follow", signal: AbortSignal.timeout(timeoutMs),
        headers: { "User-Agent": UA, Accept: as === "json" ? "application/json" : "*/*", ...headers }
      });
      if (res.status === 429 || res.status >= 500) {
        lastErr = new Error(`HTTP ${res.status}`);
        if (attempt < retries) { const ra = parseInt(res.headers?.get?.("retry-after") || "", 10); await sleep(Math.min(3000, (Number.isFinite(ra) ? ra : 1) * 1000)); continue; }
        throw lastErr;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      if (text.length > 3_000_000) throw new Error("réponse trop volumineuse");
      const value = as === "json" ? JSON.parse(text) : text;
      if (ck) cacheSet(ck, value, ttlMs);
      return value;
    } catch (e) {
      lastErr = e;
      const retryable = e.name === "TimeoutError" || /fetch failed|ECONN|ENOTFOUND|socket/i.test(e.message);
      if (!retryable || attempt >= retries) throw e;
      await sleep(400);
    }
  }
  throw lastErr;
}

function friendlyError(e) {
  if (e instanceof UserError) return e.message;
  if (e?.name === "TimeoutError") return "La source n'a pas répondu à temps. Réessaie dans un instant.";
  if (/HTTP 429/.test(e?.message)) return "La source limite les requêtes pour le moment. Réessaie dans quelques secondes.";
  if (/HTTP 4/.test(e?.message)) return `La source a refusé la requête (${e.message}).`;
  if (/HTTP 5/.test(e?.message)) return "La source est momentanément indisponible.";
  return "Impossible de joindre la source pour le moment.";
}

// ---------------------------------------------------------------------------------
// §5 — Définition des outils
// ---------------------------------------------------------------------------------
const TOOLS = {};
function defTool(name, description, properties, required, source, exec) {
  TOOLS[name] = {
    source,
    schema: { type: "function", function: { name, description, parameters: { type: "object", properties, required } } },
    exec
  };
}
const P = {
  query: (d = "Termes de recherche") => ({ type: "string", description: d }),
  limit: (max = 10, def = 5) => ({ type: "integer", description: `Nombre de résultats (1-${max}, défaut ${def})` })
};

// ===== ACTUALITÉS ================================================================
defTool("search_google_news",
  "Actualités récentes par mot-clé (Google News RSS), filtrables par langue, pays et fraîcheur.",
  { query: P.query(), language: { type: "string", description: "Code langue ISO, ex. fr, en (défaut fr)" }, country: { type: "string", description: "Code pays ISO, ex. CD, FR, US (défaut CD)" }, recency: { type: "string", enum: ["1h", "1d", "7d", "30d"], description: "Fraîcheur maximale" }, limit: P.limit(10, 6) },
  ["query"], "googlenews",
  async (a) => {
    const lang = /^[a-z]{2}$/i.test(a.language || "") ? a.language.toLowerCase() : "fr";
    const cc = /^[a-z]{2}$/i.test(a.country || "") ? a.country.toUpperCase() : "CD";
    const rec = oneOf(a.recency, ["1h", "1d", "7d", "30d"], null);
    const q = needText(a.query) + (rec ? ` when:${rec}` : "");
    const xml = await http(`https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=${lang}&gl=${cc}&ceid=${cc}:${lang}`, { as: "text", ttlMs: 5 * 60e3 });
    const articles = xmlBlocks(xml, "item").slice(0, clampInt(a.limit, 6, 1, 10)).map((b) => {
      const source = stripTags(xmlText(b, "source"));
      let title = stripTags(xmlText(b, "title"));
      if (source && title.endsWith(` - ${source}`)) title = title.slice(0, -(source.length + 3));
      return { title, source: source || null, sourceUrl: xmlAttr(b, "source", "url") || null, url: xmlText(b, "link"), publishedAt: toIso(xmlText(b, "pubDate")) };
    });
    return { success: true, query: needText(a.query), count: articles.length, articles };
  });

defTool("search_hacker_news",
  "Hacker News : recherche d'articles tech, derniers articles, ou classement du moment (top).",
  { query: P.query("Termes (inutile en mode top)"), mode: { type: "string", enum: ["search", "latest", "top"], description: "search (pertinence), latest (récents), top (classement actuel)" }, limit: P.limit(10, 5) },
  [], "hackernews",
  async (a) => {
    const mode = oneOf(a.mode, ["search", "latest", "top"], a.query ? "search" : "top");
    const limit = clampInt(a.limit, 5, 1, 10);
    const link = (id) => `https://news.ycombinator.com/item?id=${id}`;
    if (mode === "top") {
      const ids = await http("https://hacker-news.firebaseio.com/v0/topstories.json", { ttlMs: 5 * 60e3 });
      const items = await Promise.all(ids.slice(0, limit).map((id) => http(`https://hacker-news.firebaseio.com/v0/item/${id}.json`, { ttlMs: 5 * 60e3 }).catch(() => null)));
      const stories = items.filter(Boolean).map((i) => ({ title: cut(i.title, 200), url: i.url || link(i.id), discussion: link(i.id), points: i.score, comments: i.descendants ?? 0, author: i.by, createdAt: i.time ? new Date(i.time * 1000).toISOString() : null }));
      return { success: true, mode, count: stories.length, stories };
    }
    const q = needText(a.query);
    const endpoint = mode === "latest" ? "search_by_date" : "search";
    const data = await http(`https://hn.algolia.com/api/v1/${endpoint}?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=${limit}`, { ttlMs: 5 * 60e3 });
    const stories = (data.hits || []).map((h) => ({ title: cut(h.title, 200), url: h.url || link(h.objectID), discussion: link(h.objectID), points: h.points, comments: h.num_comments ?? 0, author: h.author, createdAt: h.created_at }));
    return { success: true, mode, query: q, count: stories.length, stories };
  });

// ===== SCIENCE ===================================================================
function abstractFromIndex(idx, max = 450) {
  if (!idx || typeof idx !== "object") return null;
  const words = [];
  for (const [w, positions] of Object.entries(idx)) for (const p of positions) words[p] = w;
  return cut(words.filter(Boolean).join(" "), max) || null;
}

defTool("search_openalex",
  "Articles scientifiques (250 M+ travaux) via OpenAlex : titre, auteurs, année, citations, accès libre.",
  { query: P.query("Sujet ou titre"), year_from: { type: "integer", description: "Année de publication minimale" }, sort: { type: "string", enum: ["relevance", "cited_by_count", "date"], description: "Tri (défaut relevance)" }, limit: P.limit(10, 5) },
  ["query"], "openalex",
  async (a) => {
    const q = needText(a.query);
    const params = new URLSearchParams({ search: q, "per-page": String(clampInt(a.limit, 5, 1, 10)), select: "id,doi,title,publication_year,cited_by_count,authorships,open_access,primary_location,abstract_inverted_index" });
    if (a.year_from) params.set("filter", `from_publication_date:${clampInt(a.year_from, 2000, 1900, 2100)}-01-01`);
    const sort = oneOf(a.sort, ["relevance", "cited_by_count", "date"], "relevance");
    if (sort === "cited_by_count") params.set("sort", "cited_by_count:desc");
    if (sort === "date") params.set("sort", "publication_date:desc");
    if (CONTACT) params.set("mailto", CONTACT);
    const data = await http(`https://api.openalex.org/works?${params}`, { ttlMs: 60 * 60e3 });
    const works = (data.results || []).map((w) => ({
      title: cut(w.title, 250), year: w.publication_year, citations: w.cited_by_count,
      authors: (w.authorships || []).slice(0, 5).map((x) => x.author?.display_name).filter(Boolean),
      venue: w.primary_location?.source?.display_name || null, doi: w.doi || null,
      openAccessUrl: w.open_access?.oa_url || null, abstract: abstractFromIndex(w.abstract_inverted_index)
    }));
    return { success: true, query: q, total: data.meta?.count ?? works.length, count: works.length, works };
  });

defTool("search_crossref",
  "Métadonnées d'articles et de livres (Crossref) : recherche par mots-clés ou résolution d'un DOI.",
  { query: P.query("Mots-clés (ignoré si doi fourni)"), doi: { type: "string", description: "DOI exact, ex. 10.1038/nature14539" }, limit: P.limit(10, 5) },
  [], "crossref",
  async (a) => {
    const map = (w) => ({
      title: cut(Array.isArray(w.title) ? w.title[0] : w.title, 250),
      authors: (w.author || []).slice(0, 5).map((x) => [x.given, x.family].filter(Boolean).join(" ")).filter(Boolean),
      year: w.issued?.["date-parts"]?.[0]?.[0] ?? null, journal: Array.isArray(w["container-title"]) ? w["container-title"][0] : null,
      doi: w.DOI, url: w.URL, citations: w["is-referenced-by-count"] ?? null, type: w.type || null
    });
    const mail = CONTACT ? `mailto=${encodeURIComponent(CONTACT)}` : "";
    if (a.doi) {
      const doi = cut(a.doi, 200).replace(/^https?:\/\/(dx\.)?doi\.org\//i, "");
      if (!/^10\.\d{4,9}\/\S+$/.test(doi)) fail("DOI invalide (format attendu : 10.xxxx/...).");
      const data = await http(`https://api.crossref.org/works/${encodeURIComponent(doi)}${mail ? `?${mail}` : ""}`, { ttlMs: 24 * 3600e3 });
      return { success: true, count: 1, works: [map(data.message || {})] };
    }
    const q = needText(a.query);
    const select = "DOI,title,author,issued,container-title,URL,is-referenced-by-count,type";
    const data = await http(`https://api.crossref.org/works?query=${encodeURIComponent(q)}&rows=${clampInt(a.limit, 5, 1, 10)}&select=${select}${mail ? `&${mail}` : ""}`, { ttlMs: 60 * 60e3 });
    const works = (data.message?.items || []).map(map);
    return { success: true, query: q, total: data.message?.["total-results"] ?? works.length, count: works.length, works };
  });

defTool("search_arxiv",
  "Prépublications scientifiques arXiv (maths, physique, informatique, IA…) avec résumé et PDF.",
  { query: P.query("Mots-clés (anglais de préférence)"), category: { type: "string", description: "Catégorie arXiv facultative, ex. cs.AI, math.CO" }, sort: { type: "string", enum: ["relevance", "date"], description: "Tri (défaut relevance)" }, limit: P.limit(10, 5) },
  ["query"], "arxiv",
  async (a) => {
    const q = needText(a.query);
    const terms = q.split(/\s+/).slice(0, 8).map((t) => `all:${encodeURIComponent(t)}`).join("+AND+");
    const cat = /^[a-z-]+(\.[A-Za-z-]+)?$/.test(a.category || "") ? `+AND+cat:${a.category}` : "";
    const sort = oneOf(a.sort, ["relevance", "date"], "relevance") === "date" ? "submittedDate" : "relevance";
    const xml = await http(`https://export.arxiv.org/api/query?search_query=${terms}${cat}&start=0&max_results=${clampInt(a.limit, 5, 1, 10)}&sortBy=${sort}&sortOrder=descending`, { as: "text", ttlMs: 60 * 60e3, gate: { key: "arxiv", gapMs: 3000 } });
    const papers = xmlBlocks(xml, "entry").map((b) => ({
      title: cut(stripTags(xmlText(b, "title")), 250), summary: cut(stripTags(xmlText(b, "summary")), 500),
      authors: xmlBlocks(b, "author").slice(0, 6).map((x) => stripTags(xmlText(x, "name"))),
      published: toIso(xmlText(b, "published")), category: xmlAttr(b, "arxiv:primary_category", "term") || null,
      url: xmlText(b, "id").replace(/^http:/, "https:"), pdf: httpsify((b.match(/<link[^>]*title="pdf"[^>]*href="([^"]+)"/) || b.match(/<link[^>]*href="([^"]+)"[^>]*title="pdf"/) || [])[1] || null)
    }));
    return { success: true, query: q, count: papers.length, papers };
  });

// ===== LIVRES ====================================================================
defTool("search_openlibrary",
  "Livres (Open Library) : recherche par titre/auteur/sujet, ou fiche détaillée par ISBN.",
  { query: P.query("Titre, auteur ou sujet"), isbn: { type: "string", description: "ISBN-10 ou ISBN-13 (prioritaire sur query)" }, limit: P.limit(10, 5) },
  [], "openlibrary",
  async (a) => {
    if (a.isbn) {
      const isbn = String(a.isbn).replace(/[^0-9Xx]/g, "");
      if (![10, 13].includes(isbn.length)) fail("ISBN invalide (10 ou 13 chiffres).");
      const data = await http(`https://openlibrary.org/api/books?bibkeys=ISBN:${isbn}&format=json&jscmd=data`, { ttlMs: 24 * 3600e3 });
      const b = data[`ISBN:${isbn}`];
      if (!b) return { success: true, count: 0, books: [], note: "Aucun livre trouvé pour cet ISBN." };
      return { success: true, count: 1, books: [{ title: b.title, authors: (b.authors || []).map((x) => x.name), publishers: (b.publishers || []).map((x) => x.name), published: b.publish_date, pages: b.number_of_pages || null, subjects: (b.subjects || []).slice(0, 8).map((x) => x.name), cover: b.cover?.medium || null, url: b.url ? `https://openlibrary.org${b.url}` : null }] };
    }
    const q = needText(a.query);
    const fields = "key,title,author_name,first_publish_year,cover_i,subject,edition_count,isbn";
    const data = await http(`https://openlibrary.org/search.json?q=${encodeURIComponent(q)}&limit=${clampInt(a.limit, 5, 1, 10)}&fields=${fields}`, { ttlMs: 6 * 3600e3 });
    const books = (data.docs || []).map((d) => ({
      title: cut(d.title, 200), authors: (d.author_name || []).slice(0, 3), firstPublished: d.first_publish_year || null, editions: d.edition_count || null,
      subjects: (d.subject || []).slice(0, 5), isbn: (d.isbn || [])[0] || null,
      cover: d.cover_i ? `https://covers.openlibrary.org/b/id/${d.cover_i}-M.jpg` : null, url: d.key ? `https://openlibrary.org${d.key}` : null
    }));
    return { success: true, query: q, total: data.numFound ?? books.length, count: books.length, books };
  });

// ===== CARTES (OpenStreetMap) ====================================================
const NOMINATIM_GATE = { key: "nominatim", gapMs: 1100 };       // politique d'usage : 1 requête/s max
const OVERPASS_GATE = { key: "overpass", gapMs: 1000 };

async function nominatimSearch(q, limit = 3) {
  const data = await http(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=jsonv2&limit=${limit}&addressdetails=1&accept-language=fr`, { ttlMs: 24 * 3600e3, gate: NOMINATIM_GATE });
  return (data || []).map((p) => ({ name: cut(p.display_name, 220), lat: parseFloat(p.lat), lon: parseFloat(p.lon), category: p.category || null, type: p.type || null, country: p.address?.country || null, city: p.address?.city || p.address?.town || p.address?.village || null }));
}

defTool("osm_geocode",
  "Géocodage OpenStreetMap : adresse/lieu → coordonnées GPS, ou coordonnées → adresse.",
  { query: P.query("Adresse ou lieu, ex. « Gare centrale, Kinshasa »"), lat: { type: "number", description: "Latitude (géocodage inverse)" }, lon: { type: "number", description: "Longitude (géocodage inverse)" }, limit: P.limit(5, 3) },
  [], "openstreetmap",
  async (a) => {
    if (a.lat !== undefined && a.lon !== undefined) {
      const lat = clampNum(a.lat, NaN, -90, 90), lon = clampNum(a.lon, NaN, -180, 180);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) fail("Coordonnées invalides.");
      const p = await http(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=jsonv2&addressdetails=1&accept-language=fr`, { ttlMs: 24 * 3600e3, gate: NOMINATIM_GATE });
      return { success: true, count: p?.display_name ? 1 : 0, places: p?.display_name ? [{ name: cut(p.display_name, 220), lat, lon, country: p.address?.country || null, city: p.address?.city || p.address?.town || p.address?.village || null }] : [] };
    }
    const q = needText(a.query);
    const places = await nominatimSearch(q, clampInt(a.limit, 3, 1, 5));
    return { success: true, query: q, count: places.length, places };
  });

const OSM_CATEGORIES = {
  pharmacy: ["amenity", "pharmacy"], hospital: ["amenity", "hospital"], clinic: ["amenity", "clinic"], restaurant: ["amenity", "restaurant"],
  cafe: ["amenity", "cafe"], bank: ["amenity", "bank"], atm: ["amenity", "atm"], fuel: ["amenity", "fuel"], school: ["amenity", "school"],
  university: ["amenity", "university"], police: ["amenity", "police"], market: ["amenity", "marketplace"], supermarket: ["shop", "supermarket"],
  hotel: ["tourism", "hotel"], bus_station: ["amenity", "bus_station"], place_of_worship: ["amenity", "place_of_worship"]
};

defTool("osm_nearby_places",
  "Lieux proches (pharmacies, hôpitaux, banques, restaurants, stations-service…) autour d'un lieu ou de coordonnées GPS.",
  { category: { type: "string", enum: Object.keys(OSM_CATEGORIES), description: "Type de lieu" }, place: { type: "string", description: "Lieu de référence (ou fournir lat/lon)" }, lat: { type: "number" }, lon: { type: "number" }, radius_m: { type: "integer", description: "Rayon en mètres (100-5000, défaut 1500)" }, limit: P.limit(15, 8) },
  ["category"], "openstreetmap",
  async (a) => {
    const cat = OSM_CATEGORIES[a.category];
    if (!cat) fail(`Catégorie inconnue. Choisis parmi : ${Object.keys(OSM_CATEGORIES).join(", ")}.`);
    let lat = clampNum(a.lat, NaN, -90, 90), lon = clampNum(a.lon, NaN, -180, 180), origin = null;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const found = (await nominatimSearch(needText(a.place, "place"), 1))[0];
      if (!found) return { success: true, count: 0, places: [], note: "Lieu de référence introuvable." };
      lat = found.lat; lon = found.lon; origin = found.name;
    }
    const radius = clampInt(a.radius_m, 1500, 100, 5000), limit = clampInt(a.limit, 8, 1, 15);
    const ql = `[out:json][timeout:15];nwr(around:${radius},${lat},${lon})["${cat[0]}"="${cat[1]}"];out center ${limit * 3};`;
    const data = await http("https://overpass-api.de/api/interpreter", { method: "POST", body: `data=${encodeURIComponent(ql)}`, headers: { "Content-Type": "application/x-www-form-urlencoded" }, ttlMs: 30 * 60e3, timeoutMs: 18000, gate: OVERPASS_GATE });
    const places = (data.elements || []).map((e) => {
      const plat = e.lat ?? e.center?.lat, plon = e.lon ?? e.center?.lon;
      return { name: e.tags?.name || e.tags?.["name:fr"] || `(${a.category} sans nom)`, lat: plat, lon: plon, distanceM: Number.isFinite(plat) ? haversine(lat, lon, plat, plon) : null, phone: e.tags?.phone || e.tags?.["contact:phone"] || null, hours: e.tags?.opening_hours || null, address: [e.tags?.["addr:housenumber"], e.tags?.["addr:street"]].filter(Boolean).join(" ") || null };
    }).filter((p) => p.distanceM !== null).sort((x, y) => x.distanceM - y.distanceM).slice(0, limit);
    return { success: true, category: a.category, around: origin || `${lat.toFixed(4)},${lon.toFixed(4)}`, radiusM: radius, count: places.length, places };
  });

// ===== MÉTÉO (Open-Meteo) ========================================================
const WMO = { 0: "Ciel dégagé", 1: "Plutôt dégagé", 2: "Partiellement nuageux", 3: "Couvert", 45: "Brouillard", 48: "Brouillard givrant", 51: "Bruine légère", 53: "Bruine", 55: "Bruine dense", 56: "Bruine verglaçante", 57: "Bruine verglaçante dense", 61: "Pluie faible", 63: "Pluie modérée", 65: "Pluie forte", 66: "Pluie verglaçante", 67: "Pluie verglaçante forte", 71: "Neige faible", 73: "Neige modérée", 75: "Neige forte", 77: "Grains de neige", 80: "Averses faibles", 81: "Averses modérées", 82: "Averses violentes", 85: "Averses de neige", 86: "Fortes averses de neige", 95: "Orage", 96: "Orage avec grêle", 99: "Orage violent avec grêle" };

defTool("get_weather_open_meteo",
  "Météo actuelle et prévisions jusqu'à 7 jours (Open-Meteo) pour une ville ou des coordonnées.",
  { city: { type: "string", description: "Nom de la ville, ex. Lubumbashi" }, latitude: { type: "number" }, longitude: { type: "number" }, days: { type: "integer", description: "Jours de prévision (1-7, défaut 3)" } },
  [], "openmeteo",
  async (a) => {
    let lat = clampNum(a.latitude, NaN, -90, 90), lon = clampNum(a.longitude, NaN, -180, 180), where = null;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const city = needText(a.city, "city", 100);
      const g = await http(`https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(city)}&count=1&language=fr&format=json`, { ttlMs: 24 * 3600e3 });
      const r = g.results?.[0];
      if (!r) return { success: true, found: false, note: `Ville introuvable : ${city}.` };
      lat = r.latitude; lon = r.longitude; where = [r.name, r.admin1, r.country].filter(Boolean).join(", ");
    }
    const days = clampInt(a.days, 3, 1, 7);
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max&timezone=auto&forecast_days=${days}`;
    const d = await http(url, { ttlMs: 10 * 60e3 });
    const c = d.current || {}, dl = d.daily || {};
    return {
      success: true, found: true, location: where || `${lat},${lon}`, timezone: d.timezone,
      current: { time: c.time, temperatureC: c.temperature_2m, feelsLikeC: c.apparent_temperature, humidityPct: c.relative_humidity_2m, precipitationMm: c.precipitation, windKmh: c.wind_speed_10m, conditions: WMO[c.weather_code] || "Inconnu" },
      forecast: (dl.time || []).map((t, i) => ({ date: t, conditions: WMO[dl.weather_code?.[i]] || "Inconnu", minC: dl.temperature_2m_min?.[i], maxC: dl.temperature_2m_max?.[i], rainMm: dl.precipitation_sum?.[i], rainChancePct: dl.precipitation_probability_max?.[i] ?? null }))
    };
  });

// ===== DONNÉES (Banque mondiale) =================================================
const WB_INDICATORS = {
  population: "SP.POP.TOTL", gdp: "NY.GDP.MKTP.CD", gdp_per_capita: "NY.GDP.PCAP.CD", gdp_growth: "NY.GDP.MKTP.KD.ZG", inflation: "FP.CPI.TOTL.ZG",
  life_expectancy: "SP.DYN.LE00.IN", internet_users: "IT.NET.USER.ZS", unemployment: "SL.UEM.TOTL.ZS", electricity_access: "EG.ELC.ACCS.ZS",
  poverty: "SI.POV.DDAY", literacy: "SE.ADT.LITR.ZS", urban_population: "SP.URB.TOTL.IN.ZS"
};
const WB_COUNTRIES = {
  rdc: "COD", "republique democratique du congo": "COD", "rd congo": "COD", drc: "COD", congo: "COD", "congo-kinshasa": "COD",
  "congo-brazzaville": "COG", "republique du congo": "COG", france: "FRA", belgique: "BEL", rwanda: "RWA", burundi: "BDI", angola: "AGO", zambie: "ZMB",
  tanzanie: "TZA", ouganda: "UGA", kenya: "KEN", nigeria: "NGA", "afrique du sud": "ZAF", cameroun: "CMR", senegal: "SEN", "cote d'ivoire": "CIV",
  maroc: "MAR", algerie: "DZA", tunisie: "TUN", egypte: "EGY", ghana: "GHA", ethiopie: "ETH", chine: "CHN", inde: "IND", "etats-unis": "USA", usa: "USA",
  canada: "CAN", bresil: "BRA", monde: "WLD", world: "WLD", afrique: "SSF"
};

defTool("worldbank_indicator",
  "Statistiques officielles par pays (Banque mondiale) : population, PIB, inflation, espérance de vie, internet, chômage…",
  { country: { type: "string", description: "Pays (nom ou code ISO, ex. RDC, COD, France, monde)" }, indicator: { type: "string", description: `Alias (${Object.keys(WB_INDICATORS).join(", ")}) ou code Banque mondiale` }, years: { type: "integer", description: "Nombre de dernières valeurs (1-20, défaut 5)" } },
  ["country", "indicator"], "worldbank",
  async (a) => {
    const raw = needText(a.country, "country", 80);
    const code = WB_COUNTRIES[unaccent(raw)] || (/^[A-Za-z]{2,3}$/.test(raw) ? raw.toUpperCase() : null);
    if (!code) fail(`Pays non reconnu : « ${raw} ». Utilise un code ISO (ex. COD, FRA) ou un nom courant.`);
    const ind = WB_INDICATORS[unaccent(a.indicator).replace(/[\s-]+/g, "_")] || (/^[A-Z0-9]{2,4}(\.[A-Z0-9]+)+$/i.test(a.indicator || "") ? String(a.indicator).toUpperCase() : null);
    if (!ind) fail(`Indicateur inconnu. Alias disponibles : ${Object.keys(WB_INDICATORS).join(", ")}.`);
    const n = clampInt(a.years, 5, 1, 20);
    const data = await http(`https://api.worldbank.org/v2/country/${code}/indicator/${ind}?format=json&mrv=${n}`, { ttlMs: 12 * 3600e3 });
    const rows = Array.isArray(data) ? data[1] : null;
    if (!rows?.length) return { success: true, count: 0, values: [], note: "Aucune donnée disponible pour ce pays et cet indicateur." };
    return {
      success: true, country: rows[0].country?.value, countryCode: code, indicator: rows[0].indicator?.value, indicatorCode: ind, count: rows.length,
      values: rows.map((r) => ({ year: r.date, value: r.value })).sort((x, y) => x.year - y.year),
      latest: rows.find((r) => r.value !== null) ? { year: rows.find((r) => r.value !== null).date, value: rows.find((r) => r.value !== null).value } : null
    };
  });

// ===== VIDÉO (youtubei.js, paquet optionnel) =====================================
let ytModuleOverride = null, ytClient = null;
async function getInnertube() {
  if (ytClient) return ytClient;
  let mod = ytModuleOverride;
  if (!mod) { try { mod = await import("youtubei.js"); } catch { fail("Recherche YouTube indisponible : le paquet « youtubei.js » n'est pas installé."); } }
  const Innertube = mod.Innertube || mod.default?.Innertube || mod.default;
  if (!Innertube?.create) fail("Version de youtubei.js incompatible.");
  ytClient = await Innertube.create({ generate_session_locally: true });
  return ytClient;
}
const txt = (x) => (x && typeof x === "object" ? x.text ?? x.simpleText ?? "" : x || "");

defTool("search_youtube_innertube",
  "Recherche de vidéos YouTube (sans clé API) : titre, chaîne, durée, vues, miniature.",
  { query: P.query(), limit: P.limit(10, 5) },
  ["query"], "youtube",
  async (a) => {
    const q = needText(a.query);
    const key = `yt|${q}`;
    const hit = cacheGet(key);
    if (hit) return hit;
    let res;
    try { res = await (await getInnertube()).search(q, { type: "video" }); }
    catch (e) { if (!(e instanceof UserError)) ytClient = null; throw e; }       // client périmé → recréé au prochain appel
    const videos = (res.results || []).filter((r) => (r.type === "Video" || r.video_id || r.id) && r.title).slice(0, clampInt(a.limit, 5, 1, 10)).map((v) => {
      const videoId = v.video_id || v.id;
      return { videoId, url: `https://www.youtube.com/watch?v=${videoId}`, title: cut(txt(v.title), 200), channel: txt(v.author?.name ?? v.author) || null, duration: txt(v.duration) || null, views: txt(v.short_view_count ?? v.view_count) || null, published: txt(v.published) || null, thumbnail: httpsify(v.thumbnails?.[0]?.url || null) };
    });
    const out = { success: true, query: q, count: videos.length, videos };
    cacheSet(key, out, 30 * 60e3);
    return out;
  });

// ===== MUSIQUE ===================================================================
defTool("search_deezer",
  "Musique sur Deezer : morceaux (avec extrait audio de 30 s), artistes ou albums.",
  { query: P.query("Titre, artiste ou album"), type: { type: "string", enum: ["track", "artist", "album"], description: "Type (défaut track)" }, limit: P.limit(10, 5) },
  ["query"], "deezer",
  async (a) => {
    const q = needText(a.query), type = oneOf(a.type, ["track", "artist", "album"], "track");
    const path = type === "track" ? "search" : `search/${type}`;
    const data = await http(`https://api.deezer.com/${path}?q=${encodeURIComponent(q)}&limit=${clampInt(a.limit, 5, 1, 10)}`, { ttlMs: 30 * 60e3 });
    const results = (data.data || []).map((d) => {
      if (type === "artist") return { artist: d.name, fans: d.nb_fan ?? null, picture: d.picture_medium || null, url: d.link };
      if (type === "album") return { album: d.title, artist: d.artist?.name, tracks: d.nb_tracks ?? null, cover: d.cover_medium || null, url: d.link };
      return { title: cut(d.title, 150), artist: d.artist?.name, album: d.album?.title, durationSec: d.duration, previewUrl: d.preview || null, cover: d.album?.cover_medium || null, url: d.link };
    });
    return { success: true, query: q, type, count: results.length, results };
  });

defTool("search_itunes",
  "Apple Music / iTunes : morceaux, albums, artistes, podcasts, films — pochette HD et extrait de 30 s.",
  { query: P.query(), entity: { type: "string", enum: ["song", "album", "musicArtist", "podcast", "movie"], description: "Type (défaut song)" }, country: { type: "string", description: "Boutique ISO 2 lettres (défaut FR)" }, limit: P.limit(10, 5) },
  ["query"], "itunes",
  async (a) => {
    const q = needText(a.query);
    const entity = oneOf(a.entity, ["song", "album", "musicArtist", "podcast", "movie"], "song");
    const media = { song: "music", album: "music", musicArtist: "music", podcast: "podcast", movie: "movie" }[entity];
    const cc = /^[a-z]{2}$/i.test(a.country || "") ? a.country.toUpperCase() : "FR";
    const data = await http(`https://itunes.apple.com/search?term=${encodeURIComponent(q)}&media=${media}&entity=${entity}&country=${cc}&limit=${clampInt(a.limit, 5, 1, 10)}`, { ttlMs: 30 * 60e3 });
    const results = (data.results || []).map((r) => ({
      title: cut(r.trackName || r.collectionName || r.artistName, 150), artist: r.artistName, album: r.collectionName || null, genre: r.primaryGenreName || null,
      released: r.releaseDate ? r.releaseDate.slice(0, 10) : null, durationSec: r.trackTimeMillis ? Math.round(r.trackTimeMillis / 1000) : null,
      previewUrl: r.previewUrl || null, cover: (r.artworkUrl100 || "").replace("100x100bb", "600x600bb") || null, url: r.trackViewUrl || r.collectionViewUrl || r.artistViewUrl || null
    }));
    return { success: true, query: q, entity, count: results.length, results };
  });

// ===== IMAGES (Library of Congress) ==============================================
defTool("search_loc_gov",
  "Archives de la Library of Congress (domaine public) : photos historiques, cartes, journaux, livres, manuscrits.",
  { query: P.query("Sujet, personne, lieu (anglais conseillé)"), format: { type: "string", enum: ["photos", "maps", "newspapers", "books", "manuscripts", "audio", "film-and-videos", "all"], description: "Type de collection (défaut photos)" }, limit: P.limit(10, 5) },
  ["query"], "locgov",
  async (a) => {
    const q = needText(a.query), format = oneOf(a.format, ["photos", "maps", "newspapers", "books", "manuscripts", "audio", "film-and-videos", "all"], "photos");
    const base = format === "all" ? "https://www.loc.gov/search/" : `https://www.loc.gov/${format}/`;
    const data = await http(`${base}?q=${encodeURIComponent(q)}&fo=json&c=${clampInt(a.limit, 5, 1, 10)}`, { ttlMs: 6 * 3600e3, timeoutMs: 12000, gate: { key: "locgov", gapMs: 1500 } });
    const items = (data.results || []).map((r) => {
      const imgs = [].concat(r.image_url || []).map(httpsify).filter(Boolean);
      return { title: cut(Array.isArray(r.title) ? r.title[0] : r.title, 200), date: r.date || null, format: [].concat(r.original_format || r.format || [])[0] || null, description: cut([].concat(r.description || [])[0] || "", 300) || null, url: httpsify(r.url || r.id), imageUrl: imgs[imgs.length > 1 ? 1 : 0] || null };
    });
    // `images` : repris automatiquement par Luba pour afficher les photos dans la réponse
    const images = items.filter((i) => i.imageUrl).slice(0, 4).map((i) => ({ url: i.imageUrl, title: i.title, source: "Library of Congress", pageUrl: i.url }));
    return { success: true, query: q, format, total: data.pagination?.total ?? items.length, count: items.length, items, images };
  });

// ---------------------------------------------------------------------------------
// §6 — API publique du module
// ---------------------------------------------------------------------------------
const enabledSet = () => {
  const env = (process.env.FREE_SOURCES_TOOLS || "").split(",").map((s) => s.trim()).filter(Boolean);
  return new Set(env.length ? env.filter((n) => TOOLS[n]) : Object.keys(TOOLS));
};

/** Schémas des outils actifs (format OpenAI / function calling). */
function schemas() { const on = enabledSet(); return Object.keys(TOOLS).filter((n) => on.has(n)).map((n) => TOOLS[n].schema); }
/** Cet outil est-il fourni (et activé) par ce module ? */
function has(name) { return Boolean(TOOLS[name]) && enabledSet().has(name); }

/** Exécute un outil. Ne lève jamais : renvoie { result:{success:false,error}, sourceKeys:[] } en cas de problème. */
async function execute(name, args = {}, _ctx = {}) {
  const tool = TOOLS[name];
  if (!tool || !enabledSet().has(name)) return { result: { success: false, error: "Outil indisponible." }, sourceKeys: [] };
  try {
    const result = await tool.exec(args && typeof args === "object" ? args : {});
    return { result, sourceKeys: result?.success && result.count !== 0 ? [tool.source] : [] };
  } catch (e) {
    return { result: { success: false, error: friendlyError(e), source: SOURCES[tool.source]?.[0] }, sourceKeys: [] };
  }
}

module.exports = {
  VERSION: "1.0.0", SOURCES, TOOLS, schemas, has, execute,
  names: () => Object.keys(TOOLS),
  // — réservé aux tests —
  __setFetch: (fn) => { doFetch = fn || ((...a) => globalThis.fetch(...a)); },
  __setYoutubei: (m) => { ytModuleOverride = m; ytClient = null; },
  __clearCache: () => cache.clear(),
  __internals: { decodeXml, xmlBlocks, xmlText, abstractFromIndex, haversine, UA }
};
