// Test hors ligne de free-sources.js : faux fetch avec réponses typiques de chaque API.
"use strict";
process.env.CONTACT_EMAIL = "test@hiklon.io";
const F = require("./free-sources.js");
let pass = 0, fail = 0; const calls = [];
const eq = (a, b, m = "") => { if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${m} attendu ${JSON.stringify(b)} reçu ${JSON.stringify(a)}`); };
const ok = (c, m) => { if (!c) throw new Error(m || "faux"); };
async function test(n, f) { try { F.__clearCache(); await f(); pass++; console.log(`  ✅ ${n}`); } catch (e) { fail++; console.log(`  ❌ ${n}\n       ${String(e.stack || e).split("\n").slice(0, 3).join("\n       ")}`); } }
const resp = (body, { status = 200, text = false } = {}) => ({ ok: status < 400, status, headers: { get: () => null }, text: async () => (text ? body : JSON.stringify(body)) });
let routes = [];
F.__setFetch(async (url, opts) => { calls.push({ url: String(url), opts }); for (const [m, r] of routes) if (String(url).includes(m)) return typeof r === "function" ? r(url, opts) : r; return resp({}, { status: 404 }); });
const run = async (name, args, rs) => { routes = rs; calls.length = 0; return F.execute(name, args); };

(async () => {
  await test("14 outils exposés, schémas OpenAI valides, noms uniques", () => {
    const s = F.schemas(); eq(s.length, 14);
    ok(s.every((x) => x.type === "function" && x.function.name && x.function.parameters.type === "object" && Array.isArray(x.function.parameters.required)));
    eq(new Set(s.map((x) => x.function.name)).size, 14);
    ok(JSON.stringify(s).length < 16000, `schémas trop lourds: ${JSON.stringify(s).length} octets`);
  });
  await test("FREE_SOURCES_TOOLS limite les outils exposés (prompt plus léger)", async () => {
    process.env.FREE_SOURCES_TOOLS = "search_arxiv,search_deezer,inconnu"; eq(F.schemas().map((x) => x.function.name), ["search_arxiv", "search_deezer"]);
    ok(!F.has("search_loc_gov")); const r = await F.execute("search_loc_gov", { query: "x" }); eq(r.result.success, false); delete process.env.FREE_SOURCES_TOOLS;
  });

  await test("Google News RSS: URL (langue/pays/fraîcheur) + parsing, suffixe « - Source » retiré", async () => {
    const xml = `<?xml version="1.0"?><rss><channel><item><title>Kinshasa : crue du fleuve &amp; alerte - Radio Okapi</title><link>https://news.google.com/rss/articles/CBMi123</link><pubDate>Wed, 08 Oct 2026 10:00:00 GMT</pubDate><source url="https://www.radiookapi.net">Radio Okapi</source></item><item><title><![CDATA[Autre titre - RFI]]></title><link>https://news.google.com/x2</link><pubDate>Tue, 07 Oct 2026 08:00:00 GMT</pubDate><source url="https://rfi.fr">RFI</source></item></channel></rss>`;
    const r = await run("search_google_news", { query: "Kinshasa crue", recency: "7d", limit: 5 }, [["news.google.com/rss/search", resp(xml, { text: true })]]);
    const u = decodeURIComponent(calls[0].url); ok(u.includes("Kinshasa crue when:7d") && u.includes("hl=fr&gl=CD&ceid=CD:fr"), u);
    eq(r.result.articles[0], { title: "Kinshasa : crue du fleuve & alerte", source: "Radio Okapi", sourceUrl: "https://www.radiookapi.net", url: "https://news.google.com/rss/articles/CBMi123", publishedAt: "2026-10-08T10:00:00.000Z" });
    eq(r.result.articles[1].title, "Autre titre"); eq(r.sourceKeys, ["googlenews"]);
  });
  await test("Hacker News: recherche (Algolia) et top (Firebase)", async () => {
    let r = await run("search_hacker_news", { query: "rust", mode: "latest", limit: 2 }, [["hn.algolia.com/api/v1/search_by_date", resp({ hits: [{ objectID: "1", title: "Rust 2.0", url: "https://r.io", points: 120, num_comments: 45, author: "bob", created_at: "2026-10-01T00:00:00Z" }, { objectID: "2", title: "Ask HN", url: null, points: 3, num_comments: 1, author: "al", created_at: "2026-10-02T00:00:00Z" }] })]]);
    eq(r.result.stories[1].url, "https://news.ycombinator.com/item?id=2"); ok(calls[0].url.includes("hitsPerPage=2") && calls[0].url.includes("tags=story"));
    r = await run("search_hacker_news", { mode: "top", limit: 2 }, [["topstories.json", resp([11, 12, 13])], ["item/11.json", resp({ id: 11, title: "A", score: 10, by: "x", descendants: 2, time: 1790000000, url: "https://a.io" })], ["item/12.json", resp({ id: 12, title: "B", score: 5, by: "y", time: 1790000100 })]]);
    eq(r.result.count, 2); eq(r.result.stories[1].comments, 0);
  });
  await test("OpenAlex: filtre année, tri, mailto, résumé reconstitué depuis l'index inversé", async () => {
    const r = await run("search_openalex", { query: "malaria vaccine", year_from: 2020, sort: "cited_by_count", limit: 3 }, [["api.openalex.org/works", resp({ meta: { count: 999 }, results: [{ title: "R21 vaccine", publication_year: 2023, cited_by_count: 88, doi: "https://doi.org/10.1/x", authorships: [{ author: { display_name: "A. One" } }, { author: { display_name: "B. Two" } }], open_access: { oa_url: "https://oa.io/p.pdf" }, primary_location: { source: { display_name: "The Lancet" } }, abstract_inverted_index: { Malaria: [0], is: [1], preventable: [2] } }] })]]);
    const u = calls[0].url; ok(u.includes("filter=from_publication_date%3A2020-01-01") && u.includes("sort=cited_by_count%3Adesc") && u.includes("mailto=test%40hiklon.io") && u.includes("per-page=3"), u);
    eq(r.result.works[0].abstract, "Malaria is preventable"); eq(r.result.total, 999); eq(r.result.works[0].authors, ["A. One", "B. Two"]);
  });
  await test("Crossref: recherche + résolution DOI (URL de préfixe nettoyée, DOI invalide refusé)", async () => {
    let r = await run("search_crossref", { doi: "https://doi.org/10.1038/nature14539" }, [["api.crossref.org/works/10.1038%2Fnature14539", resp({ message: { DOI: "10.1038/nature14539", title: ["Deep learning"], author: [{ given: "Yann", family: "LeCun" }], issued: { "date-parts": [[2015, 5, 28]] }, "container-title": ["Nature"], URL: "http://dx.doi.org/10.1038/nature14539", "is-referenced-by-count": 70000 } })]]);
    eq([r.result.works[0].title, r.result.works[0].year, r.result.works[0].authors[0]], ["Deep learning", 2015, "Yann LeCun"]);
    r = await run("search_crossref", { doi: "pas-un-doi" }, []); eq(r.result.success, false); ok(/DOI invalide/.test(r.result.error));
    r = await run("search_crossref", { query: "graphene", limit: 2 }, [["api.crossref.org/works?query=graphene", resp({ message: { "total-results": 5, items: [{ DOI: "10.1/a", title: ["G"], issued: { "date-parts": [[2019]] } }] } })]]); eq(r.result.count, 1);
  });
  await test("arXiv: requête AND + catégorie, parsing Atom (auteurs, PDF https, catégorie)", async () => {
    const xml = `<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>http://arxiv.org/abs/2401.00001v1</id><published>2024-01-01T00:00:00Z</published><title>Attention
  is all   you need</title><summary> We propose a new architecture. </summary><author><name>A. Vaswani</name></author><author><name>N. Shazeer</name></author><link href="http://arxiv.org/abs/2401.00001v1" rel="alternate" type="text/html"/><link title="pdf" href="http://arxiv.org/pdf/2401.00001v1" rel="related" type="application/pdf"/><arxiv:primary_category xmlns:arxiv="http://arxiv.org/schemas/atom" term="cs.CL" scheme="x"/></entry></feed>`;
    const r = await run("search_arxiv", { query: "attention transformer", category: "cs.CL", sort: "date" }, [["export.arxiv.org/api/query", resp(xml, { text: true })]]);
    const u = calls[0].url; ok(u.includes("all:attention+AND+all:transformer+AND+cat:cs.CL") && u.includes("sortBy=submittedDate"), u);
    eq(r.result.papers[0], { title: "Attention is all you need", summary: "We propose a new architecture.", authors: ["A. Vaswani", "N. Shazeer"], published: "2024-01-01T00:00:00.000Z", category: "cs.CL", url: "https://arxiv.org/abs/2401.00001v1", pdf: "https://arxiv.org/pdf/2401.00001v1" });
  });
  await test("Open Library: recherche (couverture) et ISBN (api/books)", async () => {
    let r = await run("search_openlibrary", { query: "Lumumba" }, [["openlibrary.org/search.json", resp({ numFound: 12, docs: [{ key: "/works/OL1W", title: "Lumumba", author_name: ["Ludo De Witte"], first_publish_year: 1999, cover_i: 777, isbn: ["123"], subject: ["Congo"] }] })]]);
    eq(r.result.books[0].cover, "https://covers.openlibrary.org/b/id/777-M.jpg"); ok(calls[0].url.includes("fields=key,title"));
    r = await run("search_openlibrary", { isbn: "978-2-07-036822-8" }, [["api/books?bibkeys=ISBN:9782070368228", resp({ "ISBN:9782070368228": { title: "Le Petit Prince", authors: [{ name: "Saint-Exupéry" }], publishers: [{ name: "Gallimard" }], number_of_pages: 96, url: "/books/OL1M/x", subjects: [{ name: "Fables" }] } })]]);
    eq([r.result.books[0].title, r.result.books[0].pages], ["Le Petit Prince", 96]); r = await run("search_openlibrary", { isbn: "12" }, []); eq(r.result.success, false);
  });
  await test("OSM: géocodage direct/inverse, User-Agent identifié, limite 1 req/s respectée", async () => {
    let r = await run("osm_geocode", { query: "Gare centrale Kinshasa" }, [["nominatim.openstreetmap.org/search", resp([{ display_name: "Gare Centrale, Gombe, Kinshasa, RDC", lat: "-4.3", lon: "15.3", category: "railway", type: "station", address: { country: "RDC", city: "Kinshasa" } }])]]);
    eq([r.result.places[0].lat, r.result.places[0].city], [-4.3, "Kinshasa"]); ok(/LubaAI\/17/.test(calls[0].opts.headers["User-Agent"]) && calls[0].url.includes("accept-language=fr"));
    const t0 = Date.now(); await run("osm_geocode", { lat: -4.3, lon: 15.3 }, [["nominatim.openstreetmap.org/reverse", resp({ display_name: "Quelque part", address: { country: "RDC", town: "Gombe" } })]]);
    ok(Date.now() - t0 >= 900, `pas de throttle (${Date.now() - t0} ms)`); eq((await run("osm_geocode", { lat: 999, lon: 0 }, [])).result.success, false);
  });
  await test("OSM nearby: géocode le lieu puis Overpass (POST), tri par distance", async () => {
    const r = await run("osm_nearby_places", { category: "pharmacy", place: "Gombe Kinshasa", radius_m: 800, limit: 2 }, [
      ["nominatim.openstreetmap.org/search", resp([{ display_name: "Gombe", lat: "-4.30", lon: "15.30" }])],
      ["overpass-api.de/api/interpreter", resp({ elements: [{ type: "node", lat: -4.305, lon: 15.30, tags: { name: "Pharmacie Loin", opening_hours: "24/7" } }, { type: "way", center: { lat: -4.3005, lon: 15.30 }, tags: { name: "Pharmacie Proche", phone: "+243" } }, { type: "node", lat: -4.31, lon: 15.31, tags: {} }] })]]);
    eq(r.result.places.map((p) => p.name), ["Pharmacie Proche", "Pharmacie Loin"]); ok(r.result.places[0].distanceM < r.result.places[1].distanceM);
    const post = calls.find((c) => c.url.includes("overpass")); eq(post.opts.method, "POST"); ok(decodeURIComponent(post.opts.body).includes('nwr(around:800,-4.3,15.3)["amenity"="pharmacy"]'));
    eq((await run("osm_nearby_places", { category: "licorne", place: "x" }, [])).result.success, false);
  });
  await test("Open-Meteo: géocodage ville → prévisions, codes WMO en français", async () => {
    const r = await run("get_weather_open_meteo", { city: "Lubumbashi", days: 2 }, [
      ["geocoding-api.open-meteo.com", resp({ results: [{ name: "Lubumbashi", admin1: "Haut-Katanga", country: "RD Congo", latitude: -11.66, longitude: 27.48 }] })],
      ["api.open-meteo.com/v1/forecast", resp({ timezone: "Africa/Lubumbashi", current: { time: "2026-10-09T10:00", temperature_2m: 28.4, apparent_temperature: 29, relative_humidity_2m: 40, precipitation: 0, weather_code: 2, wind_speed_10m: 11 }, daily: { time: ["2026-10-09", "2026-10-10"], weather_code: [2, 95], temperature_2m_max: [31, 29], temperature_2m_min: [17, 18], precipitation_sum: [0, 12], precipitation_probability_max: [5, 80] } })]]);
    eq(r.result.location, "Lubumbashi, Haut-Katanga, RD Congo"); eq(r.result.current.conditions, "Partiellement nuageux"); eq(r.result.forecast[1].conditions, "Orage"); ok(calls[1].url.includes("forecast_days=2"));
    eq((await run("get_weather_open_meteo", { city: "Zzzz" }, [["geocoding-api", resp({})]])).result.found, false);
  });
  await test("Banque mondiale: alias pays/indicateur (accents), mrv, valeurs triées", async () => {
    const r = await run("worldbank_indicator", { country: "RDC", indicator: "Espérance de vie".replace("Espérance de vie", "life expectancy"), years: 3 }, [["api.worldbank.org/v2/country/COD/indicator/SP.DYN.LE00.IN", resp([{ page: 1 }, [{ indicator: { value: "Life expectancy" }, country: { value: "Congo, Dem. Rep." }, date: "2023", value: 62.1 }, { indicator: { value: "Life expectancy" }, country: { value: "Congo, Dem. Rep." }, date: "2022", value: 61.5 }, { indicator: {}, country: {}, date: "2024", value: null }]])]]);
    ok(calls[0].url.includes("mrv=3")); eq(r.result.values.map((v) => v.year), ["2022", "2023", "2024"]); eq(r.result.latest, { year: "2023", value: 62.1 });
    eq((await run("worldbank_indicator", { country: "Atlantide", indicator: "population" }, [])).result.success, false);
    eq((await run("worldbank_indicator", { country: "France", indicator: "bidule" }, [])).result.success, false);
  });
  await test("YouTube (youtubei.js simulé): vidéos normalisées au format { videoId, … } ; client recréé après erreur", async () => {
    let created = 0, boom = false;
    F.__setYoutubei({ Innertube: { create: async () => { created++; return { search: async () => { if (boom) throw new Error("session expirée"); return { results: [{ type: "Video", id: "abc123", title: { text: "Rumba congolaise" }, author: { name: "Chaîne X" }, duration: { text: "4:12" }, short_view_count: { text: "1,2 M vues" }, published: { text: "il y a 1 an" }, thumbnails: [{ url: "//i.ytimg.com/vi/abc123/hq.jpg" }] }, { type: "Channel", id: "c1", title: { text: "x" } }] }; } }; } } });
    let r = await run("search_youtube_innertube", { query: "rumba" }, []); eq(r.result.videos[0], { videoId: "abc123", url: "https://www.youtube.com/watch?v=abc123", title: "Rumba congolaise", channel: "Chaîne X", duration: "4:12", views: "1,2 M vues", published: "il y a 1 an", thumbnail: "https://i.ytimg.com/vi/abc123/hq.jpg" });
    await run("search_youtube_innertube", { query: "rumba" }, []); eq(created, 1, "cache"); boom = true; r = await run("search_youtube_innertube", { query: "autre" }, []); eq(r.result.success, false); boom = false; await run("search_youtube_innertube", { query: "troisième" }, []); eq(created, 2);
  });
  await test("Deezer: morceau (extrait 30 s), artiste, album", async () => {
    let r = await run("search_deezer", { query: "Fally Ipupa" }, [["api.deezer.com/search?q=", resp({ data: [{ title: "Eloko Oyo", duration: 240, preview: "https://cdn.dz/p.mp3", link: "https://deezer.com/t/1", artist: { name: "Fally Ipupa" }, album: { title: "Tokooos", cover_medium: "https://c/1.jpg" } }] })]]);
    eq([r.result.results[0].previewUrl, r.result.results[0].album], ["https://cdn.dz/p.mp3", "Tokooos"]);
    r = await run("search_deezer", { query: "Koffi", type: "artist" }, [["api.deezer.com/search/artist", resp({ data: [{ name: "Koffi Olomide", nb_fan: 500000, picture_medium: "https://p", link: "https://deezer.com/a/2" }] })]]); eq(r.result.results[0].fans, 500000);
  });
  await test("iTunes: pochette 600x600, extrait, boutique pays, entité → media", async () => {
    const r = await run("search_itunes", { query: "Werrason", entity: "podcast", country: "be" }, [["itunes.apple.com/search", resp({ results: [{ trackName: "Ep 1", artistName: "Werra", collectionName: "Show", artworkUrl100: "https://is/100x100bb.jpg", previewUrl: "https://a/p.m4a", trackViewUrl: "https://apple/1", releaseDate: "2025-05-01T07:00:00Z", trackTimeMillis: 61000, primaryGenreName: "Music" }] })]]);
    ok(calls[0].url.includes("media=podcast&entity=podcast&country=BE")); eq([r.result.results[0].cover, r.result.results[0].durationSec, r.result.results[0].released], ["https://is/600x600bb.jpg", 61, "2025-05-01"]);
  });
  await test("loc.gov: collection photos, URLs d'images https, `images` prêt pour l'affichage automatique", async () => {
    const r = await run("search_loc_gov", { query: "Congo river", format: "photos", limit: 3 }, [["loc.gov/photos/", resp({ pagination: { total: 321 }, results: [{ title: "Congo river steamer", date: "1910", url: "http://www.loc.gov/item/2001/", image_url: ["//tile.loc.gov/storage/thumb.gif", "//tile.loc.gov/storage/big.jpg"], description: ["Un vapeur"], original_format: ["photo, print"] }, { title: "Sans image", url: "https://www.loc.gov/item/2/" }] })]]);
    ok(calls[0].url.includes("fo=json&c=3")); eq(r.result.items[0].imageUrl, "https://tile.loc.gov/storage/big.jpg"); eq(r.result.images.length, 1); eq(r.result.images[0].url, "https://tile.loc.gov/storage/big.jpg"); eq(r.result.total, 321);
  });

  await test("Robustesse: 429 puis succès (retry), 500 persistant → message clair, jamais d'exception", async () => {
    let n = 0; let r = await run("search_deezer", { query: "x" }, [["api.deezer.com", () => (++n === 1 ? resp({}, { status: 429 }) : resp({ data: [] }))]]); eq(r.result.success, true); eq(n, 2);
    r = await run("search_arxiv", { query: "x" }, [["arxiv.org", resp("", { status: 503, text: true })]]); eq(r.result.success, false); ok(/indisponible/.test(r.result.error)); eq(r.sourceKeys, []);
    r = await run("search_deezer", { query: "   " }, []); ok(/manquant/.test(r.result.error));
    r = await run("search_deezer", { query: "requête-timeout" }, [["api.deezer.com", () => { throw Object.assign(new Error("t"), { name: "TimeoutError" }); }]]); ok(/pas répondu/.test(r.result.error));
  });
  await test("Cache: 2e appel identique = aucun appel réseau ; sourceKeys vide si 0 résultat", async () => {
    routes = [["api.deezer.com", resp({ data: [{ title: "T", artist: {}, album: {} }] })]]; calls.length = 0; await F.execute("search_deezer", { query: "cache" }); await F.execute("search_deezer", { query: "cache" }); eq(calls.length, 1);
    const r = await run("search_deezer", { query: "rien" }, [["api.deezer.com", resp({ data: [] })]]); eq(r.sourceKeys, []);
  });

  console.log(`\n${pass} réussis · ${fail} échoués`); process.exit(fail ? 1 : 0);
})();
