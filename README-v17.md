# Luba v17 « Pro Max » — audit, correctifs, migration

## Failles trouvées dans la v16.5 (et corrigées)

| # | Faille | Effet | Correctif v17 |
|---|---|---|---|
| 1 | Appels LLM non streamés, timeout fixe 20-40 s | Question lourde = timeout = « panne » | Streaming réel, timeouts adaptatifs (1er token / inactivité / total) par lane |
| 2 | Santé : 1 échec **par clé** + seuil 3 | 1 requête lente = provider coupé 2 min pour TOUS | 1 échec = 1 requête, fenêtre glissante, 429/erreurs client quasi non comptés, half-open, jamais exclu définitivement |
| 3 | Aucune file ni limite de concurrence | N requêtes lourdes = 429 en cascade | RunManager : file, lanes light/heavy, limite par utilisateur, 503 + Retry-After propre |
| 4 | `CHAT_GLOBAL_MS` jamais appliqué, pas de heartbeat SSE, pas de nginx.conf | Proxy coupe à 60 s, réponse perdue | Deadline réelle, heartbeat 12 s, `nginx.conf` fourni |
| 5 | « Streaming » simulé (réponse complète puis `sleep` par token) | Latence perçue énorme | Tokens relayés en direct du provider |
| 6 | Historique lu sur Firestore, écritures fire-and-forget, erreurs avalées | Messages manquants / désordonnés / perdus | SQLite = vérité, transactions, `idx` par conversation, miroirs via outbox durable |
| 7 | `fastUpsertUser` en `setImmediate` → FK `users` | **1er message d'un nouvel utilisateur perdu** | Profil créé avant la requête |
| 8 | 3 écritures SQLite par requête authentifiée + N+1 dans `/conversations` | Writer saturé par le polling de sync | 0 écriture par requête, 1 requête SQL pour la liste |
| 9 | Quota : lecture puis incrément (non atomique), débité avant succès | Dépassement + quota perdu en cas d'échec | Réservation atomique + remboursement |
| 10 | Limiteurs par IP, 200 req/15 min sur tout | NAT mobile = utilisateurs bloqués, sync 429 | Token bucket par utilisateur |
| 11 | Token mis en cache 5 min sans regarder `exp` ; panne Firebase = 401 | Token expiré accepté ; utilisateurs déconnectés | Cache borné par `exp`, single-flight, panne infra = 503 |
| 12 | Outils séquentiels ; appels au-delà de la limite sans réponse | Lenteur + erreur 400 provider | Outils en parallèle, tous les `tool_call` répondus |
| 13 | `SOURCE_LABELS` hors portée → ReferenceError | Crash après génération quand des sources existent | Réécrit |
| 14 | Pas de reprise si le client se déconnecte | Réponse perdue | Run indépendant de la connexion, `Last-Event-ID`, sauvegarde partielle |
| 15 | Étiquettes de métriques = URL | Fuite mémoire (cardinalité) | Étiquette = route déclarée |
| 16 | `server.close()` sans délai, SSE ouvertes | Arrêt bloqué / réponses coupées | Drain des runs, `/ready` → 503, partiel sauvegardé |
| 17 | Modération LLM bloquante avant le stream | +0,3-5 s avant le 1er octet | Parallèle, bornée 2,5 s, fail-open |
| 18 | `conversation_locks` jamais utilisée | Messages concurrents entrelacés | Une génération à la fois par conversation |

## Déploiement
1. Renomme l'ancien fichier en `legacy.js` (optionnel : garde tes 19 outils météo/crypto/news/images…).
2. Copie `index.js` v17, `nginx.conf`, `.env.example`.
3. `node index.js` — les migrations s'appliquent seules et conservent tes données (`idx`, titres et compteurs rattrapés).
4. Vérifie : `GET /api/health?full=1&token=$DEBUG_TOKEN`.
Tests : `node selftest.js` · `node selftest-http.js` · `node stress.js 300 2` (aucun réseau requis).

## Contrat client (rétro-compatible)
- `POST /api/chat` accepte `clientMessageId` (idempotence) ; en SSE : `accepted`, `status`, `reasoning`, `token`, `images`, `videos`, `sources`, `suggestions`, `done` (`done.reply` = texte final nettoyé). Les anciens clients qui accumulent `token` continuent de fonctionner.
- Reprise : `GET /api/runs/:id/stream` avec `Last-Event-ID`. Annulation : `POST /api/runs/:id/cancel`.
- Sync : `GET /api/sync?since=<curseur>` (snapshot si 0), push `GET /api/sync/stream` (SSE) ou `/ws`.
- Messages : `GET /api/conversations/:id/messages?after=<idx>|before=<idx>&limit=`.

## Limites à connaître
- Non testé contre les vrais providers, `sqlite3` natif et Express réel (testé avec faux LLM, `node:sqlite`, mini-Express, vraie lib `ws`).
- Non portés : voix, WhatsApp, calques qualité/auto-critique, cache sémantique. `LEGACY_MOUNT=true` remonte les anciennes routes (expérimental).
- Reprise d'un run : instance qui l'exécute (sticky session si plusieurs instances) ; la sync, elle, est multi-instances via Redis.
