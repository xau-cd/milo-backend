# Rapport de validation technique — Luba Engine v1.0.0
*Rapport interne (pas une certification externe). Exécuté le 15/10/2026 — Node v22.22.2, Python 3.12.3, g++/gcc présents, `unshare` et `prlimit` présents, `bwrap` absent.*

## Environnement de test — limites à connaître
* Le `node_modules` du ZIP est **incomplet** : `multer, pino, helmet, ws, compression, express-rate-limit, prom-client, dotenv, cheerio, @supabase/supabase-js, @google/genai, duck-duck-scrape, youtubei.js` sont absents et le réseau est coupé. `index.js` a donc démarré avec ses chemins de repli (`tryRequire`). **Les tests ne prouvent pas le comportement avec ces paquets installés** (en production `npm install` les fournit).
* Aucun service externe (Groq, Gemini, OpenRouter, Cerebras, Firebase, Render) n'était accessible.

## Résultat global : 68 tests · **67 réussis · 0 échoué · 1 non exécuté**
Commande : `node --test --test-force-exit "test/*.test.js"` (66 s). Tests existants du dépôt : `selftest-free-sources.js` **18/18 avant et après** modification ; `node --check index.js` et chargement `FAKE_LLM=1` **OK avant et après**.

| Catégorie | Résultat | Détail |
|---|---|---|
| A. Base (import, init, config, validation, fermeture) | **TEST RÉUSSI** 9/9 | dont désactivation propre et refus d'un dossier de travail dangereux |
| B. Python worker (protocole, invalide, erreur, délai, sortie énorme, interruption) | **TEST RÉUSSI** 8/8 | délai/sortie/réponse invalide simulés par un faux interpréteur ; annulation et plafond de sortie sur vrais processus |
| C. Fichiers (ZIP valide/corrompu/malveillant/bombe/lien, vide, trop gros, mal formé, DOCX/XLSX/PDF/CSV/PNG valides, création, original préservé) | **TEST RÉUSSI** 9/9 | |
| D. Code (Python, JS, C++ compilable/non, outil absent, TypeScript) | **TEST RÉUSSI** 6/6 | |
| E. Sécurité (traversée, liens, injection, `#include` hostile, secrets, délai, plan hostile, exécution refusée, `unshare`, isolation entre utilisateurs) | **TEST RÉUSSI** 12/12 | |
| F-pipeline (correction, réparation bornée, annulation, budget, LLM indisponible, redémarrage, nettoyage) | **TEST RÉUSSI** 12/12 | |
| F-Milo (vrai `index.js` : démarrage, routes, outils, `callLLM` réel avec fournisseur factice, panne du moteur, LLM absent, mode dégradé) | **TEST RÉUSSI** 5/5 | |
| G. Régression (original vs modifié) | **TEST RÉUSSI** 2/2 | 40/40 routes et 27/27 outils conservés ; +9 routes `/api/engine/*` et +2 outils `engine_*` uniquement ; ≤ 3 lignes d'origine touchées |
| H. Routes HTTP (Express réel, auth simulée) | **TEST RÉUSSI** 4/4 · **NON EXÉCUTÉ** 1 | upload `multipart` réel : `multer` absent |

## Défauts trouvés par les tests pendant la validation (corrigés)
1. **Blocage au démarrage** : `mkdir` récursif boucle indéfiniment sous `/proc` ; un `ENGINE_WORKDIR` erroné aurait pu bloquer Milo → création sûre bornée + refus des dossiers système.
2. `/api/engine/capabilities` masquait les formats Python alors que le worker fonctionnait (résultat de sonde enveloppé).
3. Demandes invalides (fix sans instruction) rejetées trop tard → refus immédiat.
4. `shutdown()` purgeait les résultats terminés → ne retire plus que les espaces jetables.
5. Multipart sans `multer` renvoyait un 400 trompeur → 501 explicite + alternative JSON `file_base64`.
6. CSV à séparateur indéterminable non analysé → repli sur la virgule.

## Non exécuté / à valider manuellement
* **NON EXÉCUTÉ** — upload `multipart` réel (`multer`) ; chemin JSON `file_base64` testé.
* **NON EXÉCUTÉ** — isolation forte `bwrap` : absente ici. Seule la construction de la ligne de commande est testée (`buildCommand`). `engine_run_code` reste **non exposé** tant que `bwrap` n'est pas détecté. **VALIDATION MANUELLE NÉCESSAIRE** sur toute machine où tu l'activerais.
* **NON EXÉCUTÉ** — vrais fournisseurs LLM : adaptateur testé avec le fournisseur `fake` de Milo et avec « aucune clé » ; la qualité réelle des plans/correctifs d'un vrai modèle n'est **pas** mesurée.
* **NON EXÉCUTÉ** — authentification Firebase réelle : testé seulement que les routes répondent 401/403 sans jeton.
* **VALIDATION MANUELLE NÉCESSAIRE** — Render : présence de `python3`, `g++`, `prlimit` dans ton service, chemin du venv (`ENGINE_PYTHON`), mémoire/CPU réels (0,1 CPU : un contrôle g++ ou l'extraction d'un gros ZIP peut approcher les délais par défaut), comportement du proxy sur les tâches longues (le pipeline est asynchrone : l'appelant interroge).
* **NON EXÉCUTÉ** — `pip install -r requirements.txt` (réseau coupé).
* **FONCTIONNALITÉ NON DISPONIBLE** — exécution de tests du projet dans le pipeline de tâches (`tests.status = "not_run"`) ; vérification TypeScript ; OCR de PDF ; stockage durable branché ; interface utilisateur.

## Autres constats à connaître
* **Sécurité de ton dépôt** : le ZIP contient un fichier ` (1).env` (espace + `(1)` dans le nom) qui a l'apparence d'un vrai fichier de configuration (clés de fournisseurs, compte de service Firebase). `.gitignore` ne protège que `.env`. S'il a été poussé sur GitHub, **régénère ces clés**. Je n'ai lu aucune valeur et n'en ai copié aucune.
* `corrections_v17.2.diff` et le workflow `.github/workflows/fix.yml` (qui réécrit `index.js` par `sed`) n'ont pas été touchés ; note que ce workflow modifierait automatiquement `index.js` à chaque push sur `main`.
* Contrôle de secrets sur les fichiers livrés : aucun secret ; seules des valeurs factices de test (`gsk_test_secret`) figurent dans un test d'épuration d'environnement.
