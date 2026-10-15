# Luba Engine v1.0.0 — installation, configuration, déploiement

> **Le LLM est le cerveau. Luba Engine est les bras et les yeux.**
> Le modèle propose ; le moteur valide, applique **sur une copie**, vérifie pour de vrai, et rapporte ce qui a réellement été fait.

## 1. Ce qui est livré (et ce qui ne l'est pas)

| Fichier | Rôle |
|---|---|
| `luba-engine.js` | Moteur Node (pipeline, sécurité, outils, routes HTTP). Aucune dépendance npm nouvelle. |
| `luba_worker.py` | Worker Python (protocole JSON v1) : ZIP, analyse de fichiers, contrôles syntaxiques, diff, archives. |
| `index.js` | Ton `index.js` v18 + **31 lignes ajoutées + 2 lignes d’origine étendues** (repère : `[LUBA ENGINE]`). Voir `CHANGES_INDEX.md`. |
| `index.original.js` | Sauvegarde exacte de ton `index.js` d'origine (restauration : la renommer en `index.js`). |
| `requirements.txt` | Paquets Python **facultatifs** (chacun active un format). |
| `.env.engine.example` | Variables `ENGINE_*` (aucun secret). |
| `test/` | 68 tests automatisés + sonde d'intégration. |

`package.json` : **inchangé** (aucune dépendance npm ajoutée ; `multer` y est déjà). Les tests se lancent avec `node --test "test/*.test.js"`.

**Fonctionnalités réellement implémentées** : analyse de fichiers/projets/ZIP ; contrôles de syntaxe (Python, JS, JSON, YAML, XML, HTML, CSS, C/C++) ; création et correction de fichiers sur copie de travail avec diff, intégrité de l'original (SHA-256) et archive résultat ; boucle de correction bornée avec les **vraies** erreurs ; outils de chat `engine_check_code` / `engine_capabilities` ; routes `/api/engine/*` ; exécution isolée (si isolation réelle).
**Partiel** : exécution de tests du projet (non lancée par le pipeline : `verification.tests.status = "not_run"`) ; TypeScript (non vérifié, signalé) ; HTML/CSS (structure seulement, signalé) ; PDF (pas d'OCR) ; stockage durable (interface prête, non branchée).
**Non fait** : interface utilisateur (le front doit appeler `/api/engine/*`) ; l'envoi d'un ZIP *dans le chat* reste refusé par `processUploads` de Milo (non modifié, volontairement).

## 2. Comment il se branche sur Milo (aucun second orchestrateur)

* Même schéma que `free-sources.js` : `schemas()` / `has()` / `execute()` → les outils `engine_*` passent par `executeTool` existant, donc par la file de runs, les délais et l'annulation de Milo.
* **LLM** : `lubaEngineLlm` (dans `index.js`) appelle le `callLLM` et le `tierChain` **réels** de Milo (basculement entre fournisseurs, santé des clés, limites apprises). Aucune clé dupliquée, aucun fournisseur ajouté.
* **Auth / quotas** : les routes utilisent le `auth` et le `rateLimit("API"|"STRICT"|"SYNC")` de Milo.
* **Désactivation** : `ENGINE_ENABLED=false` (ou supprimer `luba-engine.js`) → aucune route, aucun outil, Milo inchangé (testé).
* **Panne du moteur** : toute erreur d'initialisation est journalisée (`Luba Engine : initialisation échouée (non bloquant)`), le moteur reste désactivé, **Milo démarre normalement** (testé, y compris un `ENGINE_WORKDIR` invalide).
* Aucun outil n'est annoncé au LLM tant que le moteur n'est pas prêt.

## 3. API HTTP (toutes authentifiées)

| Route | Rôle |
|---|---|
| `GET /api/engine/capabilities` | Ce que le serveur peut **réellement** faire (formats, compilateurs, isolation, limites). |
| `POST /api/engine/check` `{language, code}` | Contrôle immédiat d'un extrait (sans l'exécuter). |
| `POST /api/engine/tasks` | Crée une tâche (**202**). Entrée : `multipart` (`file`, `mode`, `instruction`, `tier`) **ou** JSON `{mode, instruction, file_name, file_base64}` **ou** `{files:[{path,content}]}`. |
| `GET /api/engine/tasks/:id` | État + résultat + rapport de vérification (à interroger toutes les 1-2 s). |
| `POST /api/engine/tasks/:id/cancel` · `DELETE /api/engine/tasks/:id` | Annuler / supprimer. |
| `GET /api/engine/tasks/:id/download?what=result\|original` | ZIP du résultat ou de l'original (restauration). |
| `POST /api/engine/run` | Exécution de code — **refusée** sauf conditions du §5. |

Modes : `analyze` (aucun fichier modifié), `fix` (correction d'un fichier/projet), `create` (création), `auto` (déduit de l'instruction ; sans verbe de modification → analyse seule).
Statuts de résultat : `succeeded` (≥ 1 contrôle réel réussi, 0 échec) · `unverified` (modifié, aucun contrôle applicable) · `partial` (modifié, des contrôles échouent encore) · `failed`.

## 4. Pipeline (ce qui se passe réellement)

1. **Réception** : taille, type, ZIP (inspection puis extraction bornée), limites par utilisateur.
2. **Classification** déterministe (pas de LLM pour une simple analyse).
3. **Analyse locale** : inventaire, relations d'imports, contrôles syntaxiques sur tous les fichiers pertinents.
4. **Contexte** structuré (demande, projet, erreurs constatées, extraits classés par pertinence, limites connues) ; le LLM peut réclamer d'autres fichiers (`need_files`, 2 tours max).
5. **Cerveau** : réponse JSON validée (opérations `write_file` / `replace_in_file` / `delete_file`, chemins relatifs, extensions autorisées, 40 opérations max). Une sortie invalide n'est jamais exécutée.
6. **Application sur la copie** (atomique : retour arrière complet si une opération échoue ; `replace_in_file` exige un texte unique).
7. **Vérification réelle** des fichiers touchés ; si elle échoue : renvoi des erreurs réelles au LLM (2 corrections max, budget de temps global 180 s).
8. **Livraison** : diff, fichiers produits, `result.zip`, contrôle d'intégrité de l'original, risques non vérifiés.

## 5. Sécurité

* **Aucun shell** : `spawn` avec arguments séparés ; les exécutables (`python`, `node`, `g++`, `gcc`, `prlimit`, `bwrap`, `unshare`) sont résolus par le moteur, jamais fournis par l'utilisateur ni par le LLM.
* **Environnement épuré** : les sous-processus ne reçoivent ni clé API ni compte de service (testé).
* **Limites** : délai par étape + budget de tâche, plafond de sortie, `prlimit` (CPU, fichiers, taille ; mémoire pour Python), processus tués **par groupe**, worker Python plafonné à 384 Mo.
* **Chemins** : `realpath` + liens symboliques refusés, `..`/absolu/NUL refusés ; écriture uniquement dans la copie de travail.
* **ZIP** : refus des chemins absolus/`..`, liens et fichiers spéciaux, entrées chiffrées, doublons ; limites de nombre/volume/ratio ; **le flux réel est compté** (on ne fait pas confiance aux tailles annoncées) ; archives imbriquées jamais extraites ; **aucun script d'archive n'est jamais lancé**.
* **C/C++** : `#include` absolu ou remontant, `__has_include`, `#embed` refusés avant compilation ; compilation en `-fsyntax-only` (aucun binaire produit par le pipeline de tâches).
* **Original** : copie en lecture seule + empreintes SHA-256 revérifiées à la livraison.
* **Isolation entre utilisateurs** : une tâche d'autrui répond « introuvable » (pas d'oracle d'existence).
* **Exécution de code — REFUSÉE par défaut.** Elle n'est possible que si `ENGINE_ALLOW_EXEC=true` **et** une isolation forte est détectée (`bwrap` : système de fichiers + réseau + processus). Sans `bwrap`, l'exécution est refusée (`ISOLATION_REQUIRED`), sauf dérogation **ADMIN** explicite `ENGINE_ALLOW_UNSANDBOXED_EXEC=true` via `POST /api/engine/run` uniquement (isolation partielle `unshare` : réseau + processus, **pas** le système de fichiers → ne pas l'activer pour du code non fiable). L'outil de chat `engine_run_code` n'est **jamais** exposé sans isolation forte.
* Une analyse statique ou quelques contrôles réussis **ne prouvent pas** l'absence de bogues ni de vulnérabilités : chaque rapport le rappelle.

## 6. Déploiement sur Render

Le moteur dégrade proprement : sans Python/g++, il le dit et ne fait que ce qui reste possible.

1. Copier à la racine du dépôt : `luba-engine.js`, `luba_worker.py`, `index.js` (remplace l'ancien), `requirements.txt`, `test/`, `.env.engine.example` (référence). Garder `index.original.js` si tu veux pouvoir revenir en arrière.
2. **Si le service est en runtime Node natif** — Build Command :
   `npm install && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
   et variable `ENGINE_PYTHON=/opt/render/project/src/.venv/bin/python3` (chemin absolu du dépôt sur Render : **à vérifier** sur ton service). Sans cette étape, le moteur fonctionne avec la bibliothèque standard seule (ZIP, code, JSON, XML, HTML, CSS, CSV) ; YAML/DOCX/PDF/XLSX/Pillow seront signalés « indisponibles ».
3. **Si le service utilise ton `Dockerfile`** (`node:22-slim` : **pas de Python ni de g++**) : le moteur démarre en mode dégradé (aucune fonction Python). Pour l'activer, ajouter au Dockerfile (non modifié ici) :
   ```dockerfile
   RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv g++ util-linux && rm -rf /var/lib/apt/lists/*
   COPY requirements.txt ./
   RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
   ENV ENGINE_PYTHON=/opt/venv/bin/python3
   ```
   Pour que le déploiement contienne le moteur, `COPY . .` (déjà présent) suffit.
4. Variables : aucune n'est obligatoire. Recommandé sur Render Free : laisser les défauts (`ENGINE_MAX_CONCURRENT=1`, 10 Mo, 384 Mo worker).
5. **Vérifier après déploiement** : `GET /api/engine/capabilities` (connecté) → `python.available`, `compilers`, `isolation`, `execution`. Dans les journaux : `luba-engine : prêt`.
6. Le disque Render Free est **éphémère** : tâches et résultats disparaissent au redémarrage ; téléchargez les résultats. Pour un stockage durable : `createEngine({ storage, onTaskFinished })` (interface prête, non branchée).

## 7. Tests

`node --test --test-force-exit "test/*.test.js"` — aucun réseau ni clé requis. Les tests dépendant d'un outil absent sont **ignorés avec leur raison**, jamais comptés comme réussis. Voir `RAPPORT_VALIDATION.md`.
