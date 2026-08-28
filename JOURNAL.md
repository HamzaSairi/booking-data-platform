# Journal de bord

## Jour 1 — 2026-08-25

**Fait**
- Environnement installé : Docker Desktop, Python 3.12, Git, gcloud CLI 581.0.0
- Bascule de PowerShell vers WSL2 / Ubuntu 24.04 comme environnement de travail
- Dépôt créé, arborescence des 9 dossiers, .gitignore commité en premier
- README initial : problème métier, les 4 tables, architecture cible en ASCII
- ADR-001 : Docker Compose plutôt qu'une installation native
- Dépôt public poussé sur GitHub

**Bloqué sur / temps perdu** (~1 h au total)
- `docker` introuvable dans WSL : l'intégration WSL doit être cochée dans les
  paramètres de Docker Desktop, ce n'est pas automatique.
- `permission denied` sur /var/run/docker.sock : il faut appartenir au groupe
  `docker`, et surtout redémarrer WSL entièrement (`wsl --shutdown` depuis
  Windows) — fermer le terminal ne suffit pas, l'appartenance à un groupe est
  fixée à la connexion.
- Confusion PowerShell / Ubuntu : `wsl` est une commande Windows, `groups` une
  commande Linux. Réflexe à prendre : lire le prompt avant de taper.
- `git remote remove origin` alors que `gh repo create` avait déjà réussi —
  j'ai corrigé une erreur sans lire toute la sortie du terminal.

**À retenir**
- Les undercurrents commencent au jour 1 : .gitignore avant le premier fichier
  de code, sinon un secret commité reste dans l'historique même après
  suppression.
- Un projet écrit sur /mnt/c depuis WSL est lent (passerelle 9P) : le dépôt vit
  dans ~/ côté Linux.
- Lire l'intégralité d'un message d'erreur avant de corriger. « Repository not
  found » et « Name already exists » décrivaient deux états opposés ; la ligne
  décisive avait défilé hors écran.


-
## Jour 2 — Postgres et modélisation transactionnelle
**Date** : 2026-08-25 · **Temps passé** : ~2 h

### Fait
- docker-compose.yml : Postgres 16, wal_level=logical, healthcheck, volume nommé
- 01_schema.sql : 4 tables, fonction set_updated_at() + 4 triggers, 8 index
- REPLICA IDENTITY FULL sur les 4 tables
- Vérifié : wal_level=logical, relreplident=f partout, trigger_ok=t

### Appris
- Le WAL sert d'abord à la durabilité ; `logical` y ajoute de quoi reconstituer
  les lignes. C'est ce qui rend le CDC possible — sans ce réglage, illisible
  de l'extérieur.
- `now()` renvoie l'heure de DÉBUT de transaction, identique pour toutes les
  lignes d'un même lot. J'ai utilisé `clock_timestamp()` dans le trigger.
- Mais même `clock_timestamp()` ne suffit pas : une ligne modifiée à 10h00
  dans une transaction qui commite à 10h05 devient visible APRÈS mon extraction
  de 10h02, avec un updated_at antérieur au watermark → perdue définitivement.
  C'est une limite structurelle de l'incrémental par colonne.
  → à chiffrer au Jour 10 dans docs/limites-batch.md

### Temps perdu / frictions
- git push rejeté : le distant avait un commit que je n'avais pas.
  Résolu par `git pull --rebase`. Configuré `pull.rebase true` en global.
- Réflexe à retenir : les scripts de /docker-entrypoint-initdb.d ne rejouent
  QUE si le volume est vide → `docker compose down -v` obligatoire après
  chaque modif du schéma.

### Questions ouvertes
- Est-ce que REPLICA IDENTITY FULL va vraiment peser sur le volume de WAL à
  mon échelle ? À mesurer au Jour 22 avec pg_stat_replication.
- Faut-il un index composite (status, updated_at) sur bookings, ou les deux
  index séparés suffisent-ils ? À trancher quand j'aurai des volumes réels.

  ## Jour 3 — Simulateur, partie 1

### Fait
- venv créé (oublié en fin de Jour 2), dépendances installées :
  psycopg 3.3.4, faker, click, python-dotenv → simulator/requirements.txt
- simulator/generate.py : squelette complet, connect(), utilitaires de date,
  insert_hotels() opérationnelle, CLI click avec seed --truncate
- 50 hôtels insérés, distribution des étoiles pondérée vers 3★

### Appris
- psycopg 3 : autocommit=False par défaut, le `with conn:` commite à la sortie.
  Une seule transaction pour tout le seed → soit tout existe, soit rien.
- executemany(..., returning=True) produit UN jeu de résultats PAR LIGNE, d'où
  la boucle sur nextset(). Sans elle on ne récupère que le premier id, et les
  2000 réservations pointeraient toutes vers le même hôtel.
- os.environ[...] plutôt que os.getenv(...) : échouer tôt et bruyamment sur une
  variable manquante, au lieu d'un None qui produit une erreur incompréhensible
  trois couches plus bas.
- make_conninfo() plutôt qu'une URL concaténée : gère l'échappement, et évite
  de dupliquer le mot de passe déjà présent dans les POSTGRES_* du .env.

### Temps perdu / frictions
- `source .venv/bin/activate` → No such file. Le venv n'avait jamais été créé.
  Réflexe acquis : le prompt DOIT commencer par (.venv), c'est le seul
  indicateur fiable.
- `Connection refused` sur 127.0.0.1:5432 : le conteneur Postgres ne tournait
  plus après redémarrage de la machine. Trois erreurs à ne pas confondre —
  Connection refused = rien n'écoute (serveur absent) ; authentication failed
  = serveur présent, identifiants faux ; timeout = réseau ou mauvais hôte.
  → à reprendre dans docs/runbook.md au Jour 14.
- Rituel d'ouverture désormais fixé : docker compose up -d, puis
  source .venv/bin/activate, puis code .

### À anticiper
- POSTGRES_HOST=localhost ne vaut que tant que le script tourne sur l'hôte.
  Au Jour 12, Airflow exécutera ce code DEPUIS un conteneur : localhost
  désignera le conteneur Airflow lui-même. La valeur devra devenir `postgres`,
  le nom du service Compose.
- Les hôtels ont un created_at sur 2 ans, les réservations seront sur 90 jours :
  une dimension doit toujours précéder les faits qui la référencent.

### Questions ouvertes
- Faut-il seeder les payments dès le Jour 3 (base propre) ou tout laisser au
  Jour 4 avec les défauts ? Tranché pour le Jour 3, à réévaluer si les tests
  du Jour 20 manquent de matière.


## Jour 4 — Simulateur, partie 2 : activité et défauts

### Fait
- commande simulate : vieillissement, créations, transitions, mutations
  de clients, suppressions physiques, injection de 7 défauts documentés
- state/simulation_log.jsonl : vérité terrain, sans laquelle le chiffrage
  du Jour 10 serait impossible (la source ne garde que l'état final)
- test binaire converti en test à seuil (< 2 %)

### Appris
- J'ai choisi les défauts APRÈS avoir inventorié les contraintes du schéma,
  pas avant. Quatre étaient structurellement impossibles (CHECK) — je ne les
  ai pas retirés : un CHECK en source est une garantie dont le pipeline peut
  dépendre. Les tests correspondants seront quand même écrits, pour détecter
  le jour où quelqu'un supprimerait la contrainte.
- CHECK (total_amount >= 0) autorise 0, qui n'a aucun sens métier. Règle
  technique ≠ règle métier : la première est dans le SGBD, la seconde doit
  être dans les tests dbt.
- Aucun index unique sur customers.email — je croyais l'avoir déclaré.
  Les tests d'unicité ne doivent jamais se fier au schéma.
- Un test de donnée n'est pas déterministe : test_statut_coherent a échoué
  du jour au lendemain sans qu'une ligne de code change. Le monde avait bougé.

### Temps perdu / frictions
- Fonction définie deux fois : Python n'émet aucun avertissement, la dernière
  écrase la première. ruff le détecte (F811). Réflexe : quand ruff signale des
  F821/F811, corriger AVANT de lancer le script.
- Fins de ligne CRLF héritées de Windows → .gitattributes avec eol=lf.
- Déséquilibre production/consommation : advance_statuses vidait le stock de
  'pending' plus vite qu'il ne se reconstituait. Un simulateur a des flux à
  équilibrer, comme une file d'attente réelle.

### Chiffres à retenir pour le Jour 10
- ~10 % des changements de statut passent par un état intermédiaire
  qu'aucune extraction incrémentale ne peut voir.
- Les suppressions physiques ne laissent aucune trace en base.

## Rétrospective sprint 1 — 2026-08-28

**Objectif du sprint** : une base métier réaliste tourne en local et génère de
l'activité avec des défauts contrôlés. → Atteint.

**Ce qui a marché**
- Écrire le .gitignore avant tout le reste : aucun secret n'a jamais approché
  l'index Git.
- Les tests écrits au Jour 3, avant le simulateur du Jour 4. Ils ont attrapé
  deux bugs que je n'aurais pas vus (distribution des paliers, statuts périmés).

**Ce qui a coûté du temps**
- Indentation et fins de ligne CRLF : ~1 h perdue au Jour 4. Corrigé par
  .gitattributes + ruff. Le vrai coût était de ne pas avoir lancé ruff dès
  le premier fichier.
- Déséquilibre production/consommation dans simulate : le stock de bookings
  'pending' s'épuisait. Un simulateur doit respecter un régime stationnaire.

**Ce que j'ai compris et que je n'avais pas anticipé**
- Un test de donnée n'est pas déterministe comme un test de code : le mien est
  passé le 26 et a échoué le 27 sans qu'une ligne change. Le monde avait bougé.
- L'état hors conteneur (state/) survit à `docker compose down -v`. Toute
  remise à zéro doit couvrir les deux, sinon le reset est partiel et silencieux.

**Dette assumée pour le sprint 2**
- [à compléter]

**Pour le sprint 2** : le watermark du Jour 7 est exactement le même piège que
state/. La cible `reset` le couvre déjà.