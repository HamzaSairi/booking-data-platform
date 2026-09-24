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


**Pour le sprint 2** : le watermark du Jour 7 est exactement le même piège que
state/. La cible `reset` le couvre déjà.

## Jour 6 — GCP, BigQuery, sécurité

### Un test vert devenu rouge sans le moindre commit

`pytest` a échoué au lancement du jour sur `test_statut_coherent_avec_les_dates` :
76 réservations avec un `check_out` passé encore en `pending` ou `confirmed`.
Aucun code n'avait changé depuis le jour 5.

Diagnostic en trois requêtes. `completed` existait bien et représentait 1098
lignes : pas de bug de génération. `count(*) FILTER (WHERE updated_at >
created_at)` valait 0 sur les 76 : aucune n'avait été touchée par `simulate`.
Les `check_out` s'étalaient du 28 au 31 août alors que le seed datait du 27.

Verdict : au moment du seed, ces lignes étaient parfaitement cohérentes. Quatre
jours ont passé. Le calendrier a rendu les données fausses sans qu'une seule
ligne de code s'exécute.

Ce que j'en retiens : un invariant qui dépend de l'heure de son évaluation doit
être **maintenu** par le système, pas établi une fois au seed. Corriger
uniquement le seed aurait redonné du rouge trois jours plus tard. La règle
appartient à `simulate`, pas à la génération initiale.

C'est le même piège que `datetime.now()` dans une tâche Airflow au jour 13,
rencontré cinq jours plus tôt et côté données au lieu du code. Et c'est ma
réponse à la question 6 d'entretien : tester de la donnée, ce n'est pas tester
du code, parce que le résultat peut changer sans que le code bouge.

**Dette ouverte** : la commande `age` lève `psycopg.ProgrammingError: no result
available`. Contournée par un UPDATE manuel en psql, donc les tests passent mais
l'invariant n'est pas encore maintenu automatiquement. À reprendre à froid — le
jour 10 en aura besoin pour provoquer du mouvement sans créer de volume.

### GCP

Projet `booking-data-platform-b7768d`, trois datasets en EU, service account
avec exactement `bigquery.dataEditor` et `bigquery.jobUser`.

Le moindre privilège vérifié par le comportement plutôt que par la console :
`create_dataset` avec le service account renvoie un 403. C'est le résultat
attendu — mon compte humain crée les datasets, le service account écrit dedans.

Trois ADR écrits (région, mode d'authentification, bac à sable).

### Frictions

- Terminal PowerShell sur un chemin `\\wsl.localhost` : `source` inconnu, et le
  venv Linux inutilisable depuis Windows. Corrigé en rouvrant le dossier via
  *WSL: Reopen Folder in WSL*.
- `$booking12` au lieu de `booking12` : le `$` ne se met qu'à la lecture d'une
  variable. gcloud a reçu une chaîne vide. Réflexe pris : toujours `echo` une
  variable avant de la passer à une commande qui crée quelque chose.

## Jour 7 — Extraction incrémentale

### Le décorateur détourné

`simulate` avait disparu des commandes Click, et `age` levait
`psycopg.ProgrammingError: no result available`. Une seule cause : la fonction
`expire_past_stays` s'était insérée entre les `@click.option` et `def simulate`.
Les décorateurs s'appliquent à ce qui les suit IMMÉDIATEMENT, donc ils se sont
posés sur `expire_past_stays`, qui est devenue un objet `click.Command`.
Quand `age` l'appelait avec un curseur, Click l'interprétait comme une liste
d'arguments de ligne de commande.

Python n'émet aucun avertissement, et l'erreur se manifeste quatre appels plus
bas, dans les entrailles de Click. À rapprocher des `@task` d'Airflow au Jour 12,
où un décorateur mal placé produit un DAG qui se charge sans erreur mais dont
une tâche n'apparaît jamais dans le graphe.

Corrigé en supprimant `expire_past_stays`, qui doublonnait `complete_past_stays`
— déjà écrite, et meilleure puisqu'elle journalise chaque transition.
Réflexe à prendre : `grep` sur le fichier avant d'y ajouter une fonction.

### Le watermark empoisonné par une donnée du futur

Premier `watermarks.json` produit :
`"payments": "2026-09-03T16:14:36.392128+00:00"` — deux jours dans le futur,
alors qu'une seule ligne avait été extraite sur 1720.

Cause : `insert_payments` écrivait `paid_at` (jusqu'à +48 h) dans `created_at`
et `updated_at`. Le watermark mémorisait donc une date que l'horloge ne
rattraperait qu'au surlendemain, et pendant deux jours l'extraction aurait
ignoré tous les paiements réels. Aucune erreur, aucun symptôme visible.

Deux corrections :
- côté source, séparer la date MÉTIER de la métadonnée TECHNIQUE
  (`tech = min(paid_at, now_utc())`) : une capture différée est plausible,
  une ligne modifiée dans le futur ne l'est pas ;
- côté extraction, plafonner le watermark à `now()` avec un message d'alerte.
  Le Parquet garde la ligne telle quelle — la couche raw copie, elle ne
  corrige pas ; le nettoyage appartient au `stg_*` du Jour 17.

Ce que j'en retiens : un watermark n'est jamais plus fiable que la colonne dont
il dérive. En production, ce motif se manifeste par un job dont la durée
augmente lentement pendant des semaines, sans qu'aucune alerte ne se déclenche.
C'est ma réponse à la question 3 d'entretien.

L'étage suivant de la parade serait d'exclure les lignes futures vers une table
de quarantaine plutôt que de les copier — même logique que les paiements
orphelins du Jour 20, qu'on compte au lieu de les cacher. Pas fait.

### La marge de sécurité, vue en vrai

Seconde exécution après correction : 1 ligne relue pour hotels, customers et
bookings, mais 28 pour payments. Explication : le `min(paid_at, now_utc())`
ramène tous les paiements « futurs » à l'instant du seed, donc ils partagent un
`updated_at` groupé dans la dernière seconde, et la marge de 5 s les rattrape
en bloc.

Pas un bug — la démonstration que `updated_at` n'est pas unique et qu'un
watermark seul ne découpe jamais proprement. Sans la marge, ces 28 lignes
auraient été du côté de la PERTE au lieu du doublon, et rien ne me l'aurait dit.

Chiffre à retenir : 1,6 % de redondance, 0 perte.

### Constaté pour le Jour 10

`delete_old_pending` a supprimé 3 réservations pendant la simulation. Aucun
`updated_at` ne bouge lors d'un DELETE physique : l'extraction incrémentale ne
peut structurellement pas les voir. Premier élément chiffré de
`docs/limites-batch.md`.

### Reste à faire
- `tests/test_extract.py` : seuil de redondance à la seconde exécution
  (surtout pas un test à zéro, il interdirait la marge de sécurité).

## Jour 8 — Chargement BigQuery

Premier lot chargé : hotels 159, customers 1533, bookings 6072,
payments 5379, depuis 12 fichiers Parquet par table.

État de `raw_booking.customers` :
- 1533 lignes brutes
- 500 clés distinctes  ← identique à la source, aucune perte
- 12 fichiers sources

Facteur de redondance : 3,07. Origine identifiée : suppressions
répétées de `state/watermarks.json` pendant le jour 7, qui ont fait
relire chaque table depuis 1970. La marge de sécurité de 5 s n'y est
pour presque rien.

La couche raw assume ces doublons — elle est append-only par
construction. Ce qui compte est qu'ils soient traçables (`_source_file`)
et réversibles (`_ingested_at` comme identifiant de lot). La
déduplication est le sujet du jour 9, puis du `stg_*` au jour 17.

### À faire avant le jour 10
Laisser la base vivre entre le jour 9 et le jour 10, sinon il n'y aura
rien à mesurer :
    python simulator/generate.py simulate --minutes 10 --defect-rate 0.1
Objectif : des suppressions physiques et des transitions de statut
multiples entre deux extractions, pour chiffrer ce que le batch rate.

## Jour 9 — Idempotence

**Fait** : vues de déduplication `staging_booking.v_*`, test d'idempotence sous
rejeu forcé, mise en place de ruff et pytest.

**Ce que j'ai compris aujourd'hui**
L'idempotence n'est pas une propriété de chaque table, mais de la *sortie
observable* du pipeline. Ma raw ne sera jamais idempotente en nombre de lignes —
c'est un journal append-only, c'est sa fonction. Le contrat correct est :
l'état métier après N exécutions est identique à l'état après 1 exécution.
J'ai perdu du temps au début en cherchant à stabiliser le mauvais compteur.

**Le test creux, et pourquoi je ne l'ai pas écrit**
Le réflexe naturel est de lancer le pipeline trois fois de suite et de vérifier
que le compte ne bouge pas. Ce test passe toujours et ne prouve rien : après la
première exécution le watermark a avancé, les deux suivantes n'extraient rien.
On teste que ne rien faire ne change rien. Le test utile efface l'état avant
chaque rejeu — c'est le vrai scénario de crash — et assert que la raw a grossi,
faute de quoi il se saurait vide.

**Un incident qui a servi de preuve**
J'ai interrompu le test par Ctrl+C en pleine requête BigQuery, potentiellement
juste après l'effacement de l'état. Aucune réparation manuelle n'a été
nécessaire : la relance a tout re-extrait et reconstruit l'état seule.
L'idempotence a été vérifiée par accident avant de l'être par assertion.
Si j'avais dû réparer à la main ici, la conception aurait été fausse.

**Chiffres relevés**
- Ratio de duplication avant déduplication : ~3,0 sur les quatre tables.
  `hotels` est la plus haute (3,18) parce qu'elle est statique, `bookings` la
  plus basse (2,97) parce qu'elle croît. Le ratio de duplication d'une table
  décroît mécaniquement avec son taux de croissance — utile pour diagnostiquer.
- Test d'idempotence : 2 passed en 99 s, 3 rejeux complets, sortie inchangée.

**Découverte non cherchée**
En vérifiant les paiements orphelins (attendus par l'ADR-003), j'ai trouvé 0 —
en cible *et* en source. Le défaut n'est pas injecté par le simulateur, l'ADR-003
est à corriger. Mais la comparaison source/cible a révélé autre chose :

| table    | source | staging | écart |
|----------|--------|---------|-------|
| bookings |   2000 |    2046 |   +46 |
| payments |   1720 |    1741 |   +21 |
| customers|    500 |     500 |     0 |
| hotels   |     50 |      50 |     0 |

Les tables sujettes aux suppressions physiques divergent, les autres non.
2,3 % de lignes fantômes dans `bookings`, invisibles à tous mes tests actuels,
et l'écart grandira à chaque exécution du simulateur. C'est le sujet du jour 10.
À noter : une vérification négative m'a révélé un problème que je ne cherchais pas.

**Limite de mon test, à ne pas oublier**
`test_idempotence.py` suppose une source figée. Si le simulateur tourne pendant
son exécution, de nouvelles lignes remontent légitimement et l'empreinte change —
échec pour une bonne raison, le pire type d'échec. La parade propre (comparer sur
un périmètre gelé plutôt que sur la table entière) est le même problème que la
réconciliation CDC du jour 25.

**Effet de bord du test** : `test_idempotence.py` quadruple la raw à chaque
exécution. Le lancer en boucle n'est pas gratuit — 24 225 lignes après un seul
passage, et ça se cumule. Raison supplémentaire de le laisser derrière le
marqueur `bigquery` et hors de la CI.

**280 réservations sans paiement** : 154 `cancelled`, 126 `pending`, 0 `confirmed`.
Métier légitime, pas un défaut de qualité. En revanche le zéro sur `confirmed`
révèle une invariante que je n'avais pas formalisée — toute réservation confirmée
a un paiement — qui devient un test singulier dbt au jour 20. Une règle métier
trouvée en regardant les données vaut mieux qu'une règle recopiée d'un tutoriel.

**À faire demain**
- Corriger `simulate` pour injecter réellement des paiements orphelins, et
  mettre à jour l'ADR-003.
- Chiffrer les suppressions et les transitions de statut perdues
  (`docs/limites-batch.md`).


  ## Jour 10 — Ce que le batch ne voit pas

**Fait** : trigger d'audit comme vérité terrain, campagne de simulation de
10 min, mesure des pertes, `docs/limites-batch.md`, ADR-007.

**Les chiffres**
21 transitions perdues sur 209 (10 %). 13 lignes fantômes sur 2 322 (0,56 %).
0 détectée par le pipeline.

Le zéro est le vrai résultat. Les deux premiers chiffres disent qu'il y a un
problème ; le troisième dit qu'il est invisible. Un pipeline vert, des tests qui
passent, une fraîcheur correcte — et 34 anomalies dans la cible.

**La réservation 931**
Supprimée à 15:51:32, toujours en cible, statut `pending`, `updated_at` au
5 août. Un analyste y voit une réservation en attente depuis un mois. Ce qui
frappe, c'est qu'elle n'a l'air de rien : bien typée, cohérente, plausible.
C'est une ligne propre qui ment. J'ai enfin une image concrète de ce qu'on
appelle un échec silencieux.

**Ce que j'ai raté, et corrigé**
Première mesure : 46 fantômes. Mesure propre : 13. L'écart venait de mon
protocole — comptes source et cible pris à des instants différents pendant que
le simulateur écrivait. Je mesurais du bruit et j'allais l'écrire dans un ADR.

Règle retenue : une comparaison source/cible n'a de sens que sur une source
figée, ou sur un périmètre borné par une clé et un instant. C'est le même
problème que la limite de `test_idempotence.py` notée hier, et ce sera le même
au jour 25. Trois occurrences en deux jours — ce n'est pas un détail, c'est un
motif.

**Une hypothèse réfutée**
J'ai supposé un troisième mode d'échec : des lignes créées puis supprimées entre
deux extractions, invisibles partout. Mesure : 0. Le simulateur ne supprime que
des `pending` anciennes. Le mode d'échec existe dans l'absolu, pas dans mes
données. Je le laisse écrit avec sa réfutation.

**L'ironie du jour**
Pour prouver qu'il me manquait un journal de changements, j'ai dû construire un
journal de changements avec un trigger. Postgres en tient un depuis le premier
jour — le WAL. Le sprint 5 consistera à le lire au lieu de le dupliquer. C'est,
je crois, la meilleure façon d'expliquer le CDC par log en entretien.

**Réserve honnête**
14 suppressions, c'est un ordre de grandeur, pas une statistique. Le résultat sur
les transitions (188 observations) est bien plus solide. Ne pas survendre le
second chiffre.

### Jour 11 — une base qui n'avait pas le mot de passe de son .env
Airflow refusait de se connecter avec `air123456`. Trois hypothèses fausses
avant la bonne : caractère à encoder, gabarit non substitué (vrai, mais pas
la cause racine), puis enfin le test direct depuis un conteneur tiers.

Cause racine : `POSTGRES_PASSWORD` a été modifié dans `.env` après
l'initialisation du volume. L'image Postgres ne lit cette variable qu'au
premier démarrage sur un volume vide. La base a gardé l'ancien mot de passe
et le fichier décrivait depuis un état qui n'existait pas.

Pourquoi personne ne l'a vu pendant plusieurs jours : mon seul chemin d'accès
quotidien était `docker compose exec psql`, qui passe par le socket local en
`trust` et ne demande aucun mot de passe. Les tests, eux, échouaient déjà —
je ne les avais pas relancés depuis le changement. Il a fallu un second
client (Airflow, en TCP depuis un autre conteneur) pour révéler la panne.

Deux règles retenues :
- avant toute hypothèse sur une erreur d'authentification, `printenv` dans
  le conteneur puis un `psql` direct. Ce qu'on croit avoir configuré et ce
  qui tourne sont deux choses différentes.
- un secret écrit à deux endroits finit toujours par diverger. L'URI Airflow
  est désormais dérivée de POSTGRES_PASSWORD dans le compose.

## Jour 12 — Le DAG d'ingestion

**Fait** : DAG `ingestion_batch`, quatre TaskGroups en parallèle
(extract → validate → load), `schedule='@daily'`, `catchup=False`,
`max_active_runs=1`. Image Airflow applicative construite depuis
`airflow/Dockerfile`. Refactor complet de `ingestion/` : chemins ancrés
sur la racine du dépôt, état partitionné par table, `extract_one` et
`load_one`. Run complet en 25 s, `state=success`.

**Ce que l'orchestration a révélé** — c'est le vrai contenu de la journée.
Aucun des trois obstacles n'était un problème tant qu'un humain lançait
les scripts : chemins relatifs au répertoire courant, état partagé entre
quatre processus, dépendances absentes de l'image. L'orchestrateur ne les
a pas créés, il les a exposés. À retenir comme grille de lecture : ce qui
marche parce qu'un humain fait toujours la même chose est une hypothèse
non écrite, et l'automatisation la révèle.

**Temps perdu, et sur quoi** : environ une heure sur l'indentation du
`docker-compose.yml`. Deux causes enchaînées — des clés (`profiles`,
`depends_on`, `volumes`) sorties de l'ancre `x-airflow-common` par un
décalage de deux espaces, et un extrait collé littéralement avec ses
commentaires de position, qui a fait disparaître `FERNET_KEY` et
`SQL_ALCHEMY_CONN`. Symptômes successifs : montages ignorés,
`services.volumes not allowed`, puis `mapping key "volumes" already
defined`. Aucun ne désignait la vraie cause.
→ Réflexe pris : `docker compose config` après **chaque** édition du YAML,
en routine et non en dépannage. YAML n'a aucune redondance syntaxique :
un mauvais niveau d'indentation ne produit pas une erreur, il produit un
fichier différent mais valide, qui échoue plus loin.

**Deuxième perte de temps** : le nouveau code d'`extract.py` collé dans
`__init__.py`. L'import réussissait (`ok`), donc j'ai cru le refactor
appliqué. Un import réussi ne prouve que le chemin Python, jamais le
contenu du module.
→ Réflexe pris : vérifier ce qu'on va appeler, pas ce qu'on importe —
`grep -n "def extract_one" ingestion/extract.py`. Coût nul, question close.

**Diagnostic Airflow** : `dags test` est un mauvais outil de diagnostic de
parse — il consomme le DagBag déjà construit et renvoie « could not be
found », sans distinguer l'absence de l'erreur de parse. Les trois bonnes
commandes : `dags list-import-errors`, `dags list`, et surtout
`python /opt/airflow/dags/mon_dag.py`, qui court-circuite Airflow et donne
la traceback complète en deux secondes.

**Découverte à consigner — l'idempotence ne se mesure pas en octets.**
Deux exécutions consécutives du DAG, source figée, aucun simulateur en
cours : quatre fichiers Parquet écrits quand même. Cause : la
`SAFETY_MARGIN` de 5 s fait relire à chaque passage toute ligne dont
l'`updated_at` tombe juste avant le watermark. Sur `hotels`, dont le
`max(updated_at)` égale exactement le watermark, la même ligne est
réextraite indéfiniment.
Chiffres en cible : **393 lignes brutes pour 50 hôtels distincts**,
soit un facteur 7,9 sur la table la plus statique du modèle.
Ce n'est pas un défaut, c'est le contrat at-least-once qui fonctionne :
jamais de perte, doublons assumés, déduplication en aval.
→ Formulation pour l'entretien (question 1) : *l'idempotence d'un
pipeline ne se mesure pas au nombre d'octets écrits, mais à la stabilité
de l'état observable en sortie.*

**Piège Airflow appris** : une tâche qui lève `AirflowSkipException` n'écrit
pas d'XCom, et la tâche suivante échoue alors à résoudre son argument
(`XComArg ... is not found`). D'où le choix de faire skipper `load`, dont
personne ne consomme la sortie, et de faire renvoyer une liste vide à
`extract`.

**À suivre** : `hotels` a un watermark au 30 août quand les trois autres
sont à aujourd'hui. Ce n'est pas un retard, c'est une absence de
changement — mais ça complique le test de fraîcheur du jour 28 : une
table statique ne peut pas partager le seuil d'une table transactionnelle.

**Reste à faire** : vérification de l'échec forcé sur `validate` via la
conf `{"echec_validate": "bookings"}` dans l'interface.

## Jour 13 — backfill : le fond acquis, l'exécution bloquée

**Acquis, démontré à la main :** extraction par fenêtre `[start, end)`,
chemin de sortie déterministe, partition BigQuery sur `_interval_start`,
écrasement par décorateur. Deux chargements consécutifs de la fenêtre du
3 septembre : 632 lignes, une seule partition, compte stable. C'est
l'idempotence temporelle, vérifiée sur la cible.

**Non abouti :** aucun des quatre backfills créés (id 1 à 5) n'a produit
de fichier. Cause identifiée en fin de journée : un run manuel créé à
15:40, avant le refactor, relancé jusqu'à `try_number=8`, occupait
l'unique créneau de `max_active_runs=1` et rejouait la version du DAG
épinglée à sa création — d'où « 0 ligne » dans la tâche alors que le
même appel rendait 632 lignes dans le même conteneur. Il s'est éteint à
16:37:20. La file contient les reliquats des backfills 1 à 5.

**Reprise :** `airflow backfill list`, supprimer les backfills obsolètes,
purger les runs en `queued`, relancer une seule fois.

**Quatre succès silencieux dans la même journée :** `charger` sans
`return` (None, aucun job soumis), `job.output_rows` à None sur un
WRITE_TRUNCATE avec décorateur, sept runs verts sans donnée, et le
zombie vert en relance. Matière directe pour la question 3.

**Blocage non résolu** : les runs de backfill sont créés (backfill_id 1 à 7)
mais aucun ne s'exécute — ou s'exécute sans produire de fichier. Les cinq
conteneurs sont sains, le scheduler tourne. Deux causes possibles à
explorer demain :
  - le scheduler ne ramasse pas les runs (état `queued` persistant) :
    regarder les slots de pool et `max_active_runs`
  - les runs s'exécutent et l'extraction rend 0 ligne : reprendre le log
    d'une tâche `extract` d'un run de backfill récent, pas d'un run manuel

## Jour 13 — idempotence temporelle : démontré

Backfill de 8 intervalles (2026-09-01 → 09-08), joué deux fois.
Distribution identique aux deux passages, et conforme à la source :
1, 255, 632, 311, 301, 310, 762, 190. Total 2 762.
Doublons sur (booking_id, updated_at) : 0.


## Jour 14 — callbacks, runbook et pannes réelles

**Livré.** Callbacks de relance et d'échec via `default_args`, dans
`airflow/dags/commun/callbacks.py` : pas d'import d'Airflow, 10 tests.
Chaque relance ou échec écrit une ligne JSON dans
`logs/pipeline/evenements.jsonl` et dans le log de la tâche, avec un renvoi
au runbook. `docs/runbook.md` couvre trois scénarios : S1 Postgres
injoignable, S2 BigQuery refuse le chargement, S3 intervalle vert mais
cible fausse.

**Le scénario 3 du plan n'existait plus.** « Watermark corrompu » supposait
un watermark, supprimé au jour 13. Remplacé par l'incident équivalent :
Airflow dit « fait », la cible dit autre chose. C'est le seul que les
callbacks ne voient pas.

**Accrocs d'installation.** Dossier créé sous le nom `commum` au lieu de
`commun` (ModuleNotFoundError). Fichiers `*:Zone.Identifier` laissés par
la copie depuis Windows, supprimés et ajoutés au .gitignore.

**Bug révélé par le journal lui-même.** Un run déclenché sans
`--logical-date` n'a pas de date logique en Airflow 3 : `fenetre()` levait
KeyError, relancé trois fois pour rien (~5 min). Le journal l'a signalé
avec `scenario_runbook: null`. Corrigé en AirflowFailException : le run
échoue en 1,3 s (ADR-019). Erreur de manipulation dans la foulée :
`dags trigger --logical-date` sur le 05/09, déjà couvert par un run →
UniqueViolation sur (dag_id, logical_date).

**Expérience A — panne courte.** Postgres arrêté, run du 03/09 rejoué,
Postgres redémarré après la première relance. 4 relances classées S1,
run vert. Message réel : `failed to resolve host 'postgres': [Errno -2]
Name or service not known` (conteneur retiré du réseau Docker). Le run est
vert malgré la panne : sans les lignes « relance », aucune trace. Il a
pourtant mis 10 min 11 s à reprendre, Postgres revenu depuis longtemps :
premier signe du plafond de relance, pas compris sur le moment.

**Expérience B — panne longue.** Panne à 10:11:43 UTC, Postgres laissé
arrêté.
- Relances en tentatives 7, 8, 9, échec en tentative 10 à 10:41:55, les
  4 tables à la même seconde. Cumul avec A : 16 relances, 4 échecs, comme
  prédit.
- try_number est cumulé à travers les clear (A en 5-6, B en 7-10). Le
  clear fixe max_tries à dernière tentative + retries (6 + 3 = 9) :
  chaque reprise garde 4 tentatives.
- Délai mesuré entre relances : 10 min 02 s. Le commentaire de DEFAUTS
  annonçait 30 s, 1 min, 2 min : c'est le plafond max_retry_delay
  (30 s × 2^6, écrêté à 10 min). Le commentaire n'était vrai que pour un
  run neuf.
- Partition BigQuery du 03/09 pendant la panne : 632, identique à la
  référence. Données retardées, pas perdues.
- Reprise par la procédure S1 : run en success, `--only-failed` a bien
  inclus les upstream_failed, source = cible = 632 à 10:46:20.

**Le chiffre du jour.** Détection 30 min 12 s, résolution 4 min 25 s,
incident total 34 min 37 s : 87 % de l'incident, c'est de la détection.
La reprise est rapide grâce à l'idempotence du jour 13. La détection est
lente à cause d'un plafond jamais mesuré, et elle l'est justement pour les
tâches en reprise d'incident (ADR-018).

**Ce que l'égalité ne prouvait pas.** Source = cible = 632 après la reprise
ne prouvait rien à elle seule : le simulateur était arrêté et la partition
valait déjà 632 avant la panne. La preuve est l'état success du run, avec
une fin postérieure à l'échec.

**Callback amélioré en cours de route.** « tentative 7, retries 3 » était
illisible : ajout de `relances_max` (`ti.max_tries`). Mesuré : max_tries
compte les relances, l'échec final s'écrit tentative 10 / relances_max 9.

**Non fait.**
- Expérience C (Postgres gelé par `docker compose pause`) et timeouts :
  reportés. Pas de décision sur les timeouts tant que le blocage n'est
  pas mesuré.
- Le nouveau plafond de 2 min n'est pas encore mesuré.
- S2 (quota BigQuery) jamais exécuté, marqué comme tel dans le runbook.

## Jour 15 — Revue de sprint 3 (1/2) : expérience C, Postgres gelé

**Prédiction réfutée.** Attendu : `docker compose pause` bloque la tâche
indéfiniment, sans relance ni callback. Observé : chaque tentative échoue
après 130,3 s sur `ConnectionTimeout`, puis est relancée. Deux relevés
`running` portaient des `start_date` différentes : c'étaient deux
tentatives. Un état seul ne dit rien, la `start_date` identifie la
tentative.

**Un timeout que personne n'avait choisi.** 130 s, c'est
`_DEFAULT_CONNECT_TIMEOUT` de psycopg 3.3.4, une constante privée.
Détection en 14 min 43 s, contre ~6 min annoncées par l'ADR-025, qui
ignorait la durée des tentatives en échec. Formule vérifiée :
`(retries + 1) × durée d'une tentative + retries × délai`.

**Faux négatif du runbook.** Les 4 lignes étaient classées `null`.
`ConnectionTimeout` hérite d'`OperationalError`, mais `classer()` compare
un nom, pas une classe. Le test du jour 14 fabriquait une
`OperationalError("timeout expired")` : l'exception qu'on imaginait, pas
celle que psycopg lève.

**Correction (ADR-027).** `connect_timeout=10` dans `extract.py`,
signature `S1b` dans `classer()`, test aligné sur l'exception réelle.
Vérifié avant de mesurer : le conteneur exécute bien le code modifié
(`/opt/airflow/project/ingestion`).

**Re-mesure.**

| | Défaut (130 s) | `connect_timeout` 10 s |
|---|---|---|
| Tentative en échec | 130,3 s | 10,3 s |
| Délai de relance | 2 min 00 s | 2 min 01 s |
| Détection | 14 min 43 s | 6 min 44 s (prédit : 6 min 43 s) |
| Reprise (échec → run vert) | 4 min 20 s | 3 min 05 s |
| Incident total | 19 min 03 s | 9 min 49 s |
| Classement | `null` | `S1b` |

Détection calculée depuis le journal seul : horodatage de la première
ligne moins sa durée, jusqu'à la ligne `echec`. Reprise et total vont
jusqu'à la `end_date` du run. L'incident total est divisé par 2. Les
délais de relance forment près de 90 % de la détection restante : le
prochain levier est `retries` ou le plafond, et non plus le timeout.

**Reprises.** Jour 14 : 4 min 25 s. Expérience C : 4 min 20 s, dont 9 s
de rejeu. Re-mesure : 3 min 05 s, dont 2 min 25 s avant le `unpause`. La
reprise reste entre 3 et 4 min 30 s alors que le rejeu prend quelques
secondes : c'est du temps humain, un argument pour une vraie alerte
(jour 28) plutôt que pour un pipeline plus rapide.

**try_number, troisième confirmation.** Tentatives 12 à 15, puis 17 à 20 ;
`relances_max` vaut la dernière tentative avant le `clear` plus 3.

**Trouvé en chemin.**
- `tasks clear -t` filtre par sous-chaîne littérale : `^bookings\.` et
  `b.okings` ne trouvent rien, `bookings.` trouve les 3 tâches. Le premier
  lancement de l'expérience n'a rien nettoyé, sans aucun message.
- Un `clear` sans tâche correspondante rend la main en silence.
- Une tâche `upstream_failed` affiche les dates de sa dernière exécution
  réelle.
- La fin d'une reprise se prouve par la `end_date` de `list-runs`, pas par
  un `date` lancé à la main après le `clear`.
- La commande `pytest` nue ne trouvait pas `simulator` (collecte
  interrompue) ; depuis quand, non établi. Racine ajoutée au `pythonpath`
  de `pyproject.toml`, `tests/conftest.py` supprimé.
- `tests/test_idempotence.py` existe, mais ne tourne pas par défaut : il
  est marqué `bigquery` et `addopts` exclut ce marqueur, donc la collecte
  ne l'affiche pas (conclusion hâtive « il n'existe pas », corrigée en
  2/2). Il fait encore référence à `state/watermarks.json` et
  `state/loaded_files.json`, supprimés au jour 13 : la preuve
  d'idempotence du jour 9 ne teste plus le pipeline actuel.
- venv de l'hôte en Python 3.12, conteneur en 3.13 : les tests ne
  tournent pas sur l'interpréteur de production (à régler en CI, jour 27).
- `extract.py` lit les variables `POSTGRES_*` ; la Connection
  `postgres_source` est définie dans le compose
  (`AIRFLOW_CONN_POSTGRES_SOURCE`) et survit à un `down -v`. Utilisée
  par : `verif_source.py` seulement (DAG de vérification du jour 11),
  aucune tâche d'ingestion.

**Non fait.**
- `execution_timeout` reporté (ADR-027) : un gel en pleine requête n'est
  pas couvert.
- S1 (Postgres arrêté) pas re-mesuré avec le plafond de 2 min.
- `tests/test_extract.py` (jour 7) : vérifier s'il teste encore le
  watermark supprimé au jour 13.

## Jour 15 (2/2) — Lecture avant le test depuis zéro

Le test depuis zéro passe au 3/3 : la lecture du code, prévue comme simple
préparation, a trouvé des défauts que le test n'aurait fait que retrouver.

**1. Les runs planifiés lisaient une journée à venir (ADR-028).** Avec
`"@daily"`, Airflow 3 fixe `logical_date` à l'heure du déclenchement :
`fenetre()` lisait la journée qui commence. Métadonnées : runs `scheduled`
du 09/09 et du 10/09 terminés à 11:52 et 09:32 le jour même de leur
fenêtre ; les 8 `backfill`, sur des journées closes, masquaient le défaut.
Aucune perte : 0 ligne source ces deux jours. Preuve hors ligne, sans run :

    CronTriggerTimetable       part le 2026-09-01 00:00  intervalle [09-01, 09-01)
    CronDataIntervalTimetable  part le 2026-09-02 00:00  intervalle [09-01, 09-02)

Correction : calendrier à intervalles et garde-fou dans `fenetre()`. DAG
chargé sans erreur, fenêtre du 10/09 acceptée, fenêtre du 11/09 refusée.

**2. Le plafond de 2 min n'avait jamais été commité.** Décidé au jour 14
(ADR-025) et appliqué dans le dossier de travail que lit le conteneur :
les mesures du 1/2 ont tourné sur du code non versionné. HEAD gardait
10 min : un clone aurait détecté S1b en ≈ 30 min 41 s (calculé).

**3. `raw_booking.hotels` n'existe pas, `v_hotels` est cassée.**
Explication probable : les hôtels ne sont jamais modifiés, et le seed
étale leur `updated_at` sur 730 jours, avant le `start_date`. L'extraction
par fenêtre ne voit que ce qui change, jamais l'état initial. Décision à
prendre avant dbt (ADR-029).

**4. Expiration à 60 jours au niveau du dataset**
(`defaultPartitionExpirationMs`, `defaultTableExpirationMs`) : une
partition de plus de 60 jours est supprimée, retraiter 3 mois est
impossible en l'état. `bookings` n'a pas d'expiration de table, raison non
établie.

**5. Un test vert sans assertion.** La copie de travail de
`test_callbacks.py` avait perdu l'`assert` de `test_classement` et le cas
`S1b` : 10 passed sur un test vide. Restauré depuis HEAD, 11 passed. À
bloquer en CI (ruff `B018`, jour 27).

**6. 17 fichiers stockés en CRLF** (Makefile, Dockerfile, SQL d'init,
simulateur...) : `.gitattributes` ne convertit qu'à la réindexation.
Normalisés ce jour ; c'est aussi pourquoi le commit du plafond réécrit tout
le DAG.

**7. Numérotation des ADR décalée de 7** depuis le jour 13. L'ADR du
timeout n'avait jamais été ajoutée : son bloc « Vérifié » était collé sous
l'ADR-026. Ajoutée comme ADR-027 ; renvois corrigés dans le runbook, le
code du jour 15 et ce journal ; note de correspondance dans DECISIONS.md.

**8. Écarts pour le test** : section « Lancer » du README restée au jour 5
(ni GCP ni Airflow) ; `FERNET_KEY` exigée par le compose et absente de
`.env.example` ; `container_name` fixes ; `./state` encore monté, vestige
du watermark.

**Prédictions pour le test depuis zéro (3/3)**, écrites le 11/09 après
lecture, avant tout test :
1. Phase A : `make check` échoue dès sa première commande, le compose
   exigeant des variables absentes ou vides (`FERNET_KEY`, `GCP_PROJECT_ID`).
2. `AIRFLOW_UID` absent : UID 50000 par défaut, fichiers d'Airflow sur
   l'hôte appartenant à un autre utilisateur (probable).
   `BQ_MAX_BYTES_BILLED` absent : défaut de 10 Gio, sans effet visible.
3. Le clone ne démarre pas tant que les conteneurs d'origine existent
   (`container_name`, ports 5432 et 8080).
4. `data/` et `state/` créés en root par Docker : `PermissionError` au
   premier `extract` (probable).
5. DAG en pause à la création : aucun run sans activation.
6. Rattrapage le 12/09 : 11 runs (fenêtres du 01/09 au 11/09), un par un,
   aucun pour le 12/09 ; `fenetre_inachevee` et `intervalle_nul` à `f`
   partout.
7. Sans ADR-029 : seules les lignes dont le dernier `updated_at` est
   postérieur au 01/09 arrivent ; `raw_booking.hotels` jamais créée.
8. Fenêtres sans ligne : `load` en `skipped`, aucune partition créée.
9. Temps du clone au dernier run vert : ____ min, build de l'image compris.

**Non fait.**
- Transition du calendrier sur la base de métadonnées actuelle : non
  vérifiée.
- Renvois d'ADR antérieurs au jour 15 dans le code et la documentation : à
  relire (`git grep "ADR-0"`).
- ADR-029 (chargement initial), test depuis zéro, démo, rétrospective.
- `test_idempotence.py` obsolète ; `test_extract.py` à vérifier.

**Test depuis zéro (12/09)** — clone de origin/main, README suivi à la
lettre, terminal neuf. Chronomètre au clone. Journal des écarts :

## Rétrospective du sprint 3 (jours 11 à 15)

**Objectif du sprint** : le pipeline s'exécute seul, se relance en cas
d'échec et sait rattraper le passé. Atteint, mais deux défauts de fond ont
été trouvés en fin de sprint, et ni l'un ni l'autre n'a été révélé par un
test.

### Ce qui a le mieux marché

**Prédire avant de mesurer.** L'échec définitif de l'expérience C a été
prédit à 2 s près (14:43:00 annoncé, 14:42:58 mesuré), et la détection
après correction à une demi-seconde près (6 min 43 s contre 6 min 44 s).
Une prédiction écrite avant transforme une observation en expérience : si
elle tombe juste, le modèle est bon ; sinon, on a appris quelque chose. Les
prédictions réfutées ont été les plus instructives — le blocage indéfini
attendu de `pause` était en réalité un timeout de 130 s que personne
n'avait choisi.

**Les métadonnées comme source de preuve.** Le défaut du calendrier a été
prouvé par une requête sur `dag_run` et par deux appels aux timetables,
sans lancer un seul run. Mesurer sans exécuter évite de modifier ce qu'on
observe.

### Ce qui a coûté le plus cher

**Supposer au lieu de vérifier.** Quatre fois le même schéma :
- le plafond de relance de 2 min (ADR-025) appliqué dans le dossier de
  travail mais jamais commité : toutes les mesures du jour 15 ont tourné
  sur du code absent de HEAD ;
- un commit annonçant « Voir ADR-029 » alors que l'ADR n'existait pas ;
- le README déclaré corrigé alors qu'il était resté au jour 5 ;
- `make check` lancé dans le dossier de développement au lieu du clone,
  détruisant le volume Postgres.

Chaque fois, dix secondes de vérification auraient suffi. Coût cumulé :
environ une heure, plus une base à reconstruire.

**Des tests qui survivent à la refonte qu'ils devaient valider.**
`test_extract.py` était cassé depuis le jour 13 : il appelait `extract.py`
sans les arguments devenus obligatoires et dépendait d'une fixture définie
dans un autre fichier. Il n'a jamais été exécuté depuis, parce que
personne ne lançait la suite complète. `test_idempotence.py`, marqué
`bigquery`, est exclu par défaut et référence encore un watermark
supprimé.

### Trouvé par hasard, aurait dû l'être par un test

**Les runs planifiés lisaient une journée à venir.** Airflow 3 fixe
`logical_date` à l'heure du déclenchement : le run du 9 septembre s'est
terminé à 11h52 après avoir lu la journée du 9, non close. Vert, et faux.
Découvert en lisant le code pour préparer autre chose — ni un test ni une
alerte ne l'a signalé. C'est le scénario S3 : aucune tâche n'échoue, aucun
callback ne part, le journal reste vide.

**80 % des faits n'arrivaient jamais en cible.** L'extraction par fenêtre
ne voit que ce qui change. Invisible sur la base de développement par
coïncidence de dates ; 1 596 réservations sur 2 000 manquantes sur une
base neuve. Trouvé en réinstallant le projet depuis zéro, pas autrement.

### Ce qu'on change pour le sprint 4

1. **Vérifier avant d'affirmer.** Un commit qui cite une ADR suppose
   qu'elle existe ; une correction annoncée suppose qu'elle est écrite.
   `grep` et `git diff` coûtent dix secondes.
2. **`pwd &&` devant toute commande destructrice** tant que deux
   environnements coexistent. `make check` détruit sans confirmer.
3. **Des tests qui cassent quand la donnée est fausse**, pas seulement
   quand le code plante. C'est exactement l'objet des tests dbt du jour 20,
   et la seule vraie réponse au scénario S3.
4. **Lancer la suite complète, pas un fichier.** Trois tests cassés
   depuis le jour 13 sont passés inaperçus faute de `pytest tests/`.

### Chiffres du sprint

| | |
|---|---|
| Incidents mesurés | 4 (S1 ×2, S1b ×2), latence de 6 min 44 s à 30 min 12 s |
| Détection S1b après correction | 6 min 44 s contre 14 min 43 s, soit ÷ 2,2 |
| Tests depuis zéro | 2 — 11 écarts au premier, 0 casse au second |
| Réconciliation finale | 2 000 = 1 970 en cible + 30 en fenêtre non close |
| ADR écrites | 025 à 029 |

## Jour 16 — dbt : mise en place (sprint 4)

**Fait** : projet `dbt/booking_analytics` initialisé sans assistant
(`--skip-profile-setup`), profil en variables d'environnement, macro de
schéma, modèle jetable construit puis supprimé. ADR-030. Relecture de
DECISIONS.md : renvois cassés corrigés, statuts de remplacement ajoutés.

**Contrôle préalable raw vs source** (4 tables) :
| Table | Source | Raw (lignes) | Raw (clés) | Écart |
|---|---|---|---|---|
| customers | 500 | 498 | 498 | 2 |
| hotels | 50 | 50 | 50 | 0 |
| bookings | 2 000 | 1 970 | 1 970 | 30 |
| payments | 1 720 | 1 675 | 1 675 | 45 |

Écart entièrement expliqué : exactement 2 / 0 / 30 / 45 lignes source ont un
`updated_at` postérieur à la fin de la dernière fenêtre chargée (2026-09-14).
Fenêtre non close, pas une perte.
Aucun doublon (lignes = clés) : environnement reconstruit au jour 15, chaque
ligne n'a changé que dans une fenêtre. La déduplication des `stg_*` (jour 17)
sera donc invisible sur ces données — il faudra modifier une ligne sur deux
journées pour la tester réellement.

**Preuve** :
- `dbt debug` : All checks passed
- manifest : `smoke_test` → dataset `staging_booking`, vue (confirmé,
  aucun `staging_booking_staging_booking`)
- `smoke_test` : `count(*)` sur `raw_booking.bookings`, soit 1 970 lignes
  (valeur remesurée après suppression du modèle, aucun chargement entre-temps)

**Incidents** :
- DECISIONS.md, JOURNAL.md et README.md retrouvés modifiés sans commit,
  dans une version antérieure au jour 15 (ni ADR-027 à 029, ni
  rétrospective). Probable onglet d'éditeur resté ouvert puis sauvegardé.
  Détecté parce que le script de l'ADR-030 exigeait l'ADR-029. Copies
  gardées dans ~/sauvegarde-j16, fichiers restaurés depuis Git.
  Cause exacte : inconnue. Hypothèse non vérifiée : onglet d'éditeur
  ouvert avant le jour 15, puis sauvegardé.
- `cd dbt && dbt init` lancé depuis `dbt/` : chaîne arrêtée par `&&`, mais
  `mkdir` lancé séparément au mauvais endroit. `pwd &&` n'est plus une option.
- `python` absent sur Ubuntu (`python3`) : venv non créé, `pip install`
  refusé par le Python système (PEP 668), `pip freeze` système écrit dans
  `requirements.txt`. Corrigé en vérifiant le `dbt` actif avant d'écrire.
- `.user.yml` commité (identifiant anonyme de dbt) : retiré avant push.

**Pour le jour 17** : les vues `v_*` de `staging_booking` sont celles de
l'ADR-013, qui prévoyait leur remplacement par dbt. Avant de les supprimer,
chercher ce qui les lit encore (`git grep -n "v_bookings\|v_hotels"`),
notamment `tests/test_idempotence.py`.

## Jour 17 — Staging dbt (sprint 4)

**Fait** : sources déclarées avec fraîcheur, quatre vues `stg_*`, 10 tests de
données, 2 tests unitaires, vues `v_*` supprimées (seul lecteur :
`sql/checks/raw_doublons.sql`, repointé sur `stg_*`). ADR-031.

**Inspection** :
- Source et raw sans aucun défaut : volumes du seed exacts (500 / 50 / 2 000),
  le simulateur n'avait pas tourné depuis le jour 15.
- Doublon de paiement : nouveau `payment_id`, invisible pour une
  déduplication par clé primaire → clé métier (ADR-031).

**Fraîcheur** : `ERROR STALE` sur customers, bookings et payments (Airflow
arrêté depuis le jour 15) ; hotels exclue, 3 sources testées.
Prédiction : non faite avant l'exécution — le protocole « prédire, puis
mesurer » n'a pas été appliqué ici.

**Tests unitaires** : `PASS=2`. Mutation `order by updated_at asc` →
`FAIL`, diff `confirmed→pending` sur la réservation 1 ; fichier restauré.
`dbt build` exécute les tests unitaires avant de créer la vue concernée.

**Build** : `PASS=16`, vues créées sans aucun octet traité. Le vert porte sur
une raw sans défaut : il prouve que les modèles tournent, pas qu'ils corrigent.

**Simulateur** : 10 min à `--defect-rate 0.1`, interrompu au tour 7, 6 défauts
affichés. Source ensuite : 4 devises non normalisées, 1 double soumission,
3 montants à zéro, 5 emails partagés, 3 paiements orphelins,
4 surencaissements (20 au total).
Vérité terrain (`state/simulation_log.jsonl`, champ `kind`), 2026-09-17 :

| Défaut | Journal de simulation | Constaté en source |
|---|---|---|
| `currency_case` | 4 | 4 |
| `duplicate_payment` | 1 | 1 |
| `zero_amount` | 3 | 3 |
| `duplicate_email` | 5 | 5 |
| `orphan_payment` | 3 | 3 |
| `overpayment` | 1 | 4 |

Tout écart entre les deux colonnes est à expliquer au début du jour 18.
Pistes : une rafale non notée, ou des défauts qui se chevauchent (un montant
mis à zéro rend « surencaissés » tous les paiements de la réservation).

**Incidents** :
- Dates de l'ADR-030, de l'ADR-031 et de ce journal écrites sans vérification ;
  contrôlées après coup (Git, journal de simulation) : elles étaient justes.
- Fichier `EUR` créé à la racine : ligne SQL `currency <> 'EUR'` exécutée
  par bash, où `<>` est une redirection qui crée le fichier.
- Terminal dbt resté en `(.venv)` après le lancement du simulateur : `dbt`
  introuvable, deux fois. Le garde-fou `[ ... ]` a bien bloqué la chaîne, mais
  en silence → il affiche désormais `ECHEC : mauvais venv`.
  Règle : un terminal par environnement.
- `\ ` (espace après la barre oblique) dans une commande collée : la
  continuation de ligne casse.

**Pour demain (avant le jour 18)** : charger la journée du 17/09, close à
minuit, par le DAG et non à la main (ADR-027). Puis, dans `stg_*` : devises
toutes `EUR` alors que la raw en contient 4 autres ; exactement 1
`is_duplicate_submission` ; 1 réservation supprimée restée fantôme.

## Jour 17 (suite) — Chargement des journées manquées

**Prédiction** — calculée par l'assistant, pas par moi, avant le démarrage
d'Airflow (horodatée par le commit qui la contient). Méthode : lignes source
dont `updated_at` tombe dans chaque fenêtre UTC, c'est-à-dire exactement ce
que l'extraction doit lire.

- Dernière journée chargée : 2026-09-17 ; aujourd'hui (UTC) : 2026-09-22
- Runs de rattrapage : 4, du 2026-09-18 au 2026-09-21
  (réserve : Airflow compte ses runs dans sa base de métadonnées ; une
  journée déjà traitée à vide n'a laissé aucune partition, ADR-020)

| Journée | customers | hotels | bookings | payments | Attendu |
|---|---|---|---|---|---|
| 2026-09-18 | 0 | 0 | 0 | 0 | vide : `load` `skipped` partout |
| 2026-09-19 | 0 | 0 | 0 | 0 | vide : `load` `skipped` partout |
| 2026-09-20 | 0 | 0 | 0 | 0 | vide : `load` `skipped` partout |
| 2026-09-21 | 0 | 0 | 0 | 0 | vide : `load` `skipped` partout |

- Chaque partition chargée doit contenir exactement ces chiffres. Un écart
  est un défaut du pipeline, pas de la source.
- Une ligne modifiée plusieurs fois ne compte qu'une fois (état final).
  La suppression de la simulation reste invisible (limite du jour 10).
- Fraîcheur après chargement : **error**, âge 106 h — dernière
  journée active 2026-09-17, donc `loaded_at` = 2026-09-18 00:00 UTC.
  Une source calme ressemble à un pipeline arrêté (ADR-031, décision 4).

**Reconstruction de la raw — prédiction** (avant exécution) : 21 runs de
backfill (01/09 → 21/09) plus le snapshot du 31/08 ; chaque partition égale
aux lignes source de sa fenêtre ; décalage raw − source à 0,0 s partout ;
doubles soumissions 1 = 1 ; `stg_bookings` à 2 114 lignes, soit les 8 fantômes
disparus — la reconstruction lit l'état actuel, les lignes supprimées n'y
figurent plus (la sauvegarde conserve la preuve).

**Correction de la prédiction** : `list-runs` montre que les runs des
journées du 17 au 21/09 ont démarré le 22/09 entre 09:57 et 09:58 UTC, au
redémarrage de Docker (politique de redémarrage des conteneurs), donc
**avant** le commit de la prédiction. Les « 4 runs de rattrapage » étaient
déjà exécutés : ce n'était pas une prédiction. Le tableau par journée reste
juste (journées du 18 au 21 vides en source).

**Vérification de staging sur données réelles** : partition du 17/09
identique à la source (76 / 230 / 94) ; devise 4 en raw → 0 en staging ;
fantômes 8 = 8 `hard_delete`. Mais doubles soumissions : source 1, stg **0**.

**Diagnostic** : paiement 1106 en raw avec `paid_at` 11:09:40.47, contre
11:07:10.26 en source. Décalage raw − source mesuré par partition : +150,2 s
sur le 31/08, du 01 au 04/09 et du 06 au 10/09 ; 0,0 s sur le 05/09 et du 11
au 17/09. Deux instances de source mélangées dans la raw → ADR-032.

**Écart 6 → 20 expliqué** (journal de simulation, champ `kind`) : le
simulateur a tourné bien plus que les 7 tours affichés (122 réservations
insérées). 17 défauts journalisés ; les 3 de plus viennent des montants mis
à zéro, qui rendent « surencaissés » les paiements existants.

**Reconstruction** : sauvegarde, suppression des partitions (12, 2, 16, 16),
snapshot (49 / 413 / 1 514 / 1 388), backfill de 21 runs en ~2 min 15 s,
aucun échec. Réconciliation conforme sur tous les critères de la prédiction.
Script devenu permanent : `sql/checks/reconciliation_raw.py`.

**Leçons** :
- Des comptages égaux ne prouvent pas que deux jeux de données sont les
  mêmes. Il faut comparer des valeurs.
- Le test depuis zéro du jour 15 a réussi sur son propre critère, et
  corrompu la raw en silence.
- Airflow redémarre avec Docker et rattrape seul les journées manquées :
  à noter dans le runbook.

**À reporter dans `docs/limites.md`** : expiration à 60 jours **par date de
partition** (bac à sable) ; le snapshot du 31/08 disparaîtra vers le 30/10.
Sauvegardes `*_avant_reconstruction` conservées comme pièce à conviction.

## Jour 18 — Modèle dimensionnel (sprint 4)

**Grain de `fct_bookings`** : une ligne = une réservation, dans son dernier
état connu. Écarté : une ligne par nuit (aucune question d'occupation), une
ligne par changement de statut (impossible en batch, jour 10).
Questions du dashboard (US-29) : introuvables dans le dépôt — à écrire au jour 29, et à confronter au grain.

**Inspection** : réservations du 16/06 au 17/09, départs jusqu'au 18/02/2027 ;
paiements tous `captured` ; 305 réservations sans paiement, 1 808 avec un,
1 avec deux (la double soumission) ; 3 paiements orphelins hors grain ;
séjours de 1 à 14 nuits ; 3 montants à zéro.
Réservations sans paiement, par statut : completed 0/1140, confirmed 35/704, cancelled 195/195, pending 75/75.

**Prédiction** (requête sur staging, avant construction) : 757 réservations
antérieures au 24/07 perdues si le fait est partitionné en bac à sable.
**Mesure** : 757 exactement, première date = 2026-07-24. Voir ADR-033.

**Essai d'une expiration à 3 650 jours** : erreur dbt, mais table complète
avec l'expiration demandée — état incohérent, option abandonnée.

**Modèle final** : `dim_hotels`, `dim_customers` (type 1, SCD2 demain),
`dim_dates` (plage fixe 2026-2027), `int_payments_by_booking`, `fct_bookings`
clusterisée. `dbt build` : `PASS=35`. Requête d'analyste sur l'étoile :

| Mois | Réservations | Réservé (€) | Encaissé (€) | Annulées |
|---|---|---|---|---|
| 2026-06 | 298 | 345226.15 | 317582.75 | 20 |
| 2026-07 | 623 | 751320 | 670897.95 | 57 |
| 2026-08 | 667 | 767242.75 | 659132.4 | 61 |
| 2026-09 | 526 | 605470.8 | 451485.25 | 57 |

**Incidents (de mon fait)** :
- Commentaire `#` dans un bloc Jinja `config()` : erreur de compilation.
- `dbt build --select +fct_bookings+` ne construit pas les dimensions (ni
  ancêtres ni descendants du fait) : tests `relationships` en erreur.

## Jour 19 — SCD2 (sprint 4), 23 et 24/09

*Entrée rédigée le 24/09 : celle du 23/09 n'avait pas été écrite.*

**Prévu** : snapshot dbt. **Réalisé** : SCD2 recalculé depuis la raw (ADR-034).

### 23/09 : pourquoi pas de snapshot

| Mesure | Ma prédiction | Prédiction Claude | Résultat |
|---|---|---|---|
| Snapshot : second run | non notée | réussite | refusé (DML interdit en bac à sable) |
| Raw : clients à ≥ 2 versions | non notée | > 0 | 0 |
| Sauvegarde du 12/09 = même base ? | non notée | non | non (0 date de création commune) |
| Réservations sans version, jointure du plan | non notée | ~430 | 297 |
| `dbt build` | non notée | PASS=40 | PASS=40 |
| Clients modifiés par la rafale | non notée | 20–80 | 188 |

### 24/09 : chargement et preuve

Chargement de l'intervalle du 23/09 par backfill (`--reprocess-behavior none`).
Resté en `queued` tant que le DAG était en pause ; après la remise en service,
2 runs planifiés en plus (21 et 22/09, 0 ligne). Raw : 188 clients (= Postgres),
531 réservations ; partitions du 31/08 et du 17/09 intactes.

| Mesure | Ma prédiction | Prédiction Claude | Résultat |
|---|---|---|---|
| Réservations en raw au 23/09 | non notée | 350–500 | 531 (oubli des 127 réservations vieillies) |
| Clients à 2 versions | non notée | 184 | 184 |
| Paliers changés | non notée | ~90 | 131 (71 % des modifications) |
| Réservations au mauvais palier, jointure naïve | non notée | ~350 | 522 sur 2 451 (21 %) |
| Clients utilisables pour la démonstration | non notée | 10–40 | 36 |
| Réservations sans version | non notée | 0 | 4 (dimension tardive) |

**Preuve** : le client 499 réserve le 23/09 à 11:51 en `silver`, puis à 11:57
en `gold`. Le client 318 est `standard` sur 7 réservations de juin à
septembre, puis `gold` le 23/09. Une jointure naïve afficherait `gold` partout.

**Dimension tardive** : 4 réservations (clients 501 à 504), antidatées de 2 à
3 jours, sans version. Le test `not_null` a échoué ; première version désormais
valide depuis toujours, cas exposés par `is_late_arriving_customer`. PASS=40.

**Leçons** :
- Idempotent n'est pas reproductible : un rejeu relit une source mutable
  (87 historiques perdus le 22/09).
- Même nom ≠ même client après une remise à zéro (Faker à graine fixe).
- Le catchup d'Airflow agit comme un backfill silencieux.
- Mon runbook contenait une commande qui n'existe pas (`backfill list`).
- Prédictions du 23/09 non écrites : on ne peut plus savoir ce que j'attendais.

**Reste** : dataset `doit_echouer` (journal d'audit), à traiter au jour 20.
