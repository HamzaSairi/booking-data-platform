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

À réutiliser pour la question 3 d'entretien (« comment détectes-tu un échec
silencieux ? ») : celui-ci était silencieux parce qu'un seul chemin d'accès
était emprunté. La détection est venue de la diversité des clients, pas d'une
alerte.