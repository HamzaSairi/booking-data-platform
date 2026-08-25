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
**Date** : 2026-08-25 · **Temps passé** : ~3 h

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