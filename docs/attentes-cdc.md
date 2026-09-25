# Attentes vis-à-vis du CDC — écrites avant le code

Point de départ : `docs/limites-batch.md` et ADR-014. Faits nouveaux depuis :
87 historiques effacés par un rejeu (22/09), au moins 57 modifications de
clients invisibles au batch (23/09), suppressions physiques absentes de la cible.

Note (25/09) : la première version de ce fichier (commit 561164c) recopiait les
prédictions de Claude sans les attribuer. Tableau reconstruit avec leur auteur ;
mes prédictions sur les questions déjà mesurées sont marquées « non notée ».

| # | Question | Jour | Ma prédiction | Prédiction Claude | Mesure |
|---|---|---|---|---|---|
| 1 | Délai UPDATE → message dans le topic | 22 | non notée | ~1 s, toujours < 5 s | 405 à 874 ms (commit → Debezium : 60 à 499 ms) |
| 2 | Deux UPDATE du même statut dans la minute : messages, et `before` du second | 23 | 2 msg op = u | 2 `op='u'` ; `before` = état intermédiaire | |
| 3 | Messages produits par un DELETE | 23 | 2 msg op = d | 2 : `op='d'` avec `before` complet, puis tombstone | |
| 4 | Messages `op='r'` par table à l'instantané | 22 | non notée | lignes en source ; bookings < 2 451 | 504 / 50 / 2 448 / 2 067 = source. bookings : 2 448 contre 2 451 en cible (3 suppressions du 23/09) |
| 5 | WAL retenu, rafale de 10 min, Connect arrêté | 22 | non notée | plusieurs dizaines de Mo | 1,5 Mo ; dossier WAL inchangé à 32 Mo (prédiction fausse d'un facteur 20 à 50) |
| 6 | Événements CDC par client modifié (rafale) | 24 | non notée | ~1,3 | 1,18 (212 messages pour 179 clients), mesuré dès le jour 22 |
| 7 | Écart de lignes actives source / cible après dbt | 25 | 0 écart | 0 | |
| 8 | Ce que le CDC ne résoudra pas | 25 | perte de message | passé antérieur au slot ; perte au-delà de la rétention du topic ; changements de schéma | |
| 9 | Fixer `max_slot_wal_keep_size` ? | 22 | non notée | oui, ~1 Go | 1 Go (ADR-037) ; marge mesurée 1 039 Mo |
