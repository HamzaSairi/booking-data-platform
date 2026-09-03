## Résultat 1 — Les transitions intermédiaires

| Mesure | Valeur |
|---|---|
| Transitions réellement survenues (audit, `op = U`) | 209 |
| Réservations concernées (clés distinctes) | 188 |
| Transitions au mieux capturables par un batch | 188 |
| **Transitions perdues** | **21 (10,0 %)** |

Distribution : 167 réservations ont changé d'état une fois, 21 en ont changé
deux fois. Ce sont ces 21 qui sont perdues — le batch les voit `cancelled` et
ne saura jamais qu'elles sont passées par `confirmed`.

Formulation : **une réservation sur neuf ayant changé d'état a changé plus
d'une fois, et la trace intermédiaire est définitivement perdue.**

Conséquence métier : impossible de répondre à « combien de temps une réservation
reste-t-elle en attente avant confirmation ? » ou « quel est le taux d'annulation
après confirmation ? ». Ce ne sont pas des questions exotiques.

## Résultat 2 — Les suppressions physiques

| Mesure | Valeur |
|---|---|
| Suppressions survenues (audit, `op = D`) | 14 |
| Réservations en source après campagne | 2 322 |
| Réservations dans `v_bookings` | 2 335 |
| **Lignes fantômes** | **13 (0,56 %)** |
| Détectées par le pipeline | **0** |

L'écart de 1 entre 14 suppressions et 13 fantômes n'est pas expliqué. Piste
probable : une ligne supprimée avant d'avoir jamais été ingérée. Non vérifié.

### Cas nominatif — réservation 931

Supprimée de Postgres le 2026-09-03 à 15:51:32. Toujours présente dans
`staging_booking.v_bookings`, statut `pending`, `updated_at` au 2026-08-05.

Un analyste qui interroge la cible voit une réservation en attente depuis
quatre semaines. Elle n'existe plus depuis des heures. La ligne est bien typée,
cohérente, et rien ne la distingue d'une ligne valide — c'est précisément ce
qui rend l'échec dangereux.

Aucune exécution future du pipeline ne la retirera : un `DELETE` ne modifie
aucun `updated_at`, il ne laisse aucune trace lisible par une extraction
incrémentale.

### Réserve méthodologique

14 suppressions sur 2 322 lignes est un ordre de grandeur, pas une mesure
statistiquement solide — le taux de suppression du simulateur est bas. Le
résultat 1 (188 observations) est en revanche robuste. Le chiffre à retenir
est le zéro : **aucun test, aucune alerte, aucun compteur du pipeline n'a
bougé.**

### Hypothèse formulée puis réfutée

J'ai supposé qu'existait un troisième mode d'échec : des lignes créées puis
supprimées entre deux extractions, invisibles partout — ni en source, ni en
cible, ni dans un écart de comptage. Mesure : 0. Le simulateur ne supprime que
des réservations `pending` anciennes, jamais celles qu'il vient de créer. Le
mode d'échec est réel dans l'absolu mais absent de ce jeu de données.

## Piège de mesure rencontré

Une première comparaison donnait 46 lignes fantômes ; la mesure propre en donne
13. L'écart venait du protocole : compte source et compte cible pris à des
instants différents pendant que le simulateur écrivait. On mesurait le bruit.

**Règle générale : une comparaison source/cible n'a de sens que sur une source
figée, ou sur un périmètre borné par une clé et un instant.** Même problème que
la limite de `test_idempotence.py`, et même problème que la réconciliation CDC
du jour 25.