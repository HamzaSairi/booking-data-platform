# Batch vs CDC — ce que chaque approche voit

Mesures du 01/10/2026 sur `booking.public.bookings` (Debezium 3.0.8, pgoutput,
REPLICA IDENTITY FULL). La comparaison chiffrée complète viendra au jour 25.

## 1. L'expérience : annuler puis rétablir une réservation en 20 secondes

Réservation 9, `confirmed`. Deux transactions séparées de 20 s :
`confirmed → cancelled`, puis `cancelled → confirmed`. Puis un `DELETE`.

**Ce que voit le batch** (`WHERE updated_at > watermark`) :

| booking_id | status | updated_at |
|---|---|---|
| 9 | confirmed | 2026-10-01 15:01:18.766534+00 |

Aucun changement de statut visible : l'annulation n'a jamais existé.
Après le `DELETE`, le batch ne voit plus rien du tout, et la ligne reste
dans l'entrepôt comme réservation fantôme.

**Ce que voit le CDC** :

| offset | op | before.status → after.status | lsn | txId | délai commit → Debezium |
|---|---|---|---|---|---|
| 2847 | u | confirmed → cancelled | 34834928 | 908 | 321 ms |
| 2848 | u | cancelled → confirmed | 34884632 | 909 | 477 ms |
| 2849 | d | confirmed → (null) | 34893816 | 910 | 76 ms |
| 2850 | tombstone | clé `{"booking_id":9}`, valeur nulle | — | — | — |

Le `before` du message 2848 contient `cancelled` : l'état intermédiaire est
capturé. Les quatre événements d'une même clé arrivent dans l'ordre des LSN.

## 2. Anatomie d'un événement (le DELETE, offset 2849)

Clé : `{"booking_id": 9}` (pas de schéma embarqué : `schemas.enable=false`).

```json
{
  "before": {
    "booking_id": 9, "customer_id": 474, "hotel_id": 41,
    "check_in": 20739, "check_out": 20752,
    "status": "confirmed", "total_amount": "2691.00", "currency": "EUR",
    "created_at": "2026-09-07T13:13:12.306656Z",
    "updated_at": "2026-10-01T15:01:18.766534Z"
  },
  "after": null,
  "source": {
    "version": "3.0.8.Final", "connector": "postgresql", "name": "booking",
    "ts_ms": 1790866917741, "snapshot": "false", "db": "booking_db",
    "sequence": "[\"34893696\",\"34893816\"]",
    "schema": "public", "table": "bookings",
    "txId": 910, "lsn": 34893816, "xmin": null
  },
  "transaction": null,
  "op": "d",
  "ts_ms": 1790866917817
}
```

(Les champs `ts_us` et `ts_ns`, présents à la racine et dans `source`, sont omis.)

| Champ | Sens | À retenir pour le consommateur (jour 24) |
|---|---|---|
| `op` | `c` création, `u` mise à jour, `d` suppression, `r` instantané | absent du tombstone |
| `before` / `after` | état avant / après ; `before` complet grâce à REPLICA IDENTITY FULL | `after` nul sur `d` |
| `source.ts_ms` | heure du commit en base | heure métier de l'événement |
| `ts_ms` (racine) | heure de traitement par Debezium | `ts_ms - source.ts_ms` = délai de capture |
| `source.lsn` | position dans le WAL, strictement croissante | **clé d'ordre** pour la déduplication |
| `source.txId` | transaction Postgres | regroupe les changements d'une même transaction |
| `source.sequence` | [LSN du dernier commit, LSN courant] | ordre fin entre transactions |
| `source.snapshot` | `first`, `first_in_data_collection`, `true`, `last_in_data_collection`, `last` pendant l'instantané ; `"false"` ensuite | distinguer instantané et flux |

## 3. Pièges d'encodage

- **`DATE`** → entier, en jours depuis le 01/01/1970 : `check_in: 20739` = 13/10/2026.
- **`timestamptz`** → chaîne ISO 8601 en UTC (`...Z`).
- **`NUMERIC`** → chaîne (`"2691.00"`), grâce à `decimal.handling.mode=string`.
  Sans ce réglage : octets en base64.
- **Tombstone** → valeur nulle, clé présente. Un `json.loads` sur la valeur
  plante : cas à traiter explicitement.

## 4. Les messages d'instantané

L'offset 0 du topic : `op='r'`, `before` nul, `source.snapshot =
first_in_data_collection`. L'instantané initial du 25/09 a produit exactement
une ligne `r` par ligne en source (504 / 50 / 2 448 / 2 067). Le CDC capture
l'état **présent** au démarrage, pas l'historique antérieur au slot.
