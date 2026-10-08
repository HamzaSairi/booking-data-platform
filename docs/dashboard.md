# Dashboard — US-29

Source unique : `marts_booking.rpt_bookings_dashboard` (vue). Aucune table raw
ou staging n'est exposée à l'outil de BI. Grain : une ligne = une réservation,
dans son dernier état connu (jour 18).

| # | Question | Définition exacte |
|---|---|---|
| Q1 | Combien rapporte chaque ville, mois par mois ? | SUM(total_amount) des réservations `confirmed` ou `completed` (séjours à venir ou réalisés), par `city` × `year_month` |
| Q2 | Quel est le taux d'annulation ? | cancelled / (confirmed + completed + cancelled) par semaine ISO ; les `pending` sont exclues du dénominateur |
| Q3 | Combien de temps à l'avance réserve-t-on ? | Répartition de `lead_time_days` en tranches (0-7, 8-30, 31-90, > 90 j) |
| Q4 | Quelle part du CA vient de chaque niveau de fidélité ? | CA par `tier_at_booking` (version SCD2 valide à la réservation), à côté du CA par `tier_current`. Indicateur : nombre de réservations où les deux diffèrent |
| Q5 | Le réservé est-il encaissé ? | SUM(total_amount) vs SUM(amount_paid) ; réservations sans paiement ; paiements orphelins (`orphan_payments`) |

Q4 est la démonstration du SCD2 : l'écart entre les deux séries est l'erreur
d'un analyste qui joindrait sur la version courante du client.
