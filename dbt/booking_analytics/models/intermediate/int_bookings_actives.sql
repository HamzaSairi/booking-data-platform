-- Grain : une réservation existant en source, d'après le CDC.
-- Définition UNIQUE de « ce qui existe » : fct_bookings, orphan_payments et les
-- tests de montants la lisent tous. Deux définitions divergent toujours (03/10 :
-- paiements comptés deux fois, une fois dans le fait, une fois orphelins).
select * except (is_deleted, last_op, last_lsn)
from {{ ref('stg_bookings_cdc') }}
where not is_deleted
