-- En batch, une réservation ne quitte jamais fct_bookings : les suppressions
-- physiques sont invisibles (ADR-014). Le volume ne peut que croître ; une
-- baisse est une perte de données (partition expirée, jointure qui élimine).
-- Plancher mesuré le 24/09. À relever à chaque revue de sprint ; à revoir au
-- sprint 5, quand le CDC propagera les suppressions (ADR-036).
select count(*) as lignes, 2451 as plancher
from {{ ref('fct_bookings') }}
having count(*) < 2451
