-- Invariant (CHECK >= 0 en source) : aucun montant négatif, réservé ou payé.
select 'reservation' as objet, booking_id as id, total_amount as montant
from {{ ref('int_bookings_actives') }} where total_amount < 0
union all
select 'paiement', payment_id, amount
from {{ ref('stg_payments') }} where amount < 0
