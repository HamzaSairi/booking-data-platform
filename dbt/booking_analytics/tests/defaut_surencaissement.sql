{{ config(severity='warn') }}
-- Plan : « paiement <= montant réservé ». Défaut injecté (overpayment) : warn.
select booking_id, total_amount, amount_paid
from {{ ref('fct_bookings') }} where is_overpaid
