{{ config(severity='warn') }}
-- Défaut injecté (orphan_payment) : voir le modèle orphan_payments.
select payment_id, booking_id from {{ ref('orphan_payments') }}
