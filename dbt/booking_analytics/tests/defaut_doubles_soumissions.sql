{{ config(severity='warn') }}
-- Défaut injecté (duplicate_payment) : marqué en staging, exclu de amount_paid.
select payment_id from {{ ref('stg_payments') }} where is_duplicate_submission
