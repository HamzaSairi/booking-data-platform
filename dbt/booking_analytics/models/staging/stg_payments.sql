-- Grain : un paiement (payment_id), dernière version connue.
-- Les doubles soumissions reçoivent un nouveau payment_id : la clé primaire
-- ne les voit pas. Elles sont MARQUÉES, pas supprimées (ADR-002).
-- Clé métier : même réservation, même moyen, même instant de paiement.
-- amount exclu (le défaut de surencaissement le modifie après coup) ;
-- paid_at nul = paiement non encore réalisé, jamais un doublon.
with dernieres_versions as (
    select
        payment_id, booking_id, amount,
        upper(trim(currency)) as currency,
        payment_method, status, paid_at, created_at, updated_at
    from {{ source('raw_booking', 'payments') }}
    qualify row_number() over (
        partition by payment_id order by updated_at desc, _interval_start desc) = 1
)
select
    *,
    paid_at is not null
      and row_number() over (
            partition by booking_id, payment_method, paid_at
            order by created_at, payment_id) > 1
      as is_duplicate_submission
from dernieres_versions
