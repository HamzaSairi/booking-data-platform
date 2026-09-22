-- Grain : une réservation ayant au moins un paiement encaissé.
-- Doubles soumissions exclues du montant encaissé (décision laissée aux
-- marts par l'ADR-031), mais comptées pour rester visibles.
select
    booking_id,
    count(*) as payments_count,
    countif(is_duplicate_submission) as duplicate_submissions_count,
    sum(if(is_duplicate_submission, 0, amount)) as amount_paid,
    min(if(is_duplicate_submission, null, paid_at)) as first_paid_at
from {{ ref('stg_payments') }}
where status = 'captured'
group by booking_id
