-- Conservation : tout encaissement de staging se retrouve dans le fait,
-- ou est explicitement écarté (double soumission, paiement orphelin).
-- L'argent ne doit jamais disparaître entre deux couches.
with p as (select * from {{ ref('stg_payments') }} where status = 'captured'),
orphelins as (
    select coalesce(sum(p.amount), 0) as m
    from p left join {{ ref('stg_bookings') }} b using (booking_id)
    where b.booking_id is null and not p.is_duplicate_submission
),
bilan as (
    select
        (select sum(amount) from p) as encaisse_staging,
        (select sum(amount_paid) from {{ ref('fct_bookings') }})
          + (select coalesce(sum(amount), 0) from p where is_duplicate_submission)
          + (select m from orphelins) as ventile
)
select * from bilan where encaisse_staging != ventile
