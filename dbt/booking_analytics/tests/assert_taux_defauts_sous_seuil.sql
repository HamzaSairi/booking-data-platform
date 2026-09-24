-- Chaque défaut connu reste sous environ 3 fois son taux du 24/09 (ADR-036).
-- Au-delà, la source se dégrade : ce n'est plus le défaut habituel.
-- Seuils par défaut, car les populations diffèrent (504 clients, 2 451 réservations).
with f as (select * from {{ ref('fct_bookings') }}),
     p as (select * from {{ ref('stg_payments') }}),
     c as (select * from {{ ref('stg_customers') }}),
taux as (
    select 'montant_zero' as defaut, countif(is_zero_amount) as n, count(*) as total, 0.01 as seuil from f
    union all select 'surencaissement', countif(is_overpaid), count(*), 0.02 from f
    union all select 'client_tardif', countif(is_late_arriving_customer), count(*), 0.01 from f
    union all select 'double_soumission', countif(is_duplicate_submission), count(*), 0.01 from p
    union all select 'paiement_orphelin', (select count(*) from {{ ref('orphan_payments') }}), (select count(*) from p), 0.01
    union all select 'email_en_double',
        (select count(*) from (select email from c group by email having count(*) > 1)),
        (select count(*) from c), 0.04
)
select defaut, n, total, round(100 * safe_divide(n, total), 2) as pct, 100 * seuil as seuil_pct
from taux
where safe_divide(n, total) > seuil
