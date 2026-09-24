{{ config(severity='warn') }}
-- Jour actif dont le volume dépasse 4 fois la médiane des 14 jours actifs
-- précédents. Information sur la source (rafale du simulateur), pas une panne.
-- Jours actifs seulement : le simulateur ne tourne pas tous les jours.
with jours as (
    select booking_date as jour, count(*) as n
    from {{ ref('fct_bookings') }} group by 1
),
fenetre as (
    select jour, n,
           array_agg(n) over (order by jour rows between 14 preceding and 1 preceding) as precedents
    from jours
),
ref as (
    select jour, n, array_length(precedents) as nb_ref,
           (select percentile_cont(x, 0.5) over () from unnest(precedents) as x limit 1) as mediane
    from fenetre
)
select jour, n, mediane, round(n / mediane, 1) as facteur
from ref
where nb_ref >= 7 and n > 4 * mediane
