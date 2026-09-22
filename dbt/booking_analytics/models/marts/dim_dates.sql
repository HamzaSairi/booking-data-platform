-- Grain : un jour calendaire.
-- Plage fixe plutôt que déduite des données : la dimension reste stable
-- d'un run à l'autre. Couvre les réservations (depuis 06/2026) et les
-- départs (jusqu'à 02/2027) ; un test vérifie que le fait n'en sort pas.
select
    d as date_day,
    extract(year from d) as year,
    extract(quarter from d) as quarter,
    extract(month from d) as month,
    format_date('%Y-%m', d) as year_month,
    extract(isoweek from d) as iso_week,
    extract(dayofweek from d) as day_of_week,        -- 1 = dimanche (BigQuery)
    extract(dayofweek from d) in (1, 7) as is_weekend
from unnest(generate_date_array('2026-01-01', '2027-12-31')) as d
