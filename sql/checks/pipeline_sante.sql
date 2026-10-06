-- Jour 28 : santé du pipeline à partir de pipeline_metrics.
-- Remplacer PROJET par l'identifiant du projet GCP.

-- 1. Pipeline arrêté : aucun succès depuis plus de 26 h, par DAG.
--    Distingue enfin une source calme (fraîcheur en erreur, succès récents)
--    d'un pipeline mort (fraîcheur en erreur, aucun succès).
SELECT dag_id, MAX(horodatage) AS dernier_succes,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(horodatage), HOUR) AS heures
FROM `PROJET.raw_booking.pipeline_metrics`
WHERE evenement = 'succes'
GROUP BY dag_id
HAVING heures > 26;

-- 2. Tâche anormalement lente : plus de 2 fois sa médiane sur 14 jours.
WITH succes AS (
  SELECT dag_id, task_id, horodatage, duree_tentative_s
  FROM `PROJET.raw_booking.pipeline_metrics`
  WHERE evenement = 'succes'
    AND horodatage >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
), mediane AS (
  SELECT dag_id, task_id,
         APPROX_QUANTILES(duree_tentative_s, 2)[OFFSET(1)] AS med_s
  FROM succes GROUP BY 1, 2
)
SELECT s.dag_id, s.task_id, s.horodatage, s.duree_tentative_s, m.med_s
FROM succes s JOIN mediane m USING (dag_id, task_id)
WHERE s.duree_tentative_s > 2 * m.med_s
ORDER BY s.horodatage DESC;

-- 3. Incidents non couverts par le runbook (scenario_runbook nul).
SELECT exception_type, exception_message, COUNT(*) AS n, MAX(horodatage) AS dernier
FROM `PROJET.raw_booking.pipeline_metrics`
WHERE evenement IN ('echec', 'relance') AND scenario_runbook IS NULL
GROUP BY 1, 2 ORDER BY n DESC;
