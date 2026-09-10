"""Rend importables les modules partagés des DAGs (airflow/dags/commun)
sans installer Airflow : le dag-processor ajoute ce dossier au chemin,
pytest non."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))
