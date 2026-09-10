"""Callbacks de tâche : une ligne JSON par relance ou échec.

Chaque événement part vers deux destinations :
- le log de la tâche, pour l'humain qui ouvre l'UI ;
- un fichier JSONL partagé, pour la machine (grep, jq, et au jour 28
  la table pipeline_metrics).

Contraintes de conception :
- Aucun import d'Airflow ni de bibliothèque tierce. Le module se teste
  dans le .venv hors conteneur, et ne coûte rien au parse du DAG.
- Un callback ne lève JAMAIS. Airflow avale ses exceptions et les
  consigne hors du log de tâche : un callback cassé est un échec
  silencieux de la surveillance elle-même.
- Les champs sont choisis un par un. Sérialiser le contexte entier
  exposerait connexions et variables dans un fichier de log.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Motif cherché dans « TypeException message » (minuscules) -> scénario
# de docs/runbook.md. Un scénario null dans le journal signale un
# incident que le runbook ne couvre pas encore : c'est une information.
SIGNATURES: list[tuple[str, str]] = [
    ("rate limits", "S2a"),  # BigQuery, limite court terme
    ("quota exceeded", "S2b"),  # BigQuery, quota long terme
    ("connectiontimeout", "S1b"),  # psycopg : serveur muet (gelé, saturé)
    ("operationalerror", "S1"),  # psycopg : connexion refusée
]


def fichier_evenements() -> Path:
    """Lu à chaque appel et non à l'import : les tests le redirigent."""
    home = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
    defaut = home / "logs" / "pipeline" / "evenements.jsonl"
    return Path(os.environ.get("PIPELINE_EVENTS_FILE", defaut))


def classer(exc: BaseException | None) -> str | None:
    if exc is None:
        return None
    texte = f"{type(exc).__name__} {exc}".lower()
    for motif, scenario in SIGNATURES:
        if motif in texte:
            return scenario
    return None


def _iso(valeur) -> str | None:
    return valeur.isoformat() if hasattr(valeur, "isoformat") else None


def construire_evenement(context, evenement: str) -> dict:
    ti = context.get("ti")
    tache = context.get("task")
    exc = context.get("exception")

    # Horodatage d'OBSERVATION : now() est légitime ici. La règle du
    # jour 13 interdit l'heure courante pour décider QUELLES données
    # traiter ; un callback ne traite rien, il date un incident. Les deux
    # temps (horodatage, logical_date) sont consignés côte à côte.
    maintenant = datetime.now(UTC)
    debut = getattr(ti, "start_date", None)
    if debut is not None and debut.tzinfo is None:
        debut = debut.replace(tzinfo=UTC)

    return {
        "horodatage": maintenant.isoformat(),
        "evenement": evenement,
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "logical_date": _iso(context.get("logical_date")),
        "tentative": getattr(ti, "try_number", None),
        "retries": getattr(tache, "retries", None),
        # try_number est cumulé à travers les clear, et max_tries compte
        # les RELANCES : l'échec définitif s'écrit tentative = relances_max + 1
        # (mesuré au jour 14 : tentative 10, relances_max 9).
        "relances_max": getattr(ti, "max_tries", None),        "duree_tentative_s": (
            round((maintenant - debut).total_seconds(), 1) if debut else None
        ),
        "exception_type": type(exc).__name__ if exc is not None else None,
        # Tronqué : une trace psycopg ou BigQuery peut faire des Ko.
        "exception_message": str(exc)[:500] if exc is not None else None,
        "scenario_runbook": classer(exc) if isinstance(exc, BaseException) else None,
    }


def ecrire(evenement: dict) -> None:
    ligne = json.dumps(evenement, ensure_ascii=False, default=str)
    niveau = logging.ERROR if evenement["evenement"] == "echec" else logging.WARNING
    log.log(niveau, "PIPELINE_EVENT %s", ligne)

    chemin = fichier_evenements()
    chemin.parent.mkdir(parents=True, exist_ok=True)
    # Append d'une ligne courte en une écriture : les quatre tâches
    # parallèles partagent le fichier sans s'entrelacer.
    with chemin.open("a", encoding="utf-8") as f:
        f.write(ligne + "\n")


def _proteger(context, evenement: str) -> None:
    try:
        ecrire(construire_evenement(context, evenement))
    except Exception:
        # Dernier filet : on le dit dans le log, on ne relève jamais.
        log.exception("callback '%s' en échec : événement perdu", evenement)


def sur_relance(context) -> None:
    """on_retry_callback : la tentative a échoué, une relance est programmée."""
    _proteger(context, "relance")


def sur_echec(context) -> None:
    """on_failure_callback : relances épuisées, ou AirflowFailException."""
    _proteger(context, "echec")
