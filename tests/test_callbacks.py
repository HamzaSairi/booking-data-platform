"""Le callback est le seul code qui tourne quand tout le reste a échoué.
S'il est cassé, personne ne le saura : Airflow avale ses exceptions.
D'où ces tests, exécutables sans Airflow installé."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from commun.callbacks import classer, sur_echec, sur_relance


def exception(nom: str, message: str) -> Exception:
    """Exception du nom voulu, sans importer psycopg ni google-cloud."""
    return type(nom, (Exception,), {})(message)


def contexte(exc, tentative=4, duree_s=12):
    ti = SimpleNamespace(
        dag_id="ingestion_batch",
        task_id="ingerer.extract",
        run_id="scheduled__2026-09-03T00:00:00+00:00",
        try_number=tentative,
        start_date=datetime.now(UTC) - timedelta(seconds=duree_s),
    )
    return {
        "ti": ti,
        "task": SimpleNamespace(retries=3),
        "exception": exc,
        "logical_date": datetime(2026, 9, 3, tzinfo=UTC),
    }


@pytest.fixture
def journal(tmp_path, monkeypatch):
    chemin = tmp_path / "evenements.jsonl"
    monkeypatch.setenv("PIPELINE_EVENTS_FILE", str(chemin))
    return chemin


def lire(chemin: Path) -> list[dict]:
    return [
        json.loads(ligne) for ligne in chemin.read_text(encoding="utf-8").splitlines()
    ]


def test_echec_ecrit_une_ligne_complete(journal):
    exc = exception("OperationalError", "connection failed: Connection refused")
    sur_echec(contexte(exc))

    (ev,) = lire(journal)
    assert ev["evenement"] == "echec"
    assert ev["task_id"] == "ingerer.extract"
    assert ev["logical_date"] == "2026-09-03T00:00:00+00:00"
    assert ev["tentative"] == 4
    assert ev["retries"] == 3
    assert ev["exception_type"] == "OperationalError"
    assert ev["scenario_runbook"] == "S1"
    assert 11 <= ev["duree_tentative_s"] < 60


def test_les_evenements_s_ajoutent_sans_ecraser(journal):
    exc = exception("ConnectionTimeout", "connection timeout expired")
    sur_relance(contexte(exc, tentative=1))
    sur_relance(contexte(exc, tentative=2))
    sur_echec(contexte(exc, tentative=4))
    assert [e["evenement"] for e in lire(journal)] == ["relance", "relance", "echec"]


@pytest.mark.parametrize(
    ("exc", "attendu"),
    [
        (exception("OperationalError", "failed to resolve host 'postgres'"), "S1"),
        (
            exception(
                "Forbidden",
                "403 Exceeded rate limits: too many table update operations",
            ),
            "S2a",
        ),
        (
            exception(
                "Forbidden", "403 Quota exceeded: Your table exceeded quota for imports"
            ),
            "S2b",
        ),
        (exception("ConnectionTimeout", "connection timeout expired"), "S1b"),
        (exception("AirflowFailException", "doublons sur booking_id"), None),
        (None, None),
    ],
)
def test_classement(exc, attendu):
    assert classer(exc) == attendu


@pytest.mark.parametrize(
    "ctx", [{}, None, {"ti": object(), "exception": "pas une exception"}]
)
def test_le_callback_ne_leve_jamais(ctx, tmp_path, monkeypatch, caplog):
    # Chemin impossible : le « dossier » parent est un fichier.
    obstacle = tmp_path / "obstacle"
    obstacle.write_text("")
    monkeypatch.setenv("PIPELINE_EVENTS_FILE", str(obstacle / "evenements.jsonl"))

    sur_echec(ctx)  # ne doit pas lever

    # ... mais l'échec du callback doit rester visible dans les logs.
    assert "événement perdu" in caplog.text
