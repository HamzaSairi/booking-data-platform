"""Chargement initial (snapshot) des lignes anterieures au start_date.

L'extraction par fenetre ne voit que ce qui change pendant la fenetre :
une ligne creee avant le start_date et jamais modifiee depuis n'arrive
jamais en cible (ADR-029). Ce module la charge une fois, a l'amorcage.

Bornes [EPOCH, CUTOFF) : exactement le complement des fenetres du DAG.
Aucun recouvrement, donc aucun doublon a arbitrer.

Partition dediee, datee de la veille du start_date : aucune fenetre
reguliere ne vise cette date, donc aucun `tasks clear` ne peut l'ecraser.

A lancer une seule fois, avant le premier run. Rejoue plus tard, il
melangerait deux instants dans la meme table : d'ou le garde-fou.
"""

from datetime import UTC, datetime

from google.api_core.exceptions import NotFound

from ingestion.bq import get_client
from ingestion.extract import (
    TABLES,
    connect,
    ecrire_parquet,
    extract_table,
    rel,
)
from ingestion.load import DATASET, load_one, projet

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CUTOFF = datetime(2026, 9, 1, tzinfo=UTC)          # start_date du DAG
DATE_SNAPSHOT = datetime(2026, 8, 31, tzinfo=UTC)  # sa veille


def partition_existe(table: str) -> bool:
    """La partition du snapshot contient-elle deja des lignes ?

    get_table accepte n'importe quel decorateur de partition, meme
    inexistante (verifie le 12/09) : tester l'absence de NotFound
    reviendrait a tester l'existence de la table. C'est num_rows qui
    distingue une partition ecrite d'une partition vide.
    """
    client = get_client()
    ref = f"{projet()}.{DATASET}.{table}"
    try:
        return client.get_table(f"{ref}${DATE_SNAPSHOT:%Y%m%d}").num_rows > 0
    except NotFound:
        return False


def snapshot_one(table: str, force: bool = False) -> int:
    if not force and partition_existe(table):
        print(f"{table:>10} : partition {DATE_SNAPSHOT:%F} deja ecrite, ignore "
              f"(--force pour ecraser)")
        return 0

    with connect() as conn:
        resultat = extract_table(conn, table, EPOCH, CUTOFF)

    if resultat is None:
        print(f"{table:>10} : 0 ligne anterieure au {CUTOFF:%F}")
        return 0

    lignes, colonnes = resultat
    chemin = ecrire_parquet(table, lignes, colonnes, DATE_SNAPSHOT)
    print(f"{table:>10} : {len(lignes)} lignes -> {rel(chemin)}")
    return load_one(table, [rel(chemin)], DATE_SNAPSHOT)


def run(force: bool = False) -> None:
    total = sum(snapshot_one(table, force) for table in TABLES)
    print(f"\nSnapshot termine : {total} lignes dans la partition "
          f"{DATE_SNAPSHOT:%F}.")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--force", action="store_true",
                   help="ecrase la partition du snapshot si elle existe")
    run(p.parse_args().force)
