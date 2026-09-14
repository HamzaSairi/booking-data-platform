.PHONY: up down reset seed simulate age start test lint check install airflow airflow-down snapshot
#                                    ^^^^^^^^^  ← ajoute ces deux noms

install:
	python -m venv .venv
	.venv/bin/pip install -q -r simulator/requirements.txt \
	  -r ingestion/requirements.txt -r requirements-dev.txt
	@echo "Fait. Active l'environnement : source .venv/bin/activate"

# Airflow est derriere un profil Compose : il ne demarre qu'ici.
airflow:
	docker compose --profile airflow up -d
	@echo "UI : http://localhost:8080 — activer le DAG : make dag-on"

airflow-down:
	docker compose --profile airflow down

dag-on:
	docker compose exec airflow-scheduler airflow dags unpause ingestion_batch

snapshot:
	python -m ingestion.snapshot

up:
	docker compose up -d
	@echo "Attente de Postgres..."
	@until docker compose exec -T postgres pg_isready -U booking -d booking_db -q; \
	  do sleep 1; done
	@echo "Postgres pret."

down:
	docker compose down

reset:
	docker compose down -v
	rm -rf data/
	$(MAKE) up
	$(MAKE) seed

seed:
	python simulator/generate.py seed

age:
	python simulator/generate.py age

# Le rituel du matin : infra + rattrapage du calendrier.
start: up age

simulate:
	python simulator/generate.py simulate --minutes 5 --defect-rate 0.05

test:
	pytest -q

lint:
	ruff check simulator/ tests/

check: reset test
	@echo "OK — reproductibilite verifiee."