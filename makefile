.PHONY: up down reset seed simulate age start test lint check
#                                    ^^^^^^^^^  ← ajoute ces deux noms

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
	rm -rf state/ data/
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