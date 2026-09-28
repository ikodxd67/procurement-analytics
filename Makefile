.PHONY: venv install up down lint type test test-unit test-integration fmt migrate migration check-drift airflow-up airflow-down airflow-logs backfill seed-reference bench-sql api bench-api refresh-marts profile profile-debug bench-json

venv:
	py -3.12 -m venv .venv

install:
	.venv/Scripts/python -m pip install -U pip
	.venv/Scripts/python -m pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down

airflow-up:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build

airflow-down:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml down

airflow-logs:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f airflow-scheduler airflow-dag-processor

api:
	.venv/Scripts/uvicorn procurement.api.app:app --reload --port 8000

bench-api:
	.venv/Scripts/python scripts/bench_api.py

refresh-marts:
	.venv/Scripts/python -m procurement.jobs.cli marts

seed-reference:
	.venv/Scripts/python scripts/seed_reference.py

bench-sql:
	.venv/Scripts/python scripts/load_postgres_facts.py
	.venv/Scripts/python scripts/bench_sql.py

backfill:
	.venv/Scripts/python -m procurement.jobs.cli backfill --entity contracts --from 2023-01 --to 2025-12

migrate:
	.venv/Scripts/alembic upgrade head

migration:
	.venv/Scripts/alembic revision --autogenerate -m "$(m)"

check-drift:
	.venv/Scripts/alembic check

fmt:
	.venv/Scripts/ruff format src tests
	.venv/Scripts/ruff check --fix src tests

lint:
	.venv/Scripts/ruff check src tests
	.venv/Scripts/ruff format --check src tests

type:
	.venv/Scripts/mypy

profile:
	.venv/Scripts/py-spy record --subprocesses --rate 250 --format raw -o docs/bench/loader_folded_after.txt -- .venv/Scripts/python scripts/profile_loader.py --records 200000
	.venv/Scripts/python scripts/fold_summary.py docs/bench/loader_folded_after.txt

profile-debug:
	.venv/Scripts/python scripts/profile_loader.py --records 50000 --debug --slow-callback-s 0.02

bench-json:
	.venv/Scripts/python scripts/bench_json.py

test-unit:
	.venv/Scripts/pytest -m "not integration and not live"

test-integration:
	.venv/Scripts/pytest -m integration

test: test-unit
