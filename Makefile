.PHONY: venv install up down lint type test test-unit test-integration fmt

venv:
	py -3.12 -m venv .venv

install:
	.venv/Scripts/python -m pip install -U pip
	.venv/Scripts/python -m pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down

fmt:
	.venv/Scripts/ruff format src tests
	.venv/Scripts/ruff check --fix src tests

lint:
	.venv/Scripts/ruff check src tests
	.venv/Scripts/ruff format --check src tests

type:
	.venv/Scripts/mypy

test-unit:
	.venv/Scripts/pytest -m "not integration and not live"

test-integration:
	.venv/Scripts/pytest -m integration

test: test-unit
