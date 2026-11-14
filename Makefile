.PHONY: install lint typecheck test integration check up down logs live clean

install:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy

test:
	pytest -m "not integration"

integration:
	pytest -m integration --no-cov

check: lint typecheck test

up:
	docker compose up -d --build
	@echo "Kafka UI on http://localhost:8088"

live:
	docker compose --profile live up -d --build live

down:
	docker compose --profile live down

logs:
	docker compose logs -f demo

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage
