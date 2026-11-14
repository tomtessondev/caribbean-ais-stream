.PHONY: install lint typecheck test integration check up down logs live e2e-up e2e clean

install:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy

test:
	pytest -m "not integration and not e2e"

integration:
	pytest -m integration --no-cov

check: lint typecheck test

up:
	docker compose up -d --build
	@echo "Kafka UI on http://localhost:8088, Flink on http://localhost:8082"

live:
	docker compose --profile live up -d --build live

down:
	docker compose --profile live down

logs:
	docker compose logs -f demo flink-jobs

# A fresh stack without the demo producer: the e2e tests own the traffic and its event
# times (old events would be late for a watermark that live traffic already pushed on).
# The tests submit the Flink job once their traffic is in Kafka.
e2e-up:
	docker compose down -v
	docker compose up -d --build --wait kafka postgres flink-jobmanager flink-taskmanager \
		kafka-exporter prometheus grafana
	docker compose run --rm topics

e2e:
	pytest -m e2e --no-cov

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage
