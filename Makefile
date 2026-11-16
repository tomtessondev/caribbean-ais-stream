.PHONY: install lint typecheck test integration check up down logs live e2e-up e2e fast-replay clean

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
		schema-registry kafka-exporter prometheus grafana
	docker compose run --rm --build topics

e2e:
	pytest -m e2e --no-cov

# Six hours of synthetic traffic, ending now, replayed sixty times faster on a fresh stack:
# ships visibly move on the map within minutes (used for the screenshots and the GIF, see
# docs/images/README.md). Grafana on http://localhost:3002.
fast-replay:
	$(MAKE) e2e-up
	docker compose run --rm --no-deps flink-jobs
	docker compose run --rm --no-deps --build -v ./data:/app/data topics \
		synthesize --hours 6 --out data/fast-replay.jsonl
	docker compose run --rm --no-deps -v ./data:/app/data topics \
		replay data/fast-replay.jsonl --speed 60

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage
