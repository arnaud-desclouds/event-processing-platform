SHELL := /bin/bash
PYTHON := ./.venv/bin/python

.PHONY: help up down restart ps logs build clean topics psql format lint typecheck typecheck-common typecheck-worker typecheck-api test dlq-replay dlq-replay-dry load load-200rps load-1k migrate test-e2e

help:
	@echo ""
	@echo "Targets:"
	@echo "  make up              Start the stack, then create topics and run migrations"
	@echo "  make down            Stop the stack"
	@echo "  make restart         Restart the stack"
	@echo "  make ps              Show container status"
	@echo "  make logs            Tail all logs"
	@echo "  make build           Build app images"
	@echo "  make topics          Create default topics (best effort)"
	@echo "  make migrate         Apply database migrations"
	@echo "  make psql            Open psql shell"
	@echo "  make format          Format Python code (ruff)"
	@echo "  make lint            Lint Python code (ruff)"
	@echo "  make typecheck       Run mypy and pyright across shared code and both services"
	@echo "  make test            Run tests (pytest)"
	@echo "  make test-e2e        Run the Docker-backed end-to-end test"
	@echo "  make dlq-replay      Replay DLQ messages to the target topic"
	@echo "  make dlq-replay-dry  Dry-run DLQ replay"
	@echo "  make load            Run k6 load test (50 rps, 30s)"
	@echo "  make load-200rps     Run k6 load test (200 rps, 30s)"
	@echo "  make load-1k         Run k6 load test (1000 rps, 30s)"
	@echo "  make clean           Remove volumes (DANGER)"
	@echo ""

up:
	docker compose up --build -d
	$(MAKE) topics
	$(MAKE) migrate

down:
	docker compose down

restart:
	$(MAKE) down
	$(MAKE) up

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

build:
	docker compose build

topics:
	@echo "Ensuring required topics exist..."
	@for topic in $${EVENTS_TOPIC:-events.raw} $${DLQ_TOPIC:-events.dlq}; do \
		ok=0; \
		for attempt in 1 2 3 4 5; do \
			if docker compose exec -T redpanda rpk topic create "$$topic" >/dev/null 2>&1; then \
				echo "Topic $$topic created"; \
				ok=1; \
				break; \
			fi; \
			if docker compose exec -T redpanda rpk topic describe "$$topic" >/dev/null 2>&1; then \
				echo "Topic $$topic already exists"; \
				ok=1; \
				break; \
			fi; \
			echo "Retrying topic bootstrap for $$topic ($$attempt/5)..."; \
			sleep 2; \
		done; \
		if [ "$$ok" -ne 1 ]; then \
			echo "Failed to ensure topic $$topic after retries" >&2; \
			exit 1; \
		fi; \
	done

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-events_user} -d $${POSTGRES_DB:-events}

format:
	$(PYTHON) -m ruff format services tests alembic

lint:
	$(PYTHON) -m ruff check services tests alembic

typecheck:
	$(PYTHON) -m mypy services/common services/processor_worker services/ingestion_api
	$(PYTHON) -m pyright services/common services/processor_worker services/ingestion_api

typecheck-common:
	$(PYTHON) -m mypy services/common
	$(PYTHON) -m pyright services/common

typecheck-worker:
	$(PYTHON) -m mypy services/processor_worker
	$(PYTHON) -m pyright services/processor_worker

typecheck-api:
	$(PYTHON) -m mypy services/ingestion_api
	$(PYTHON) -m pyright services/ingestion_api

test:
	$(PYTHON) -m pytest

dlq-replay:
	docker compose run --rm processor-worker python -m services.processor_worker.dlq_replay --from-beginning --commit-offsets

dlq-replay-dry:
	docker compose run --rm processor-worker python -m services.processor_worker.dlq_replay --from-beginning --dry-run --max-messages=50

load:
	docker compose run --rm -e RATE=50 -e DURATION=30s k6 run /scripts/k6-ingest.js

load-200rps:
	docker compose run --rm -e RATE=200 -e DURATION=30s k6 run /scripts/k6-ingest.js

load-1k:
	docker compose run --rm -e RATE=1000 -e DURATION=30s -e VUS=200 -e MAX_VUS=400 k6 run /scripts/k6-ingest.js

clean:
	docker compose down -v

migrate:
	docker compose run --rm migrate

test-e2e:
	$(PYTHON) -m pytest -q tests/e2e
