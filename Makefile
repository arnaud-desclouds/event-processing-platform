SHELL := /bin/bash

.PHONY: help up down restart ps logs build clean topics psql redis-cli format lint test dlq-replay dlq-replay-dry load load-200rps load-1k

help:
	@echo ""
	@echo "Targets:"
	@echo "  make up              Start the full stack"
	@echo "  make down            Stop the stack"
	@echo "  make restart         Restart the stack"
	@echo "  make ps              Show container status"
	@echo "  make logs            Tail all logs"
	@echo "  make build           Build app images"
	@echo "  make topics          Create default topics"
	@echo "  make psql            Open psql shell"
	@echo "  make redis-cli       Open redis-cli"
	@echo "  make format          Format Python code (ruff)"
	@echo "  make lint            Lint Python code (ruff)"
	@echo "  make test            Run tests (pytest)"
	@echo "  make dlq-replay      Replay DLQ messages back to main topic"
	@echo "  make dlq-replay-dry  Dry-run DLQ replay"
	@echo "  make load            Run k6 load test (50 rps, 30s)"
	@echo "  make load-200rps     Run k6 load test (200 rps, 30s)"
	@echo "  make load-1k         Run k6 load test (1000 rps, 30s)"
	@echo "  make clean           Remove volumes (DANGER)"
	@echo ""

up:
	docker compose up -d
	$(MAKE) topics

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d
	$(MAKE) topics

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

build:
	docker compose build

topics:
	@echo "Creating topics (if they don't exist)..."
	@docker compose exec -T redpanda rpk topic create $${EVENTS_TOPIC:-events.raw} 2>/dev/null || true
	@docker compose exec -T redpanda rpk topic create $${DLQ_TOPIC:-events.dlq} 2>/dev/null || true

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-events_user} -d $${POSTGRES_DB:-events}

redis-cli:
	docker compose exec redis redis-cli

format:
	ruff format services/

lint:
	ruff check services/

test:
	pytest

dlq-replay:
	docker compose run --rm processor-worker python -m app.dlq_replay --from-beginning --commit-offsets

dlq-replay-dry:
	docker compose run --rm processor-worker python -m app.dlq_replay --from-beginning --dry-run --max-messages=50

load:
	docker compose run --rm -e RATE=50 -e DURATION=30s k6 run /scripts/k6-ingest.js

load-200rps:
	docker compose run --rm -e RATE=200 -e DURATION=30s k6 run /scripts/k6-ingest.js

load-1k:
	docker compose run --rm -e RATE=1000 -e DURATION=30s -e VUS=200 -e MAX_VUS=400 k6 run /scripts/k6-ingest.js

clean:
	docker compose down -v
