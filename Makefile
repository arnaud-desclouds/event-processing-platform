SHELL := /bin/bash

.PHONY: help up down restart ps logs build clean topics psql redis-cli format lint test

help:
	@echo ""
	@echo "Targets:"
	@echo "  make up           Start the full stack"
	@echo "  make down         Stop the stack"
	@echo "  make restart      Restart the stack"
	@echo "  make ps           Show container status"
	@echo "  make logs         Tail all logs"
	@echo "  make build        Build app images"
	@echo "  make topics       Create default topics"
	@echo "  make psql         Open psql shell"
	@echo "  make redis-cli    Open redis-cli"
	@echo "  make format       Format Python code (ruff)"
	@echo "  make lint         Lint Python code (ruff)"
	@echo "  make test         Run tests (pytest)"
	@echo "  make clean        Remove volumes (DANGER)"
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

clean:
	docker compose down -v
