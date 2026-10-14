SHELL := /bin/bash

.PHONY: up down restart logs ps smoke test benchmark clean topics

up:
	docker compose up --build -d
	@echo "Stack starting. Run 'make ps' and then 'make smoke'."

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

smoke:
	./scripts/smoke-test.sh

topics:
	docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

test:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -q -r requirements-dev.txt && pytest -q

benchmark:
	python3 scripts/benchmark_generator.py --events 100000
