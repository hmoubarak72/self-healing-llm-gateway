.PHONY: setup up down logs test lint chaos demo

setup:
	cp -n .env.example .env || true
	pip install -r requirements.txt

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f gateway

test:
	pytest -v

lint:
	ruff check app tests scripts

chaos:
	python scripts/chaos_demo.py

demo:
	python scripts/load_generator.py --duration 90
