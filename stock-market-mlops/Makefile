.PHONY: help build up down logs clean test lint format install setup

help:
	@echo "Available commands:"
	@echo "  make setup          - Setup development environment"
	@echo "  make install        - Install dependencies"
	@echo "  make build          - Build Docker images"
	@echo "  make up             - Start all services"
	@echo "  make down           - Stop all services"
	@echo "  make logs           - View all service logs"
	@echo "  make logs-producer  - View producer logs"
	@echo "  make logs-features  - View feature engineering logs"
	@echo "  make logs-predictor - View predictor logs"
	@echo "  make logs-drift     - View drift monitoring logs"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Run linting"
	@echo "  make format         - Format code with black"
	@echo "  make clean          - Clean up containers, volumes, and cache"
	@echo "  make dvc-repro      - Run DVC pipeline"
	@echo "  make dvc-dag        - Show DVC pipeline DAG"
	@echo "  make mlflow-ui      - Open MLflow UI"

setup:
	@echo "Setting up development environment..."
	python -m venv venv
	./venv/bin/python -m pip install --upgrade pip
	./venv/bin/python -m pip install -r requirements.txt
	@if [ -f .env.example ]; then cp .env.example .env; fi
	@echo "Setup complete! Run 'source venv/bin/activate' (or venv\Scripts\activate on Windows)"

install:
	pip install -r requirements.txt
	pip install -e .

build:
	docker-compose build

up:
	docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
	@echo "Services starting... Check with 'make logs'"

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-producer:
	docker-compose logs -f producer

logs-features:
	docker-compose logs -f feature-engineering

logs-predictor:
	docker-compose logs -f prediction

logs-drift:
	docker-compose logs -f drift-monitor

test:
	@echo "Running tests..."
	pytest tests/ -v --cov=src

lint:
	@echo "Running linting..."
	flake8 src/ tests/
	black --check src/ tests/

format:
	@echo "Formatting code..."
	black src/ tests/

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage .mypy_cache
	@echo "Cleanup complete!"

dvc-repro:
	dvc repro

dvc-dag:
	dvc dag

mlflow-ui:
	mlflow ui --host 0.0.0.0 --port 5000

ps:
	docker-compose ps

restart:
	docker-compose restart

stop:
	docker-compose stop

start:
	docker-compose start
