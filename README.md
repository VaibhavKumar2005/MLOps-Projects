# MLOps Projects

This repository collects practical MLOps experiments and production-oriented ML engineering examples. The current flagship project is an event-driven stock market pipeline that demonstrates streaming data ingestion, feature engineering, model inference, monitoring, and reproducible experimentation.

## What is included

- A streaming ML workflow built around Kafka, Docker Compose, and MLflow
- A modular Python project structure for ingestion, transformation, prediction, and drift monitoring
- Reproducible data and experiment tracking using DVC and MLflow
- Documentation for setup, local execution, and deployment

## Repository layout

- [stock-market-mlops/](stock-market-mlops/) – the main streaming pipeline project
- [SETUP.md](SETUP.md) – environment setup instructions
- [DEPLOYMENT.md](DEPLOYMENT.md) – local and container deployment guidance
- [API.md](API.md) – API-related notes and reference material

## Quick start

1. Clone the repository:
   ```bash
   git clone https://github.com/VaibhavKumar2005/MLOps-Projects.git
   cd MLOps-Projects
   ```

2. Create and activate a Python environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install the project dependencies:
   ```bash
   cd stock-market-mlops
   pip install -e ".[dev]"
   ```

4. Configure the environment file:
   ```bash
   cp .env.example .env
   ```
   Update the values in [.env.example](stock-market-mlops/.env.example) and the copied [.env](stock-market-mlops/.env) file, especially the Twelve Data API key.

5. Start the services:
   ```bash
   docker compose up -d
   ```

6. Watch the logs or stop the stack when needed:
   ```bash
   docker compose ps
   docker compose logs -f
   docker compose down
   ```

## Documentation

- See [SETUP.md](SETUP.md) for local environment preparation.
- See [DEPLOYMENT.md](DEPLOYMENT.md) for Docker, MLflow, and operational guidance.
- See [stock-market-mlops/README.md](stock-market-mlops/README.md) for the detailed project overview.

## Current focus

- Build reliable ML pipelines with clear interfaces and data contracts
- Improve observability and monitoring for drift and model health
- Make deployment workflows repeatable and documentation-first

## Status

The repository is actively evolving and is intended as a practical portfolio project for MLOps engineering.
