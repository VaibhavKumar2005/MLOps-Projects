# Setup Guide

This guide covers the local setup for the streaming ML pipeline project in [stock-market-mlops](stock-market-mlops).

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Git
- A Twelve Data API key if you want to stream live market data

## 1. Clone the repository

```bash
git clone https://github.com/VaibhavKumar2005/MLOps-Projects.git
cd MLOps-Projects/stock-market-mlops
```

## 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

## 3. Install dependencies

```bash
pip install -e ".[dev]"
```

## 4. Configure environment variables

Copy the sample environment file and update it with your values:

```bash
cp .env.example .env
```

At minimum, provide your API key in the copied environment file:

```bash
TWELVEDATA_API_KEY=your-key-here
```

## 5. Start the local stack

The project includes Docker Compose services for Kafka, Zookeeper, MLflow, and the streaming workers.

```bash
docker compose up -d
```

You can verify that the containers are running with:

```bash
docker compose ps
```

## 6. Run tests

```bash
pytest tests/
```

## Useful commands

```bash
docker compose logs -f
make logs
docker compose down
```

If you prefer a script-based start, the repository also includes [startup.sh](stock-market-mlops/startup.sh), but Docker Compose is the primary path for local execution.
