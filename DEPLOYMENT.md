# Deployment Guide

This project is primarily designed for local Docker-based deployment, with optional support for Helm-based MLflow deployment.

## Local deployment with Docker Compose

From the project directory, start the full stack:

```bash
cd stock-market-mlops
docker compose up -d
```

Useful operational commands:

```bash
docker compose ps
docker compose logs -f
docker compose down
docker compose restart
```

## Service endpoints

- MLflow UI: http://localhost:5000
- Kafka broker: localhost:9092
- Zookeeper: localhost:2181

## Configuration variables

The Compose stack reads configuration from the environment file in [stock-market-mlops/.env.example](stock-market-mlops/.env.example). Common variables include:

- `TWELVEDATA_API_KEY` – required for live market streaming
- `TWELVEDATA_SYMBOLS` – comma-separated ticker list
- `DRIFT_THRESHOLD` – sensitivity for drift detection
- `DRIFT_WINDOW_SIZE` – number of samples used for drift checks

## MLflow and model tracking

MLflow runs are stored locally under [stock-market-mlops/mlruns](stock-market-mlops/mlruns). The Docker deployment exposes the UI at port 5000 so you can inspect experiments and model artifacts during development.

## Optional Kubernetes deployment

If you want to deploy the MLflow server to a cluster, use the provided Helm chart:

```bash
cd stock-market-mlops
helm install mlflow ./helm/mlflow
```

## Troubleshooting

- If services fail to start, verify Docker is running and that the environment file exists.
- If Kafka or MLflow containers are unhealthy, inspect logs with `docker compose logs -f`.
- If the producer cannot connect to the market feed, confirm that `TWELVEDATA_API_KEY` is set correctly.

This deployment flow is intended for development and experimentation; production deployment should include hardened secrets management and persistent storage.
