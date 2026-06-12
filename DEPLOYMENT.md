# Deployment Guide

## Docker Deployment

1. Build the Docker image:
```bash
docker build -t mlops-app .
```

2. Start services with Docker Compose:
```bash
docker-compose up -d
```

## Kubernetes Deployment

1. Install Helm chart:
```bash
helm install mlflow ./helm/mlflow
```

2. Apply Kubernetes configurations:
```bash
kubectl apply -f kubernetes/
```

## MLflow Server
Access MLflow UI at `http://localhost:5000`

## Monitoring
DVC tracks data pipelines. View experiments in MLflow dashboard.
