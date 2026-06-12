# API Documentation

## Data Ingestion Service
- **Endpoint**: `/ingest`
- **Method**: POST
- **Purpose**: Fetch and store stock market data

## Feature Engineering Service
- **Endpoint**: `/features`
- **Method**: POST
- **Purpose**: Transform raw data into features

## Prediction Service
- **Endpoint**: `/predict`
- **Method**: POST
- **Params**: `ticker`, `days`
- **Purpose**: Generate stock price predictions

## Drift Monitoring
- **Endpoint**: `/drift`
- **Method**: GET
- **Purpose**: Check for data drift alerts

## Configuration
Set environment variables in `.env` file before running services.
