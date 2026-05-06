# 📈 Stock Market MLOps Pipeline

## 🚀 Overview

This project is an end-to-end **MLOps pipeline** for stock market data.
It focuses on building a production-style system that ingests real-world data, processes it, and prepares it for machine learning.

The goal is not just prediction, but to simulate how ML systems work in real-world environments.

---

## 🧠 Problem Statement

Stock market data is dynamic and continuously evolving.
This project aims to:

* Fetch real-time-like stock data
* Store and manage datasets
* Prepare data for machine learning pipelines

---

## 🛠️ Tech Stack

* Python
* Pandas
* yFinance
* Git & GitHub

---

## 📂 Project Structure

```text
stock-market-mlops/
│
├── data/                  # Stored stock data (CSV files)
├── src/
│   └── data_ingestion.py  # Script to fetch stock data
├── notebooks/             # (Optional) Experiments
├── README.md
└── .gitignore
```

---

## ⚙️ Features Implemented

* 📊 Fetch stock data using yFinance
* 🔁 Multi-stock support (AAPL, TSLA, MSFT, etc.)
* 💾 Store data locally in CSV format
* 🧪 Basic debugging and validation
* Kafka streaming stubs for raw and feature topics

---

## ▶️ How to Run

### 1. Install dependencies

```bash
pip install yfinance pandas
```

### 2. Run the script

```bash
python src/data_ingestion.py
```

### 3. Output

CSV files will be saved in the `data/` folder.

---

## Kafka (Local Dev)

### 1. Start Kafka

```bash
cd stock-market-mlops
docker compose up -d
```

### 2. Stream CSV data to Kafka

```bash
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv --sleep 0.1
```

### 3. Run feature consumer

```bash
python src/kafka_feature_engineering.py
```

Raw messages go to `stock.raw`, and engineered features go to `stock.features`.

---

## 🔜 Upcoming Features

* Feature Engineering (moving averages, returns)
* Model Training
* Experiment Tracking (MLflow)
* API Deployment (FastAPI)
* Pipeline Automation (Airflow)
* Containerization (Docker)
* Kubernetes Deployment

---

## 🎯 Learning Objectives

This project demonstrates:

* Real-world data ingestion
* Clean project structuring
* Foundation of MLOps pipelines
* Transition from ML → Production systems

---

## ⚠️ Disclaimer

This project is for educational purposes only.
It does not provide financial advice or accurate stock predictions.

---

## 👨‍💻 Author

Vaibhav Kumar
