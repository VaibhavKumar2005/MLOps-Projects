# MLOps Portfolio

A portfolio of machine learning and MLOps projects, showcasing my journey in building production-level systems and learning best practices in ML engineering.

## 🎯 Purpose

This repository contains multiple projects focused on:

- **Production-grade ML systems** — not just models, but complete pipelines with data ingestion, processing, and validation
- **MLOps practices** — automation, CI/CD, monitoring, and reproducibility  
- **Real-world engineering** — version control, testing, documentation, and deployment strategies

## 📚 Projects

### 1. [Stock Market MLOps Pipeline](./stock-market-mlops/)  
**Status:** In Progress  

An end-to-end data pipeline for financial data processing:

- Real-time-like stock data ingestion using yFinance
- Multi-source data management (AAPL, MSFT, TSLA, etc.)
- Data validation and feature engineering
- Focus on scalability and reproducibility

**Tech Stack:** Python, Pandas, yFinance, Git  
**What I'm Learning:** Data pipelines, data quality, version control best practices

---

### 2. Machine Learning System (Planned)

A production ML system covering model training, evaluation, and inference with focus on:

- Experiment tracking
- Model versioning
- Hyperparameter tuning
- Cross-validation and evaluation metrics

---

### 3. Kubernetes & Kubeflow (Planned)

Container orchestration and workflow management:

- Containerizing ML pipelines
- Orchestrating distributed workflows
- Scaling inference services
- Managing ML experiments at scale

---

## 🛠️ Tech Stack (Current)

- **Languages:** Python
- **Data Processing:** Pandas, NumPy, DVC (Data Version Control)
- **ML/Modeling:** (To be integrated)
- **DevOps:** Git, Docker, GitHub Actions (CI/CD)
- **Cloud:**GCP (planned)

## 🚀 Getting Started

### Quick Start (Stock Market Pipeline - Working ✅)

```bash
cd stock-market-mlops
pip install -r requirements.txt
docker compose up -d
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv --sleep 0.1
# Open 3 more terminals and run:
python src/kafka_feature_engineering.py
python src/prediction_consumer.py
python src/drift_monitor.py
```

See [stock-market-mlops/PIPELINE_DEMO.md](./stock-market-mlops/PIPELINE_DEMO.md) for full walkthrough.

### Detailed Setup

Each project has its own detailed README with setup instructions:

```bash
cd stock-market-mlops
cat README.md
```

## 📖 Repository Structure

```text
MLOps-Projects/
├── stock-market-mlops/     # Financial data pipeline
│   ├── data/               # Raw and processed data
│   ├── src/                # Source code
│   ├── notebooks/          # Experiments (optional)
│   ├── .dvc/               # DVC configuration
│   ├── dvc.yaml            # DVC pipeline definition
│   └── README.md           # Project documentation
├── README.md               # This file
└── .gitignore
```

## 🎓 Learning Goals

- [ ] Build complete, documented ML pipelines
- [ ] Implement data validation and quality checks
- [ ] Containerize ML applications
- [ ] Set up CI/CD workflows
- [ ] Deploy models to production environments
- [ ] Monitor model performance

## 📝 Notes

- Each project is self-contained but follows consistent practices
- Focus is on engineering best practices, not just accuracy metrics
- All projects are works in progress and will be continuously improved

## 📧 Contact

Part of my portfolio as a student exploring MLOps and AI Engineering.

---

**Last Updated:** April 2026
