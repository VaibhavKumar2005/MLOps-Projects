# Databricks Dependencies Update Guide

## Updated pyproject.toml

Below is the updated `pyproject.toml` with Databricks integration dependencies:

```toml
[build-system]
requires = ["setuptools>=65.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "stock-market-mlops"
version = "0.2.0"
description = "Event-Driven Streaming ML Pipeline for Financial Market Analysis with Databricks"
readme = "README.md"
requires-python = ">=3.11"
license = {text = "MIT"}
authors = [
    {name = "Your Name", email = "your.email@example.com"}
]
keywords = ["mlops", "kafka", "streaming", "financial", "machine-learning", "databricks"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
]

dependencies = [
    # Data & ML
    "yfinance>=0.2.28",
    "pandas>=2.0.0",
    "scikit-learn>=1.2.0",
    "xgboost>=2.0.0",
    "polars>=0.18.0",
    
    # Streaming & Messaging
    "kafka-python>=2.0.2",
    "websocket-client>=1.5.0",
    
    # Data Versioning & Orchestration
    "dvc>=3.0.0",
    "prefect>=2.10.0",
    
    # Model Tracking & Registry
    "mlflow>=2.0.0",
    
    # Data Quality & Validation
    "great-expectations>=0.16.0",
    "pandera>=0.14.0",
    
    # Testing
    "pytest>=7.0.0",
    
    # Code Quality
    "black>=23.0.0",
    "flake8>=6.0.0",
    
    # Database & SQL
    "sqlalchemy>=2.0.0",
    
    # Visualization
    "matplotlib>=3.6.0",
    "seaborn>=0.12.0",
    "plotly>=5.12.0",
    
    # ===== DATABRICKS INTEGRATION =====
    "databricks-sdk>=0.20.0",
    "databricks-sql-connector>=3.0.0",
    "databricks-feature-engineering>=0.3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "black>=23.0.0",
    "flake8>=6.0.0",
    "mypy>=1.0.0",
    "isort>=5.12.0",
]

databricks = [
    "databricks-sdk>=0.20.0",
    "databricks-sql-connector>=3.0.0",
    "databricks-feature-engineering>=0.3.0",
    "databricks-labs-ucx>=0.15.0",  # Optional: Unity Catalog Explorer
]

ml-tracking = [
    "mlflow>=2.0.0",
    "mlflow[databricks]>=2.0.0",  # MLflow Databricks plugin
]

streaming = [
    "kafka-python>=2.0.2",
    "confluent-kafka>=2.0.0",  # Alternative Kafka client
]

quality = [
    "great-expectations>=0.16.0",
    "pandera>=0.14.0",
    "evidently>=0.4.0",  # Data drift detection
]

all = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "black>=23.0.0",
    "flake8>=6.0.0",
    "mypy>=1.0.0",
    "isort>=5.12.0",
    "databricks-sdk>=0.20.0",
    "databricks-sql-connector>=3.0.0",
    "databricks-feature-engineering>=0.3.0",
    "mlflow[databricks]>=2.0.0",
    "kafka-python>=2.0.2",
    "great-expectations>=0.16.0",
    "pandera>=0.14.0",
    "evidently>=0.4.0",
]

[tool.black]
line-length = 100
target-version = ['py311']
include = '\.pyi?$'
exclude = '''
/(
    \.git
  | \.hg
  | \.mypy_cache
  | \.tox
  | \.venv
  | _build
  | buck-out
  | build
  | dist
)/
'''

[tool.isort]
profile = "black"
line_length = 100

[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = false

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py", "*_test.py"]
addopts = "--cov=src --cov-report=html --cov-report=term"
```

---

## Installation Commands

### Install Core Dependencies

```bash
# Install from updated pyproject.toml
pip install -e .

# Or specific groups
pip install -e ".[databricks]"
pip install -e ".[ml-tracking]"
pip install -e ".[quality]"
pip install -e ".[all]"  # Everything
```

### Install for Development

```bash
pip install -e ".[dev]"
```

### Verify Installation

```bash
python -c "import databricks; print(f'Databricks SDK: {databricks.__version__}')"
python -c "import mlflow; print(f'MLflow: {mlflow.__version__}')"
```

---

## Package Explanations

### Core Databricks Packages

| Package | Purpose | Version |
|---------|---------|---------|
| `databricks-sdk` | Official Databricks Python SDK for API interactions | >=0.20.0 |
| `databricks-sql-connector` | SQL connector for Databricks SQL Warehouse queries | >=3.0.0 |
| `databricks-feature-engineering` | Databricks Feature Store client | >=0.3.0 |

### Optional Databricks Extensions

| Package | Purpose |
|---------|---------|
| `databricks-labs-ucx` | Unity Catalog explorer and governance tools |
| `mlflow[databricks]` | MLflow integration with Databricks backend |
| `evidently` | Data and ML model drift detection |

---

## Migration Checklist

- [ ] Update `pyproject.toml` with new dependencies
- [ ] Run `pip install -e ".[databricks]"`
- [ ] Verify all imports work: `python -c "from src.databricks_utils import *"`
- [ ] Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables
- [ ] Test connection: `python -c "from src.databricks_utils import DatabricksConnector; db = DatabricksConnector()"`
- [ ] Create Databricks catalog and schemas
- [ ] Upload notebooks to Databricks workspace
- [ ] Run data migration notebook
- [ ] Monitor in MLflow UI

---

## Troubleshooting Installation

### Issue: `ModuleNotFoundError: No module named 'databricks'`

```bash
# Reinstall
pip uninstall databricks-sdk databricks-sql-connector -y
pip install databricks-sdk databricks-sql-connector
```

### Issue: Version Conflicts

```bash
# Update pip first
pip install --upgrade pip

# Install specific versions
pip install databricks-sdk==0.20.0 databricks-sql-connector==3.0.0
```

### Issue: Databricks SQL Connector Compilation Error

```bash
# On Linux, ensure build tools are installed
sudo apt-get install build-essential python3-dev

# Then reinstall
pip install --no-cache-dir databricks-sql-connector
```

---

## Environment Configuration

Create `.env` file in project root:

```bash
# .env
DATABRICKS_HOST=https://your-workspace-id.cloud.databricks.com
DATABRICKS_TOKEN=dapi123456789...
DATABRICKS_WAREHOUSE_ID=abc123def456
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123def456
```

Load in Python:

```python
from dotenv import load_dotenv
import os

load_dotenv()
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
```

---

## Verify All Components

```bash
#!/bin/bash
# verify_databricks.sh

echo "Checking Databricks packages..."
python -c "import databricks; print('✓ databricks-sdk')" || echo "✗ databricks-sdk"
python -c "import databricks.sql; print('✓ databricks.sql')" || echo "✗ databricks-sql-connector"
python -c "from databricks.feature_engineering import FeatureEngineeringClient; print('✓ databricks-feature-engineering')" || echo "✗ databricks-feature-engineering"

echo "Checking MLflow..."
python -c "import mlflow; print('✓ mlflow')" || echo "✗ mlflow"

echo "Checking data tools..."
python -c "import pandas; print('✓ pandas')" || echo "✗ pandas"
python -c "import xgboost; print('✓ xgboost')" || echo "✗ xgboost"

echo "Checking streaming..."
python -c "import kafka; print('✓ kafka-python')" || echo "✗ kafka-python"

echo "\n✅ All dependencies installed!"
```

---

## Next Steps

1. Update your `pyproject.toml`
2. Install dependencies: `pip install -e ".[databricks]"`
3. Follow Quick Start: [DATABRICKS_QUICKSTART.md](./DATABRICKS_QUICKSTART.md)
4. See full guide: [DATABRICKS_INTEGRATION.md](./DATABRICKS_INTEGRATION.md)
