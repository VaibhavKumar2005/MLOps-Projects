# Setup Guide

## Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Git

## Installation

1. Clone the repository:
```bash
git clone https://github.com/VaibhavKumar2005/MLOps-Projects.git
cd MLOps-Projects/stock-market-mlops
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run startup script:
```bash
./startup.sh
```

## Configuration
Update `src/config.py` with your API keys and settings.

## Running Tests
```bash
pytest tests/
```
