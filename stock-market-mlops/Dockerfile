FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories for logs and models
RUN mkdir -p /app/logs /app/models /app/mlruns

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Create non-root user for security
RUN groupadd -r mlops && useradd -r -g mlops mlops
RUN chown -R mlops:mlops /app
USER mlops

# Default command (can be overridden)
CMD ["python", "-m", "src.kafka_producer"]
