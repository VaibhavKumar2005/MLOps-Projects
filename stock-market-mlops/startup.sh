#!/bin/bash

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Stock Market MLOps Pipeline...${NC}"

# Create necessary directories
echo -e "${YELLOW}📁 Creating directories...${NC}"
mkdir -p logs models data/processed data/features mlruns metrics

# Copy .env if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}📝 Creating .env file from .env.example...${NC}"
    cp .env.example .env
    echo -e "${YELLOW}⚠️  Please update .env with your configuration (especially TWELVEDATA_API_KEY)${NC}"
fi

# Check Docker daemon
echo -e "${YELLOW}🐳 Checking Docker daemon...${NC}"
if ! docker ps > /dev/null 2>&1; then
    echo -e "${YELLOW}❌ Docker daemon is not running. Please start Docker first.${NC}"
    exit 1
fi

# Build images
echo -e "${YELLOW}🔨 Building Docker images...${NC}"
docker-compose build

# Start services
echo -e "${YELLOW}⚡ Starting services...${NC}"
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d

# Wait for services to be ready
echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
sleep 10

# Check service health
echo -e "${BLUE}📊 Service Status:${NC}"
docker-compose ps

# Display URLs
echo -e "${GREEN}✅ Pipeline started successfully!${NC}"
echo ""
echo -e "${BLUE}📊 Service URLs:${NC}"
echo -e "  ${GREEN}MLflow UI:${NC} http://localhost:5000"
echo -e "  ${GREEN}Kafka Broker:${NC} localhost:9092"
echo -e "  ${GREEN}Zookeeper:${NC} localhost:2181"
echo -e "  ${GREEN}Live data source:${NC} Twelve Data WebSocket -> Kafka stock.raw"
echo -e "  ${GREEN}Symbols:${NC} ${TWELVEDATA_SYMBOLS:-AAPL,MSFT,TSLA}"
echo ""
echo -e "${BLUE}📋 Useful commands:${NC}"
echo -e "  ${GREEN}View logs:${NC} make logs"
echo -e "  ${GREEN}Stop services:${NC} make down"
echo -e "  ${GREEN}Run DVC pipeline:${NC} make dvc-repro"
echo -e "  ${GREEN}View DVC DAG:${NC} make dvc-dag"
echo ""
