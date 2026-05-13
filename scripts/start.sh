#!/bin/bash

echo "Starting ETL Pipeline..."

# Navigate to project directory
cd ~/etl-pipeline

# Start Docker services
echo "Starting Docker containers..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 15

# Check if containers are running
echo "Checking container status..."
docker ps

echo ""
echo "ETL Pipeline Started!"
echo "Access Kafka UI at: http://localhost:8080"
echo "Access Airflow UI at: http://localhost:8081"
echo ""
echo "To run producer: python3 producer.py"
echo "To run consumer: python3 consumer.py"