#!/bin/bash

echo "Stopping ETL Pipeline..."

# Stop Docker services
docker-compose down

echo "ETL Pipeline stopped."