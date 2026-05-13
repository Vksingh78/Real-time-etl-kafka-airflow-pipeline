# 📊 Real-Time ETL Pipeline with Apache Kafka & Airflow

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.5-green)](https://kafka.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.3-orange)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0-blue)](https://www.docker.com/)

---

## 📌 Overview

This project implements a real-time event-driven ETL pipeline using Apache Kafka, Apache Airflow, Docker, and Python. The pipeline reads telecom customer churn data from a CSV dataset, streams records through Kafka topics, and processes them in real time using Kafka consumers. The system also measures end-to-end latency metrics including P50, P95, and P99 latency.

The project is fully containerized using Docker Compose and includes Airflow DAGs for workflow orchestration and health monitoring. The architecture demonstrates real-time streaming, event-driven processing, latency benchmarking, and monitoring in a portable local environment.

---

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Total Records Processed | 3,333 |
| Producer Throughput | ~3,600 RPS |
| P50 Latency | ~170–180 ms |
| P95 Latency | ~210–220 ms |
| P99 Latency | ~240–250 ms |
| Data Loss | 0% |

---

## 🏗️ Architecture

```text
telecom_churn.csv → Kafka Producer → Kafka Broker → Kafka Consumer → Metrics Output
                               ↑
                     Airflow Monitoring DAGs
```

The producer reads CSV data and publishes JSON messages to Kafka topics. Kafka stores messages across partitions for scalable processing. The consumer subscribes to Kafka topics, processes records in real time, and calculates latency metrics and churn statistics. Apache Airflow is used for orchestration and health monitoring.

---

## 🔄 Airflow DAGs

The project includes Airflow DAGs for ETL orchestration and monitoring:

- `etl_health_check` → Monitors Kafka broker and consumer health
- `event_driven_etl_demo` → Demonstrates event-driven ETL execution flow

These DAGs help automate monitoring and improve pipeline observability.

---

## 📁 Project Structure

```text
cdr-pipeline/
├── airflow/dags/            # Airflow DAGs
├── producers/               # Kafka producer scripts
├── scripts/                 # Start/stop scripts
├── producer_csv.py          # CSV Kafka producer
├── consumer_csv.py          # Kafka consumer with latency metrics
├── docker-compose.yml       # Docker services configuration
├── Dockerfile.airflow       # Custom Airflow image
├── telecom_churn.csv        # Telecom dataset
├── requirements.txt         # Python dependencies
├── test_latency.py          # Latency testing script
└── .gitignore
```

---

## 🚀 Quick Start

### Step 1: Clone Repository

```bash
git clone https://github.com/Vksingh78/Real-time-etl-kafka-airflow-pipeline.git

cd Real-time-etl-kafka-airflow-pipeline
```

### Step 2: Start Docker Containers

```bash
docker-compose up -d
```

### Step 3: Create Kafka Topic

```bash
docker exec -it kafka kafka-topics --create --topic telecom-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Step 4: Run Consumer

```bash
python consumer_csv.py
```

### Step 5: Run Producer

```bash
python producer_csv.py
```

---

## 🌐 Web Interfaces

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Airflow UI | http://localhost:8081 |

### Airflow Login

```text
Username: admin
Password: admin
```

---

## 📈 Sample Consumer Output

```text
Received 1000 records
Rate: 3789 RPS
P50: 180 ms | P95: 220 ms | P99: 250 ms
Churn True: 145 | False: 855
```

---

## 🛠️ Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| ZooKeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Message broker |
| PostgreSQL | 5432 | Airflow metadata database |
| Airflow | 8081 | Workflow orchestration |
| Kafka UI | 8080 | Kafka monitoring UI |

---

## 📊 Dataset Information

The dataset `telecom_churn.csv` contains 3,333 telecom customer records with 21 columns including:

- Customer account information
- Call usage statistics
- International plan details
- Customer service call counts
- Churn label (True/False)

---

## 📈 Performance Analysis

The pipeline achieved high-throughput real-time processing with low latency in a local Dockerized environment. The architecture demonstrates scalable event-driven ETL processing using Kafka and Airflow while maintaining low end-to-end latency and zero data loss.

---

## 🔄 Future Enhancements

- Integrate Apache Spark Streaming
- Deploy using Kubernetes
- Add PostgreSQL CDC pipelines
- Implement exactly-once semantics
- Add ML-based fraud detection
- Deploy on AWS cloud infrastructure

---

## 👨‍💻 Author

**Vivek Singh**

---

## 📄 License

This project is created for educational and portfolio purposes.
