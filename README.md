# FraudLens — Real-Time Fraud Detection Engine

### Production-Grade Indian Fintech Fraud Detection System

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-black)
![Spark](https://img.shields.io/badge/Apache_Spark-3.4.1-orange)
![FastAPI](https://img.shields.io/badge/FastAPI-0.117-green)
![XGBoost](https://img.shields.io/badge/XGBoost-ML-red)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.7.3-teal)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![PowerBI](https://img.shields.io/badge/Power_BI-Dashboard-yellow)

---

## What is FraudLens?

FraudLens is a **production-grade, real-time fraud detection engine** built specifically for the Indian fintech ecosystem. It simulates the kind of system used at Razorpay, PhonePe, and CRED to detect fraudulent UPI transactions, NEFT transfers, and card payments in real time.

The entire system is containerized with Docker and processes transactions end-to-end — from generation to ML scoring to dashboard visualization — with zero manual intervention.

---

## Live Dashboard

The Power BI dashboard shows:

- **Total Transactions** processed in real time
- **Fraud Count** and **Fraud Rate %**
- **Total Amount (INR)** flowing through the system
- **Fraud by Merchant Category** — which merchants have highest fraud
- **Fraud by Payment Method** — UPI vs NEFT vs Credit Card
- **Fraud by Hour of Day** — when fraud spikes (2am-5am)
- **Fraud by City** — world map showing fraud locations

---

## Architecture

```
Transaction Generator
        │
        ▼
   Apache Kafka          ← real-time message queue
  (transactions-raw)
        │
        ▼
   Apache Spark          ← stream processor (10-second micro-batches)
  (spark_stream.py)
        │
        ▼
   PostgreSQL            ← data warehouse
(transactions_enriched)
        │
     ┌──┴──┐
     │     │
     ▼     ▼
 XGBoost  Airflow        ← ML model + hourly pipeline orchestration
  Model   (DAG)
     │
     ▼
  FastAPI                ← REST API serving fraud predictions
 /predict
     │
     ▼
  Power BI               ← live fraud analytics dashboard
```

---

## Tech Stack

| Component         | Technology              | Purpose                          |
| ----------------- | ----------------------- | -------------------------------- |
| Data Ingestion    | Apache Kafka 3.4        | Real-time transaction streaming  |
| Stream Processing | Apache Spark 3.4.1      | Feature engineering + enrichment |
| ML Model          | XGBoost + Scikit-learn  | Fraud probability scoring        |
| REST API          | FastAPI                 | Real-time prediction serving     |
| Data Warehouse    | PostgreSQL 15           | Transaction storage              |
| Caching           | Redis 7                 | Prediction result caching        |
| Orchestration     | Apache Airflow 2.7.3    | Hourly pipeline automation       |
| Dashboard         | Power BI                | Live fraud analytics             |
| Containerization  | Docker + Docker Compose | Full environment management      |

---

## Indian Fintech Context

Built specifically for the Indian market:

```
Currency        → INR (Indian Rupees)
Payment Methods → UPI, NEFT, RTGS, IMPS, Credit Card, Debit Card, Net Banking
Merchants       → Swiggy, Zomato, Amazon India, Flipkart, BigBasket, MakeMyTrip
Cities          → Mumbai, Delhi, Bangalore, Chennai, Hyderabad, Pune, Kolkata
Fraud Cities    → Lagos, Moscow, Bucharest, Kyiv, Unknown Location
High Amount     → Transactions above ₹50,000 flagged
Odd Hours       → Transactions between 12am–5am flagged
```

---

## Project Structure

```
fraudlens/
├── local/
│   ├── docker-compose.yml          ← all 8 containers
│   ├── data_generator/
│   │   └── generator.py            ← Indian transaction generator
│   ├── spark/
│   │   └── spark_stream.py         ← Spark Structured Streaming job
│   ├── ml/
│   │   ├── train_model.py          ← XGBoost training script
│   │   ├── fraud_model.pkl         ← trained model (generated)
│   │   └── label_encoders.pkl      ← category encoders (generated)
│   ├── fastapi/
│   │   ├── main.py                 ← FastAPI fraud scoring API
│   │   └── Dockerfile              ← FastAPI container
│   ├── airflow/
│   │   └── dags/
│   │       └── fraud_pipeline.py   ← Airflow DAG
│   └── warehouse/
│       └── schema.sql              ← PostgreSQL schema
├── fraudlens_dashboard.pbix        ← Power BI dashboard
└── README.md
```

---

## Quick Start

**Prerequisites:**

- Docker Desktop
- Python 3.11+
- Power BI Desktop (free)

---

### Step 1 — Clone repository

```powershell
git clone https://github.com/harikrishnannm278-bot/fraudlens.git
cd fraudlens/local
```

### Step 2 — Start all Docker containers

```powershell
docker-compose up -d
```

Wait 30 seconds then verify:

```powershell
docker ps
# Should show 8 containers: zookeeper, kafka, kafka-ui, postgres, redis, spark, airflow, fastapi
```

### Step 3 — Create database tables

```powershell
Get-Content warehouse/schema.sql | docker exec -i postgres psql -U fraud_user -d fraud_db
```

Then create the Indian fintech schema:

```powershell
docker exec postgres psql -U fraud_user -d fraud_db -c "DROP TABLE IF EXISTS transactions_enriched CASCADE; CREATE TABLE transactions_enriched (transaction_id VARCHAR(36) PRIMARY KEY, account_id VARCHAR(20) NOT NULL, amount_inr DECIMAL(12,2), merchant_name VARCHAR(200), merchant_category VARCHAR(50), payment_method VARCHAR(30), city VARCHAR(50), hour_of_day INT, day_of_week INT, is_odd_hour BOOLEAN, is_foreign_city BOOLEAN, is_high_amount BOOLEAN, is_weekend BOOLEAN, is_fraud INT, fraud_probability DECIMAL(6,4) DEFAULT NULL, risk_level VARCHAR(10) DEFAULT NULL, created_at TIMESTAMP DEFAULT NOW());"
```

### Step 4 — Start transaction generator

```powershell
# Open new terminal
python local/data_generator/generator.py
```

### Step 5 — Verify data is flowing

```powershell
# Wait 2 minutes then check
docker exec postgres psql -U fraud_user -d fraud_db -c "SELECT COUNT(*) FROM transactions_enriched;"
# Should show growing number like 500, 1000, 2000...
```

### Step 6 — Open all dashboards

```
Kafka UI  → http://localhost:8081          (see messages flowing)
FastAPI   → http://localhost:8000/docs     (test predictions)
Airflow   → http://localhost:8080          (admin/admin123)
```

### Step 7 — Open Power BI Dashboard

- Open `fraudlens_dashboard.pbix` in Power BI Desktop
- Click **Refresh** to load latest live data

---

## API Endpoints

### Fraud Prediction

```
POST /predict
```

**Request body:**

```json
{
  "transaction_id": "txn-001",
  "account_id": "IN1000042",
  "amount_inr": 450000,
  "merchant_name": "Unknown Merchant",
  "merchant_category": "upi",
  "payment_method": "net_banking",
  "city": "Lagos",
  "hour_of_day": 2,
  "day_of_week": 6
}
```

**Response:**

```json
{
  "transaction_id": "txn-001",
  "fraud_probability": 1.0,
  "is_fraud": true,
  "risk_level": "HIGH",
  "cached": false,
  "scored_at": "2026-03-06T10:00:00"
}
```

### Health Check

```
GET /health
```

```json
{ "status": "healthy", "model": "loaded", "redis": "connected" }
```

### Power BI Data Endpoints

```
GET /powerbi/summary                  → KPI metrics
GET /powerbi/transactions/hourly      → hourly transaction trend
GET /powerbi/fraud/by-category        → fraud by merchant category
GET /powerbi/fraud/by-city            → fraud by city
GET /powerbi/fraud/by-hour            → fraud by hour of day
GET /powerbi/fraud/by-payment-method  → fraud by payment type
GET /powerbi/alerts/recent            → recent fraud alerts
```

---

## ML Model Details

**Algorithm:** XGBoost Classifier

**10 Features used:**

```
amount_inr           → transaction amount in INR
hour_of_day          → hour of transaction (0-23)
day_of_week          → day of transaction (0-6)
merchant_category    → encoded merchant type
payment_method       → encoded payment method
city                 → encoded transaction city
is_odd_hour          → 1 if between 12am-5am
is_foreign_city      → 1 if in high-risk city (Lagos, Moscow etc)
is_high_amount       → 1 if amount > ₹50,000
is_weekend           → 1 if Saturday or Sunday
```

**Model Performance:**

```
Training Data   → 51,534 transactions
Fraud Rate      → 2.0% (class imbalance handled with scale_pos_weight=49)
ROC-AUC Score   → 1.0000
Precision       → 1.00
Recall          → 1.00
F1 Score        → 1.00
```

---

## Airflow Pipeline

Runs **every hour** automatically:

```
start
  ↓
data_quality_check     → verifies new transactions arrived in last hour
  ↓
refresh_daily_summary  → rebuilds daily fraud summary table
  ↓
should_retrain         → checks if 10,000+ new rows since last training
  ↓              ↓
retrain_model   skip_retrain
  ↓              ↓
        end
```

---

## Power BI Dashboard Setup

**Connect to live data:**

1. Open Power BI Desktop
2. Home → Get Data → Web
3. Add each URL as a separate table:

| Table Name        | URL                                                   |
| ----------------- | ----------------------------------------------------- |
| summary           | http://localhost:8000/powerbi/summary                 |
| by-category       | http://localhost:8000/powerbi/fraud/by-category       |
| by-city           | http://localhost:8000/powerbi/fraud/by-city           |
| by-hour           | http://localhost:8000/powerbi/fraud/by-hour           |
| by-payment-method | http://localhost:8000/powerbi/fraud/by-payment-method |
| hourly            | http://localhost:8000/powerbi/transactions/hourly     |
| recent            | http://localhost:8000/powerbi/alerts/recent           |

4. For each URL: Connect → List → Into Table → Close & Apply
5. Apply **Midnight** theme: View → Themes → Midnight
6. Build visuals using connected tables

---

## Docker Services

```
Service      Port    Purpose
─────────────────────────────────────
zookeeper    2181    Kafka coordination
kafka        9092    Message broker
kafka-ui     8081    Kafka visual interface
postgres     5432    Data warehouse
redis        6379    Prediction cache
spark        -       Stream processor
airflow      8080    Pipeline orchestration
fastapi      8000    ML scoring API
```

---

## Common Errors and Fixes

| Error                            | Fix                                                   |
| -------------------------------- | ----------------------------------------------------- |
| Generator: NoBrokersAvailable    | Docker not running — run `docker-compose up -d`       |
| Spark: Name or service not known | Run `docker-compose down` then `docker-compose up -d` |
| Tables don't exist               | Run schema commands from Step 3                       |
| Airflow: Connection refused      | Check postgres is on fraudlens network                |
| Power BI: empty data             | Verify FastAPI URL works in browser first             |
| FastAPI: Model not loaded        | Check ml/fraud_model.pkl exists                       |

---

## AWS Migration Plan

Each component maps directly to a managed AWS service:

```
Local Docker     →    AWS Service
──────────────────────────────────
Kafka            →    Amazon MSK
Spark            →    Amazon EMR
PostgreSQL       →    Amazon RDS
FastAPI          →    AWS EC2
Redis            →    ElastiCache
Airflow          →    Amazon MWAA
Power BI         →    stays same
```

---

## What I Learned

- Real-time data streaming with Apache Kafka
- Spark Structured Streaming with foreachBatch
- XGBoost for imbalanced fraud classification
- Feature engineering for fraud detection
- Building production REST APIs with FastAPI
- Docker multi-container orchestration and networking
- Airflow DAG design with branching logic
- Power BI integration with REST API endpoints
- PostgreSQL schema design for analytics
- Redis caching for ML predictions

---

## Author

Built as a portfolio project demonstrating production-grade data engineering skills for the Indian fintech industry.

**Stack:** Python · Kafka · Spark · XGBoost · FastAPI · PostgreSQL · Redis · Airflow · Docker · Power BI

---

> "Real fraud detection doesn't happen in Jupyter notebooks. It happens in pipelines like this."
