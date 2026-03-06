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

---

## What is FraudLens?

FraudLens is a **production-grade, real-time fraud detection engine** built specifically for the Indian fintech ecosystem. It simulates the kind of system used by companies like Razorpay, PhonePe, and CRED to detect fraudulent UPI transactions, NEFT transfers, and card payments in real time.

The entire system is containerized with Docker and processes transactions end-to-end — from generation to ML scoring to dashboard visualization — with zero manual intervention.

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
 Dashboard
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

This project is built specifically for the Indian market:

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

## Features

- **Real-time streaming** — processes 10 transactions per second via Kafka
- **Feature engineering** — 10 features including is_odd_hour, is_foreign_city, is_high_amount
- **ML fraud scoring** — XGBoost model with 1.0 ROC-AUC on test data
- **Class imbalance handling** — scale_pos_weight = 49x for 2% fraud rate
- **Redis caching** — predictions cached for 1 hour, same transaction never scored twice
- **Automatic retraining** — Airflow triggers retraining every 10,000 new rows
- **Power BI dashboard** — 8+ live visuals connected to FastAPI endpoints
- **10 REST endpoints** — summary, hourly, daily, by-category, by-city, by-hour, alerts, model performance

---

## Project Structure

```
fraudlens/
├── local/
│   ├── docker-compose.yml          ← all 7 containers
│   ├── data_generator/
│   │   └── generator.py            ← Indian transaction generator
│   ├── kafka/                      ← Kafka configuration
│   ├── spark/
│   │   └── spark_stream.py         ← Spark Structured Streaming job
│   ├── ml/
│   │   ├── train_model.py          ← XGBoost training script
│   │   ├── fraud_model.pkl         ← trained model (generated)
│   │   └── label_encoders.pkl      ← category encoders (generated)
│   ├── fastapi/
│   │   └── main.py                 ← FastAPI fraud scoring API
│   ├── airflow/
│   │   └── dags/
│   │       └── fraud_pipeline.py   ← Airflow DAG
│   └── warehouse/
│       └── schema.sql              ← PostgreSQL schema
└── README.md
```

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- Power BI Desktop (free)

---

## Quick Start

**Step 1 — Clone and start Docker:**

```powershell
git clone https://github.com/YOURUSERNAME/fraudlens.git
cd fraudlens/local
docker-compose up -d
```

**Step 2 — Install Python dependencies:**

```powershell
pip install kafka-python psycopg2-binary pandas xgboost scikit-learn joblib fastapi uvicorn redis
```

**Step 3 — Start the generator (Terminal 2):**

```powershell
python local/data_generator/generator.py
```

**Step 4 — Wait for 5,000+ rows then train model (Terminal 3):**

```powershell
docker exec spark python3 /train_model.py
docker cp spark:/local/ml/fraud_model.pkl local/ml/fraud_model.pkl
docker cp spark:/local/ml/label_encoders.pkl local/ml/label_encoders.pkl
```

**Step 5 — Start FastAPI (Terminal 4):**

```powershell
cd local/fastapi
python -m uvicorn main:app --reload --port 8000
```

**Step 6 — Verify everything is running:**

```
Kafka UI   → http://localhost:8081
FastAPI    → http://localhost:8000/docs
Airflow    → http://localhost:8080  (admin/admin123)
```

---

## API Endpoints

### Fraud Prediction

```
POST /predict
```

Request:

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

Response:

```json
{
  "transaction_id": "txn-001",
  "account_id": "IN1000042",
  "amount_inr": 450000.0,
  "fraud_probability": 1.0,
  "is_fraud": true,
  "risk_level": "HIGH",
  "cached": false,
  "scored_at": "2026-03-06T10:00:00"
}
```

### Power BI Endpoints

```
GET /powerbi/summary                  → KPI cards
GET /powerbi/transactions/hourly      → hourly trend
GET /powerbi/transactions/daily       → daily summary
GET /powerbi/fraud/by-category        → fraud by merchant category
GET /powerbi/fraud/by-city            → fraud by city
GET /powerbi/fraud/by-hour            → fraud by hour of day
GET /powerbi/fraud/by-payment-method  → fraud by payment type
GET /powerbi/fraud/by-risk-level      → alerts by risk level
GET /powerbi/alerts/recent            → recent fraud alerts table
GET /powerbi/accounts/top-flagged     → top flagged accounts
GET /powerbi/model/performance        → precision, recall, F1, accuracy
```

---

## ML Model

**Algorithm:** XGBoost Classifier

**Features:**

```
amount_inr           → transaction amount in INR
hour_of_day          → hour of transaction (0-23)
day_of_week          → day of transaction (0-6)
merchant_category    → encoded merchant type
payment_method       → encoded payment method
city                 → encoded transaction city
is_odd_hour          → 1 if between 12am-5am
is_foreign_city      → 1 if in high-risk city
is_high_amount       → 1 if amount > ₹50,000
is_weekend           → 1 if Saturday or Sunday
```

**Results:**

```
Training Data   → 51,534 transactions
Fraud Rate      → 2.0% (1,034 fraud cases)
ROC-AUC Score   → 1.0000
Precision       → 1.00
Recall          → 1.00
F1 Score        → 1.00
```

**Class Imbalance:**

```
scale_pos_weight = 48.9x
Handles 98% legit vs 2% fraud imbalance
```

---

## Airflow Pipeline

Runs **every hour** automatically:

```
start
  ↓
data_quality_check     → verifies new transactions arrived
  ↓
refresh_daily_summary  → rebuilds daily fraud summary table
  ↓
should_retrain         → checks if 10,000 new rows exist
  ↓              ↓
retrain_model   skip_retrain
  ↓              ↓
        end
```

---

## Database Schema

**transactions_enriched** — main table written by Spark

```sql
transaction_id      VARCHAR(36) PRIMARY KEY
account_id          VARCHAR(20)
amount_inr          DECIMAL(12,2)
merchant_name       VARCHAR(200)
merchant_category   VARCHAR(50)
payment_method      VARCHAR(30)
city                VARCHAR(50)
hour_of_day         INT
day_of_week         INT
is_odd_hour         BOOLEAN
is_foreign_city     BOOLEAN
is_high_amount      BOOLEAN
is_weekend          BOOLEAN
is_fraud            INT
fraud_probability   DECIMAL(6,4)
risk_level          VARCHAR(10)
created_at          TIMESTAMP
```

**fraud_alerts** — written by FastAPI when probability ≥ 0.5

```sql
alert_id            SERIAL PRIMARY KEY
transaction_id      VARCHAR(36) UNIQUE
account_id          VARCHAR(20)
amount_inr          DECIMAL(12,2)
city                VARCHAR(50)
fraud_probability   DECIMAL(6,4)
risk_level          VARCHAR(10)
alerted_at          TIMESTAMP
```

---

## Power BI Dashboard

Connect Power BI Desktop to these endpoints:

| Visual             | Endpoint                         | Chart Type   |
| ------------------ | -------------------------------- | ------------ |
| Total Transactions | /powerbi/summary                 | Card         |
| Fraud Rate %       | /powerbi/summary                 | Card         |
| Hourly Trend       | /powerbi/transactions/hourly     | Line Chart   |
| Fraud by Category  | /powerbi/fraud/by-category       | Bar Chart    |
| Fraud by City      | /powerbi/fraud/by-city           | Map          |
| Fraud by Hour      | /powerbi/fraud/by-hour           | Column Chart |
| Payment Methods    | /powerbi/fraud/by-payment-method | Donut Chart  |
| Recent Alerts      | /powerbi/alerts/recent           | Table        |

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
```

---

## Restart Commands

```powershell
# Start all services
cd local
docker-compose up -d

# Terminal 2 - Generator
python local/data_generator/generator.py

# Terminal 3 - FastAPI
cd local/fastapi
python -m uvicorn main:app --reload --port 8000

# Check data
docker exec postgres psql -U fraud_user -d fraud_db -c "SELECT COUNT(*) FROM transactions_enriched;"
```

---

## AWS Migration Plan

Each component maps directly to a managed AWS service:

```
Local Docker     →    AWS Service
──────────────────────────────────
Kafka            →    Amazon MSK
Spark            →    Amazon EMR
PostgreSQL       →    Amazon RDS
FastAPI          →    AWS EC2 / Lambda
Redis            →    ElastiCache
Airflow          →    Amazon MWAA
Power BI         →    stays same
```

---

## What I Learned

- Real-time data streaming with Apache Kafka
- Spark Structured Streaming with foreachBatch
- XGBoost for imbalanced classification
- Feature engineering for fraud detection
- Building production REST APIs with FastAPI
- Docker multi-container orchestration
- Airflow DAG design with branching
- Power BI integration with REST APIs
- PostgreSQL schema design for analytics
- Redis caching for ML predictions

---

## Author

Built as a portfolio project demonstrating production-grade data engineering skills for the Indian fintech industry.

**Stack:** Python · Kafka · Spark · XGBoost · FastAPI · PostgreSQL · Redis · Airflow · Docker · Power BI

---

> "Real fraud detection doesn't happen in Jupyter notebooks. It happens in pipelines like this."
