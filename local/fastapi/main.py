"""
FraudLens API — Full Version
Serves real-time fraud predictions + Power BI data endpoints.

Start with:
    uvicorn main:app --reload --port 8000

Swagger UI:  http://localhost:8000/docs
Power BI connects to endpoints under /powerbi/*
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import joblib
import redis
import psycopg2
import psycopg2.extras
import os
from datetime import datetime

# ── App Setup ─────────────────────────────────────────────────────
app = FastAPI(
    title='FraudLens API',
    description='FraudLens — Real-time fraud scoring + Power BI data endpoints',
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Constants ─────────────────────────────────────────────────────
FRAUD_CITIES = [
    'Lagos', 'Moscow', 'Bucharest', 'Kyiv',
    'Unknown Location', 'Minsk', 'Caracas', 'Jakarta'
]

# ── Load Model ────────────────────────────────────────────────────
MODEL_PATH = "/app/ml/fraud_model.pkl"
ENCODER_PATH = "/app/ml/label_encoders.pkl"
model = None
encoders = None


@app.on_event("startup")
def load_model():
    global model, encoders
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
        encoders = joblib.load(ENCODER_PATH)
        print("Model loaded successfully")
    else:
        print(f"Model not found at {MODEL_PATH}. Run train_model.py first.")


# ── Redis ─────────────────────────────────────────────────────────
try:
    redis_client = redis.Redis(
        host="localhost", port=6379, decode_responses=True)
    redis_client.ping()
    REDIS_OK = True
    print("Redis connected")
except Exception:
    redis_client = None
    REDIS_OK = False

# ── DB Helpers ────────────────────────────────────────────────────


def get_pg():
    return psycopg2.connect(
        "dbname=fraud_db user=fraud_user password=fraud_pass host=postgres port=5432"
    )


def query(sql: str, params=None) -> list:
    try:
        conn = get_pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"DB query error: {e}")
        return []


def execute(sql: str, params=None):
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB execute error: {e}")

# ── Request / Response Models ─────────────────────────────────────


class TransactionRequest(BaseModel):
    transaction_id:    str
    account_id:        str
    amount_inr:        float
    merchant_name:     str
    merchant_category: str
    payment_method:    str
    city:              str
    hour_of_day:       int
    day_of_week:       int


class FraudResponse(BaseModel):
    transaction_id:    str
    account_id:        str
    amount_inr:        float
    fraud_probability: float
    is_fraud:          bool
    risk_level:        str
    cached:            bool = False
    scored_at:         str

# ── Helpers ───────────────────────────────────────────────────────


def risk_label(prob: float) -> str:
    if prob >= 0.8:
        return "HIGH"
    if prob >= 0.5:
        return "MEDIUM"
    return "LOW"


def safe_encode(col: str, val: str) -> int:
    if encoders and col in encoders:
        le = encoders[col]
        if val in le.classes_:
            return int(le.transform([val])[0])
    return 0


def build_features(txn: TransactionRequest) -> list:
    return [[
        txn.amount_inr,
        txn.hour_of_day,
        txn.day_of_week,
        safe_encode("merchant_category", txn.merchant_category),
        safe_encode("payment_method",    txn.payment_method),
        safe_encode("city",              txn.city),
        int(0 <= txn.hour_of_day <= 5),
        int(txn.city in FRAUD_CITIES),
        int(txn.amount_inr > 50000),
        int(txn.day_of_week in (5, 6)),
    ]]

# ═══════════════════════════════════════════════════════════════════
# GENERAL
# ═══════════════════════════════════════════════════════════════════


@app.get("/", tags=["General"])
def root():
    return {
        "message":      "FraudLens API is running",
        "docs":         "http://localhost:8000/docs",
        "powerbi_base": "http://localhost:8000/powerbi/"
    }


@app.get("/health", tags=["General"])
def health():
    return {
        "status":    "healthy",
        "model":     "loaded" if model else "not loaded",
        "redis":     "connected" if REDIS_OK else "unavailable",
        "timestamp": datetime.now().isoformat()
    }

# ═══════════════════════════════════════════════════════════════════
# FRAUD PREDICTION
# ═══════════════════════════════════════════════════════════════════


@app.post("/predict", response_model=FraudResponse, tags=["Prediction"])
def predict(txn: TransactionRequest):
    """Score a single transaction for fraud. Returns probability + risk level."""
    if model is None:
        raise HTTPException(503, "Model not loaded.")

    cache_key = f"fraud:{txn.transaction_id}"
    if redis_client:
        cached_val = redis_client.get(cache_key)
        if cached_val:
            prob = float(cached_val)
            return FraudResponse(
                transaction_id=txn.transaction_id,
                account_id=txn.account_id,
                amount_inr=txn.amount_inr,
                fraud_probability=round(prob, 4),
                is_fraud=prob >= 0.5,
                risk_level=risk_label(prob),
                cached=True,
                scored_at=datetime.now().isoformat()
            )

    prob = float(model.predict_proba(build_features(txn))[0][1])
    level = risk_label(prob)

    if redis_client:
        redis_client.setex(cache_key, 3600, str(prob))

    if prob >= 0.5:
        execute("""
            INSERT INTO fraud_alerts
                (transaction_id, account_id, amount_inr, city, fraud_probability, risk_level)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
        """, (txn.transaction_id, txn.account_id, txn.amount_inr,
              txn.city, round(prob, 4), level))

    return FraudResponse(
        transaction_id=txn.transaction_id,
        account_id=txn.account_id,
        amount_inr=txn.amount_inr,
        fraud_probability=round(prob, 4),
        is_fraud=prob >= 0.5,
        risk_level=level,
        cached=False,
        scored_at=datetime.now().isoformat()
    )

# ═══════════════════════════════════════════════════════════════════
# POWER BI ENDPOINTS
# ═══════════════════════════════════════════════════════════════════


@app.get("/powerbi/summary", tags=["Power BI"])
def powerbi_summary():
    rows = query("""
        SELECT
            COUNT(*)                                                                AS total_transactions,
            SUM(is_fraud)                                                           AS fraud_count,
            COUNT(*) - SUM(is_fraud)                                                AS legit_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)             AS fraud_rate_pct,
            ROUND(SUM(amount_inr)::numeric, 2)                                      AS total_amount_inr,
            ROUND(SUM(CASE WHEN is_fraud=1 THEN amount_inr ELSE 0 END)::numeric, 2) AS fraud_amount_inr,
            ROUND(AVG(amount_inr)::numeric, 2)                                      AS avg_transaction_inr,
            ROUND(MAX(amount_inr)::numeric, 2)                                      AS max_transaction_inr
        FROM transactions_enriched
    """)
    return rows[0] if rows else {}


@app.get("/powerbi/transactions/hourly", tags=["Power BI"])
def powerbi_hourly(hours: int = Query(default=24)):
    return query("""
        SELECT
            DATE_TRUNC('hour', created_at)                                      AS hour_bucket,
            COUNT(*)                                                            AS total_txns,
            SUM(is_fraud)                                                       AS fraud_count,
            COUNT(*) - SUM(is_fraud)                                            AS legit_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)         AS fraud_rate_pct,
            ROUND(AVG(amount_inr)::numeric, 2)                                  AS avg_amount_inr
        FROM transactions_enriched
        WHERE created_at >= NOW() - INTERVAL '1 hour' * %s
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY hour_bucket DESC
    """, (hours,))


@app.get("/powerbi/transactions/daily", tags=["Power BI"])
def powerbi_daily(days: int = Query(default=30)):
    return query("""
        SELECT
            DATE(created_at)                                                            AS txn_date,
            COUNT(*)                                                                    AS total_txns,
            SUM(is_fraud)                                                               AS fraud_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)                 AS fraud_rate_pct,
            ROUND(SUM(amount_inr)::numeric, 2)                                          AS total_amount_inr,
            ROUND(SUM(CASE WHEN is_fraud=1 THEN amount_inr ELSE 0 END)::numeric, 2)     AS fraud_amount_inr
        FROM transactions_enriched
        WHERE created_at >= NOW() - INTERVAL '1 day' * %s
        GROUP BY DATE(created_at)
        ORDER BY txn_date DESC
    """, (days,))


@app.get("/powerbi/fraud/by-category", tags=["Power BI"])
def powerbi_by_category():
    return query("""
        SELECT
            merchant_category,
            COUNT(*)                                                                    AS total_txns,
            SUM(is_fraud)                                                               AS fraud_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)                 AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN is_fraud=1 THEN amount_inr ELSE 0 END)::numeric, 2)     AS fraud_amount_inr
        FROM transactions_enriched
        GROUP BY merchant_category
        ORDER BY fraud_count DESC
    """)


@app.get("/powerbi/fraud/by-city", tags=["Power BI"])
def powerbi_by_city():
    return query("""
        SELECT
            city,
            COUNT(*)                                                                    AS total_txns,
            SUM(is_fraud)                                                               AS fraud_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)                 AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN is_fraud=1 THEN amount_inr ELSE 0 END)::numeric, 2)     AS fraud_amount_inr
        FROM transactions_enriched
        GROUP BY city
        ORDER BY fraud_count DESC
    """)


@app.get("/powerbi/fraud/by-hour", tags=["Power BI"])
def powerbi_by_hour():
    return query("""
        SELECT
            hour_of_day,
            COUNT(*)                                                        AS total_txns,
            SUM(is_fraud)                                                   AS fraud_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)    AS fraud_rate_pct
        FROM transactions_enriched
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """)


@app.get("/powerbi/fraud/by-payment-method", tags=["Power BI"])
def powerbi_by_payment():
    return query("""
        SELECT
            payment_method,
            COUNT(*)                                                                    AS total_txns,
            SUM(is_fraud)                                                               AS fraud_count,
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2)                 AS fraud_rate_pct
        FROM transactions_enriched
        GROUP BY payment_method
        ORDER BY fraud_count DESC
    """)


@app.get("/powerbi/fraud/by-risk-level", tags=["Power BI"])
def powerbi_by_risk():
    return query("""
        SELECT
            risk_level,
            COUNT(*)                                    AS alert_count,
            ROUND(AVG(fraud_probability)::numeric, 4)   AS avg_probability,
            ROUND(SUM(amount_inr)::numeric, 2)          AS total_amount_inr
        FROM fraud_alerts
        GROUP BY risk_level
        ORDER BY alert_count DESC
    """)


@app.get("/powerbi/alerts/recent", tags=["Power BI"])
def powerbi_recent_alerts(limit: int = Query(default=100)):
    return query("""
        SELECT
            alert_id, transaction_id, account_id,
            ROUND(amount_inr::numeric, 2)           AS amount_inr,
            city,
            ROUND(fraud_probability::numeric, 4)    AS fraud_probability,
            risk_level, alerted_at
        FROM fraud_alerts
        ORDER BY alerted_at DESC
        LIMIT %s
    """, (limit,))


@app.get("/powerbi/accounts/top-flagged", tags=["Power BI"])
def powerbi_top_flagged(limit: int = Query(default=20)):
    return query("""
        SELECT
            account_id,
            COUNT(*)                                    AS alert_count,
            ROUND(MAX(fraud_probability)::numeric, 4)   AS max_probability,
            ROUND(SUM(amount_inr)::numeric, 2)          AS total_flagged_inr,
            MAX(alerted_at)                             AS last_alert_at
        FROM fraud_alerts
        GROUP BY account_id
        ORDER BY alert_count DESC
        LIMIT %s
    """, (limit,))


@app.get("/powerbi/model/performance", tags=["Power BI"])
def powerbi_model_performance():
    rows = query("""
        SELECT
            COUNT(*)                                                                    AS total_scored,
            SUM(CASE WHEN is_fraud=1 AND fraud_probability >= 0.5 THEN 1 ELSE 0 END)   AS true_positives,
            SUM(CASE WHEN is_fraud=0 AND fraud_probability >= 0.5 THEN 1 ELSE 0 END)   AS false_positives,
            SUM(CASE WHEN is_fraud=1 AND fraud_probability <  0.5 THEN 1 ELSE 0 END)   AS false_negatives,
            SUM(CASE WHEN is_fraud=0 AND fraud_probability <  0.5 THEN 1 ELSE 0 END)   AS true_negatives
        FROM transactions_enriched
        WHERE fraud_probability IS NOT NULL
    """)
    if not rows or not rows[0]["total_scored"]:
        return {"message": "No scored transactions yet."}
    r = rows[0]
    tp = r["true_positives"] or 0
    fp = r["false_positives"] or 0
    fn = r["false_negatives"] or 0
    tn = r["true_negatives"] or 0
    precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0
    recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0
    f1 = round(2 * precision * recall / (precision + recall),
               4) if (precision + recall) > 0 else 0
    accuracy = round((tp + tn) / (tp + fp + fn + tn),
                     4) if (tp + fp + fn + tn) > 0 else 0
    return {
        "total_scored":    int(r["total_scored"]),
        "true_positives":  tp, "false_positives": fp,
        "false_negatives": fn, "true_negatives":  tn,
        "precision": precision, "recall": recall,
        "f1_score":  f1,        "accuracy": accuracy,
    }

# ═══════════════════════════════════════════════════════════════════
# STATS & ALERTS
# ═══════════════════════════════════════════════════════════════════


@app.get("/alerts/recent", tags=["Alerts"])
def recent_alerts(limit: int = 20):
    return query("SELECT * FROM fraud_alerts ORDER BY alerted_at DESC LIMIT %s", (limit,))


@app.get("/alerts/stats", tags=["Alerts"])
def alert_stats():
    return {
        "all_time": query("""
            SELECT risk_level, COUNT(*) AS count,
                   ROUND(SUM(amount_inr)::numeric,2) AS total_inr
            FROM fraud_alerts GROUP BY risk_level ORDER BY count DESC
        """),
        "today": query("""
            SELECT risk_level, COUNT(*) AS count
            FROM fraud_alerts WHERE DATE(alerted_at) = CURRENT_DATE
            GROUP BY risk_level ORDER BY count DESC
        """)
    }


@app.get("/stats/summary", tags=["Stats"])
def stats_summary():
    rows = query("""
        SELECT COUNT(*) AS total, SUM(is_fraud) AS fraud,
               ROUND(SUM(is_fraud)::numeric/NULLIF(COUNT(*),0)*100,2) AS fraud_pct
        FROM transactions_enriched
    """)
    return rows[0] if rows else {}
