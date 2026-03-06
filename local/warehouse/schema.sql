-- ============================================================
-- FRAUDLENS — Database Schema
-- Runs automatically when PostgreSQL container starts
-- ============================================================

-- Raw transactions (written by Spark streaming job)
CREATE TABLE IF NOT EXISTS transactions_raw (
    transaction_id    VARCHAR(36) PRIMARY KEY,
    account_id        VARCHAR(20) NOT NULL,
    amount            DECIMAL(12,2),
    merchant_category VARCHAR(50),
    merchant_name     VARCHAR(200),
    country           VARCHAR(5),
    hour_of_day       INT,
    day_of_week       INT,
    is_fraud          INT,
    created_at        TIMESTAMP DEFAULT NOW()
);

-- Enriched transactions with engineered features (written by Spark)
CREATE TABLE IF NOT EXISTS transactions_enriched (
    transaction_id      VARCHAR(36) PRIMARY KEY,
    account_id          VARCHAR(20) NOT NULL,
    amount              DECIMAL(12,2),
    merchant_category   VARCHAR(50),
    country             VARCHAR(5),
    hour_of_day         INT,
    day_of_week         INT,
    is_odd_hour         BOOLEAN,
    is_foreign_country  BOOLEAN,
    is_high_amount      BOOLEAN,
    is_weekend          BOOLEAN,
    is_fraud            INT,
    fraud_probability   DECIMAL(6,4) DEFAULT NULL,
    risk_level          VARCHAR(10)  DEFAULT NULL,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Fraud alerts (written by FastAPI when probability > 0.8)
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id          SERIAL PRIMARY KEY,
    transaction_id    VARCHAR(36),
    account_id        VARCHAR(20),
    amount            DECIMAL(12,2),
    country           VARCHAR(5),
    fraud_probability DECIMAL(6,4),
    risk_level        VARCHAR(10),
    alerted_at        TIMESTAMP DEFAULT NOW()
);

-- Daily fraud summary (built by analytics query / dbt)
CREATE TABLE IF NOT EXISTS fraud_daily_summary (
    summary_date         DATE PRIMARY KEY,
    total_transactions   INT,
    fraud_count          INT,
    fraud_rate_pct       DECIMAL(5,2),
    total_amount_usd     DECIMAL(15,2),
    fraud_amount_usd     DECIMAL(15,2),
    updated_at           TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- ANALYTICS VIEWS (used by Grafana directly)
-- ============================================================

CREATE OR REPLACE VIEW v_fraud_by_category AS
SELECT
    merchant_category,
    COUNT(*)                                           AS total_txns,
    SUM(is_fraud)                                      AS fraud_count,
    ROUND(SUM(is_fraud)::numeric / COUNT(*) * 100, 2)  AS fraud_rate_pct
FROM transactions_enriched
GROUP BY merchant_category
ORDER BY fraud_count DESC;

CREATE OR REPLACE VIEW v_fraud_by_country AS
SELECT
    country,
    COUNT(*)                                           AS total_txns,
    SUM(is_fraud)                                      AS fraud_count,
    ROUND(SUM(is_fraud)::numeric / COUNT(*) * 100, 2)  AS fraud_rate_pct,
    SUM(amount)                                        AS total_amount
FROM transactions_enriched
GROUP BY country
ORDER BY fraud_count DESC;

CREATE OR REPLACE VIEW v_fraud_by_hour AS
SELECT
    hour_of_day,
    COUNT(*)        AS total_txns,
    SUM(is_fraud)   AS fraud_count
FROM transactions_enriched
GROUP BY hour_of_day
ORDER BY hour_of_day;

CREATE OR REPLACE VIEW v_recent_fraud_alerts AS
SELECT *
FROM fraud_alerts
ORDER BY alerted_at DESC
LIMIT 100;

CREATE OR REPLACE VIEW v_hourly_stats AS
SELECT
    DATE_TRUNC('hour', created_at)                     AS hour_bucket,
    COUNT(*)                                           AS total_txns,
    SUM(is_fraud)                                      AS fraud_count,
    ROUND(SUM(is_fraud)::numeric / COUNT(*) * 100, 2)  AS fraud_rate_pct,
    ROUND(AVG(amount)::numeric, 2)                     AS avg_amount
FROM transactions_enriched
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY hour_bucket DESC;

-- Seed with some initial data so Grafana doesn't show empty dashboards
INSERT INTO fraud_daily_summary (summary_date, total_transactions, fraud_count, fraud_rate_pct, total_amount_usd, fraud_amount_usd)
VALUES (CURRENT_DATE, 0, 0, 0.00, 0.00, 0.00)
ON CONFLICT DO NOTHING;
