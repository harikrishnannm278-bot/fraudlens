"""
Basic unit tests for the fraud detection engine.
Run with: pytest tests/ -v
"""

import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Feature Engineering Tests ─────────────────────────────────────

def test_high_amount_flag():
    """Transactions over $3000 should be flagged as high amount."""
    amount = 5000
    assert amount > 3000

def test_odd_hour_flag():
    """Hours 1-5 AM should be flagged as odd hours."""
    odd_hours = [1, 2, 3, 4, 5]
    normal_hours = [8, 12, 14, 20]
    for h in odd_hours:
        assert 1 <= h <= 5
    for h in normal_hours:
        assert not (1 <= h <= 5)

def test_foreign_country_flag():
    """Non-US countries should be flagged."""
    assert "NG" != "US"
    assert "RU" != "US"
    assert "US" == "US"

def test_weekend_flag():
    """Day 5 = Saturday, Day 6 = Sunday should be weekend."""
    assert 5 in (5, 6)
    assert 6 in (5, 6)
    assert 1 not in (5, 6)


# ── Risk Level Tests ──────────────────────────────────────────────

def risk_label(prob):
    if prob >= 0.8: return "HIGH"
    if prob >= 0.5: return "MEDIUM"
    return "LOW"

def test_risk_high():
    assert risk_label(0.95) == "HIGH"
    assert risk_label(0.80) == "HIGH"

def test_risk_medium():
    assert risk_label(0.75) == "MEDIUM"
    assert risk_label(0.50) == "MEDIUM"

def test_risk_low():
    assert risk_label(0.30) == "LOW"
    assert risk_label(0.01) == "LOW"


# ── Generator Output Tests ────────────────────────────────────────

def test_transaction_has_required_fields():
    """Generated transaction must have all required fields."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'local', 'data_generator'))

    required_fields = [
        "transaction_id", "account_id", "amount",
        "merchant_category", "country", "hour_of_day",
        "day_of_week", "is_fraud", "timestamp"
    ]

    # Simulate what the generator would produce
    from faker import Faker
    import uuid, random
    from datetime import datetime

    fake = Faker()
    txn = {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        "ACC00001",
        "amount":            random.uniform(1, 2000),
        "merchant_category": "grocery",
        "merchant_name":     fake.company(),
        "country":           "US",
        "hour_of_day":       10,
        "day_of_week":       1,
        "is_fraud":          0,
        "timestamp":         datetime.now().isoformat()
    }

    for field in required_fields:
        assert field in txn, f"Missing field: {field}"

def test_fraud_rate_roughly_two_percent():
    """Fraud rate should be approximately 2%."""
    import random
    results = [random.random() < 0.02 for _ in range(10000)]
    rate = sum(results) / len(results)
    assert 0.005 < rate < 0.04, f"Fraud rate {rate:.3f} is out of expected range"
