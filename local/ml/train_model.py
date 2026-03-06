"""
FraudLens — XGBoost Model Training
Loads Indian fintech transaction data from PostgreSQL
Trains fraud detection model and saves to disk
Run: python local/ml/train_model.py
"""

import pandas as pd
import xgboost as xgb
import joblib
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')

DB_CONN = "dbname=fraud_db user=fraud_user password=fraud_pass host=postgres port=5432"
MODEL_PATH = "local/ml/fraud_model.pkl"
ENCODER_PATH = "local/ml/label_encoders.pkl"

FEATURES = [
    'amount_inr', 'hour_of_day', 'day_of_week',
    'merchant_category_enc', 'payment_method_enc', 'city_enc',
    'is_odd_hour', 'is_foreign_city', 'is_high_amount', 'is_weekend'
]


def load_data():
    print("Loading data from PostgreSQL...")
    conn = psycopg2.connect(DB_CONN)
    df = pd.read_sql("""
        SELECT
            amount_inr, hour_of_day, day_of_week,
            merchant_category, payment_method, city,
            is_odd_hour, is_foreign_city,
            is_high_amount, is_weekend, is_fraud
        FROM transactions_enriched
        WHERE is_fraud IS NOT NULL
    """, conn)
    conn.close()
    print(
        f"Loaded {len(df):,} rows | Fraud: {df['is_fraud'].sum():,} ({df['is_fraud'].mean()*100:.1f}%)")
    return df


def encode_features(df):
    encoders = {}
    for col in ['merchant_category', 'payment_method', 'city']:
        le = LabelEncoder()
        df[f'{col}_enc'] = le.fit_transform(df[col].fillna('unknown'))
        encoders[col] = le
    for col in ['is_odd_hour', 'is_foreign_city', 'is_high_amount', 'is_weekend']:
        df[col] = df[col].astype(int)
    return df, encoders


def train(df, encoders):
    X = df[FEATURES]
    y = df['is_fraud']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    fraud_count = y_train.sum()
    scale_weight = (len(y_train) - fraud_count) / max(fraud_count, 1)
    print(f"Class imbalance ratio: {scale_weight:.1f}x")

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        scale_pos_weight=scale_weight,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric='auc',
        random_state=42,
        verbosity=0
    )

    print("Training XGBoost model...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50
    )

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)

    print(f"\nROC-AUC Score: {auc:.4f}")
    print(classification_report(y_test, y_pred,
          target_names=['Legit', 'Fraud']))

    cm = confusion_matrix(y_test, y_pred)
    print(f"True Negatives  (Correct Legit) : {cm[0][0]:>6,}")
    print(f"False Positives (False Alarms)  : {cm[0][1]:>6,}")
    print(f"False Negatives (Missed Fraud)  : {cm[1][0]:>6,}")
    print(f"True Positives  (Caught Fraud)  : {cm[1][1]:>6,}")

    return model


def main():
    print("=" * 50)
    print("  FRAUDLENS MODEL TRAINING")
    print("=" * 50)

    df = load_data()

    if len(df) < 500:
        print(f"Only {len(df)} rows — need at least 500. Wait for more data.")
        return

    df, encoders = encode_features(df)
    model = train(df, encoders)

    joblib.dump(model,    MODEL_PATH)
    joblib.dump(encoders, ENCODER_PATH)

    print(f"\nModel saved to {MODEL_PATH}")
    print("Training complete! Now start FastAPI.")


if __name__ == '__main__':
    main()
