"""
FraudLens — Spark Structured Streaming
Reads from Kafka → Feature Engineering → Writes to PostgreSQL
Runs inside Docker container
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)
import psycopg2
from datetime import datetime

# ── Schema — must match generator output exactly ───────────────────
TXN_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("account_id",        StringType(),  True),
    StructField("amount_inr",        DoubleType(),  True),
    StructField("merchant_name",     StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("payment_method",    StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("hour_of_day",       IntegerType(), True),
    StructField("day_of_week",       IntegerType(), True),
    StructField("is_fraud",          IntegerType(), True),
    StructField("timestamp",         StringType(),  True),
])

FRAUD_CITIES = [
    'Lagos', 'Moscow', 'Bucharest', 'Kyiv',
    'Unknown Location', 'Minsk', 'Caracas', 'Jakarta'
]

DB = "dbname=fraud_db user=fraud_user password=fraud_pass host=postgres port=5432"

# ── Write each batch to PostgreSQL ─────────────────────────────────


def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"  Batch {batch_id}: empty — skipping")
        return

    rows = batch_df.collect()
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    ok = 0
    flagged = 0

    for r in rows:
        try:
            cur.execute("""
                INSERT INTO transactions_enriched (
                    transaction_id, account_id, amount_inr,
                    merchant_name, merchant_category, payment_method,
                    city, hour_of_day, day_of_week,
                    is_odd_hour, is_foreign_city,
                    is_high_amount, is_weekend,
                    is_fraud, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (transaction_id) DO NOTHING
            """, (
                r.transaction_id,
                r.account_id,
                r.amount_inr,
                r.merchant_name,
                r.merchant_category,
                r.payment_method,
                r.city,
                r.hour_of_day,
                r.day_of_week,
                bool(0 <= r.hour_of_day <= 5),
                bool(r.city in FRAUD_CITIES),
                bool(r.amount_inr > 50000),
                bool(r.day_of_week in (5, 6)),
                r.is_fraud,
                datetime.now()
            ))
            ok += 1
            if r.is_fraud:
                flagged += 1
        except Exception as e:
            print(f"  Row error: {e}")

    conn.commit()
    cur.close()
    conn.close()

    ts = datetime.now().strftime('%H:%M:%S')
    print(f"  [{ts}]  Batch {batch_id}  |  Written: {ok}  |  Fraud: {flagged}")


def main():
    print("=" * 55)
    print("  FraudLens  |  Spark Stream Processor")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Kafka → Feature Engineering → PostgreSQL")
    print("=" * 55)

    spark = SparkSession.builder \
        .appName("FraudLensStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/fraud_checkpoint") \
        .config("spark.hadoop.io.nativeio.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("  Connecting to Kafka...")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "transactions-raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), TXN_SCHEMA).alias("d")) \
        .select("d.*")

    print("  Connected to Kafka. Waiting for transactions...")
    print()
    print(f"  {'TIME':<10}  {'BATCH':<7}  {'WRITTEN':<10}  {'FRAUD'}")
    print(f"  {'-'*10}  {'-'*7}  {'-'*10}  {'-'*5}")

    query = parsed.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
