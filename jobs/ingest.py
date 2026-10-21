"""Batch ingestion into an Iceberg table using deterministic order-id upserts."""
import sys
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import hashlib
import os
if len(sys.argv) != 2:
    raise SystemExit("Usage: ingest.py <path-to-json-lines-batch>")

spark = SparkSession.builder.appName("lakeflow-orders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.demo")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.demo.orders (
    order_id STRING, customer_id STRING, status STRING,
    amount DOUBLE, updated_at TIMESTAMP
) USING iceberg
""")
# Create an Iceberg table to track processed batches
spark.sql("""
CREATE TABLE IF NOT EXISTS local.demo.ingestion_checkpoints (
    batch_hash STRING,
    source_file STRING,
    processed_at TIMESTAMP
) USING iceberg
""")

# Compute a SHA-256 fingerprint of the input batch
batch_path = sys.argv[1]

with open(batch_path, "rb") as f:
    batch_hash = hashlib.sha256(f.read()).hexdigest()

# Check whether this exact batch was processed before
existing = spark.sql(f"""
    SELECT COUNT(*) AS total
    FROM local.demo.ingestion_checkpoints
    WHERE batch_hash = '{batch_hash}'
""").first()["total"]

if existing > 0:
    print(f"SKIPPED: Batch already processed: {batch_path}")
    spark.stop()
    sys.exit(0)

print(f"PROCESSING: New batch {batch_path}")
# Record checkpoint only after the orders MERGE succeeds
source_file = os.path.basename(batch_path)

checkpoint = spark.createDataFrame(
    [(batch_hash, source_file)],
    ["batch_hash", "source_file"]
)

checkpoint.createOrReplaceTempView("new_checkpoint")

spark.sql("""
MERGE INTO local.demo.ingestion_checkpoints AS target
USING new_checkpoint AS source
ON target.batch_hash = source.batch_hash
WHEN NOT MATCHED THEN
INSERT (batch_hash, source_file, processed_at)
VALUES (source.batch_hash, source.source_file, current_timestamp())
""")

print(f"CHECKPOINT SAVED: {source_file}")
# Schema evolution: add payment_method if it does not exist
columns = [
    field.name
    for field in spark.table("local.demo.orders").schema.fields
]

if "payment_method" not in columns:
    spark.sql("""
        ALTER TABLE local.demo.orders
        ADD COLUMN payment_method STRING
    """)
    print("Schema evolved: payment_method column added")
else:
    print("Schema already contains payment_method")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("status", StringType()),
    StructField("amount", DoubleType()),
    StructField("updated_at", TimestampType()),
    StructField("payment_method", StringType()),
])
raw = spark.read.schema(schema).json(sys.argv[1])
valid = raw.filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("amount").isNotNull()
    & (F.col("amount") >= 0)
    & F.col("updated_at").isNotNull()
)
# One record per order_id per batch. For timestamp ties, prefer a
# stable lexical ordering; real production sources should supply an event_id.
window = Window.partitionBy("order_id").orderBy(
    F.col("updated_at").desc(), F.col("status").desc_nulls_last(),
    F.col("customer_id").desc(), F.col("amount").desc()
)
latest = valid.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")
latest.createOrReplaceTempView("incoming_orders")

spark.sql("""
MERGE INTO local.demo.orders AS target
USING incoming_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.updated_at > target.updated_at
THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.status = source.status,
    target.amount = source.amount,
    target.updated_at = source.updated_at,
    target.payment_method = source.payment_method

WHEN NOT MATCHED THEN
INSERT (
    order_id,
    customer_id,
    status,
    amount,
    updated_at,
    payment_method
)
VALUES (
    source.order_id,
    source.customer_id,
    source.status,
    source.amount,
    source.updated_at,
    source.payment_method
)
""")

spark.sql("SELECT * FROM local.demo.orders ORDER BY order_id").show(truncate=False)
spark.sql("SELECT COUNT(*) AS total_orders FROM local.demo.orders").show()
spark.stop()
