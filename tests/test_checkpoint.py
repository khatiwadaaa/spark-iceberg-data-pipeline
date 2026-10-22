
import hashlib
import uuid

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("checkpoint_warehouse")

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("lakeflow-checkpoint-tests")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.checkpoint",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.checkpoint.type", "hadoop")
        .config(
            "spark.sql.catalog.checkpoint.warehouse",
            str(warehouse),
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE NAMESPACE IF NOT EXISTS checkpoint.demo")

    yield session
    session.stop()


@pytest.fixture
def checkpoint_table(spark):
    table = f"checkpoint.demo.batches_{uuid.uuid4().hex[:12]}"

    spark.sql(f"""
        CREATE TABLE {table} (
            batch_hash STRING,
            source_file STRING,
            processed_at TIMESTAMP
        ) USING iceberg
    """)

    return table


def calculate_hash(content):
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def record_checkpoint(spark, table, batch_hash, source_file):
    # Register the checkpoint input as a temporary SQL view
    incoming = spark.createDataFrame(
        [(batch_hash, source_file)],
        ["batch_hash", "source_file"],
    )

    incoming.createOrReplaceTempView("new_checkpoint")

    spark.sql(f"""
        MERGE INTO {table} AS target
        USING new_checkpoint AS source
        ON target.batch_hash = source.batch_hash
        WHEN NOT MATCHED THEN
          INSERT (batch_hash, source_file, processed_at)
          VALUES (
            source.batch_hash,
            source.source_file,
            current_timestamp()
          )
    """)


def test_duplicate_batch_does_not_duplicate_checkpoint(
    spark, checkpoint_table
):
    batch_hash = calculate_hash('{"order_id":"o-1"}')

    record_checkpoint(
        spark, checkpoint_table, batch_hash, "batch_1.json"
    )

    record_checkpoint(
        spark, checkpoint_table, batch_hash, "batch_1.json"
    )

    count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {checkpoint_table}"
    ).first()["total"]

    assert count == 1


def test_different_batches_create_separate_checkpoints(
    spark, checkpoint_table
):
    hash_1 = calculate_hash('{"order_id":"o-1"}')
    hash_2 = calculate_hash('{"order_id":"o-2"}')

    record_checkpoint(spark, checkpoint_table, hash_1, "batch_1.json")
    record_checkpoint(spark, checkpoint_table, hash_2, "batch_2.json")

    count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {checkpoint_table}"
    ).first()["total"]

    assert count == 2


def test_content_hash_detects_changes():
    original = '{"order_id":"o-1","status":"pending"}'
    updated = '{"order_id":"o-1","status":"paid"}'

    assert calculate_hash(original) != calculate_hash(updated)
