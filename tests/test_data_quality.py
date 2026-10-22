
import pytest
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from datetime import datetime


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("lakeflow-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_orders(spark):
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("status", StringType()),
        StructField("amount", DoubleType()),
        StructField("updated_at", TimestampType()),
    ])

    rows = [
        ("o-1", "c-1", "pending", 50.0, datetime(2026, 10, 20, 10)),
        ("o-1", "c-1", "paid", 50.0, datetime(2026, 10, 21, 10)),
        ("o-2", "c-2", "paid", -10.0, datetime(2026, 10, 21, 11)),
        ("o-3", "c-3", "paid", 75.0, datetime(2026, 10, 21, 12)),
    ]

    return spark.createDataFrame(rows, schema)


def test_reject_negative_amounts(sample_orders):
    valid = sample_orders.filter(F.col("amount") >= 0)

    assert valid.count() == 3
    assert valid.filter(F.col("amount") < 0).count() == 0


def test_latest_order_wins(sample_orders):
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc()
    )

    latest = (
        sample_orders
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    result = latest.filter(
        F.col("order_id") == "o-1"
    ).first()

    assert result["status"] == "paid"


def test_no_duplicate_orders(sample_orders):
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc()
    )

    latest = (
        sample_orders
        .filter(F.col("amount") >= 0)
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    assert latest.count() == 2
    assert latest.select("order_id").distinct().count() == 2
