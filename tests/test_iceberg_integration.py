
import uuid
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def iceberg_spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("lakeflow-iceberg-integration")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.test",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.test.type", "hadoop")
        .config("spark.sql.catalog.test.warehouse", str(warehouse))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark
    spark.stop()


@pytest.fixture
def orders_table(iceberg_spark):
    table_name = f"test.orders_{uuid.uuid4().hex[:12]}"

    iceberg_spark.sql("CREATE NAMESPACE IF NOT EXISTS test.demo")

    table = table_name.replace("test.orders_", "test.demo.orders_")

    iceberg_spark.sql(f"""
        CREATE TABLE {table} (
            order_id STRING,
            status STRING,
            amount DOUBLE
        ) USING iceberg
    """)

    yield table


def test_iceberg_upsert(iceberg_spark, orders_table):
    iceberg_spark.sql(f"""
        INSERT INTO {orders_table}
        VALUES ('o-1', 'pending', 50.0)
    """)

    iceberg_spark.sql(f"""
        MERGE INTO {orders_table} t
        USING (
            SELECT 'o-1' AS order_id,
                   'paid' AS status,
                   50.0 AS amount
        ) s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    result = iceberg_spark.sql(
        f"SELECT * FROM {orders_table}"
    ).collect()

    assert len(result) == 1
    assert result[0]["status"] == "paid"


def test_schema_evolution(iceberg_spark, orders_table):
    iceberg_spark.sql(f"""
        ALTER TABLE {orders_table}
        ADD COLUMN payment_method STRING
    """)

    columns = iceberg_spark.table(orders_table).columns

    assert "payment_method" in columns


def test_time_travel(iceberg_spark, orders_table):
    iceberg_spark.sql(f"""
        INSERT INTO {orders_table}
        VALUES ('o-1', 'pending', 50.0)
    """)

    snapshots = iceberg_spark.sql(f"""
        SELECT snapshot_id
        FROM {orders_table}.snapshots
        ORDER BY committed_at
    """).collect()

    first_snapshot = snapshots[0]["snapshot_id"]

    iceberg_spark.sql(f"""
        UPDATE {orders_table}
        SET status = 'paid'
        WHERE order_id = 'o-1'
    """)

    old_status = iceberg_spark.sql(f"""
        SELECT status
        FROM {orders_table}
        VERSION AS OF {first_snapshot}
    """).first()["status"]

    assert old_status == "pending"
