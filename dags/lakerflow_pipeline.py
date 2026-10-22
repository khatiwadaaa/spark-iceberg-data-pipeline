
from datetime import timedelta
from pathlib import Path
import json
import subprocess
import pendulum
from airflow.sdk import dag, task
from datetime import datetime, timezone
DATA_DIR = Path("/opt/airflow/lakeflow_data/raw")
def failure_alert(context):
    task = context["task_instance"]
    error = context.get("exception")

    print("=" * 50)
    print("LAKEFLOW PIPELINE FAILURE")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"DAG: {task.dag_id}")
    print(f"Task: {task.task_id}")
    print(f"Run ID: {task.run_id}")
    print(f"Error: {error}")
    print("=" * 50)

@dag(
    dag_id="lakeflow_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args={
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": failure_alert,
    },
    tags=["lakeflow", "spark", "iceberg"],
)
def lakeflow_pipeline():

    @task
    def discover_batches():
        files = sorted(DATA_DIR.glob("orders_batch_*.json"))

        if not files:
            raise FileNotFoundError("No order batches found")

        names = [file.name for file in files]
        print(f"Discovered batches: {names}")
        return names

    @tasdefk
    def validate_batches(names):
        for name in names:
            path = DATA_DIR / name
            count = 0

            with path.open(encoding="utf-8") as file:
                for line in file:
                    if line.strip():
                        record = json.loads(line)
                        if not record.get("order_id"):
                            raise ValueError(
                                f"Missing order_id in {name}"
                            )
                        count += 1

            print(f"Validated {name}: {count} records")

        print("All input batches validated successfully")

    validate_batches(discover_batches())

    @task
    def ingest_orders(names):
        for name in names:
            print(f"Starting Spark ingestion: {name}", flush=True)

            command = [
                "spark-submit",
                "--master", "local[2]",
                "--packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                "--conf",
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "--conf",
                "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
                "--conf",
                "spark.sql.catalog.local.type=hadoop",
                "--conf",
                "spark.sql.catalog.local.warehouse=/workspace/warehouse",
                "/workspace/jobs/ingest.py",
                f"/workspace/data/raw/{name}",
            ]

            subprocess.run(command, check=True)

            print(f"Completed ingestion: {name}", flush=True)
    batches = discover_batches()
    validation = validate_batches(batches)
    ingestion = ingest_orders(batches)

    validation >> ingestion
lakeflow_pipeline()
