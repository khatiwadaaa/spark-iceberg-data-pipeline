
import os
import json
import subprocess
import urllib.request
import urllib.error

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

DATA_DIR = Path("/workspace/data/raw")


# -------------------------------------------------
# Discord failure notification
# -------------------------------------------------

def failure_alert(context):
    ti = context["task_instance"]
    error = context.get("exception")

    message = (
        "🚨 LAKEFLOW PIPELINE FAILURE\n"
        f"Time: {datetime.now(timezone.utc).isoformat()}\n"
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Run ID: {ti.run_id}\n"
        f"Error: {str(error)[:500]}"
    )

    print(message, flush=True)

    webhook = os.environ.get("LAKEFLOW_DISCORD_WEBHOOK")

    if not webhook:
        print("Discord webhook is not configured", flush=True)
        return

    payload = json.dumps({
        "content": message
    }).encode("utf-8")

    request = urllib.request.Request(
        webhook,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "LakeFlow-Monitor/1.0"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            print(
                f"Discord notification status: {response.status}",
                flush=True
            )

    except urllib.error.HTTPError as exc:
        print(f"Discord HTTP error: {exc.code}", flush=True)

    except Exception as exc:
        print(f"Discord notification failed: {exc}", flush=True)


# -------------------------------------------------
# Apache Airflow DAG
# -------------------------------------------------

@dag(
    dag_id="lakeflow_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
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

        print(f"Discovered batches: {names}", flush=True)
        return names

    @task
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
                str(DATA_DIR / name),
            ]

            subprocess.run(command, check=True)

            print(f"Completed ingestion: {name}", flush=True)

    # Temporary task to verify Discord notifications
    

    # Task dependencies
    batches = discover_batches()
    validation = validate_batches(batches)
    ingestion = ingest_orders(batches)

    validation >> ingestion

    # Independent failure test
    


lakeflow_pipeline()
