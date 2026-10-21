# LakeFlow — Spark + Iceberg + Airflow (building step by step)

Python-based data engineering portfolio project. **Current implemented milestone:** local Spark ingestion into an Iceberg table using deduplicated, timestamp-guarded `MERGE INTO`. All other folders are scaffolded for subsequent milestones.

## Prerequisites
- Docker Desktop installed and running
- VS Code (recommended)
- Network connection for the initial Spark image and Iceberg dependency download

## Windows PowerShell
Open a terminal in the **lakeflow** project root.

```powershell
docker compose version
docker compose run --rm spark
$env:BATCH_FILE='orders_batch_2.json'; docker compose run --rm spark; Remove-Item Env:BATCH_FILE
docker compose run --rm spark
```

Or use:

```powershell
.\scripts\run-batch.ps1 orders_batch_1.json
.\scripts\run-batch.ps1 orders_batch_2.json
.\scripts\run-batch.ps1 orders_batch_1.json
```

The first run should show 3 orders. The second adds a new order and updates an existing one (4 unique orders). Replaying the first batch should retain 4 unique orders without overwriting the newer update. **Logical idempotence** does not guarantee zero new Iceberg metadata/snapshots.

## Linux/macOS

```bash
docker compose run --rm spark
BATCH_FILE=orders_batch_2.json docker compose run --rm spark
```

## Project layout

- `jobs/ingest.py`: working Spark JSONL-to-Iceberg MERGE job
- `data/raw/`: sample batches
- `warehouse/`: persisted local Iceberg tables (do not commit)
- `src/`: reusable Python code as we refactor
- `dags/`: future Airflow workflows
- `tests/`: future automated tests
- `scripts/`: convenience launch scripts
- `docs/`: architecture notes

## Planned milestones

1. Validate Spark ingestion and rerun behavior
2. Add checkpoint records and ingestion tests
3. Demonstrate Iceberg schema evolution and time travel
4. Add Airflow DAGs, retries, and alerting
5. Documentation and portfolio-ready metrics

Not yet executed in this environment: Docker runtime. Docker/engine compatibility and dependency download may need adjustment on your machine.
