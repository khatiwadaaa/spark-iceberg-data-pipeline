# LakeFlow — Reliable Data Lakehouse Pipeline

**Apache Spark • Apache Iceberg • Apache Airflow • Python • Docker • Pytest**

LakeFlow is a containerized data engineering project that demonstrates reliable batch ingestion into an Apache Iceberg data lakehouse. It processes e-commerce order events with PySpark, supports incremental upserts, handles evolving schemas, preserves historical table snapshots, and uses Apache Airflow for workflow orchestration and failure monitoring.

The project includes SHA-256-based batch checkpointing, automatic task retries, Discord failure notifications, and automated tests.

## Architecture

```text
                  E-commerce Order Events
                         (JSONL)
                            |
                            v
                    Apache Airflow
                            |
                 +----------+----------+
                 |                     |
          Discover Batches       Validate Data
                 |                     |
                 +----------+----------+
                            |
                            v
                     Apache Spark
                            |
                  Clean and Deduplicate
                            |
                            v
                   Apache Iceberg
                     MERGE INTO
                            |
               +------------+------------+
               |            |            |
          Order Table   Checkpoints   Snapshots
               |            |            |
               v            v            v
           Analytics    Skip Replays   Time Travel

             Airflow Failure Monitoring
                            |
                      Discord Alerts
```

## Key Features

### 1. PySpark Data Ingestion

- Processes e-commerce order events stored as JSON Lines.
- Validates required fields and rejects negative order amounts.
- Deduplicates records using Spark window functions.
- Uses timestamps to select the latest order update.
- Processes batches into Apache Iceberg tables.

### 2. Incremental Iceberg Upserts

Uses Iceberg's `MERGE INTO` functionality to update existing orders and insert new ones.

Timestamp-based conditions help prevent older events from overwriting more recent information.

### 3. Schema Evolution

Demonstrates adding a `payment_method` column without rebuilding the existing Iceberg table or losing historical records.

### 4. Time-Travel Queries

Uses Iceberg snapshots to query historical table states.

For example, an order originally recorded as `pending` can be retrieved from an earlier snapshot after its current status changes to `paid`.

### 5. SHA-256 Batch Checkpointing

- Computes a content-based hash for each input batch.
- Stores completed batch hashes in an Iceberg checkpoint table.
- Skips batches that have already been successfully recorded.
- Supports repeatable batch processing.

The checkpoint and order-table commits are separate transactions. Exactly-once processing under concurrent execution or arbitrary failures is not guaranteed.

### 6. Apache Airflow Orchestration

A Python-based Airflow DAG manages the ingestion workflow:

1. Discover JSON input batches.
2. Validate incoming records.
3. Execute the Spark ingestion job.
4. Record task execution and failures.

The DAG includes automatic retries and prevents overlapping DAG runs through `max_active_runs=1`.

### 7. Discord Failure Notifications

Airflow's failure callback sends notifications to a configured Discord webhook when a task reaches a failed state.

Notifications include the DAG ID, task ID, run ID, execution time, and error details.

A simulated task failure was used to verify notification delivery.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline and orchestration logic |
| Apache Spark 3.5.5 | Distributed data processing |
| Apache Iceberg 1.9.2 | Transactional table format |
| Apache Airflow 3.
