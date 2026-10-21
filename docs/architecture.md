# Proposed architecture

JSONL batches -> Spark validation and deduplication -> Iceberg `local.demo.orders` -> historical SQL queries.

Future: Airflow scheduling/retries/alerts, committed-batch checkpointing, data-quality tests and schema-evolution demos.
