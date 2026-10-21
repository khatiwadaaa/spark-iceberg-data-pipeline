#!/usr/bin/env bash
set -euo pipefail
BATCH_FILE="${1:-orders_batch_1.json}" docker compose run --rm spark
