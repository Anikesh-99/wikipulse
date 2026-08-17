#!/usr/bin/env bash
# Cost hygiene. Streaming infra left running is how a "free" project stops being
# free — a trial Kafka cluster bills by the hour. Run this when you stop for the day.
set -euo pipefail

echo "==> Stopping local Redpanda broker + console (docker compose)…"
docker compose down -v 2>/dev/null || echo "   (compose not running — skipping)"

cat <<'NOTE'

==> Confluent Cloud (if you switched .env to the managed broker):
    The free trial bills a running cluster by the hour. To stop incurring cost:
      1. Confluent Cloud console  ->  your cluster  ->  Cluster settings  ->  Delete cluster
         (or keep it but delete the topics if you only want to pause)
      2. Delete unused API keys under  Cluster overview -> API keys
    A deleted cluster costs nothing. Recreate it from the same steps in the README
    next time you want to run WikiPulse.

==> Local data:
    rm -f wikipulse.duckdb        # drop the local sink database
NOTE

echo "==> Done."
