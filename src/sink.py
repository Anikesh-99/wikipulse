"""Serving sink: consume `wiki.metrics` -> DuckDB (local file or MotherDuck).

Writes are idempotent: the fact table is keyed by (wiki, window_start) and we
use INSERT OR REPLACE, so re-processing the same window (e.g. after a consumer
restart with at-least-once delivery) never double-counts.

Run:
    python -m src.sink                    # run until Ctrl-C
    python -m src.sink --max-messages 50  # stop after N (smoke test)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal

import duckdb
from confluent_kafka import Consumer

from .config import CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("wikipulse.sink")

SCHEMA = """
CREATE TABLE IF NOT EXISTS window_metrics (
    wiki           VARCHAR NOT NULL,
    window_start   BIGINT  NOT NULL,
    window_seconds INTEGER NOT NULL,
    count          INTEGER NOT NULL,
    baseline_mean  DOUBLE,
    baseline_std   DOUBLE,
    is_spike       BOOLEAN NOT NULL,
    PRIMARY KEY (wiki, window_start)
);
"""

UPSERT = """
INSERT OR REPLACE INTO window_metrics
    (wiki, window_start, window_seconds, count, baseline_mean, baseline_std, is_spike)
VALUES (?, ?, ?, ?, ?, ?, ?);
"""


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    """Local file, or MotherDuck if DUCKDB_DATABASE starts with 'md:'."""
    if CONFIG.duckdb_database.startswith("md:") and CONFIG.motherduck_token:
        os.environ["motherduck_token"] = CONFIG.motherduck_token
    con = duckdb.connect(CONFIG.duckdb_database)
    con.execute(SCHEMA)
    return con


def _consumer() -> Consumer:
    conf = CONFIG.kafka_common()
    conf.update({
        "group.id": "wikipulse-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    return Consumer(conf)


def run(max_messages: int | None = None) -> int:
    con = connect_duckdb()
    consumer = _consumer()
    consumer.subscribe([CONFIG.topic_metrics])

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    written = 0
    log.info("sink consuming %s -> %s", CONFIG.topic_metrics, CONFIG.duckdb_database)
    try:
        while not stop["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            try:
                m = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                continue

            con.execute(UPSERT, [
                m["wiki"], int(m["window_start"]), int(m["window_seconds"]),
                int(m["count"]), m.get("baseline_mean"), m.get("baseline_std"),
                bool(m["is_spike"]),
            ])
            written += 1
            if m.get("is_spike"):
                log.info("wrote SPIKE %s @%s count=%d", m["wiki"], m["window_start"], m["count"])
            if max_messages and written >= max_messages:
                break
    finally:
        consumer.close()
        total = con.execute("SELECT count(*) FROM window_metrics").fetchone()[0]
        con.close()
        log.info("sink done: wrote %d messages this run (%d rows total)", written, total)
    return written


def main():
    ap = argparse.ArgumentParser(description="wiki.metrics -> DuckDB")
    ap.add_argument("--max-messages", type=int, default=None)
    args = ap.parse_args()
    run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
