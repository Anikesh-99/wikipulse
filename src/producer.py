"""Ingestion: Wikipedia EventStreams (SSE) -> Kafka topic `wiki.edits`.

Subscribes to the public, no-auth `recentchange` firehose, normalizes each
event, and produces it keyed by wiki. Keying by wiki means all edits for a
given wiki land on the same partition — handy if we ever scale partitions and
want per-wiki ordering.

Delivery is at-least-once: we don't block on every ack, we flush periodically
and on shutdown. A duplicated edit at most nudges one window's count by one.

Usage:
    python -m src.producer                 # run until Ctrl-C
    python -m src.producer --max-events 500  # stop after N (smoke test)
    python -m src.producer --duration 30     # stop after N seconds
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time

import requests
import sseclient
from confluent_kafka import Producer

from .config import CONFIG
from .kafka_util import ensure_topics
from .schema import normalize_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("wikipulse.producer")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "WikiPulse/0.1 (https://github.com/; portfolio data-eng project)"


def _delivery_report(err, msg):
    if err is not None:
        log.warning("delivery failed for %s: %s", msg.key(), err)


def run(max_events: int | None = None, duration: float | None = None) -> int:
    ensure_topics([CONFIG.topic_edits])
    producer = Producer(CONFIG.kafka_common())

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    deadline = time.monotonic() + duration if duration else None
    sent = seen = 0
    log.info("connecting to %s", STREAM_URL)

    resp = requests.get(
        STREAM_URL,
        stream=True,
        timeout=(10, 60),
        headers={"Accept": "text/event-stream", "User-Agent": USER_AGENT},
    )
    resp.raise_for_status()
    client = sseclient.SSEClient(resp)

    try:
        for event in client.events():
            if stop["flag"]:
                break
            if deadline and time.monotonic() > deadline:
                break
            if event.event != "message" or not event.data:
                continue

            seen += 1
            try:
                raw = json.loads(event.data)
            except json.JSONDecodeError:
                continue

            edit = normalize_event(raw)
            if edit is None:
                continue

            producer.produce(
                CONFIG.topic_edits,
                key=edit.wiki.encode("utf-8"),
                value=json.dumps(edit.to_dict()).encode("utf-8"),
                on_delivery=_delivery_report,
            )
            sent += 1

            # Serve delivery callbacks and flush the local queue periodically.
            producer.poll(0)
            if sent % 200 == 0:
                producer.flush(5)
                log.info("produced %d edits (%d events seen)", sent, seen)

            if max_events and sent >= max_events:
                break
    finally:
        remaining = producer.flush(10)
        resp.close()
        log.info("done: produced %d edits (%d in-flight unflushed)", sent, remaining)
    return sent


def main():
    ap = argparse.ArgumentParser(description="Wikipedia edits -> Kafka")
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--duration", type=float, default=None, help="seconds")
    args = ap.parse_args()
    run(max_events=args.max_events, duration=args.duration)


if __name__ == "__main__":
    main()
