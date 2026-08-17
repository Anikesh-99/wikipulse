"""Stream processing: `wiki.edits` -> tumbling windows -> `wiki.metrics`.

Uses Quix Streams for the broker plumbing and run loop, and delegates the
actual windowing/spike math to the unit-tested SpikeDetector. One incoming edit
can produce zero or more outgoing metrics — a metric is emitted only when a
window closes (its watermark passes) — so we use `apply(..., expand=True)` to
fan a single input into a list of outputs.

Run:
    python -m src.pipeline
"""
from __future__ import annotations

import logging

from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

from .config import CONFIG
from .windowing import SpikeDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("wikipulse.pipeline")


def _broker_address():
    """Return a plain host:port for PLAINTEXT (Redpanda) or a ConnectionConfig
    carrying SASL creds for Confluent Cloud — same code path either way."""
    if CONFIG.security_protocol.startswith("SASL"):
        return ConnectionConfig(
            bootstrap_servers=CONFIG.bootstrap_servers,
            security_protocol=CONFIG.security_protocol,
            sasl_mechanism=CONFIG.sasl_mechanism or "PLAIN",
            sasl_username=CONFIG.sasl_username,
            sasl_password=CONFIG.sasl_password,
        )
    return CONFIG.bootstrap_servers


def build_streaming_dataframe(app: Application):
    """Wire edits -> detector -> metrics. Returned SDF is registered on `app`."""
    detector = SpikeDetector(
        window_seconds=CONFIG.window_seconds,
        baseline_windows=CONFIG.spike_baseline_windows,
        k=CONFIG.spike_k,
        min_count=CONFIG.spike_min_count,
    )

    edits = app.topic(CONFIG.topic_edits, value_deserializer="json")
    metrics = app.topic(CONFIG.topic_metrics, value_serializer="json", key_serializer="str")

    def process(value: dict) -> list[dict]:
        wiki = value.get("wiki")
        ts = value.get("timestamp")
        if not wiki or ts is None:
            return []
        closed = detector.add(str(wiki), int(ts))
        for m in closed:
            if m.is_spike:
                log.info("SPIKE %s window=%s count=%d (baseline mean=%.1f std=%.1f)",
                         m.wiki, m.window_start, m.count, m.baseline_mean, m.baseline_std)
        return [m.to_dict() for m in closed]

    sdf = app.dataframe(edits)
    sdf = sdf.apply(process, expand=True)
    sdf = sdf.to_topic(metrics, key=lambda v: v["wiki"])
    return sdf


def run():
    app = Application(
        broker_address=_broker_address(),
        consumer_group="wikipulse-pipeline",
        auto_offset_reset="latest",
        auto_create_topics=True,
    )
    build_streaming_dataframe(app)
    log.info("pipeline running: %s -> %s (window=%ds, k=%.1f)",
             CONFIG.topic_edits, CONFIG.topic_metrics, CONFIG.window_seconds, CONFIG.spike_k)
    app.run()


if __name__ == "__main__":
    run()
