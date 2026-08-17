"""Thin helpers over confluent-kafka: client config + topic creation.

Keeping this in one place means producer.py and sink.py never worry about the
PLAINTEXT (Redpanda) vs SASL_SSL (Confluent) distinction — config.py already
encodes it.
"""
from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from .config import CONFIG

log = logging.getLogger("wikipulse.kafka")


def ensure_topics(topics: list[str], num_partitions: int = 1, replication: int = 1) -> None:
    """Create topics if they don't already exist. Idempotent.

    Confluent Cloud's free 'basic' cluster manages replication for you and may
    reject an explicit replication factor; we swallow that and rely on the
    broker default in that case.
    """
    admin = AdminClient(CONFIG.kafka_common())
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in topics if t not in existing]
    if not to_create:
        log.info("topics already exist: %s", topics)
        return

    new_topics = [NewTopic(t, num_partitions=num_partitions, replication_factor=replication) for t in to_create]
    for topic, fut in admin.create_topics(new_topics).items():
        try:
            fut.result()
            log.info("created topic %s", topic)
        except Exception as exc:  # noqa: BLE001 — best-effort; existing/managed topics are fine
            log.warning("could not create topic %s (continuing): %s", topic, exc)
