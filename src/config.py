"""Central config, loaded from environment (.env in dev).

Every broker/topic/tuning knob lives here so the rest of the code never reads
os.environ directly. Switching from local Redpanda to Confluent Cloud is purely
an .env change — no code edits.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()  # no-op if .env is absent (e.g. in CI)


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class Config:
    # --- broker ---
    bootstrap_servers: str = field(default_factory=lambda: _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    security_protocol: str = field(default_factory=lambda: _get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    sasl_mechanism: str = field(default_factory=lambda: _get("KAFKA_SASL_MECHANISM"))
    sasl_username: str = field(default_factory=lambda: _get("KAFKA_SASL_USERNAME"))
    sasl_password: str = field(default_factory=lambda: _get("KAFKA_SASL_PASSWORD"))

    # --- topics ---
    topic_edits: str = field(default_factory=lambda: _get("TOPIC_EDITS", "wiki.edits"))
    topic_metrics: str = field(default_factory=lambda: _get("TOPIC_METRICS", "wiki.metrics"))

    # --- pipeline tuning ---
    window_seconds: int = field(default_factory=lambda: int(_get("WINDOW_SECONDS", "60")))
    spike_baseline_windows: int = field(default_factory=lambda: int(_get("SPIKE_BASELINE_WINDOWS", "15")))
    spike_k: float = field(default_factory=lambda: float(_get("SPIKE_K", "3.0")))
    spike_min_count: int = field(default_factory=lambda: int(_get("SPIKE_MIN_COUNT", "10")))

    # --- sink ---
    duckdb_database: str = field(default_factory=lambda: _get("DUCKDB_DATABASE", "wikipulse.duckdb"))
    motherduck_token: str = field(default_factory=lambda: _get("MOTHERDUCK_TOKEN"))

    def kafka_common(self) -> dict:
        """librdkafka client settings shared by producer & consumers.

        PLAINTEXT (local Redpanda) omits the SASL keys; SASL_SSL (Confluent)
        includes them. Same code path, driven entirely by .env.
        """
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL"):
            conf["sasl.mechanism"] = self.sasl_mechanism or "PLAIN"
            conf["sasl.username"] = self.sasl_username
            conf["sasl.password"] = self.sasl_password
        return conf


CONFIG = Config()
