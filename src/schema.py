"""Normalize raw Wikipedia EventStreams `recentchange` payloads.

The upstream event is a large, loosely-typed JSON blob. We keep only the fields
the pipeline needs and coerce them defensively — a malformed event should be
dropped (return None), never crash the consumer.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class EditEvent:
    wiki: str          # e.g. "enwiki"
    domain: str        # e.g. "en.wikipedia.org"
    type: str          # edit | new | log | categorize | ...
    title: str
    user: str
    bot: bool
    timestamp: int     # unix seconds

    def to_dict(self) -> dict:
        return asdict(self)


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_event(raw: dict) -> EditEvent | None:
    """Return an EditEvent, or None if the payload can't be windowed.

    Requires at minimum a `wiki` and an integer `timestamp`; everything else
    falls back to a safe default.
    """
    if not isinstance(raw, dict):
        return None

    wiki = raw.get("wiki")
    ts = _to_int(raw.get("timestamp"))
    if not wiki or ts is None:
        return None

    meta = raw.get("meta") or {}
    return EditEvent(
        wiki=str(wiki),
        domain=str(raw.get("server_name") or meta.get("domain") or ""),
        type=str(raw.get("type") or ""),
        title=str(raw.get("title") or ""),
        user=str(raw.get("user") or ""),
        bot=bool(raw.get("bot", False)),
        timestamp=ts,
    )
