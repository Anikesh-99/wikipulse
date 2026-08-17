"""Tests for normalizing raw Wikipedia EventStreams payloads."""
from src.schema import normalize_event


def _raw(**over):
    base = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "type": "edit",
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "title": "Cheese",
        "user": "Alice",
        "bot": False,
        "timestamp": 1_700_000_000,
        "length": {"old": 100, "new": 150},
        "meta": {"domain": "en.wikipedia.org", "dt": "2023-11-14T22:13:20Z"},
    }
    base.update(over)
    return base


def test_normalizes_core_fields():
    ev = normalize_event(_raw())
    assert ev.wiki == "enwiki"
    assert ev.domain == "en.wikipedia.org"
    assert ev.type == "edit"
    assert ev.user == "Alice"
    assert ev.title == "Cheese"
    assert ev.bot is False
    assert ev.timestamp == 1_700_000_000


def test_bot_edits_flagged():
    ev = normalize_event(_raw(bot=True, user="CleanupBot"))
    assert ev.bot is True


def test_missing_optional_fields_do_not_crash():
    raw = {"type": "edit", "wiki": "dewiki", "timestamp": 1_700_000_500}
    ev = normalize_event(raw)
    assert ev.wiki == "dewiki"
    assert ev.title == ""          # absent -> empty, not KeyError
    assert ev.user == ""
    assert ev.bot is False         # absent -> default False


def test_returns_none_when_no_timestamp():
    # Events without a usable timestamp are unroutable for windowing.
    assert normalize_event({"type": "edit", "wiki": "enwiki"}) is None


def test_returns_none_when_no_wiki():
    assert normalize_event({"type": "edit", "timestamp": 1_700_000_000}) is None


def test_string_timestamp_is_coerced():
    ev = normalize_event(_raw(timestamp="1700000000"))
    assert ev.timestamp == 1_700_000_000
