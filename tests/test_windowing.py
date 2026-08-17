"""Tests for the tumbling-window spike detector — the streaming core.

These run with no broker and no clock: we feed synthetic (wiki, timestamp)
events and assert on the closed-window metrics. Windows are epoch-aligned, so
a window of size 60 covering t in [60, 120) closes once the watermark reaches
120 (+ grace).
"""
from src.windowing import SpikeDetector


def _counts(metrics):
    return {(m.wiki, m.window_start): m.count for m in metrics}


def test_window_only_closes_after_watermark_passes_it():
    det = SpikeDetector(window_seconds=60, baseline_windows=5, k=3.0, min_count=1)
    # Three edits inside window [60, 120).
    out = []
    out += det.add("enwiki", 65)
    out += det.add("enwiki", 90)
    out += det.add("enwiki", 119)
    # Watermark is 119 — window [60,120) is NOT closed yet.
    assert out == []
    # An edit at 120 advances the watermark into the next window; [60,120) closes.
    out = det.add("enwiki", 120)
    assert _counts(out) == {("enwiki", 60): 3}


def test_counts_are_isolated_per_wiki():
    det = SpikeDetector(window_seconds=60, baseline_windows=5, k=3.0, min_count=1)
    det.add("enwiki", 10)
    det.add("enwiki", 20)
    det.add("dewiki", 30)
    out = det.add("enwiki", 61)  # watermark 61 closes window [0,60)
    assert _counts(out) == {("enwiki", 0): 2, ("dewiki", 0): 1}


def test_flush_closes_remaining_open_windows():
    det = SpikeDetector(window_seconds=60, baseline_windows=5, k=3.0, min_count=1)
    det.add("enwiki", 5)
    det.add("enwiki", 6)
    assert det.add("enwiki", 7) == []      # still open (3 edits in [0,60))
    out = det.flush()
    assert _counts(out) == {("enwiki", 0): 3}


def test_spike_flagged_when_count_exceeds_baseline():
    det = SpikeDetector(window_seconds=60, baseline_windows=10, k=3.0, min_count=5)
    # Build a calm baseline: ~10 edits per window for several windows.
    ts = 0
    for w in range(6):               # windows [0,60)..[300,360)
        base = w * 60
        for i in range(10):
            det.add("enwiki", base + i)
        # advance watermark past this window to close it
        det.add("enwiki", base + 60)
    # Now inject a huge window: 100 edits in [360,420).
    for i in range(100):
        det.add("enwiki", 360 + i % 60)
    metrics = det.add("enwiki", 420)   # closes [360,420)
    spike = [m for m in metrics if m.window_start == 360 and m.wiki == "enwiki"][0]
    assert spike.is_spike is True
    assert spike.count >= 100
    assert spike.baseline_mean < 20    # baseline was ~10-ish


def test_no_spike_on_steady_traffic():
    det = SpikeDetector(window_seconds=60, baseline_windows=10, k=3.0, min_count=5)
    metrics_seen = []
    for w in range(8):
        base = w * 60
        for i in range(12):
            metrics_seen += det.add("enwiki", base + i)
        metrics_seen += det.add("enwiki", base + 60)
    assert all(m.is_spike is False for m in metrics_seen)


def test_low_volume_never_spikes_even_if_relatively_large():
    # min_count guards against "2 edits vs a baseline of 0.1" false spikes.
    det = SpikeDetector(window_seconds=60, baseline_windows=10, k=3.0, min_count=50)
    metrics = []
    for w in range(5):
        base = w * 60
        det.add("tinywiki", base + 1)  # 1 edit/window
        metrics += det.add("tinywiki", base + 60)
    # a "burst" of 5 — still below min_count
    for i in range(5):
        det.add("tinywiki", 300 + i)
    metrics += det.add("tinywiki", 360)
    assert all(m.is_spike is False for m in metrics)


def test_needs_at_least_two_baseline_windows_before_spiking():
    # With <2 prior windows there is no meaningful stddev; never spike.
    det = SpikeDetector(window_seconds=60, baseline_windows=10, k=3.0, min_count=1)
    for i in range(100):
        det.add("enwiki", i % 60)      # 100 edits in the very first window [0,60)
    metrics = det.add("enwiki", 60)
    assert metrics[0].is_spike is False
