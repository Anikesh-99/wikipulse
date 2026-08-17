"""Tumbling-window edit-rate aggregation + spike detection.

This is the streaming brain of the project, kept pure and clock-free so it can
be tested exhaustively without a broker. It mirrors what Quix Streams does in
`pipeline.py`, but as a plain state machine you can reason about.

Model
-----
* Windows are **epoch-aligned tumbling** windows: window_start = ts - (ts % W).
  Every wiki shares the same boundaries, so windows across wikis line up.
* A window closes once the **watermark** (max timestamp seen so far) advances
  past `window_start + W + grace`. This is how streaming systems tolerate a
  little out-of-order arrival without waiting forever.
* Per wiki we keep a rolling history of the last `baseline_windows` closed
  counts. A window is a **spike** when its count is both meaningfully large
  (`>= min_count`) and statistically unusual (`> mean + k * stddev`) relative
  to that history. We require >= 2 baseline windows so stddev is meaningful.

Simplification (documented on purpose): the baseline for a wiki includes only
windows in which that wiki actually had edits. Idle windows are not counted as
zeros. Good enough for a weekend spike detector; noted so it isn't mistaken for
a bug.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field


@dataclass(frozen=True)
class WindowMetric:
    wiki: str
    window_start: int      # unix seconds, epoch-aligned
    window_seconds: int
    count: int
    baseline_mean: float
    baseline_std: float
    is_spike: bool

    def to_dict(self) -> dict:
        return {
            "wiki": self.wiki,
            "window_start": self.window_start,
            "window_seconds": self.window_seconds,
            "count": self.count,
            "baseline_mean": round(self.baseline_mean, 3),
            "baseline_std": round(self.baseline_std, 3),
            "is_spike": self.is_spike,
        }


def _mean_std(values) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n   # population variance
    return mean, var ** 0.5


@dataclass
class SpikeDetector:
    window_seconds: int = 60
    baseline_windows: int = 15
    k: float = 3.0
    min_count: int = 10
    grace_seconds: int = 0

    # internal state
    _watermark: int = field(default=-1, init=False)
    _closed_up_to: int = field(default=-1, init=False)  # highest window_start closed
    # window_start -> wiki -> count
    _counts: dict[int, dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)), init=False)
    _history: dict[str, deque] = field(default_factory=dict, init=False)

    def _window_start(self, ts: int) -> int:
        return ts - (ts % self.window_seconds)

    def add(self, wiki: str, timestamp: int) -> list[WindowMetric]:
        """Ingest one edit; return any window metrics that closed as a result."""
        self._watermark = max(self._watermark, timestamp)
        ws = self._window_start(timestamp)
        self._counts[ws][wiki] += 1
        return self._close_ready(self._watermark - self.grace_seconds)

    def flush(self) -> list[WindowMetric]:
        """Close every remaining open window (end of stream / shutdown)."""
        # +inf watermark: everything is now in the past.
        return self._close_ready(float("inf"))

    def _close_ready(self, effective_watermark) -> list[WindowMetric]:
        metrics: list[WindowMetric] = []
        # Close windows in ascending time order so per-wiki history builds in order.
        for ws in sorted(self._counts):
            window_end = ws + self.window_seconds
            if window_end > effective_watermark:
                break  # this and all later windows are still open
            for wiki, count in sorted(self._counts[ws].items()):
                hist = self._history.setdefault(wiki, deque(maxlen=self.baseline_windows))
                baseline = list(hist)  # counts BEFORE this window
                mean, std = _mean_std(baseline)
                is_spike = (
                    count >= self.min_count
                    and len(baseline) >= 2
                    and count > mean + self.k * std
                )
                metrics.append(
                    WindowMetric(
                        wiki=wiki,
                        window_start=ws,
                        window_seconds=self.window_seconds,
                        count=count,
                        baseline_mean=mean,
                        baseline_std=std,
                        is_spike=is_spike,
                    )
                )
                hist.append(count)
            del self._counts[ws]
            self._closed_up_to = ws
        return metrics
