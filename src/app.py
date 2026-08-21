"""Live dashboard over the DuckDB sink.

IMPORTANT structure note: all rendering happens inside `main()`, which the entry
script (streamlit_app.py, or `streamlit run src/app.py`) calls on EVERY rerun.
Do not move rendering to module top level — Streamlit re-executes the entry
script each rerun, but Python caches imports, so import-time rendering only fires
once and every later rerun/session renders a blank page.

Concurrency footnote: DuckDB is single-writer *per process*. When the sink is
writing a local file, this read-only dashboard may hit a lock; use MotherDuck
(a service that handles concurrent readers) for a genuinely live local demo.
Errors are caught and shown rather than crashing the page.

Run:
    streamlit run src/app.py       # or: streamlit run streamlit_app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# `streamlit run src/app.py` puts src/ on sys.path, not the project root, so the
# `src` package isn't importable by default. Add the project root so the shared
# modules resolve the same way they do under `python -m`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
import streamlit as st

from src.config import Config


def _bridge_cloud_secrets() -> None:
    """Copy Streamlit Community Cloud secrets into the environment BEFORE the
    Config is built, so the same Config works locally (.env) and hosted
    (st.secrets). Called at the top of main(), before Config().

    Only touch st.secrets if a secrets.toml actually exists — accessing it when
    absent makes Streamlit render a "No secrets found" banner that try/except
    can't suppress (it's a UI side effect). Streamlit Cloud writes provided
    secrets to one of these paths.
    """
    candidates = [
        Path.home() / ".streamlit" / "secrets.toml",
        Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml",
    ]
    if not any(p.exists() for p in candidates):
        return  # normal local dev: fall back to .env
    try:
        secrets = dict(st.secrets)
    except Exception:
        return
    for key in ("DUCKDB_DATABASE", "MOTHERDUCK_TOKEN", "WINDOW_SECONDS", "SPIKE_K"):
        val = secrets.get(key)
        if val is not None and str(val).strip():
            os.environ.setdefault(key, str(val))


def _query(cfg: Config, sql: str) -> pd.DataFrame:
    if cfg.duckdb_database.startswith("md:") and cfg.motherduck_token:
        os.environ["motherduck_token"] = cfg.motherduck_token
    try:
        con = duckdb.connect(cfg.duckdb_database, read_only=True)
    except duckdb.Error as exc:
        st.info("Waiting for data — the database has no metrics yet. "
                "Run the pipeline (producer → pipeline → sink) to populate it.")
        st.caption(f"(database: {cfg.duckdb_database} — {type(exc).__name__})")
        return pd.DataFrame()
    try:
        return con.execute(sql).fetch_df()
    finally:
        con.close()


@st.fragment(run_every="5s")
def live_view(cfg: Config):
    df = _query(cfg, """
        SELECT wiki, window_start, count, baseline_mean, is_spike
        FROM window_metrics
        WHERE window_start >= (SELECT COALESCE(max(window_start), 0) FROM window_metrics) - 3600
        ORDER BY window_start
    """)
    if df.empty:
        st.info("No metrics yet. Start the producer, pipeline, and sink, then wait for the first window to close.")
        return

    df["time"] = pd.to_datetime(df["window_start"], unit="s")

    total_windows = len(df)
    total_edits = int(df["count"].sum())
    spikes = df[df["is_spike"]]

    c1, c2, c3 = st.columns(3)
    c1.metric("Windows (last hour)", f"{total_windows:,}")
    c2.metric("Edits counted", f"{total_edits:,}")
    c3.metric("Spikes detected", f"{len(spikes):,}")

    st.subheader("Edit rate by wiki")
    top = df.groupby("wiki")["count"].sum().nlargest(8).index
    pivot = (
        df[df["wiki"].isin(top)]
        .pivot_table(index="time", columns="wiki", values="count", aggfunc="sum")
        .fillna(0)
    )
    st.line_chart(pivot)

    st.subheader("🔥 Recent spikes")
    if spikes.empty:
        st.caption("No spikes in the current window range.")
    else:
        show = spikes.sort_values("window_start", ascending=False)[
            ["time", "wiki", "count", "baseline_mean"]
        ].head(20)
        show = show.rename(columns={"baseline_mean": "typical (baseline)"})
        st.dataframe(show, use_container_width=True, hide_index=True)


def main():
    """Entry point — runs on every Streamlit rerun. set_page_config must be the
    first Streamlit call, so it leads."""
    st.set_page_config(page_title="WikiPulse", page_icon="📈", layout="wide")
    _bridge_cloud_secrets()
    cfg = Config()  # built AFTER secrets are bridged, so it sees them

    st.title("📈 WikiPulse — live Wikipedia edit-spike detector")
    st.caption(
        f"Source: {cfg.duckdb_database}  ·  window = {cfg.window_seconds}s  "
        f"·  spike threshold = mean + {cfg.spike_k}σ"
    )
    live_view(cfg)


if __name__ == "__main__":
    # `streamlit run src/app.py` executes this module as __main__ every rerun.
    main()
