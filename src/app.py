"""Live dashboard over the DuckDB sink.

Spoiler on the concurrency footnote: DuckDB is single-writer *per process*. When
the sink is actively writing a local file, this read-only dashboard may hit a
lock. For a genuinely live local demo, point both at MotherDuck (a service that
handles concurrent readers); against a local file, the dashboard is happiest
viewing data the sink has already committed. Errors are caught and shown rather
than crashing the page.

Run:
    streamlit run src/app.py
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

from src.config import CONFIG

st.set_page_config(page_title="WikiPulse", page_icon="📈", layout="wide")


def _query(sql: str) -> pd.DataFrame:
    if CONFIG.duckdb_database.startswith("md:") and CONFIG.motherduck_token:
        os.environ["motherduck_token"] = CONFIG.motherduck_token
    try:
        con = duckdb.connect(CONFIG.duckdb_database, read_only=True)
    except duckdb.Error as exc:
        st.warning(f"Could not open the database (is the sink mid-write on a local file?): {exc}")
        return pd.DataFrame()
    try:
        return con.execute(sql).fetch_df()
    finally:
        con.close()


@st.fragment(run_every="5s")
def live_view():
    df = _query("""
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


st.title("📈 WikiPulse — live Wikipedia edit-spike detector")
st.caption(f"Source: {CONFIG.duckdb_database}  ·  window = {CONFIG.window_seconds}s  ·  spike threshold = mean + {CONFIG.spike_k}σ")
live_view()
