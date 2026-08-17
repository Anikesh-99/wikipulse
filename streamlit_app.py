"""Streamlit Community Cloud entrypoint.

Streamlit Cloud auto-detects `streamlit_app.py` at the repo root. Importing the
dashboard module runs it (the render code executes at import time). Point the
Streamlit Cloud "Main file path" at this file (or directly at src/app.py).

See README > "Deploy the dashboard for free".
"""
import src.app  # noqa: F401  — importing runs the dashboard
