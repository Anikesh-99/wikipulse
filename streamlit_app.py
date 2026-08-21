"""Streamlit Community Cloud entrypoint.

Streamlit re-executes this script on every rerun and every new session. We call
main() here (rather than relying on import side-effects) so the dashboard
renders every time — importing src.app only once would leave later reruns blank.

See README > "Deploy the dashboard for free".
"""
from src.app import main

main()
