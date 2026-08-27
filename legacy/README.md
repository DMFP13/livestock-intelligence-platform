# Legacy UI

Retired UI code, kept for reference only — not maintained, not covered by the
regular quality gate expectations beyond "still imports cleanly."

- `streamlit_app/` — the original Streamlit dashboard (`streamlit run legacy/streamlit_app/main.py`).
- `web_html_prototypes/` — six iterations of a hand-built static HTML/JS dashboard
  (`livestock_intelligence_dashboard_v1.html` ... `v6.html`). `v6` has the most complete
  feature set (report generator, upload UI, farm registration) and is the best reference
  if a feature needs porting into `web/`.
- `tests/` — tests that only exercise the retired Streamlit rendering code.

The active UI is `web/` (Next.js) talking to the FastAPI service in `apps/api/`. See the
top-level `README.md` for the current run instructions.
