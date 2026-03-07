from __future__ import annotations

import streamlit as st


def render_market_finance(payload: dict) -> None:
    st.subheader("Market & Finance")
    live_prices = payload.get("live_prices", {}) or {}
    st.markdown("#### Live Prices/FX Connector")
    st.caption(f"Status: {live_prices.get('status', 'unknown')}")
    if live_prices.get("last_success_at") or live_prices.get("last_failure_at"):
        st.caption(
            f"Last success: {live_prices.get('last_success_at') or 'n/a'} | "
            f"Last failure: {live_prices.get('last_failure_at') or 'n/a'}"
        )
    st.info(live_prices.get("message", "Prices connector status unavailable."))

    status = payload.get("status")
    if status != "ok":
        st.info(payload.get("message", "No reference series available."))
        return

    reference_df = payload.get("reference_df")
    summary_df = payload.get("summary_df")
    chart_series = payload.get("chart_series", {})
    origin = payload.get("origin", "unknown")

    if reference_df is None or reference_df.empty:
        st.info("No reference series loaded yet.")
        return

    st.caption(f"Source: {origin}")
    st.markdown("#### Trend Summary")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.markdown("#### Series Data")
    for key, chart_df in chart_series.items():
        st.caption(f"Series: {key}")
        if chart_df is not None and not chart_df.empty:
            st.line_chart(chart_df.set_index("date")[["value"]], use_container_width=True)

    st.dataframe(reference_df.tail(200), use_container_width=True, hide_index=True)
