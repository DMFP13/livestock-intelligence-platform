from __future__ import annotations

import altair as alt
import streamlit as st


def render_market_finance(payload: dict) -> None:
    st.subheader("Market & Finance")
    st.caption("Profitability signal monitoring")
    live_prices = payload.get("live_prices", {}) or {}

    status = payload.get("status")
    metrics = payload.get("profitability_metrics", {}) or {}
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Milk price", "n/a" if metrics.get("milk_price") is None else f"{float(metrics['milk_price']):.2f}")
    m2.metric("Feed cost", "n/a" if metrics.get("feed_cost") is None else f"{float(metrics['feed_cost']):.2f}")
    m3.metric("Diesel cost", "n/a" if metrics.get("diesel_cost") is None else f"{float(metrics['diesel_cost']):.2f}")
    m4.metric("FX rate", "n/a" if metrics.get("fx_rate") is None else f"{float(metrics['fx_rate']):.4f}")
    m5.metric(
        "Estimated margin",
        "n/a" if metrics.get("estimated_margin") is None else f"{float(metrics['estimated_margin']):+.2f}",
    )

    st.markdown("#### Milk Price vs Feed Cost")
    milk_vs_feed = payload.get("milk_vs_feed_chart")
    if milk_vs_feed is None or milk_vs_feed.empty:
        st.info("No overlapping milk/feed series yet.")
    else:
        melt = milk_vs_feed[["date", "milk_price", "feed_cost"]].melt(id_vars="date", var_name="series", value_name="value")
        chart = (
            alt.Chart(melt.dropna(subset=["value"]))
            .mark_line(point=False)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("value:Q", title="Value"),
                color=alt.Color("series:N", title="Series"),
                tooltip=["series", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
            )
            .interactive()
            .properties(height=260)
        )
        st.altair_chart(chart)

    outlook = str(payload.get("profitability_outlook") or "stable").lower()
    if outlook == "improving":
        st.success("Profitability outlook: improving")
    elif outlook == "declining":
        st.error("Profitability outlook: declining")
    else:
        st.warning("Profitability outlook: stable")

    st.markdown("#### Live Prices/FX Connector")
    st.caption(f"Status: {live_prices.get('status', 'unknown')}")
    st.info(live_prices.get("message", "Prices connector status unavailable."))
    free_api_sources = payload.get("free_api_sources", [])
    if free_api_sources:
        st.caption("Free API sources:")
        for item in free_api_sources:
            st.caption(f"- {item}")

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
            chart = (
                alt.Chart(chart_df.dropna(subset=["value"]))
                .mark_line(point=False)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("value:Q", title=key),
                    tooltip=[alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
                )
                .interactive()
                .properties(height=220)
            )
            st.altair_chart(chart)

    st.dataframe(reference_df.tail(200), use_container_width=True, hide_index=True)
