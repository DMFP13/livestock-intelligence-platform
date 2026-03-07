from __future__ import annotations

import streamlit as st


def render_feed_environment(payload: dict) -> None:
    st.subheader("Feed & Environment")

    remote = payload.get("remote_sensing", {}) or {}
    st.markdown("#### Remote Sensing")
    remote_status = remote.get("status", "unknown")
    if remote_status == "available":
        r1, r2, r3 = st.columns(3)
        r1.metric("Remote Status", "available")
        r2.metric("Locations Covered", f"{int(remote.get('locations_covered', 0))}")
        r3.metric("Latest Scene", remote.get("latest_at") or "n/a")
        metrics = remote.get("metrics_available", [])
        if metrics:
            st.caption(f"Metrics available: {', '.join(metrics)}")
    else:
        st.info(remote.get("message", "Remote sensing scaffold is not configured."))

    status = payload.get("status")
    if status != "ok":
        st.info(payload.get("message", "Feed/environment view unavailable."))
        return

    ts = payload.get("timeseries")
    current_metrics = payload.get("current_metrics", [])
    derived = payload.get("derived", {})

    if ts is None or ts.empty:
        st.info("No feed/environment timeseries available.")
        return

    current_cols = st.columns(min(4, max(1, len(current_metrics))))
    for i, row in enumerate(current_metrics):
        label = row.get("metric", f"metric_{i}")
        val = row.get("value")
        delta = row.get("delta")
        current_cols[i % len(current_cols)].metric(label, "n/a" if val is None else f"{float(val):.2f}", None if delta is None else f"{delta:+.2f}")

    st.markdown("#### Environment Summary")
    d1, d2, d3 = st.columns(3)
    d1.metric("Days in View", f"{int(derived.get('days', 0))}")
    heat_days = derived.get("heat_stress_days")
    d2.metric("Heat-Stress Days (THI>=72)", "n/a" if heat_days is None else f"{int(heat_days)}")
    d3.metric("Has Environment Signals", "yes" if derived.get("has_environment_signals") else "no")

    st.markdown("#### Recent Trend")
    st.line_chart(ts.set_index("date"), use_container_width=True)
    st.dataframe(ts.tail(30), use_container_width=True, hide_index=True)
