from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

def render_herd_intelligence(
    df: pd.DataFrame,
    state_frame: pd.DataFrame,
    outcome_bundle: dict,
    list_cows,
    cached_cow_pool,
    cached_cow_profile,
) -> None:
    del outcome_bundle, list_cows, cached_cow_pool, cached_cow_profile
    st.subheader("Herd Intelligence")
    st.caption("Animal-focused monitoring and ranking")

    source = state_frame if state_frame is not None and not state_frame.empty else df
    if source is None or source.empty:
        st.info("No animal records available.")
        return

    if "animal_id" not in source.columns:
        st.info("No animal-level records available.")
        return

    selected_farm = st.session_state.get("selected_farm_id")
    if selected_farm and "farm_id" in source.columns:
        source = source[source["farm_id"].astype(str) == str(selected_farm)].copy()
    if source.empty:
        st.info("No animal-level records for selected farm.")
        return

    if "date" not in source.columns:
        st.info("No timestamped animal records available.")
        return
    source["date"] = pd.to_datetime(source["date"], errors="coerce")
    source = source.dropna(subset=["date"]).copy()
    if source.empty:
        st.info("No timestamped animal records available.")
        return

    latest = source.sort_values("date").groupby("animal_id", as_index=False).tail(1).copy()
    latest["rumination_min"] = pd.to_numeric(latest.get("rumination_min"), errors="coerce")
    latest["activity_rate"] = pd.to_numeric(latest.get("activity_rate"), errors="coerce")
    latest["thi"] = pd.to_numeric(latest.get("thi"), errors="coerce")
    latest["health_risk_score"] = pd.to_numeric(latest.get("health_risk_score"), errors="coerce").fillna(0.0)
    latest["health_score"] = (100.0 - latest["health_risk_score"]).clip(lower=0.0, upper=100.0)
    latest["milk_yield_l"] = pd.to_numeric(latest.get("milk_yield_l"), errors="coerce")
    latest["estrus_likelihood_score"] = pd.to_numeric(latest.get("estrus_likelihood_score"), errors="coerce").fillna(0.0)

    def _row_status(r: pd.Series) -> str:
        if float(r.get("health_risk_score", 0.0)) >= 75 or float(r.get("thi", 0.0)) >= 80:
            return "urgent"
        if float(r.get("health_risk_score", 0.0)) >= 50 or float(r.get("thi", 0.0)) >= 72:
            return "watch"
        return "normal"

    latest["status"] = latest.apply(_row_status, axis=1)
    ranking = latest[
        [
            "animal_id",
            "rumination_min",
            "activity_rate",
            "thi",
            "health_score",
            "status",
        ]
    ].rename(
        columns={
            "animal_id": "Animal ID",
            "rumination_min": "Rumination",
            "activity_rate": "Activity",
            "thi": "Heat stress score",
            "health_score": "Health score",
            "status": "Status",
        }
    )
    ranking = ranking.sort_values(["Health score", "Heat stress score"], ascending=[False, True]).reset_index(drop=True)

    total_cows = int(latest["animal_id"].dropna().astype(str).nunique())
    milking_cows = int((latest["milk_yield_l"].fillna(0) > 0).sum()) if "milk_yield_l" in latest.columns else 0
    dry_cows = max(total_cows - milking_cows, 0)
    avg_rumination = float(latest["rumination_min"].dropna().mean()) if latest["rumination_min"].notna().any() else None
    estrus_alerts = int((latest["estrus_likelihood_score"] >= 75).sum()) if "estrus_likelihood_score" in latest.columns else 0
    health_alerts = int((latest["health_risk_score"] >= 75).sum())

    left, main = st.columns([1.0, 2.3])
    with left:
        st.markdown("#### Herd Summary")
        st.metric("Total cows", f"{total_cows:,}")
        st.metric("Milking cows", f"{milking_cows:,}")
        st.metric("Dry cows", f"{dry_cows:,}")
        st.metric("Average rumination", "n/a" if avg_rumination is None else f"{avg_rumination:.1f}")
        st.metric("Estrus alerts", f"{estrus_alerts:,}")
        st.metric("Health alerts", f"{health_alerts:,}")

    def _row_color(row: pd.Series) -> list[str]:
        status = str(row.get("Status") or "").lower()
        if status == "urgent":
            bg = "background-color: #fdecea;"
        elif status == "watch":
            bg = "background-color: #fff8e1;"
        else:
            bg = "background-color: #e8f7ee;"
        return [bg] * len(row)

    with main:
        st.markdown("#### Animal Ranking (Best to Worst)")
        st.dataframe(
            ranking.style.apply(_row_color, axis=1),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### Animal Detail")
        animal_options = ranking["Animal ID"].dropna().astype(str).tolist()
        if not animal_options:
            st.info("No animals available for detail view.")
            return

        default_animal = st.session_state.get("selected_cow_id")
        if default_animal not in animal_options:
            default_animal = animal_options[0]
        selected_animal = st.selectbox(
            "Select animal",
            options=animal_options,
            index=animal_options.index(default_animal),
            key="herd_detail_animal_selector",
        )
        st.session_state["selected_cow_id"] = selected_animal

        animal_ts = source[source["animal_id"].astype(str) == str(selected_animal)].copy().sort_values("date")
        if animal_ts.empty:
            st.info("No time-series records available for selected animal.")
            return

        latest_row = animal_ts.iloc[-1]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Rumination", "n/a" if pd.isna(latest_row.get("rumination_min")) else f"{float(latest_row.get('rumination_min')):.1f}")
        d2.metric("Activity", "n/a" if pd.isna(latest_row.get("activity_rate")) else f"{float(latest_row.get('activity_rate')):.2f}")
        d3.metric("Heat stress score", "n/a" if pd.isna(latest_row.get("thi")) else f"{float(latest_row.get('thi')):.1f}")
        health_score = 100.0 - float(latest_row.get("health_risk_score") or 0.0)
        d4.metric("Health score", f"{max(min(health_score, 100.0), 0.0):.1f}")

        trend_cols = [c for c in ["rumination_min", "activity_rate", "thi", "health_risk_score"] if c in animal_ts.columns]
        if trend_cols:
            melt = animal_ts[["date", *trend_cols]].melt(id_vars="date", var_name="series", value_name="value")
            chart = (
                alt.Chart(melt.dropna(subset=["value"]))
                .mark_line(point=False)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("value:Q", title="Value"),
                    color=alt.Color("series:N", title="Signal"),
                    tooltip=["series", alt.Tooltip("date:T"), alt.Tooltip("value:Q", format=".2f")],
                )
                .interactive()
                .properties(height=250)
            )
            st.altair_chart(chart)
