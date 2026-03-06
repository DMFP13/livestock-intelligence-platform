from __future__ import annotations

import html

import altair as alt
import pandas as pd
import streamlit as st


BAND_TO_CLASS = {
    "stable": "badge-stable",
    "low": "badge-stable",
    "watch": "badge-watch",
    "elevated": "badge-elevated",
    "high": "badge-high",
}


def inject_theme_css() -> None:
    st.markdown(
        """
<style>
:root {
  --bg: #f4f7fb;
  --card: #ffffff;
  --text: #0f172a;
  --muted: #475569;
  --line: #d2dae6;
  --stable: #2f7d4a;
  --watch: #ad7c1a;
  --elevated: #bc4b2c;
  --high: #991b1b;
  --confidence: #2f5b97;
}
.main .block-container {
  padding-top: 0.9rem;
  max-width: 1280px;
}
body, [data-testid="stAppViewContainer"] {
  background: var(--bg);
  color: var(--text);
}
[data-testid="stHeader"] {
  background: rgba(244, 247, 251, 0.96);
}
[data-testid="stSidebar"] {
  background: #ecf2f8;
  border-right: 1px solid #d6dfeb;
}
[data-testid="stSidebar"] * {
  color: var(--text);
}
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] label {
  color: #334155 !important;
}
[data-testid="stTabs"] [role="tablist"],
[data-baseweb="tab-list"] {
  gap: 0.25rem;
}
[data-testid="stTabs"] [role="tab"],
[data-baseweb="tab"] {
  color: #1e293b !important;
  background: #e9eff7 !important;
  border: 1px solid #cbd6e6 !important;
  border-radius: 8px 8px 0 0 !important;
  padding: 0.45rem 0.78rem !important;
  opacity: 1 !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"],
[data-baseweb="tab"][aria-selected="true"] {
  color: #0f172a !important;
  background: #ffffff !important;
  border-bottom-color: #ffffff !important;
  border-top: 2px solid #2f5b97 !important;
  font-weight: 700 !important;
}
.stTabs [data-baseweb="tab-panel"] {
  background: transparent !important;
}
.stSelectbox label,
.stRadio label,
.stSlider label,
.stTextInput label,
.stFileUploader label {
  color: #1f2937 !important;
  font-weight: 600;
}
.stSelectbox div[data-baseweb="select"] *,
.stMultiSelect div[data-baseweb="select"] *,
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stTextArea textarea {
  color: #111827 !important;
}
.stSelectbox div[data-baseweb="select"] > div,
.stMultiSelect div[data-baseweb="select"] > div {
  background: #ffffff !important;
  border-color: #cbd5e1 !important;
}
.stSlider [data-baseweb="slider"] * {
  color: #111827 !important;
}
.stRadio [role="radiogroup"] label,
.stCheckbox label {
  color: #1f2937 !important;
}
.stMarkdown, .stMarkdown p, .stCaption, h1, h2, h3, h4 {
  color: var(--text);
}
[data-testid="stMetricLabel"] *,
[data-testid="stMetricValue"] *,
[data-testid="stTable"] *,
[data-testid="stDataFrame"] * {
  color: #111827 !important;
}
[data-testid="stExpander"] summary {
  color: #1f2937 !important;
}
.ndi-card {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 10px;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
  padding: 0.65rem 0.85rem;
  margin-bottom: 0.55rem;
}
.ndi-card .label {
  color: #334155;
  font-size: 0.74rem;
  margin-bottom: 0.1rem;
  font-weight: 620;
  letter-spacing: 0.01em;
}
.ndi-card .value {
  color: #0f172a;
  font-size: 1.52rem;
  font-weight: 740;
  line-height: 1.2;
}
.ndi-card .sub {
  color: #64748b;
  font-size: 0.74rem;
}
.ndi-section-title {
  margin-top: 0.2rem;
  margin-bottom: 0.25rem;
  font-size: 1.08rem;
  font-weight: 700;
  color: #0f172a;
}
.ndi-section-sub {
  color: #5b6b82;
  font-size: 0.82rem;
  margin-bottom: 0.45rem;
}
.ndi-badge {
  display: inline-block;
  border-radius: 8px;
  padding: 0.18rem 0.45rem;
  font-size: 0.72rem;
  font-weight: 620;
  margin-right: 0.35rem;
  border: 1px solid transparent;
}
.badge-stable { color: var(--stable); background: #ecf8f0; border-color: #b8dfc6; }
.badge-watch { color: var(--watch); background: #fef8e7; border-color: #f1d39a; }
.badge-elevated { color: var(--elevated); background: #fff2ec; border-color: #f2c1af; }
.badge-high { color: var(--high); background: #fdecec; border-color: #e9aaaa; }
.badge-confidence { color: var(--confidence); background: #eef3ff; border-color: #bfd0f6; }
.ndi-alert {
  border-left: 4px solid #bc4b2c;
  background: #fff6f2;
}
.ndi-alert .label {
  color: #9a3412;
}
.ndi-note {
  background: #f8fafc;
  border: 1px dashed var(--line);
  border-radius: 10px;
  padding: 0.75rem 0.85rem;
  color: #475569;
}
div[data-testid="stDataFrame"] {
  background: #ffffff;
  border: 1px solid #d6dfeb;
  border-radius: 10px;
  padding: 0.2rem;
}
div[data-testid="stDataFrame"] [role="grid"] {
  font-size: 0.82rem;
}
[data-testid="stHorizontalBlock"] {
  gap: 0.65rem;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str) -> None:
    st.markdown(f"### {title}")
    st.caption(subtitle)


def render_section_header(title: str, subtitle: str | None = None) -> None:
    st.markdown(f"<div class='ndi-section-title'>{html.escape(title)}</div>", unsafe_allow_html=True)
    if subtitle:
        st.markdown(f"<div class='ndi-section-sub'>{html.escape(subtitle)}</div>", unsafe_allow_html=True)


def render_kpi_card(label: str, value: str, subtext: str | None = None) -> None:
    sub = f"<div class='sub'>{html.escape(subtext)}</div>" if subtext else ""
    st.markdown(
        f"""
<div class="ndi-card">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{html.escape(value)}</div>
  {sub}
</div>
        """,
        unsafe_allow_html=True,
    )


def render_status_card(label: str, value: str, band: str, detail: str | None = None) -> None:
    cls = BAND_TO_CLASS.get(str(band).lower(), "badge-watch")
    detail_line = f"<div class='sub'>{html.escape(detail)}</div>" if detail else ""
    st.markdown(
        f"""
<div class="ndi-card">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{html.escape(value)}</div>
  <span class='ndi-badge {cls}'>{html.escape(str(band).upper())}</span>
  {detail_line}
</div>
        """,
        unsafe_allow_html=True,
    )


def render_status_badge(label: str, band: str, *, confidence: bool = False) -> None:
    cls = BAND_TO_CLASS.get(str(band).lower(), "badge-watch")
    if confidence:
        cls = "badge-confidence"
    st.markdown(
        f"<span class='ndi-badge {cls}'>{html.escape(label)}: {html.escape(str(band).upper())}</span>",
        unsafe_allow_html=True,
    )


def render_alert_panel(title: str, lines: list[str]) -> None:
    items = "".join(f"<li>{html.escape(line)}</li>" for line in lines if line)
    st.markdown(
        f"""
<div class='ndi-card ndi-alert'>
  <div class='label'>{html.escape(title)}</div>
  <ul style='margin:0.35rem 0 0.1rem 1rem; color:#7c2d12; font-size:0.84rem;'>
    {items}
  </ul>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state(title: str, message: str) -> None:
    st.markdown(
        f"<div class='ndi-note'><strong>{html.escape(title)}</strong><br>{html.escape(message)}</div>",
        unsafe_allow_html=True,
    )


def render_ranked_bar_chart(
    df: pd.DataFrame,
    *,
    category_col: str,
    value_col: str,
    color_hex: str = "#2f5b97",
    height: int = 250,
    title: str | None = None,
) -> None:
    if df.empty or category_col not in df.columns or value_col not in df.columns:
        return
    base = df[[category_col, value_col]].dropna().copy()
    if base.empty:
        return
    chart = (
        alt.Chart(base)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3, color=color_hex)
        .encode(
            y=alt.Y(f"{category_col}:N", sort=alt.SortField(field=value_col, order="descending"), title=None),
            x=alt.X(f"{value_col}:Q", title=None),
            tooltip=[category_col, value_col],
        )
        .properties(height=height, title=title)
    )
    label = chart.mark_text(align="left", baseline="middle", dx=4, color="#0f172a", fontSize=11).encode(text=alt.Text(f"{value_col}:Q", format=".1f"))
    st.altair_chart(chart + label, use_container_width=True)


def render_distribution_strip(df: pd.DataFrame, *, category_col: str = "rating", value_col: str = "count", height: int = 110) -> None:
    if df.empty or category_col not in df.columns or value_col not in df.columns:
        return
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X(f"{category_col}:N", sort=["A", "B", "C", "D", "E"], title=None),
            y=alt.Y(f"{value_col}:Q", title="Cows"),
            color=alt.Color(
                f"{category_col}:N",
                scale=alt.Scale(domain=["A", "B", "C", "D", "E"], range=["#2f7d4a", "#6f9d4f", "#ad7c1a", "#bc4b2c", "#991b1b"]),
                legend=None,
            ),
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def render_explanation_panel(title: str, lines: list[str]) -> None:
    items = "".join(f"<li>{html.escape(line)}</li>" for line in lines if line)
    st.markdown(
        f"""
<div class='ndi-card'>
  <div class='label'>{html.escape(title)}</div>
  <ul style='margin:0.35rem 0 0.1rem 1rem; color:#4b5563; font-size:0.86rem;'>
    {items}
  </ul>
</div>
        """,
        unsafe_allow_html=True,
    )
