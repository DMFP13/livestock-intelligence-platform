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

SEMANTIC_COLORS = {
    "primary": "#0A6A75",
    "primary_dark": "#07535C",
    "primary_light": "#1B8B98",
    "accent": "#41B8C4",
    "text": "#EAF1F7",
    "text_muted": "#A9BBCB",
    "stable": "#34D399",
    "watch": "#F59E0B",
    "elevated": "#F97316",
    "high": "#EF4444",
    "confidence": "#60A5FA",
}


def configure_altair_theme() -> None:
    """Set one visual language for all charts."""

    def _theme() -> dict:
        return {
            "config": {
                "view": {"stroke": "#D6E3EA"},
                "background": "#F6FAFD",
                "axis": {
                    "labelColor": "#334155",
                    "titleColor": "#1E293B",
                    "gridColor": "#E6EEF5",
                    "domainColor": "#BFD1DE",
                    "tickColor": "#BFD1DE",
                    "labelFontSize": 11,
                    "titleFontSize": 12,
                },
                "legend": {
                    "labelColor": "#334155",
                    "titleColor": "#1E293B",
                    "orient": "top",
                    "labelFontSize": 11,
                    "titleFontSize": 12,
                },
                "title": {
                    "color": "#0F172A",
                    "fontSize": 14,
                    "fontWeight": 700,
                    "anchor": "start",
                },
                "line": {"strokeWidth": 2.2},
                "bar": {"cornerRadiusTopRight": 3, "cornerRadiusBottomRight": 3},
            }
        }

    try:
        alt.themes.register("ndi_arpexas", _theme)
    except ValueError:
        pass
    alt.themes.enable("ndi_arpexas")


def inject_theme_css() -> None:
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&display=swap');
:root {
  --primary: #087F6B;
  --primary-dark: #065C50;
  --primary-soft: #E7F6F1;
  --bg: #F6F7F3;
  --card: #FFFFFF;
  --line: #DDE3DB;
  --text: #13231F;
  --muted: #62706B;
  --stable: #15803D;
  --watch: #B45309;
  --elevated: #B91C1C;
  --high: #991B1B;
  --confidence: #1D4ED8;
}
body, [data-testid="stAppViewContainer"] {
  font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg) !important;
  color: var(--text) !important;
}
[data-testid="stHeader"] {
  background: transparent !important;
}
[data-testid="stSidebar"] {
  display: block !important;
  background: linear-gradient(180deg, #102E29 0%, #173A33 100%) !important;
  border-right: 0 !important;
}
[data-testid="stSidebarNav"] { display: none !important; }
[data-testid="stSidebar"] > div { padding-top: 1rem; }
[data-testid="stSidebar"] * { color: #EAF3EF !important; }
[data-testid="stSidebar"] .stCaption { color: #9FBAB2 !important; }
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,.12) !important; }
.ndi-sidebar-brand {
  color: #FFFFFF;
  font-size: 1.02rem;
  font-weight: 800;
  letter-spacing: .12em;
  line-height: 1.15;
  margin: .2rem 0 1.25rem;
}
.ndi-sidebar-brand span {
  color: #8ED8C5 !important;
  font-size: .72rem;
  font-weight: 600;
  letter-spacing: .03em;
}
[data-testid="stSidebar"] [role="radiogroup"] {
  gap: .32rem;
}
[data-testid="stSidebar"] [role="radiogroup"] label {
  min-height: 2.5rem;
  padding: .55rem .65rem !important;
  border-radius: 9px;
  transition: background .15s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover {
  background: rgba(255,255,255,.08);
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
  background: #EAF7F2;
}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) * {
  color: #11352E !important;
  font-weight: 700 !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"] > div,
[data-testid="stSidebar"] [data-testid="stDateInput"] > div {
  background: rgba(255,255,255,.09) !important;
  border-color: rgba(255,255,255,.2) !important;
}
[data-testid="stSidebar"] [role="combobox"],
[data-testid="stSidebar"] input {
  background: transparent !important;
  color: #FFFFFF !important;
  -webkit-text-fill-color: #FFFFFF !important;
}
[data-testid="stSidebar"] [data-testid="stPopover"] button {
  width: 100%;
  justify-content: flex-start;
  background: rgba(255,255,255,.08) !important;
  color: #FFFFFF !important;
  border-color: rgba(255,255,255,.18) !important;
}
[data-testid="stSidebar"] .ndi-top-status { justify-content: flex-start; margin: .8rem 0 .2rem; }
[data-testid="stSidebar"] .ndi-pill {
  color: #D8E9E3 !important;
  background: rgba(255,255,255,.07) !important;
  border-color: rgba(255,255,255,.14) !important;
}
[data-testid="stSidebar"] .ndi-pill-live { color: #8FE2C8 !important; }
.main .block-container {
  max-width: 1440px;
  padding: 1.4rem 2.2rem 2.5rem;
}
h1, h2, h3, h4, h5, h6, p, span, label, .stMarkdown, .stCaption {
  color: var(--text) !important;
}
.stCaption { color: var(--muted) !important; }

.ndi-shell-header {
  background: linear-gradient(125deg, #123F36 0%, #176B5B 72%, #25927D 100%);
  border: 0;
  border-radius: 18px;
  box-shadow: 0 12px 28px rgba(16, 46, 41, 0.16);
  padding: 1.45rem 1.6rem;
  margin-bottom: 0.9rem;
}
.ndi-shell-header::before {
  display: none;
}
.ndi-shell-title {
  color: #FFFFFF;
  font-size: 2.2rem;
  line-height: 1.15;
  font-weight: 800;
}
.ndi-shell-sub { color: #C7E4DB; font-size: 0.94rem; margin-top: 0.28rem; }
.ndi-brand-tag {
  color: #8ED8C5;
  font-size: 0.7rem;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-bottom: 0.16rem;
}

.ndi-context-bar {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.75rem;
  margin: 0 0 1.1rem;
  padding: 0.82rem 1rem;
  background: #FFFFFF;
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: 0 4px 14px rgba(20, 44, 37, .05);
}
.ndi-context-bar > div { min-width: 0; }
.ndi-context-bar strong {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #153E49 !important;
  font-size: 0.88rem;
}
.context-label {
  display: block;
  color: #64748B !important;
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.ndi-top-status { display:flex; flex-wrap:wrap; gap:0.42rem; justify-content:flex-end; }
.ndi-pill {
  display:inline-flex;
  align-items:center;
  gap:0.34rem;
  border-radius:999px;
  padding:0.26rem 0.56rem;
  font-size:0.72rem;
  font-weight:760;
  color:#1E293B;
  background:#EFF4F8;
  border:1px solid #C9D8E5;
}
.ndi-pill-live { background:#E9F8EF; border-color:#BDE3CA; color:#166534; }
.ndi-pill .dot { width:7px; height:7px; border-radius:999px; background:currentColor; }

[data-testid="stAppViewContainer"] .main .stRadio > div {
  border: 1px solid #C9D8E5;
  border-radius: 12px;
  padding: 0.24rem;
  background: #EEF4F9;
}
[data-testid="stAppViewContainer"] .main .stRadio [role="radiogroup"] {
  border: 0 !important;
  background: transparent !important;
  padding: 0 !important;
}
[data-testid="stAppViewContainer"] .main .stRadio [role="radiogroup"] > label {
  border-radius: 9px;
  padding: 0.42rem 0.68rem !important;
  color: #274153 !important;
  border: 1px solid transparent;
}
[data-testid="stAppViewContainer"] .main .stRadio [role="radiogroup"] > label[data-selected="true"] {
  background: var(--primary) !important;
  border-color: #0E7F8C !important;
  box-shadow: 0 3px 10px rgba(0, 107, 119, 0.28);
}
[data-testid="stAppViewContainer"] .main .stRadio [role="radiogroup"] > label[data-selected="true"] * {
  color: #FFFFFF !important;
}

[data-testid="stPopover"] button {
  background: #FFFFFF !important;
  color: #274153 !important;
  border: 1px solid #C9D8E5 !important;
  border-radius: 10px !important;
}

[data-baseweb="select"] > div,
[data-baseweb="input"] > div,
[data-testid="stDateInput"] > div,
[data-testid="stDateInput"] [data-baseweb="input"] > div {
  background: #FFFFFF !important;
  border: 1px solid #C9D8E5 !important;
  border-radius: 10px !important;
}
[data-baseweb="select"] [role="combobox"],
[data-baseweb="input"] input,
[data-baseweb="textarea"] textarea,
[data-testid="stDateInput"] input {
  color: #0F172A !important;
  -webkit-text-fill-color: #0F172A !important;
  background: #FFFFFF !important;
}
div[data-baseweb="popover"] > div,
[data-baseweb="menu"],
[role="listbox"] {
  background: #FFFFFF !important;
  border: 1px solid #C9D8E5 !important;
  box-shadow: 0 10px 20px rgba(15, 23, 42, 0.12) !important;
}
[role="option"] {
  background: #FFFFFF !important;
  color: #0F172A !important;
}
[role="option"][aria-selected="true"] {
  background: #EAF4F8 !important;
  color: #0F172A !important;
}
[role="option"]:hover { background: #F2F7FB !important; color: #0F172A !important; }

[data-testid="stFileUploaderDropzone"] {
  background: #FFFFFF !important;
  border: 1px dashed #BFD2E0 !important;
}
[data-testid="stFileUploaderDropzone"] * { color: #0F172A !important; }

.ndi-card, [data-testid="stMetric"] {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: 0 5px 18px rgba(20, 44, 37, 0.06);
  padding: 0.78rem 0.9rem;
  margin-bottom: 0.5rem;
}
.ndi-card .label {
  color: #556A80;
  font-size: 0.73rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.02em;
}
.ndi-card .value {
  color: #0B4250;
  font-size: 1.52rem;
  font-weight: 780;
  line-height: 1.2;
}
.ndi-card .sub { color: #556A80; font-size: 0.78rem; margin-top: 0.1rem; }
.ndi-card-tight .value { font-size: 1.26rem; }

.ndi-section-title {
  margin-top: 0.3rem;
  margin-bottom: 0.2rem;
  color: #0E4150;
  font-size: 1.12rem;
  font-weight: 760;
}
.ndi-section-sub { color: #596D82; font-size: 0.82rem; margin-bottom: 0.46rem; }

.ndi-badge {
  display:inline-block;
  border-radius:999px;
  padding:0.17rem 0.5rem;
  font-size:0.71rem;
  font-weight:700;
  margin-right:0.28rem;
  border:1px solid transparent;
}
.badge-stable { color: var(--stable); background:#EAF7EE; border-color:#BFE3CB; }
.badge-watch { color: var(--watch); background:#FFF5E8; border-color:#F5D8B0; }
.badge-elevated { color: var(--elevated); background:#FEECEC; border-color:#F6C4C4; }
.badge-high { color: var(--high); background:#FCE7E7; border-color:#F2B8B8; }
.badge-confidence { color: var(--confidence); background:#EAF1FF; border-color:#C6D7FF; }

.ndi-alert { border-left:4px solid #C2410C; background:#FFF7ED; }
.ndi-alert .label { color:#9A3412 !important; }
.ndi-note {
  background:#F9FCFF;
  border:1px dashed #C9D8E5;
  border-radius:10px;
  padding:0.72rem 0.82rem;
  color:#334155;
}

div[data-testid="stDataFrame"] {
  background:#FFFFFF;
  border:1px solid var(--line);
  border-radius:10px;
}
[data-testid="stDataFrame"] [role="columnheader"] *,
[data-testid="stDataFrame"] [role="gridcell"] * { color:#0F172A !important; }

.stButton button,
.stDownloadButton button,
[data-testid="stBaseButton-primary"] {
  background:var(--primary-dark) !important;
  color:#FFFFFF !important;
  border:1px solid var(--primary-dark) !important;
  border-radius:10px !important;
  font-weight:760 !important;
}
.stButton button:hover,
.stDownloadButton button:hover,
[data-testid="stBaseButton-primary"]:hover {
  background:#087F6B !important;
  border-color:#087F6B !important;
}
[data-testid="stBaseButton-secondary"] {
  background:#EFF4F8 !important;
  color:#1E293B !important;
  border:1px solid #C9D8E5 !important;
  border-radius:10px !important;
}

@media (max-width: 1100px) {
  .main .block-container { padding-left: 0.85rem; padding-right: 0.85rem; }
  .ndi-shell-title { font-size: 1.5rem; }
}
@media (max-width: 700px) {
  .ndi-context-bar { grid-template-columns: 1fr; gap: 0.45rem; }
}
</style>
        """,
        unsafe_allow_html=True,
    )


def render_shell_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
<div class='ndi-shell-header'>
  <div class='ndi-brand-tag'><span class='ndi-brand-dot'></span> Arpexas Intelligence Suite</div>
  <div class='ndi-shell-title'>{html.escape(title)}</div>
  <div class='ndi-shell-sub'>{html.escape(subtitle)}</div>
</div>
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


def render_kpi_card(label: str, value: str, subtext: str | None = None, *, tight: bool = False) -> None:
    sub = f"<div class='sub'>{html.escape(subtext)}</div>" if subtext else ""
    tight_cls = " ndi-card-tight" if tight else ""
    st.markdown(
        f"""
<div class="ndi-card{tight_cls}">
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
    color_hex: str = "#4A9BAD",
    height: int = 260,
    title: str | None = None,
    value_format: str = ".1f",
) -> None:
    if df.empty or category_col not in df.columns or value_col not in df.columns:
        return
    base = df[[category_col, value_col]].dropna().copy()
    if base.empty:
        return
    chart = (
        alt.Chart(base)
        .mark_bar(color=color_hex)
        .encode(
            y=alt.Y(f"{category_col}:N", sort=alt.SortField(field=value_col, order="descending"), title=None),
            x=alt.X(f"{value_col}:Q", title=None),
            tooltip=[category_col, alt.Tooltip(f"{value_col}:Q", format=value_format)],
        )
        .interactive()
        .properties(height=height, title=title)
    )
    label = chart.mark_text(align="left", baseline="middle", dx=4, color="#1E293B", fontSize=11).encode(
        text=alt.Text(f"{value_col}:Q", format=value_format)
    )
    st.altair_chart(chart + label)


def render_distribution_strip(df: pd.DataFrame, *, category_col: str = "rating", value_col: str = "count", height: int = 150) -> None:
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
                scale=alt.Scale(domain=["A", "B", "C", "D", "E"], range=["#2F7D4A", "#6F9D4F", "#AD7C1A", "#BC4B2C", "#991B1B"]),
                legend=None,
            ),
            tooltip=[category_col, value_col],
        )
        .interactive()
        .properties(height=height)
    )
    st.altair_chart(chart)


def render_explanation_panel(title: str, lines: list[str]) -> None:
    items = "".join(f"<li>{html.escape(line)}</li>" for line in lines if line)
    st.markdown(
        f"""
<div class='ndi-card'>
  <div class='label'>{html.escape(title)}</div>
  <ul style='margin:0.35rem 0 0.1rem 1rem; color:#4b5563; font-size:0.84rem;'>
    {items}
  </ul>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_ranked_list_card(title: str, rows: list[str], subtitle: str | None = None) -> None:
    if not rows:
        rows = ["No items available"]
    sub = f"<div class='sub'>{html.escape(subtitle)}</div>" if subtitle else ""
    items = "".join(f"<li>{html.escape(item)}</li>" for item in rows)
    st.markdown(
        f"""
<div class='ndi-card'>
  <div class='label'>{html.escape(title)}</div>
  {sub}
  <ol style='margin:0.35rem 0 0.1rem 1rem; color:#334155; font-size:0.82rem;'>
    {items}
  </ol>
</div>
        """,
        unsafe_allow_html=True,
    )
