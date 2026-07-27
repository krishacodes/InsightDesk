import streamlit as st

from components.kpi_cards import render_kpi_cards
from components.charts import render_complaint_chart
from components.summary_cards import (
    render_benchmarks,
    render_clusters_card,
    render_post_mortem,
)


def render_overview() -> None:
    title_col, filter_col, export_col = st.columns([3, 1.2, 0.8])
    with title_col:
        st.markdown(
            """
            <h1 class="id-page-title">Overview</h1>
            <div class="id-page-subtitle">Last 7 days</div>
            """,
            unsafe_allow_html=True,
        )
    with filter_col:
        st.selectbox(
            "Time window",
            ["Last 7 days", "Last 24 hours", "Last 30 days"],
            label_visibility="collapsed",
        )
    with export_col:
        st.button("Export", use_container_width=True)

    render_kpi_cards()

    col_chart, col_clusters = st.columns([1.6, 1])
    with col_chart:
        render_complaint_chart()
    with col_clusters:
        render_clusters_card()

    col_pm, col_bench = st.columns([1.6, 1])
    with col_pm:
        render_post_mortem()
    with col_bench:
        render_benchmarks()
