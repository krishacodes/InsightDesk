import streamlit as st

from utils.constants import DEMO_METRICS
from utils.helper import format_number


def render_kpi_cards() -> None:
    m = DEMO_METRICS
    st.markdown(
        f"""
        <div class="id-kpi-grid">
            <div class="id-kpi-card">
                <div class="id-kpi-label">Complaints</div>
                <div class="id-kpi-value">{format_number(m['complaints'])}</div>
                <div class="id-kpi-meta id-kpi-meta-warn">{m['complaints_delta']}</div>
            </div>
            <div class="id-kpi-card">
                <div class="id-kpi-label">Cases</div>
                <div class="id-kpi-value">{m['cases']}</div>
                <div class="id-kpi-meta id-kpi-meta-danger">{m['cases_spiking']}</div>
            </div>
            <div class="id-kpi-card">
                <div class="id-kpi-label">Anger score</div>
                <div class="id-kpi-value">{m['anger_score']}</div>
                <div class="id-kpi-meta id-kpi-meta-danger">{m['anger_model']}</div>
            </div>
            <div class="id-kpi-card">
                <div class="id-kpi-label">Topics</div>
                <div class="id-kpi-value">{m['topics']}</div>
                <div class="id-kpi-meta id-kpi-meta-info">{m['topics_model']}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
