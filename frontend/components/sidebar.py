import streamlit as st

from utils.constants import AI_PAGES, DEMO_SPIKE_ALERT, MONITOR_PAGES


def _nav_options() -> list[str]:
    return MONITOR_PAGES + AI_PAGES


def render_sidebar() -> str:
    if "page" not in st.session_state:
        st.session_state.page = "Overview"

    st.markdown(
        '<p style="font-size:1.1rem;font-weight:700;color:#1A2332;margin-bottom:0.5rem;">'
        "InsightDesk</p>",
        unsafe_allow_html=True,
    )

    st.markdown('<div class="id-nav-section">Monitor</div>', unsafe_allow_html=True)
    for label in MONITOR_PAGES:
        active = st.session_state.page == label
        btn_type = "primary" if active else "secondary"
        if st.button(label, key=f"nav_{label}", use_container_width=True, type=btn_type):
            st.session_state.page = label
            st.rerun()

    st.markdown('<div class="id-nav-section">AI</div>', unsafe_allow_html=True)
    for label in AI_PAGES:
        active = st.session_state.page == label
        btn_type = "primary" if active else "secondary"
        if st.button(label, key=f"nav_{label}", use_container_width=True, type=btn_type):
            st.session_state.page = label
            st.rerun()

    alert = DEMO_SPIKE_ALERT
    st.markdown(
        f"""
        <div class="id-sidebar-alert">
            <div class="id-sidebar-alert-title">{alert['title']}</div>
            <div class="id-sidebar-alert-headline">{alert['headline']}</div>
            <div class="id-sidebar-alert-meta">{alert['meta']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    return st.session_state.page
