import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

from components.header import render_header
from components.sidebar import render_sidebar
from views.overview import render_overview
from utils.theme import inject_theme

st.set_page_config(
    page_title="InsightDesk",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_theme()
render_header()

page = render_sidebar()

if page == "Overview":
    render_overview()
elif page == "Clusters":
    st.title("Clusters")
    st.caption("BERTopic complaint clusters")
elif page == "Sentiment":
    st.title("Sentiment")
    st.caption("RoBERTa anger scoring")
elif page == "Spikes":
    st.title("Spikes")
    st.caption("Z-score anomaly detection")
elif page == "Diagnostics":
    st.title("Diagnostics")
    st.caption("Model health and pipeline status")
elif page == "Chatbot":
    st.title("Chatbot")
    st.caption("Ask questions about complaint data")
elif page == "Admin":
    st.title("Admin")
    st.caption("Configuration and escalation settings")
