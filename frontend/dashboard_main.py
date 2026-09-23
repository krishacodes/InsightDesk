import streamlit as st

from pages.overview import render_overview
from pages.sentiment import render_sentiment
from pages.clusters import render_clusters
from pages.case_intelligence import render_case_intelligence
from pages.incident_response import render_incident_response
from pages.chatbot import render_chatbot


# --------------------------------
# PAGE CONFIG
# --------------------------------

st.set_page_config(
    page_title="InsightDesk",
    page_icon="📊",
    layout="wide"
)


# --------------------------------
# SIDEBAR
# --------------------------------

st.sidebar.title("InsightDesk")

st.sidebar.caption(
    "AI Complaint Intelligence"
)

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Sentiment",
        "Clusters",
        "Case Intelligence",
        "Incident Response",
        "AI Assistant",
    ]
)


# --------------------------------
# PAGE ROUTING
# --------------------------------

if page == "Overview":
    render_overview()

elif page == "Sentiment":
    render_sentiment()

elif page == "Clusters":
    render_clusters()

elif page == "Case Intelligence":
    render_case_intelligence()

elif page == "Incident Response":
    render_incident_response()

elif page == "AI Assistant":
    render_chatbot()