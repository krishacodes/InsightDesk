import streamlit as st

from utils.styles import load_styles

from pages.overview import render_overview
from pages.sentiment import render_sentiment
from pages.chatbot import render_chatbot
from pages.overview import render_overview
from pages.sentiment import render_sentiment
from pages.clusters import render_clusters
from pages.case_intelligence import render_case_intelligence

# --------------------------------
# PAGE CONFIG
# --------------------------------

st.set_page_config(
    page_title="InsightDesk",
    page_icon="📊",
    layout="wide"
)


# --------------------------------
# GLOBAL STYLES
# --------------------------------

# Uncomment when you want to enable
# the existing custom styling.
# load_styles()


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
        "Spikes",
        "Chatbot",
        "Admin"
    ]
)


# --------------------------------
# OVERVIEW
# --------------------------------

if page == "Overview":

    render_overview()


# --------------------------------
# SENTIMENT
# --------------------------------

elif page == "Sentiment":

    render_sentiment()


# --------------------------------
# CLUSTERS
# --------------------------------

elif page == "Clusters":

    render_clusters()

# --------------------------------
# CASE INTELLIGENCE
# --------------------------------

elif page == "Case Intelligence":
    render_case_intelligence()
# --------------------------------
# SPIKES
# --------------------------------

elif page == "Spikes":

    st.title("Spike Detection")

    st.info(
        "Spike analytics page coming soon."
    )


# --------------------------------
# RCA
# --------------------------------

elif page == "RCA":

    st.title("Root Cause Analysis")

    st.info(
        "Root cause analysis page coming soon."
    )


# --------------------------------
# AI ASSISTANT
# --------------------------------

elif page == "AI Assistant":

    render_chatbot()