import streamlit as st
from utils.styles import load_styles
st.set_page_config(
    page_title="InsightDesk",
    layout="wide"
)

#load_styles()
from pages.overview import (
    render_overview
)
from pages.sentiment import (
    render_sentiment
)


st.sidebar.title(

    "InsightDesk"

)


page = st.sidebar.radio(

    "Navigation",

    [

        "Overview",

        "Sentiment",

        "Clusters",

        "Spikes",

        "Chatbot",

        "Admin"

    ]

)



if page=="Overview":

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

    st.info(

        "Clusters page coming soon."

    )


# --------------------------------
# SPIKES
# --------------------------------

elif page == "Spikes":

    st.info(

        "Spikes page coming soon."

    )


# --------------------------------
# CHATBOT
# --------------------------------

elif page == "Chatbot":

    st.info(

        "Chatbot coming soon."

    )


# --------------------------------
# ADMIN
# --------------------------------

elif page == "Admin":

    st.info(

        "Admin page coming soon."

    )