import streamlit as st


from services.sentiment_api import (
    get_sentiment_data
)


from components.sentiment_cards import (
    render_sentiment_cards
)

from components.sentiment_trend import (
    render_sentiment_trend
)

from components.emotion_distribution import (
    render_emotion_distribution
)

from components.model_comparison import (
    render_model_comparison
)

from components.negative_cases import (
    render_negative_cases
)


# --------------------------------------------------
# PAGE HEADER
# --------------------------------------------------

def render_page_header():

    header_col, action_col = st.columns([4,2])

    with action_col:

        c1,c2 = st.columns(2)

        with c1:

            st.button(

                "Last 7 Days",

                key="sentiment_last_7_days",

                use_container_width=True

            )

        with c2:

            st.button(

                "Export",

                key="sentiment_export",

                use_container_width=True

            )


# --------------------------------------------------
# MAIN PAGE
# --------------------------------------------------

def render_sentiment():

    data = get_sentiment_data()


    # ----------------------------------
    # PAGE HEADER
    # ----------------------------------

    render_page_header()

    st.write("")


    # ----------------------------------
    # KPI CARDS
    # ----------------------------------

    render_sentiment_cards(data)

    st.write("")


    # ----------------------------------
    # SENTIMENT TREND
    # ----------------------------------

    render_sentiment_trend(

        data["trend"]

    )

    st.write("")


    # ----------------------------------
    # EMOTION DISTRIBUTION
    # ----------------------------------

    render_emotion_distribution(

        data["emotions"]

    )

    st.write("")


    # ----------------------------------
    # MODEL COMPARISON
    # ----------------------------------

    render_model_comparison(

        data["models"]

    )

    st.write("")


    # ----------------------------------
    # TOP NEGATIVE CASES
    # ----------------------------------

    render_negative_cases(

        data["negative_cases"]

    )

