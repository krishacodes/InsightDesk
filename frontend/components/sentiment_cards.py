import streamlit as st


def render_sentiment_cards(data):

    """
    Render sentiment KPI cards.

    Parameters
    ----------
    data : dict

        {
            "positive":"24%",
            "negative":"71%",
            "neutral":"5%",
            "confidence":"88%"
        }

    """

    c1, c2, c3, c4 = st.columns(4)

    with c1:

        st.metric(

            label="Positive",

            value=data["positive"]

        )

    with c2:

        st.metric(

            label="Negative",

            value=data["negative"]

        )

    with c3:

        st.metric(

            label="Neutral",

            value=data["neutral"]

        )

    with c4:

        st.metric(

            label="Average Confidence",

            value=data["confidence"]

        )