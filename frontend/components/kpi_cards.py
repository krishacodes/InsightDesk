import streamlit as st


def render_kpi_cards(data):

    c1,c2,c3,c4=st.columns(4)


    with c1:

        st.metric(

            "Complaints",

            data["complaints"],

            data["complaints_delta"]

        )


    with c2:

        st.metric(

            "Cases",

            data["cases"]

        )


    with c3:

        st.metric(

            "Sentiment",

            data["sentiment"]

        )


    with c4:

        st.metric(

            "Topics",

            data["topics"]

        )