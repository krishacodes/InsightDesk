import streamlit as st


def render_single_spike(

        title:str,
        severity:str,
        complaints:int,
        detected_at:str

):

    with st.container(border=True):

        st.subheader(title)

        st.write(

            f"Severity : {severity}"

        )

        st.write(

            f"+{complaints} complaints"

        )

        st.write(

            f"Detected : {detected_at}"

        )


def render_spike_cards():

    st.subheader(

        "Active Spikes"

    )


    render_single_spike(

        title="Authentication Failure",

        severity="HIGH",

        complaints=128,

        detected_at="10 mins ago"

    )


    render_single_spike(

        title="Payment Failure",

        severity="MEDIUM",

        complaints=41,

        detected_at="42 mins ago"

    )


    render_single_spike(

        title="Delivery Delay",

        severity="LOW",

        complaints=19,

        detected_at="1 hour ago"

    )