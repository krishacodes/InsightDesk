import streamlit as st

from services.chatbot_api import send_chat_message


def render_chatbot():

    st.title("InsightDesk AI Assistant")

    st.caption(
        "Ask questions about cases, complaints, "
        "topics and root cause analysis."
    )

    # -----------------------------
    # CHAT HISTORY
    # -----------------------------

    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    for message in st.session_state.chat_messages:

        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # -----------------------------
    # CHAT INPUT
    # -----------------------------

    question = st.chat_input(
        "Ask InsightDesk..."
    )

    if question:

        st.session_state.chat_messages.append(
            {
                "role": "user",
                "content": question
            }
        )

        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):

            with st.spinner(
                "Analyzing complaint data..."
            ):

                try:

                    answer = send_chat_message(
                        question
                    )

                except Exception as e:

                    answer = (
                        "Unable to process the request. "
                        f"{e}"
                    )

            st.markdown(answer)

        st.session_state.chat_messages.append(
            {
                "role": "assistant",
                "content": answer
            }
        )

    # -----------------------------
    # CLEAR CHAT
    # -----------------------------

    if st.session_state.chat_messages:

        if st.button(
            "Clear conversation"
        ):
            st.session_state.chat_messages = []
            st.rerun()