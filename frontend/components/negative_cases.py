import streamlit as st


def render_negative_cases(negative_cases):
    """
    Render top negative cases.
    """

    st.subheader("Top Negative Cases")

    if not negative_cases:
        st.info("No negative cases available.")
        return

    for case in negative_cases:

        case_id = case.get("case_id", "Unknown")
        severity = case.get("severity", "Unknown")
        department = case.get("department", "Unknown")
        emotion = case.get("emotion", "Unknown")
        confidence = case.get("confidence", "N/A")
        timestamp = case.get("timestamp", "")
        text = case.get(
            "representative_text",
            "No complaint text available."
        )

        with st.container(border=True):

            title_col, severity_col = st.columns(
                [4, 1]
            )

            with title_col:
                st.markdown(
                    f"#### Case #{case_id}"
                )

            with severity_col:
                st.markdown(
                    f"**{severity}**"
                )

            c1, c2, c3 = st.columns(3)

            with c1:
                st.caption("Department")
                st.write(department)

            with c2:
                st.caption("Emotion")
                st.write(emotion)

            with c3:
                st.caption("Confidence")
                st.write(confidence)

            st.markdown("**Complaint**")
            st.write(text)

            if timestamp:
                st.caption(timestamp)

            if st.button(
                "View Case",
                key=f"case_{case_id}",
                use_container_width=True,
            ):
                st.info(
                    f"Case #{case_id} selected."
                )