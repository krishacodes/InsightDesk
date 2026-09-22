import streamlit as st


def render_model_comparison(models):
    """
    Render sentiment model benchmark statistics.
    """

    st.subheader("Model Comparison")

    col1, col2 = st.columns(2)

    for model_name, column in [
        ("RoBERTa", col1),
        ("DistilBERT", col2),
    ]:
        model_data = models.get(model_name, {})

        with column:
            with st.container(border=True):

                st.markdown(f"#### {model_name}")

                if model_name == "RoBERTa":
                    st.caption("Best Accuracy")

                if not model_data:
                    st.info("No benchmark data available.")
                    continue

                for key, value in model_data.items():
                    st.metric(
                        label=key,
                        value=value
                    )