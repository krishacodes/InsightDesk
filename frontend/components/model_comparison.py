import streamlit as st


def render_model_comparison(models):
    """
    Render model benchmark statistics.

    Parameters
    ----------
    models : dict

        {

            "RoBERTa":{

                ....

            },

            "DistilBERT":{

                ....

            }

        }

    """

    col1, col2 = st.columns(2)

    model_columns = [

        ("RoBERTa", col1),

        ("DistilBERT", col2)

    ]


    for model_name, column in model_columns:

        model_data = models[model_name]

        rows = ""

        for key, value in model_data.items():

            rows += f"""

            <div class="metric-row">

                <span>

                    {key}

                </span>

                <span class="metric-val">

                    {value}

                </span>

            </div>

            """


        winner_tag = ""

        if model_name == "RoBERTa":

            winner_tag = """

            <span class="winner-tag">

                Best Accuracy

            </span>

            """


        column.markdown(

            f"""

            <div class="model-card">

                <div class="model-name">

                    {model_name}

                    {winner_tag}

                </div>


                <div style="margin-top:8px;">

                    {rows}

                </div>


            </div>

            """,

            unsafe_allow_html=True

        )