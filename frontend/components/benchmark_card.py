import streamlit as st


def render_benchmarks(benchmarks):

    rows = ""

    for key,value in benchmarks.items():

        rows += f"""

        <div class="bench-row">

            <span class="bench-key">

                {key}

            </span>


            <span class="bench-val">

                {value}

            </span>

        </div>

        """


    st.markdown(

        f"""

        <div class="card">

            <div class="section-title">

                BENCHMARKS

            </div>


            {rows}


        </div>

        """,

        unsafe_allow_html=True

    )