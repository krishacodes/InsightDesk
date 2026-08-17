import streamlit as st


def render_clusters(clusters):

    rows = ""

    for cluster in clusters:

        status = (

            "badge-spike"

            if cluster["status"]=="spike"

            else "badge-stable"

        )

        rows += f"""

        <div class="cluster-row">

            <div>

                <span class="cluster-name">

                    {cluster["name"]}

                </span>

                <span class="cluster-count">

                    {cluster["count"]}

                </span>

            </div>


            <span class="badge {status}">

                {cluster["status"]}

            </span>

        </div>

        """


    st.markdown(

        f"""

        <div class="card">

        <div class="section-title">

        CLUSTERS

        </div>


        {rows}


        </div>

        """,

        unsafe_allow_html=True

    )