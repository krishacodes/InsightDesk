import streamlit as st


def render_topbar(data):

    st.markdown(

        f"""
        <div class="topbar">
            <h1>InsightDesk
                <span class="pill pill-live">
                    ● Live
                </span>

                <span class="pill pill-spike">
                    {data["spikes"]} spikes
                </span>

            </h1>

            <p>
                Vision Helpdesk · complaint intelligence
            </p>

            <div class="topbar-stats">

                <div class="topbar-stat">
                    <div class="num">
                        {data["complaints"]}
                    </div>

                    <div class="label">
                        complaints
                    </div>
                </div>


                <div class="topbar-stat">
                    <div class="num">
                        {data["cases"]}
                    </div>

                    <div class="label">
                        cases
                    </div>
                </div>


                <div class="topbar-stat">
                    <div class="num">
                        {data["topics"]}
                    </div>

                    <div class="label">
                        topics
                    </div>
                </div>


                <div class="topbar-stat">
                    <div class="num">
                        {data["spikes"]}
                    </div>

                    <div class="label">
                        spikes
                    </div>
                </div>

            </div>

        </div>

        """,

        unsafe_allow_html=True

    )