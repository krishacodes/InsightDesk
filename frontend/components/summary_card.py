import streamlit as st


def render_ai_summary(post_mortem):

    st.markdown(

        f"""

        <div class="card">

            <div class="section-title">

                AI POST MORTEM

            </div>


            <div class="pm-label">

                ROOT CAUSE

            </div>


            <div class="pm-text">

                {post_mortem["root_cause"]}

            </div>


            <div class="pm-label">

                ACTION

            </div>


            <div class="pm-text">

                {post_mortem["action"]}

            </div>


        </div>

        """,

        unsafe_allow_html=True

    )


    c1,c2=st.columns(2)

    with c1:

        st.button(

            "Approve and Send",
            key="overview_approve_summary",

            type="primary",

            use_container_width=True

        )


    with c2:

        st.button(

            "Edit",
            key="overview_edit_summary",

            use_container_width=True

        )