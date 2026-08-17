import streamlit as st


def render_negative_cases(negative_cases):
    """
    Render top negative cases.

    Parameters
    ----------
    negative_cases : list[dict]

    """

    st.markdown(

        "### Top Negative Cases"

    )


    for case in negative_cases:

        severity = case["severity"]


        if severity == "HIGH":

            badge_color = "#e05050"

        elif severity == "MEDIUM":

            badge_color = "#f0ad4e"

        else:

            badge_color = "#1ec58c"


        st.markdown(

            f"""

            <div class="case-row">


                <div class="case-top">

                    <div>

                        <b>Case #{case["case_id"]}</b>

                    </div>


                    <div>

                        <span
                        style="
                        color:{badge_color};
                        font-weight:700;
                        ">

                        {severity}

                        </span>

                    </div>


                </div>


                <div class="case-meta">

                    Department :
                    {case["department"]}

                    &nbsp;&nbsp; | &nbsp;&nbsp;

                    Emotion :
                    {case["emotion"]}

                    &nbsp;&nbsp; | &nbsp;&nbsp;

                    Confidence :
                    {case["confidence"]}

                    &nbsp;&nbsp; | &nbsp;&nbsp;

                    {case["timestamp"]}

                </div>


                <br>


                <div class="case-snippet">

                    {case["representative_text"]}

                </div>


            </div>

            """,

            unsafe_allow_html=True

        )


        view_case = st.button(

            f"View Case {case['case_id']}",

            key=f"case_{case['case_id']}",

            use_container_width=True

        )


        if view_case:

            st.info(

                f"Opening Case #{case['case_id']}"

            )