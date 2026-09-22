import streamlit as st
import pandas as pd

from services.clusters_api import get_clusters_data


# ============================================================
# COSMETIC STYLES
# ============================================================

def _load_cluster_styles():

    st.markdown(
        """
        <style>

        div[data-testid="stMetric"] {
            background: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 1rem;
        }

        div[data-testid="stMetricLabel"] {
            color: #64748B;
            font-size: 0.8rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }

        div[data-testid="stMetricValue"] {
            color: #1E3A5F;
            font-weight: 700;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 10px;
        }

        div[data-testid="stExpander"] {
            border: 1px solid #E2E8F0;
            border-radius: 10px;
        }

        div[data-testid="stCaptionContainer"] {
            color: #64748B;
        }

        h1, h2, h3 {
            color: #1E3A5F;
        }

        hr {
            margin: 1.2rem 0;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# KPI SECTION
# ============================================================

def render_cluster_kpis(data):

    case_count = data.get(
        "case_count",
        0
    )

    topic_count = data.get(
        "topic_count",
        0
    )

    outlier_count = data.get(
        "outlier_count",
        0
    )

    outlier_rate = data.get(
        "outlier_rate",
        0
    )

    clustered_cases = max(
        case_count - outlier_count,
        0
    )

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric(
            "Topics",
            topic_count
        )

    with c2:
        st.metric(
            "Clustered Cases",
            clustered_cases
        )

    with c3:
        st.metric(
            "Outliers",
            outlier_count
        )

    with c4:
        st.metric(
            "Outlier Rate",
            f"{outlier_rate * 100:.2f}%"
        )


# ============================================================
# CLUSTER DISTRIBUTION
# ============================================================

def render_cluster_distribution(data):

    st.subheader(
        "Cluster Distribution"
    )

    st.caption(
        "Number of cases assigned to each discovered complaint theme."
    )

    topics = data.get(
        "topics",
        []
    )

    if not topics:
        st.info(
            "No cluster data available."
        )
        return

    chart_data = pd.DataFrame(
        [
            {
                "Topic":
                    topic.get(
                        "topic_name",
                        "Unknown"
                    ),

                "Cases":
                    topic.get(
                        "case_count",
                        0
                    ),
            }

            for topic in topics
        ]
    )

    chart_data = (
        chart_data
        .set_index("Topic")
    )

    with st.container(
        border=True
    ):

        st.bar_chart(
            chart_data["Cases"],
            height=320
        )


# ============================================================
# TOPIC TABLE
# ============================================================

def render_topic_table(data):

    st.subheader(
        "Topic Intelligence"
    )

    st.caption(
        "Discovered complaint themes, departmental ownership "
        "and assigned case volume."
    )

    topics = data.get(
        "topics",
        []
    )

    if not topics:
        st.info(
            "No topics available."
        )
        return

    rows = []

    for topic in topics:

        rows.append(
            {
                "Topic":
                    topic.get(
                        "topic_name",
                        "Unknown"
                    ),

                "Department":
                    topic.get(
                        "department",
                        "Unknown"
                    ),

                "Cases":
                    topic.get(
                        "case_count",
                        0
                    ),

                "Description":
                    topic.get(
                        "description",
                        ""
                    ),
            }
        )

    with st.container(
        border=True
    ):

        st.dataframe(
            rows,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# TOPIC EXPLORER
# ============================================================

def render_topic_explorer(data):

    st.subheader(
        "Explore Topics"
    )

    st.caption(
        "Inspect representative cases belonging to each cluster."
    )

    topics = data.get(
        "topics",
        []
    )

    if not topics:
        return

    for topic in topics:

        topic_name = topic.get(
            "topic_name",
            "Unknown Topic"
        )

        department = topic.get(
            "department",
            "Unknown"
        )

        case_count = topic.get(
            "case_count",
            0
        )

        description = topic.get(
            "description",
            ""
        )

        representative_cases = topic.get(
            "representative_cases",
            []
        )

        label = (
            f"{topic_name} "
            f"— {case_count} cases"
        )

        with st.expander(
            label
        ):

            st.caption(
                f"Department: {department}"
            )

            if description:

                st.write(
                    description
                )

            st.markdown(
                "**Representative Cases**"
            )

            if not representative_cases:

                st.info(
                    "No representative cases available."
                )

                continue

            for case in representative_cases:

                case_id = case.get(
                    "case_id",
                    "Unknown"
                )

                text = case.get(
                    "representative_text",
                    ""
                )

                report_count = case.get(
                    "report_count",
                    0
                )

                with st.container(
                    border=True
                ):

                    st.markdown(
                        f"**Case #{case_id}**"
                    )

                    st.write(
                        text
                    )

                    st.caption(
                        f"Reports: {report_count}"
                    )


# ============================================================
# MAIN PAGE
# ============================================================

def render_clusters():

    _load_cluster_styles()

    # ----------------------------------
    # HEADER
    # ----------------------------------

    st.title(
        "Clusters"
    )

    st.caption(
        "Recurring complaint themes discovered using BERTopic."
    )

    # ----------------------------------
    # LOAD DATA
    # ----------------------------------

    try:

        with st.spinner(
            "Loading cluster intelligence..."
        ):

            data = get_clusters_data()

    except Exception as e:

        st.error(
            "Unable to load clustering data."
        )

        st.code(
            str(e)
        )

        return

    # ----------------------------------
    # KPIs
    # ----------------------------------

    render_cluster_kpis(
        data
    )

    st.divider()

    # ----------------------------------
    # DISTRIBUTION
    # ----------------------------------

    render_cluster_distribution(
        data
    )

    st.divider()

    # ----------------------------------
    # TOPIC TABLE
    # ----------------------------------

    render_topic_table(
        data
    )

    st.divider()

    # ----------------------------------
    # TOPIC EXPLORER
    # ----------------------------------

    render_topic_explorer(
        data
    )