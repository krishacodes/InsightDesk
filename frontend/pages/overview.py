import streamlit as st
import pandas as pd

from services.dashboard_api import get_overview_data


# ============================================================
# OVERVIEW PAGE STYLES
# ============================================================

def _load_overview_css():
    """
    Cosmetic styling only.

    All page structure continues to use native Streamlit
    components. No HTML cards/tables/components are generated.
    """

    st.markdown(
        """
        <style>

        /* ====================================================
           PAGE TYPOGRAPHY
           ==================================================== */

        /* Main page title */
        h1 {
            color: #172554 !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em;
        }

        /* Section headings */
        h2, h3 {
            color: #1E3A5F !important;
            font-weight: 650 !important;
            letter-spacing: -0.01em;
        }

        /* Smaller markdown headings such as model names */
        h4 {
            color: #1E3A5F !important;
            font-weight: 650 !important;
        }

        /* General text */
        p {
            color: #334155;
        }


        /* ====================================================
           CAPTIONS
           ==================================================== */

        div[data-testid="stCaptionContainer"] {
            color: #64748B !important;
            margin-top: -0.30rem;
            margin-bottom: 0.65rem;
        }

        div[data-testid="stCaptionContainer"] p {
            color: #64748B !important;
        }


        /* ====================================================
           NATIVE STREAMLIT CARDS / CONTAINERS
           ==================================================== */

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 10px;
            border-color: #E2E8F0 !important;
            background-color: #FBFCFE;
        }


        /* ====================================================
           KPI METRICS
           ==================================================== */

        div[data-testid="stMetricLabel"] {
            font-size: 0.78rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.025em;
            color: #64748B !important;
        }

        div[data-testid="stMetricLabel"] p {
            color: #64748B !important;
        }

        div[data-testid="stMetricValue"] {
            font-weight: 700;
            color: #1D4ED8 !important;
        }


        /* ====================================================
           DATAFRAME / TOPIC TABLE
           ==================================================== */

        div[data-testid="stDataFrame"] {
            border-radius: 8px;
            overflow: hidden;
        }


        /* ====================================================
           DIVIDERS
           ==================================================== */

        hr {
            margin: 1.25rem 0;
            border-color: #E2E8F0 !important;
        }


        /* ====================================================
           SIDEBAR
           ==================================================== */

        section[data-testid="stSidebar"] {
            background-color: #F8FAFC;
            border-right: 1px solid #E2E8F0;
        }

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: #172554 !important;
        }


        /* ====================================================
           RADIO NAVIGATION
           ==================================================== */

        div[role="radiogroup"] label {
            color: #334155 !important;
        }

        div[role="radiogroup"] label:hover {
            color: #1D4ED8 !important;
        }


        /* ====================================================
           BUTTONS
           ==================================================== */

        div[data-testid="stButton"] button {
            border-radius: 8px;
            border-color: #CBD5E1;
        }

        div[data-testid="stButton"] button:hover {
            border-color: #2563EB;
            color: #1D4ED8;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# HELPERS
# ============================================================

def format_number(value):
    if isinstance(value, int):
        return f"{value:,}"
    return value


# ============================================================
# KPI SECTION
# ============================================================

def render_kpis(data):

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.metric(
                "Complaints",
                format_number(
                    data.get("complaints", 0)
                ),
            )

    with col2:
        with st.container(border=True):
            st.metric(
                "Cases",
                format_number(
                    data.get("cases", 0)
                ),
            )

    with col3:
        with st.container(border=True):
            st.metric(
                "Topics",
                format_number(
                    data.get("topics", 0)
                ),
            )

    with col4:
        with st.container(border=True):
            st.metric(
                "Active Spikes",
                format_number(
                    data.get("spikes", 0)
                ),
            )


# ============================================================
# COMPLAINT VOLUME
# ============================================================

def render_complaint_volume(data):

    st.subheader("Complaint Volume")

    st.caption(
        "Daily complaint activity across the available dataset."
    )

    volume = data.get(
        "volume_by_day",
        [],
    )

    with st.container(border=True):

        if not volume:
            st.info(
                "No complaint volume data available."
            )
            return

        dataframe = pd.DataFrame(
            volume
        )

        if (
            "date" not in dataframe.columns
            or "count" not in dataframe.columns
        ):
            st.warning(
                "Complaint volume data is unavailable "
                "in the expected format."
            )
            return

        dataframe["date"] = pd.to_datetime(
            dataframe["date"]
        )

        dataframe = (
            dataframe
            .sort_values("date")
            .set_index("date")
        )

        st.line_chart(
            dataframe["count"],
            height=300,
        )


# ============================================================
# SPIKE ALERTS
# ============================================================

def render_spikes(data):

    st.subheader("Spike Alerts")

    spikes = data.get(
        "spike_cases",
        [],
    )

    if not spikes:

        with st.container(border=True):

            st.success(
                "No active complaint spikes detected."
            )

            st.caption(
                "No cases currently exceed "
                "the configured spike threshold."
            )

        return

    for spike in spikes:

        case_id = spike.get(
            "case_id",
            "Unknown",
        )

        current = spike.get(
            "current",
            "N/A",
        )

        z_score = spike.get(
            "z_score",
            "N/A",
        )

        representative_text = spike.get(
            "representative_text",
            "",
        )

        critical = spike.get(
            "critical",
            False,
        )

        with st.container(border=True):

            if critical:
                st.error(
                    f"Critical Spike — Case #{case_id}"
                )

            else:
                st.warning(
                    f"Spike Detected — Case #{case_id}"
                )

            c1, c2 = st.columns(2)

            with c1:
                st.metric(
                    "Current Reports",
                    current,
                )

            with c2:
                st.metric(
                    "Z-Score",
                    z_score,
                )

            if representative_text:
                st.write(
                    representative_text
                )


# ============================================================
# TOPIC INTELLIGENCE
# ============================================================

def render_topics(data):

    st.subheader(
        "Topic Intelligence"
    )

    st.caption(
        "Current complaint clusters and departmental ownership."
    )

    topics = data.get(
        "topics_data",
        [],
    )

    with st.container(border=True):

        if not topics:
            st.info(
                "No topic data available."
            )
            return

        rows = []

        for topic in topics:

            rows.append(
                {
                    "Topic": topic.get(
                        "topic_name",
                        "Unknown",
                    ),

                    "Department": topic.get(
                        "department",
                        "Unknown",
                    ),

                    "Description": topic.get(
                        "description",
                        "",
                    ),
                }
            )

        st.dataframe(
            rows,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# MODEL BENCHMARKS
# ============================================================

def render_benchmarks(data):

    st.subheader(
        "Model Benchmarks"
    )

    st.caption(
        "Performance metrics from the sentiment models."
    )

    benchmarks = data.get(
        "benchmarks",
        [],
    )

    if not benchmarks:

        with st.container(border=True):

            st.info(
                "No benchmark data available."
            )

        return

    for benchmark in benchmarks:

        model_name = benchmark.get(
            "model_name",
            "Unknown",
        )

        with st.container(border=True):

            st.markdown(
                f"#### {model_name.upper()}"
            )

            c1, c2, c3, c4 = st.columns(4)

            with c1:

                st.metric(
                    "Complaints",
                    benchmark.get(
                        "complaints_processed",
                        0,
                    ),
                )

            with c2:

                latency = benchmark.get(
                    "average_latency_ms",
                    0,
                )

                st.metric(
                    "Average Latency",
                    f"{latency:.2f} ms",
                )

            with c3:

                throughput = benchmark.get(
                    "throughput",
                    0,
                )

                st.metric(
                    "Throughput",
                    f"{throughput:.2f}/sec",
                )

            with c4:

                memory = benchmark.get(
                    "memory_mb",
                    0,
                )

                st.metric(
                    "Memory",
                    f"{memory:.2f} MB",
                )


# ============================================================
# MAIN OVERVIEW PAGE
# ============================================================

def render_overview():

    _load_overview_css()

    # --------------------------------------------------------
    # HEADER
    # --------------------------------------------------------

    st.title(
        "Overview"
    )

    st.caption(
        "AI-powered complaint intelligence and system health."
    )

    # --------------------------------------------------------
    # LOAD BACKEND DATA
    # --------------------------------------------------------

    try:

        with st.spinner(
            "Loading InsightDesk..."
        ):

            data = get_overview_data()

    except Exception as e:

        st.error(
            "Unable to connect to the InsightDesk backend."
        )

        st.info(
            "Make sure the backend is running and try again."
        )

        with st.expander(
            "Technical details"
        ):

            st.code(
                str(e)
            )

        return

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    render_kpis(
        data
    )

    st.divider()

    # --------------------------------------------------------
    # COMPLAINT VOLUME + SPIKES
    # --------------------------------------------------------

    left, right = st.columns(
        [2, 1]
    )

    with left:

        render_complaint_volume(
            data
        )

    with right:

        render_spikes(
            data
        )

    st.divider()

    # --------------------------------------------------------
    # TOPIC INTELLIGENCE
    # --------------------------------------------------------

    render_topics(
        data
    )

    st.divider()

    # --------------------------------------------------------
    # MODEL BENCHMARKS
    # --------------------------------------------------------

    render_benchmarks(
        data
    )