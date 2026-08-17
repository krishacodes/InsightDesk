import streamlit as st
import pandas as pd

from services.dashboard_api import get_overview_data


# ============================================================
# COLORS
# ============================================================

BG = "#0B1220"
CARD = "#111827"
CARD_HOVER = "#172033"

BORDER = "#243044"

TEXT = "#F8FAFC"
MUTED = "#94A3B8"

BLUE = "#3B82F6"
PURPLE = "#8B5CF6"
RED = "#EF4444"
ORANGE = "#F59E0B"
GREEN = "#22C55E"


# ============================================================
# PAGE CSS
# ============================================================

def load_overview_styles():

    st.markdown(
        f"""
        <style>

        /* =========================
           MAIN BACKGROUND
        ========================= */

        .stApp {{
            background: {BG};
        }}

        .block-container {{
            padding-top: 2rem;
            padding-bottom: 3rem;
            max-width: 1400px;
        }}

        /* =========================
           GENERAL TEXT
        ========================= */

        h1, h2, h3, h4, p, span, label {{
            color: {TEXT};
        }}

        .muted {{
            color: {MUTED};
        }}

        /* =========================
           HEADER
        ========================= */

        .overview-header {{
            margin-bottom: 1.8rem;
        }}

        .overview-title {{
            font-size: 2rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
            color: {TEXT};
        }}

        .overview-subtitle {{
            font-size: 0.9rem;
            color: {MUTED};
        }}

        /* =========================
           CARDS
        ========================= */

        .dashboard-card {{
            background: {CARD};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 1rem;
        }}

        .dashboard-card-title {{
            font-size: 0.85rem;
            font-weight: 600;
            color: {MUTED};
            margin-bottom: 0.8rem;
        }}

        /* =========================
           KPI
        ========================= */

        .kpi-label {{
            color: {MUTED};
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .kpi-value {{
            color: {TEXT};
            font-size: 2rem;
            font-weight: 700;
            margin-top: 0.25rem;
        }}

        .kpi-accent {{
            width: 30px;
            height: 3px;
            border-radius: 3px;
            margin-top: 0.8rem;
        }}

        /* =========================
           SECTION TITLE
        ========================= */

        .section-title {{
            font-size: 1.05rem;
            font-weight: 650;
            color: {TEXT};
            margin-top: 1.4rem;
            margin-bottom: 0.8rem;
        }}

        .section-description {{
            font-size: 0.8rem;
            color: {MUTED};
            margin-top: -0.5rem;
            margin-bottom: 1rem;
        }}

        /* =========================
           TOPIC CARDS
        ========================= */

        .topic-card {{
            background: {CARD};
            border: 1px solid {BORDER};
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.7rem;
        }}

        .topic-name {{
            font-size: 0.9rem;
            font-weight: 600;
            color: {TEXT};
        }}

        .topic-department {{
            font-size: 0.75rem;
            color: {MUTED};
            margin-top: 0.3rem;
        }}

        /* =========================
           SPIKE
        ========================= */

        .spike-card {{
            background: rgba(239, 68, 68, 0.08);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-left: 3px solid {RED};
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.7rem;
        }}

        .spike-title {{
            color: {RED};
            font-weight: 650;
            font-size: 0.9rem;
        }}

        .spike-meta {{
            color: {MUTED};
            font-size: 0.75rem;
            margin-top: 0.35rem;
        }}

        /* =========================
           HEALTHY
        ========================= */

        .healthy-card {{
            background: rgba(34, 197, 94, 0.07);
            border: 1px solid rgba(34, 197, 94, 0.25);
            border-radius: 10px;
            padding: 1rem;
        }}

        .healthy-title {{
            color: {GREEN};
            font-weight: 650;
        }}

        /* =========================
           BENCHMARK
        ========================= */

        .benchmark-card {{
            background: {CARD};
            border: 1px solid {BORDER};
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.7rem;
        }}

        .benchmark-model {{
            font-size: 0.9rem;
            font-weight: 650;
            color: {TEXT};
            margin-bottom: 0.7rem;
        }}

        .benchmark-label {{
            color: {MUTED};
            font-size: 0.7rem;
        }}

        .benchmark-value {{
            color: {TEXT};
            font-size: 0.95rem;
            font-weight: 600;
        }}

        /* =========================
           STREAMLIT METRIC OVERRIDE
        ========================= */

        div[data-testid="stMetric"] {{
            background: {CARD};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1rem;
        }}

        div[data-testid="stMetricLabel"] {{
            color: {MUTED} !important;
        }}

        div[data-testid="stMetricValue"] {{
            color: {TEXT} !important;
        }}

        /* =========================
           DATAFRAME
        ========================= */

        [data-testid="stDataFrame"] {{
            border: 1px solid {BORDER};
            border-radius: 10px;
        }}

        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# HELPERS
# ============================================================

def format_number(value):

    return f"{value:,}"


def render_kpi(label, value, accent):

    st.markdown(
        f"""
        <div class="dashboard-card">

            <div class="kpi-label">
                {label}
            </div>

            <div class="kpi-value">
                {format_number(value) if isinstance(value, int) else value}
            </div>

            <div
                class="kpi-accent"
                style="background:{accent};"
            ></div>

        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# HEADER
# ============================================================

def render_header():

    st.markdown(
        """
        <div class="overview-header">

            <div class="overview-title">
                Overview
            </div>

            <div class="overview-subtitle">
                AI-powered complaint intelligence and system health
            </div>

        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# KPI SECTION
# ============================================================

def render_kpis(data):

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        render_kpi(
            "Complaints",
            data.get("complaints", 0),
            BLUE,
        )

    with col2:
        render_kpi(
            "Cases",
            data.get("cases", 0),
            PURPLE,
        )

    with col3:
        render_kpi(
            "Topics",
            data.get("topics", 0),
            BLUE,
        )

    with col4:
        render_kpi(
            "Spikes",
            data.get("spikes", 0),
            RED if data.get("spikes", 0) else GREEN,
        )


# ============================================================
# COMPLAINT VOLUME
# ============================================================

def render_complaint_volume(data):

    st.markdown(
        """
        <div class="section-title">
            Complaint Volume
        </div>

        <div class="section-description">
            Daily complaint activity across the available dataset
        </div>
        """,
        unsafe_allow_html=True,
    )

    volume = data.get(
        "volume_by_day",
        []
    )

    if not volume:

        st.info(
            "No complaint volume data available."
        )

        return
    dataframe = pd.DataFrame(volume)

    st.write("DEBUG volume:", volume)
    st.write("DEBUG columns:", dataframe.columns.tolist())
    st.write("DEBUG dataframe:", dataframe)
    dataframe = pd.DataFrame(volume)

    dataframe["date"] = pd.to_datetime(
        dataframe["date"]
    )

    dataframe = dataframe.sort_values(
        "date"
    )

    dataframe = dataframe.set_index(
        "date"
    )

    st.line_chart(
        dataframe["count"],
        height=280,
    )


# ============================================================
# TOPIC INTELLIGENCE
# ============================================================

def render_topics(data):

    st.markdown(
        """
        <div class="section-title">
            Topic Intelligence
        </div>

        <div class="section-description">
            Current complaint clusters and departmental ownership
        </div>
        """,
        unsafe_allow_html=True,
    )

    topics = data.get(
        "topics_data",
        []
    )

    if not topics:

        st.info(
            "No topic data available."
        )

        return

    # Show a compact table rather than
    # rendering all 19 topics as huge cards.

    topic_rows = []

    for topic in topics:

        topic_rows.append(
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

                "Description":
                    topic.get(
                        "description",
                        ""
                    ),
            }
        )

    st.dataframe(
        topic_rows,
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# SPIKE ALERTS
# ============================================================

def render_spikes(data):

    st.markdown(
        """
        <div class="section-title">
            Spike Alerts
        </div>
        """,
        unsafe_allow_html=True,
    )

    spikes = data.get(
        "spike_cases",
        []
    )

    if not spikes:

        st.markdown(
            """
            <div class="healthy-card">

                <div class="healthy-title">
                    ✓ No active spikes
                </div>

                <div class="spike-meta">
                    No cases currently exceed the spike threshold.
                </div>

            </div>
            """,
            unsafe_allow_html=True,
        )

        return

    for spike in spikes:

        severity = (
            "CRITICAL"
            if spike.get("critical")
            else "SPIKE"
        )

        st.markdown(
            f"""
            <div class="spike-card">

                <div class="spike-title">
                    {severity}
                </div>

                <div class="spike-meta">

                    Case ID:
                    <strong>
                        {spike.get("case_id")}
                    </strong>

                    &nbsp; · &nbsp;

                    Current:
                    <strong>
                        {spike.get("current")}
                    </strong>

                    &nbsp; · &nbsp;

                    Z-score:
                    <strong>
                        {spike.get("z_score")}
                    </strong>

                </div>

                <div class="spike-meta">
                    {spike.get("representative_text", "")}
                </div>

            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# BENCHMARKS
# ============================================================

def render_benchmarks(data):

    st.markdown(
        """
        <div class="section-title">
            Model Benchmarks
        </div>

        <div class="section-description">
            Performance metrics from the sentiment models
        </div>
        """,
        unsafe_allow_html=True,
    )

    benchmarks = data.get(
        "benchmarks",
        []
    )

    if not benchmarks:

        st.info(
            "No benchmark data available."
        )

        return

    for benchmark in benchmarks:

        model_name = benchmark.get(
            "model_name",
            "Unknown"
        )

        complaints_processed = benchmark.get(
            "complaints_processed",
            0
        )

        latency = benchmark.get(
            "average_latency_ms",
            0
        )

        throughput = benchmark.get(
            "throughput",
            0
        )

        memory = benchmark.get(
            "memory_mb",
            0
        )

        st.markdown(
            f"""
            <div class="benchmark-card">

                <div class="benchmark-model">
                    {model_name.upper()}
                </div>

                <div style="
                    display:grid;
                    grid-template-columns:
                    repeat(4, 1fr);
                    gap:1rem;
                ">

                    <div>
                        <div class="benchmark-label">
                            Complaints
                        </div>
                        <div class="benchmark-value">
                            {complaints_processed:,}
                        </div>
                    </div>

                    <div>
                        <div class="benchmark-label">
                            Latency
                        </div>
                        <div class="benchmark-value">
                            {latency:.2f} ms
                        </div>
                    </div>

                    <div>
                        <div class="benchmark-label">
                            Throughput
                        </div>
                        <div class="benchmark-value">
                            {throughput:.2f}/sec
                        </div>
                    </div>

                    <div>
                        <div class="benchmark-label">
                            Memory
                        </div>
                        <div class="benchmark-value">
                            {memory:.2f} MB
                        </div>
                    </div>

                </div>

            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# MAIN OVERVIEW
# ============================================================

def render_overview():

    load_overview_styles()

    # ----------------------------------
    # LOAD API DATA
    # ----------------------------------

    try:

        with st.spinner(
            "Loading InsightDesk..."
        ):

            data = get_overview_data()

    except Exception as e:

        st.error(
            f"Unable to connect to InsightDesk API: {e}"
        )

        st.info(
            "Make sure FastAPI is running on "
            "http://127.0.0.1:8000"
        )

        return


    # ----------------------------------
    # HEADER
    # ----------------------------------

    render_header()


    # ----------------------------------
    # KPIs
    # ----------------------------------

    render_kpis(data)


    # ----------------------------------
    # VOLUME + SPIKES
    # ----------------------------------

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


    # ----------------------------------
    # TOPICS
    # ----------------------------------

    render_topics(
        data
    )


    # ----------------------------------
    # BENCHMARKS
    # ----------------------------------

    render_benchmarks(
        data
    )