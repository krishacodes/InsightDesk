import sys
import os
from datetime import datetime

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            ".."
        )
    )
)

import streamlit as st
from backend.services.rca_service import (
    generate_rca
)
from backend.database.supabase import (
    get_benchmarks,
    get_complaints,
    get_rca,
    get_topics,
    get_cases,
    get_spike_cases
)
from backend.services.spike_detection import (
    detect_spike
)

# ==================================
# PAGE CONFIGURATION
# ==================================

st.set_page_config(
    page_title="InsightDesk V2",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==================================
# THEME — NAVY / WHITE
# ==================================
# All backend calls below are unchanged from the original implementation.
# Only presentation (CSS, layout, structure, caching, error handling) has
# been refined.

NAVY = "#0B1F3A"
NAVY_DEEP = "#081729"
NAVY_MID = "#13315C"
ACCENT = "#3B82F6"
WHITE = "#FFFFFF"
OFFWHITE = "#F5F7FA"
BORDER = "#E2E8F0"
TEXT_MUTED = "#5B6B82"

st.markdown(
    f"""
    <style>
        /* ---------- Base ---------- */
        .stApp {{
            background-color: {OFFWHITE};
        }}
        .block-container {{
            padding-top: 1.5rem;
            padding-bottom: 3rem;
            max-width: 1200px;
        }}
        html, body, [class*="css"] {{
            font-family: 'Segoe UI', 'Inter', system-ui, sans-serif;
        }}

        /* ---------- Sidebar ---------- */
        section[data-testid="stSidebar"] {{
            background-color: {NAVY};
        }}
        section[data-testid="stSidebar"] * {{
            color: {WHITE} !important;
        }}
        section[data-testid="stSidebar"] hr {{
            border-color: rgba(255,255,255,0.15);
        }}

        /* ---------- Header banner ---------- */
        .id-header {{
            background: linear-gradient(135deg, {NAVY} 0%, {NAVY_MID} 100%);
            border-radius: 14px;
            padding: 2rem 2.25rem;
            margin-bottom: 1.75rem;
            box-shadow: 0 8px 24px rgba(11, 31, 58, 0.18);
        }}
        .id-header h1 {{
            color: {WHITE};
            font-size: 2rem;
            font-weight: 700;
            margin: 0;
            letter-spacing: -0.02em;
        }}
        .id-header p {{
            color: rgba(255,255,255,0.75);
            font-size: 1rem;
            margin-top: 0.35rem;
            margin-bottom: 0;
        }}

        /* ---------- Section headers ---------- */
        .id-section-title {{
            display: flex;
            align-items: center;
            gap: 0.6rem;
            margin: 2rem 0 1rem 0;
        }}
        .id-section-title .bar {{
            width: 5px;
            height: 22px;
            background: {ACCENT};
            border-radius: 3px;
        }}
        .id-section-title h2 {{
            color: {NAVY};
            font-size: 1.3rem;
            font-weight: 700;
            margin: 0;
        }}

        /* ---------- Cards ---------- */
        .id-card {{
            background: {WHITE};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1.25rem 1.4rem;
            box-shadow: 0 1px 3px rgba(11,31,58,0.05);
        }}

        /* ---------- Metrics ---------- */
        div[data-testid="stMetric"] {{
            background: {WHITE};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1rem 1.2rem;
            box-shadow: 0 1px 3px rgba(11,31,58,0.05);
        }}
        div[data-testid="stMetricLabel"] {{
            color: {TEXT_MUTED};
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.72rem;
            letter-spacing: 0.04em;
        }}
        div[data-testid="stMetricValue"] {{
            color: {NAVY};
            font-weight: 700;
        }}

        /* ---------- Badges ---------- */
        .id-badge {{
            display: inline-block;
            padding: 0.2rem 0.65rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.02em;
        }}
        .id-badge-critical {{ background: #FDE8E8; color: #B42318; }}
        .id-badge-warning  {{ background: #FEF3E2; color: #B54708; }}
        .id-badge-ok       {{ background: #E7F6EC; color: #067647; }}
        .id-badge-info     {{ background: #E9EEFB; color: {NAVY_MID}; }}

        /* ---------- Alert cards ---------- */
        .id-alert {{
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 0.75rem;
            border-left: 4px solid;
        }}
        .id-alert-critical {{
            background: #FEF2F2;
            border-color: #DC2626;
        }}
        .id-alert-warning {{
            background: #FFF9EE;
            border-color: #F59E0B;
        }}
        .id-alert-title {{
            font-weight: 700;
            font-size: 0.95rem;
            margin-bottom: 0.15rem;
        }}
        .id-alert-critical .id-alert-title {{ color: #B42318; }}
        .id-alert-warning .id-alert-title {{ color: #B54708; }}
        .id-alert-meta {{
            color: {TEXT_MUTED};
            font-size: 0.85rem;
        }}

        /* ---------- Footer ---------- */
        .id-footer {{
            text-align: center;
            color: {TEXT_MUTED};
            font-size: 0.8rem;
            padding-top: 2rem;
        }}

        /* ---------- Streamlit element cleanup ---------- */
        [data-testid="stExpander"] {{
            border: 1px solid {BORDER};
            border-radius: 10px;
            background: {WHITE};
        }}
        hr {{
            border-color: {BORDER};
        }}
    </style>
    """,
    unsafe_allow_html=True,
)


def section_title(label: str) -> None:
    st.markdown(
        f"""
        <div class="id-section-title">
            <div class="bar"></div>
            <h2>{label}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )


def badge(text: str, kind: str = "info") -> str:
    return f'<span class="id-badge id-badge-{kind}">{text}</span>'


# ==================================
# CACHED DATA ACCESSORS
# ==================================
# Thin caching wrappers around the unchanged backend calls, so the
# dashboard doesn't re-query Supabase on every widget interaction.

@st.cache_data(ttl=60, show_spinner=False)
def load_cases():
    return get_cases()


@st.cache_data(ttl=60, show_spinner=False)
def load_complaints():
    return get_complaints()


@st.cache_data(ttl=60, show_spinner=False)
def load_topics():
    return get_topics()


@st.cache_data(ttl=60, show_spinner=False)
def load_benchmarks():
    return get_benchmarks()


@st.cache_data(ttl=30, show_spinner=False)
def load_spike_cases():
    return get_spike_cases()


# ==================================
# SIDEBAR
# ==================================

with st.sidebar:
    st.markdown("### 🧭 InsightDesk V2")
    st.caption("AI Complaint Intelligence Platform")
    st.markdown("---")

    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"Last refreshed: {datetime.now().strftime('%b %d, %Y · %H:%M:%S')}")

# ==================================
# HEADER
# ==================================

st.markdown(
    """
    <div class="id-header">
        <h1>InsightDesk V2</h1>
        <p>AI Complaint Intelligence Platform</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ==================================
# PROJECT METRICS
# ==================================

section_title("Project Metrics")

try:
    with st.spinner("Loading project metrics..."):
        cases = load_cases()
        complaints = load_complaints()
        topics = load_topics()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Complaints", len(complaints))

    with col2:
        st.metric("Cases", len(cases))

    with col3:
        st.metric("Topics", len(topics))

except Exception as e:
    st.error(f"Unable to load project metrics: {e}")
    cases, complaints, topics = [], [], []

# ==================================
# BERTOPIC
# ==================================

section_title(
    "Complaint Intelligence"
)
if topics:

    topic_data = []

    for topic in topics:

        topic_data.append(

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
                )

            }

        )

    st.dataframe(

        topic_data,

        use_container_width=True

    )

else:

    st.warning(
        "No topics found."
    )

# ==================================
# MODEL BENCHMARKS
# ==================================

section_title("Model Benchmarks")

try:
    benchmarks = load_benchmarks()
except Exception as e:
    st.error(f"Unable to load benchmarks: {e}")
    benchmarks = []

if not benchmarks:
    st.warning("No benchmark data found.")
else:
    for benchmark in benchmarks:
        with st.container():
            st.markdown('<div class="id-card">', unsafe_allow_html=True)
            st.subheader(benchmark["model_name"])

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Complaints", benchmark["complaints_processed"])

            with col2:
                st.metric("Latency", f"{benchmark['average_latency_ms']:.2f} ms")

            with col3:
                st.metric("Throughput", f"{benchmark['throughput']:.2f}/sec")

            with col4:
                st.metric("Memory", f"{benchmark['memory_mb']:.2f} MB")

            st.markdown("</div>", unsafe_allow_html=True)
        st.write("")

# ==================================
# SPIKE ALERTS
# ==================================

section_title("Spike Alerts")

spike_found = False

try:
    with st.spinner("Scanning cases for spikes..."):
        for case in cases:
            result = detect_spike(case["case_id"])

            if result["critical"]:
                spike_found = True
                st.markdown(
                    f"""
                    <div class="id-alert id-alert-critical">
                        <div class="id-alert-title">🔴 Critical Spike {badge("CRITICAL", "critical")}</div>
                        <div class="id-alert-meta">
                            Case ID: <strong>{case['case_id']}</strong> &nbsp;·&nbsp;
                            Current Reports: <strong>{result['current']}</strong> &nbsp;·&nbsp;
                            Z-Score: <strong>{result['z_score']}</strong>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            elif result["spike"]:
                spike_found = True
                st.markdown(
                    f"""
                    <div class="id-alert id-alert-warning">
                        <div class="id-alert-title">🟠 Spike Detected {badge("WARNING", "warning")}</div>
                        <div class="id-alert-meta">
                            Case: <strong>{case['representative_text']}</strong> &nbsp;·&nbsp;
                            Current Reports: <strong>{result['current']}</strong> &nbsp;·&nbsp;
                            Z-Score: <strong>{result['z_score']}</strong>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    if not spike_found:
        st.success("No spikes detected.")

except Exception as e:
    st.error(f"Unable to run spike detection: {e}")

# ==================================
# ROOT CAUSE ANALYSIS
# ==================================

# ==================================
# ROOT CAUSE ANALYSIS
# ==================================

section_title(
    "AI Root Cause Analysis"
)

try:

    # --------------------------------
    # Demo Case ID
    # --------------------------------

    case_id = 71


    # --------------------------------
    # Fetch Existing RCA
    # --------------------------------

    rca = get_rca(
        case_id
    )


    # --------------------------------
    # Generate RCA if unavailable
    # --------------------------------

    if not rca:

        with st.spinner(
            "Generating root cause analysis..."
        ):

            generate_rca(
                case_id
            )

            rca = get_rca(
                case_id
            )


    # --------------------------------
    # Display RCA
    # --------------------------------

    if rca:

        st.markdown(
            '<div class="id-card">',
            unsafe_allow_html=True
        )

        severity = str(
            rca["severity"]
        )

        severity_kind = (

            "critical"

            if severity.lower()
            in ("high", "critical")

            else "warning"

            if severity.lower()
            == "medium"

            else "ok"

        )

        st.markdown(

            f"**Probable Cause** {badge(severity.upper(), severity_kind)}",

            unsafe_allow_html=True

        )

        st.write(
            rca["probable_cause"]
        )


        # --------------------------------
        # Severity & Confidence
        # --------------------------------

        col1, col2 = st.columns(2)

        with col1:

            st.metric(

                "Severity",

                rca["severity"]

            )

        with col2:

            st.metric(

                "Confidence",

                f'{rca["confidence"] * 100:.0f}%'

            )


        # --------------------------------
        # Generated At & Model Used
        # --------------------------------

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:

            st.metric(

                "Generated At",

                str(
                    rca["generated_at"]
                )[:19]

            )

        with col2:

            st.metric(

                "Model Used",

                rca["model_used"]

            )


        # --------------------------------
        # Recommended Action
        # --------------------------------

        st.markdown(
            "**Recommended Action**"
        )

        st.write(
            rca["recommended_action"]
        )


        # --------------------------------
        # Source Evidence
        # --------------------------------

        st.markdown(
            "**Source Evidence**"
        )

        for evidence in rca["source_evidence"]:

            st.markdown(
                f"- {evidence}"
            )


        st.markdown(
            "</div>",
            unsafe_allow_html=True
        )


        with st.expander(
            "View Full RCA Object"
        ):

            st.write(
                rca
            )


    else:

        st.info(
            "No Root Cause Analysis available."
        )


except Exception as e:

    st.error(

        f"Unable to generate RCA : {e}"

    )
# ==================================
# EMAIL ESCALATION
# ==================================

section_title("Email Escalation")

st.info("📧 Email escalation module coming soon.")

# ==================================
# FOOTER
# ==================================

st.markdown(
    """
    <div class="id-footer">
        InsightDesk V2 · AI Complaint Intelligence Platform
    </div>
    """,
    unsafe_allow_html=True,
)