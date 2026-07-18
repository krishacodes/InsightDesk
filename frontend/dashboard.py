import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            ".."
        )
    )
)

import streamlit as st

from backend.database.supabase import (
    get_benchmarks,
    get_complaints,
    get_topics,
    get_cases
)
from backend.services.spike_detection import (
    detect_spike
)

# ----------------------------------
# Page Configuration
# ----------------------------------

st.set_page_config(
    page_title="InsightDesk Dashboard",
    layout="wide"
)

st.title(
    "InsightDesk V2"
)

st.subheader(
    "AI Complaint Intelligence Platform"
)

# ==================================
# PROJECT METRICS
# ==================================

st.markdown("---")

st.header(
    "Project Metrics"
)

cases = get_cases()

complaints = get_complaints()

topics = get_topics()

col1, col2, col3 = st.columns(3)

with col1:

    st.metric(
        "Complaints",
        len(complaints)
    )

with col2:

    st.metric(
        "Cases",
        len(cases)
    )

with col3:

    st.metric(
        "Topics",
        len(topics)
    )

# ==================================
# BERTopic
# ==================================

st.markdown("---")

st.header(
    "BERTopic"
)

if topics:

    for topic in topics:

        st.write(
            topic
        )

else:

    st.warning(
        "No topics found."
    )

# ==================================
# MODEL BENCHMARKS
# ==================================

st.markdown("---")

st.header(
    "Model Benchmarks"
)

benchmarks = get_benchmarks()

if not benchmarks:

    st.warning(
        "No benchmark data found."
    )

else:

    for benchmark in benchmarks:

        st.subheader(
            benchmark["model_name"]
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:

            st.metric(
                "Complaints",
                benchmark[
                    "complaints_processed"
                ]
            )

        with col2:

            st.metric(
                "Latency",
                f"{benchmark['average_latency_ms']:.2f} ms"
            )

        with col3:

            st.metric(
                "Throughput",
                f"{benchmark['throughput']:.2f}/sec"
            )

        with col4:

            st.metric(
                "Memory",
                f"{benchmark['memory_mb']:.2f} MB"
            )

# ==================================
# SPIKE ALERTS
# ==================================

st.markdown("---")

st.header(
    "Spike Alerts"
)

spike_found = False

for case in cases:

    result = detect_spike(
        case["case_id"]
    )

    if result["critical"]:

        spike_found = True

        st.error(
            f"""
            CRITICAL SPIKE

            Case ID: {case['case_id']}

            Current Reports: {result['current']}

            Z Score: {result['z_score']}
            """
        )

    elif result["spike"]:

        spike_found = True

        st.warning(
            f"""
            Spike Detected

            Case:{case['representative_text']}

            Current Reports: {result['current']}

            Z Score: {result['z_score']}
            """
        )

if not spike_found:

    st.success(
        "No spikes detected."
    )

# ==================================
# ROOT CAUSE ANALYSIS
# ==================================

st.markdown("---")

st.header(
    "Root Cause Analysis"
)

st.info(
    "RCA module coming soon."
)

# ==================================
# EMAIL ESCALATION
# ==================================

st.markdown("---")

st.header(
    "Email Escalation"
)

st.info(
    "Email escalation module coming soon."
)

# ==================================
# RAW BENCHMARK DATA
# ==================================

st.markdown("---")

with st.expander(
    "View Raw Benchmark Data"
):

    st.write(
        benchmarks
    )