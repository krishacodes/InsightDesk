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
    get_benchmarks
)

# ----------------------------------
# Page Configuration
# ----------------------------------

st.set_page_config(
    page_title="InsightDesk Dashboard",
    layout="wide"
)

st.title("InsightDesk Dashboard")

st.subheader(
    "AI Complaint Intelligence Platform"
)

# ----------------------------------
# Load Benchmark Data
# ----------------------------------

benchmarks = get_benchmarks()

if not benchmarks:

    st.warning(
        "No benchmark data found."
    )

    st.stop()

benchmark = benchmarks[0]

# ==================================
# PROJECT METRICS
# ==================================

st.markdown("---")

st.header(
    "Project Metrics"
)

col1, col2, col3 = st.columns(3)

with col1:

    st.metric(
        "Complaints",
        1758
    )

with col2:

    st.metric(
        "Cases",
        66
    )

with col3:

    st.metric(
        "Topics",
        12
    )

# ==================================
# MODEL BENCHMARKS
# ==================================

st.markdown("---")

st.header(
    "RoBERTa Benchmark"
)

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "Model",
        benchmark["model_name"]
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
# RAW DATA
# ==================================

st.markdown("---")

with st.expander(
    "View Raw Benchmark Data"
):

    st.write(
        benchmark
    )