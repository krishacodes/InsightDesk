import streamlit as st

from utils.constants import DEMO_BENCHMARKS, DEMO_CLUSTERS, DEMO_POST_MORTEM
from utils.helper import badge


def render_clusters_card() -> None:
    rows = ""
    for cluster in DEMO_CLUSTERS:
        rows += f"""
        <div class="id-cluster-row">
            <span class="id-cluster-num">{cluster['rank']}</span>
            <span class="id-cluster-name">{cluster['name']}</span>
            <span class="id-cluster-count">{cluster['count']}</span>
            {badge(cluster['status'], cluster['status'])}
        </div>
        """

    st.markdown(
        f"""
        <div class="id-card">
            <div class="id-card-header">
                <span class="id-card-title">Clusters</span>
                <a class="id-card-link" href="#">view all →</a>
            </div>
            {rows}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_post_mortem() -> None:
    pm = DEMO_POST_MORTEM
    st.markdown(
        f"""
        <div class="id-card">
            <div class="id-card-header">
                <span class="id-card-title">
                    AI Post-Mortem
                    <span class="id-pm-tag">{badge(pm['tag'], 'critical')}</span>
                </span>
            </div>
            <div class="id-pm-block">
                <div class="id-pm-block-label">Root Cause</div>
                <p class="id-pm-block-text">{pm['root_cause']}</p>
            </div>
            <div class="id-pm-block">
                <div class="id-pm-block-label">Action</div>
                <p class="id-pm-block-text">{pm['action']}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([2, 1])
    with col1:
        st.button("Approve and send ↗", type="primary", use_container_width=True)
    with col2:
        st.button("Edit", type="secondary", use_container_width=True)


def render_benchmarks() -> None:
    b = DEMO_BENCHMARKS
    st.markdown(
        f"""
        <div class="id-card">
            <div class="id-card-header">
                <span class="id-card-title">Benchmarks</span>
            </div>
            <div class="id-bench-row">
                <span class="id-bench-key">Model</span>
                <span class="id-bench-val">{b['model']}</span>
            </div>
            <div class="id-bench-row">
                <span class="id-bench-key">Macro F1</span>
                <span class="id-bench-val">{b['macro_f1']}</span>
            </div>
            <div class="id-bench-row">
                <span class="id-bench-key">Latency</span>
                <span class="id-bench-val">{b['latency']}</span>
            </div>
            <div class="id-bench-row">
                <span class="id-bench-key">Throughput</span>
                <span class="id-bench-val">{b['throughput']}</span>
            </div>
            <div class="id-bench-row">
                <span class="id-bench-key">Z-score</span>
                <span class="id-bench-val danger">{b['z_score']}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
