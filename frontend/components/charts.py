import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.constants import ACCENT, ACCENT_LIGHT, RED, DEMO_COMPLAINT_CHART


def render_complaint_chart(data: dict = None) -> None:
    data = data or DEMO_COMPLAINT_CHART
    df = pd.DataFrame(data)
    colors = [ACCENT_LIGHT] * (len(df) - 1) + [RED]

    fig = go.Figure(
        go.Bar(
            x=df["Day"],
            y=df["Complaints"],
            marker_color=colors,
            marker_line_width=0,
        )
    )

    fig.update_layout(
        title=None,
        height=320,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, tickfont=dict(size=12, color="#5B6B82")),
        yaxis=dict(showgrid=True, gridcolor="#E2E8F0", tickfont=dict(size=11, color="#5B6B82")),
        bargap=0.35,
    )

    st.markdown(
        """
        <div class="id-card">
            <div class="id-card-header">
                <span class="id-card-title">Complaint Volume</span>
                <span class="id-card-subtitle">7-day window</span>
            </div>
        """,
        unsafe_allow_html=True,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)


def render_topic_distribution(data: dict) -> None:
    """Pie chart for topic distribution (used on cluster/sentiment pages)."""
    df = pd.DataFrame(data)

    fig = go.Figure(
        go.Pie(
            labels=df["Topic"],
            values=df["Cases"],
            hole=0.45,
            marker=dict(colors=[ACCENT, ACCENT_LIGHT, "#93C5FD", "#BFDBFE"]),
        )
    )
    fig.update_layout(
        height=320,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(orientation="h", y=-0.05),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_sentiment_chart(data: dict) -> None:
    df = pd.DataFrame(data)

    fig = go.Figure(
        go.Bar(
            x=df["Sentiment"],
            y=df["Count"],
            marker_color=[ACCENT_LIGHT, ACCENT, RED],
        )
    )
    fig.update_layout(
        height=320,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#E2E8F0"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_spike_timeline(data: dict) -> None:
    df = pd.DataFrame(data)

    fig = go.Figure(
        go.Scatter(
            x=df["Date"],
            y=df["Spike Count"],
            mode="lines+markers",
            line=dict(color=RED, width=2),
            marker=dict(size=8, color=RED),
        )
    )
    fig.update_layout(
        height=320,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#E2E8F0"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
