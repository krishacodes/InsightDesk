import streamlit as st
import plotly.graph_objects as go


def render_sentiment_trend(trend_data):
    """
    Render 7-day sentiment trend graph.

    Parameters
    ----------
    trend_data : list[dict]

        [
            {
                "day":"Mon",
                "positive":40,
                "neutral":90,
                "negative":50
            },

            ....

        ]
    """

    days = [day["day"] for day in trend_data]

    fig = go.Figure()

    # ----------------------------------
    # POSITIVE
    # ----------------------------------

    fig.add_trace(

        go.Scatter(

            x=days,

            y=[day["positive"] for day in trend_data],

            name="Positive",

            stackgroup="one",

            mode="none",

            fillcolor="rgba(30,197,140,0.55)"

        )

    )


    # ----------------------------------
    # NEUTRAL
    # ----------------------------------

    fig.add_trace(

        go.Scatter(

            x=days,

            y=[day["neutral"] for day in trend_data],

            name="Neutral",

            stackgroup="one",

            mode="none",

            fillcolor="rgba(150,150,170,0.45)"

        )

    )


    # ----------------------------------
    # NEGATIVE
    # ----------------------------------

    fig.add_trace(

        go.Scatter(

            x=days,

            y=[day["negative"] for day in trend_data],

            name="Negative",

            stackgroup="one",

            mode="none",

            fillcolor="rgba(224,80,80,0.60)"

        )

    )


    # ----------------------------------
    # LAYOUT
    # ----------------------------------

    fig.update_layout(

        title="Sentiment Trend · Last 7 Days",

        height=280,

        margin=dict(

            l=0,
            r=0,
            t=40,
            b=0

        ),

        plot_bgcolor="rgba(0,0,0,0)",

        paper_bgcolor="rgba(0,0,0,0)",

        legend=dict(

            orientation="h",

            yanchor="bottom",

            y=1.02,

            xanchor="left",

            x=0

        )

    )


    st.plotly_chart(

        fig,

        use_container_width=True,

        config={

            "displayModeBar": False

        }

    )