import streamlit as st
import plotly.graph_objects as go


def render_emotion_distribution(emotions):
    """
    Render emotion distribution pie chart.

    Parameters
    ----------
    emotions : dict

        {

            "anger":43,
            "sadness":18,
            "fear":11,
            "joy":15,
            "neutral":9,
            "surprise":4

        }

    """

    labels = list(emotions.keys())

    values = list(emotions.values())


    fig = go.Figure(

        go.Pie(

            labels=labels,

            values=values,

            hole=0.45,

            textinfo="label+percent",

            hovertemplate=(
                "%{label}<br>"
                "%{value}% complaints"
                "<extra></extra>"
            )

        )

    )


    fig.update_layout(

        title="Emotion Distribution",

        height=320,

        margin=dict(

            l=0,
            r=0,
            t=40,
            b=0

        ),

        paper_bgcolor="rgba(0,0,0,0)",

        showlegend=True

    )


    st.plotly_chart(

        fig,

        use_container_width=True,

        config={

            "displayModeBar": False

        }

    )