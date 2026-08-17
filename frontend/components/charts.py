import streamlit as st
import plotly.graph_objects as go


def render_complaint_graph(volume_by_day):

    fig = go.Figure(

        go.Bar(

            x=[day["day"] for day in volume_by_day],

            y=[day["count"] for day in volume_by_day],

            marker_color=[
                "#e05050"
                if day["spike"]
                else "#a9c1f2"

                for day in volume_by_day
            ],

            hovertemplate="%{y} complaints<extra></extra>",

        )
    )

    fig.update_layout(

        height=220,

        margin=dict(
            l=0,
            r=0,
            t=10,
            b=0
        ),

        plot_bgcolor="rgba(0,0,0,0)",

        paper_bgcolor="rgba(0,0,0,0)",

        yaxis=dict(

            showgrid=False,
            visible=False

        ),

        xaxis=dict(
            showgrid=False
        ),

        bargap=0.35

    )

    st.plotly_chart(

        fig,

        use_container_width=True,

        config={
            "displayModeBar":False
        }

    )