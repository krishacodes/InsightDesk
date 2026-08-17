import streamlit as st


def load_styles():

    st.markdown(
        """
        <style>

        .topbar{
            background:#101b3d;
            color:white;
            padding:20px;
            border-radius:12px;
            margin-bottom:20px;
        }

        .topbar h1{
            margin:0;
            font-size:28px;
        }

        .topbar p{
            color:#c7d3ff;
            margin-top:6px;
        }

        .topbar-stats{
            display:flex;
            gap:40px;
            margin-top:20px;
        }

        .topbar-stat .num{
            font-size:24px;
            font-weight:bold;
        }

        .topbar-stat .label{
            font-size:13px;
            color:#c7d3ff;
        }

        .pill{
            padding:4px 10px;
            border-radius:20px;
            font-size:12px;
            margin-left:8px;
        }

        .pill-live{
            background:#1ec58c;
            color:white;
        }

        .pill-spike{
            background:#e05050;
            color:white;
        }

        </style>
        """,
        unsafe_allow_html=True
    )