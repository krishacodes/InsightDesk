import streamlit as st

from utils.helper import render_topbar


def render_header() -> None:
    st.markdown(render_topbar(), unsafe_allow_html=True)
