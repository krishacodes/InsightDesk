"""Global CSS theme for InsightDesk."""

from utils.constants import (
    ACCENT,
    BORDER,
    GREEN,
    NAVY,
    NAVY_MID,
    OFFWHITE,
    ORANGE,
    RED_SOFT,
    RED_TEXT,
    SIDEBAR_BG,
    TEXT_DARK,
    TEXT_MUTED,
    WHITE,
)


def inject_theme() -> None:
    import streamlit as st

    st.markdown(
        f"""
        <style>
            /* Hide default Streamlit chrome */
            header[data-testid="stHeader"] {{
                display: none;
            }}
            #MainMenu, footer {{
                visibility: hidden;
            }}
            .stApp {{
                background-color: {OFFWHITE};
            }}
            .block-container {{
                padding-top: 4.5rem;
                padding-bottom: 2rem;
                max-width: 1400px;
            }}
            html, body, [class*="css"] {{
                font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
            }}

            /* Fixed top header bar */
            .id-topbar {{
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                z-index: 999;
                height: 56px;
                background: linear-gradient(90deg, {NAVY} 0%, {NAVY_MID} 100%);
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 0 1.5rem;
                box-shadow: 0 2px 8px rgba(11, 31, 58, 0.25);
            }}
            .id-topbar-brand {{
                display: flex;
                flex-direction: column;
                min-width: 200px;
            }}
            .id-topbar-brand strong {{
                color: {WHITE};
                font-size: 1.05rem;
                font-weight: 700;
                letter-spacing: -0.01em;
            }}
            .id-topbar-brand span {{
                color: rgba(255,255,255,0.65);
                font-size: 0.72rem;
                margin-top: 1px;
            }}
            .id-topbar-stats {{
                display: flex;
                gap: 1.75rem;
                align-items: center;
                flex-wrap: wrap;
            }}
            .id-topbar-stat {{
                color: rgba(255,255,255,0.9);
                font-size: 0.82rem;
                white-space: nowrap;
            }}
            .id-topbar-stat b {{
                font-weight: 700;
                color: {WHITE};
            }}
            .id-topbar-stat.danger b {{
                color: #FCA5A5;
            }}
            .id-topbar-pills {{
                display: flex;
                gap: 0.6rem;
                align-items: center;
            }}
            .id-pill {{
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                padding: 0.25rem 0.7rem;
                border-radius: 999px;
                font-size: 0.72rem;
                font-weight: 600;
            }}
            .id-pill-live {{
                background: rgba(6, 118, 71, 0.25);
                color: #6EE7A0;
                border: 1px solid rgba(110, 231, 160, 0.35);
            }}
            .id-pill-live::before {{
                content: '';
                width: 6px;
                height: 6px;
                border-radius: 50%;
                background: #34D399;
            }}
            .id-pill-spike {{
                background: rgba(220, 38, 38, 0.35);
                color: {WHITE};
                border: 1px solid rgba(252, 165, 165, 0.4);
            }}

            /* Sidebar */
            section[data-testid="stSidebar"] {{
                background-color: {SIDEBAR_BG};
                border-right: 1px solid {BORDER};
                margin-top: 56px;
                padding-top: 0.5rem;
            }}
            section[data-testid="stSidebar"] > div {{
                padding-top: 0.5rem;
            }}
            section[data-testid="stSidebar"] [data-testid="stMarkdown"] p,
            section[data-testid="stSidebar"] label {{
                color: {TEXT_MUTED} !important;
            }}
            .id-nav-section {{
                font-size: 0.65rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                color: {TEXT_MUTED};
                margin: 1rem 0 0.4rem 0;
                text-transform: uppercase;
            }}
            div[data-testid="stSidebar"] .stButton > button {{
                background: transparent;
                color: {TEXT_DARK};
                border: none;
                box-shadow: none;
                text-align: left;
                justify-content: flex-start;
                font-size: 0.88rem;
                padding: 0.45rem 0.75rem;
                border-radius: 8px;
            }}
            div[data-testid="stSidebar"] .stButton > button:hover {{
                background: rgba(59, 130, 246, 0.08);
                color: {ACCENT};
            }}
            div[data-testid="stSidebar"] .stButton > button[kind="primary"] {{
                background: rgba(59, 130, 246, 0.15) !important;
                color: {ACCENT} !important;
                font-weight: 600;
            }}

            /* Page header row */
            .id-page-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 1.25rem;
            }}
            .id-page-title {{
                font-size: 1.65rem;
                font-weight: 700;
                color: {TEXT_DARK};
                margin: 0;
                line-height: 1.2;
            }}
            .id-page-subtitle {{
                font-size: 0.85rem;
                color: {TEXT_MUTED};
                margin-top: 0.15rem;
            }}

            /* KPI grid */
            .id-kpi-grid {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 1rem;
                margin-bottom: 1.25rem;
            }}
            @media (max-width: 900px) {{
                .id-kpi-grid {{
                    grid-template-columns: repeat(2, 1fr);
                }}
            }}
            .id-kpi-card {{
                background: {WHITE};
                border: 1px solid {BORDER};
                border-radius: 10px;
                padding: 1.1rem 1.25rem;
                box-shadow: 0 1px 2px rgba(11,31,58,0.04);
            }}
            .id-kpi-label {{
                font-size: 0.72rem;
                font-weight: 600;
                color: {TEXT_MUTED};
                text-transform: uppercase;
                letter-spacing: 0.04em;
                margin-bottom: 0.35rem;
            }}
            .id-kpi-value {{
                font-size: 1.75rem;
                font-weight: 700;
                color: {TEXT_DARK};
                line-height: 1.1;
            }}
            .id-kpi-meta {{
                font-size: 0.78rem;
                font-weight: 600;
                margin-top: 0.35rem;
            }}
            .id-kpi-meta-warn {{ color: {ORANGE}; }}
            .id-kpi-meta-danger {{ color: {RED_TEXT}; }}
            .id-kpi-meta-info {{ color: {ACCENT}; }}

            /* Cards */
            .id-card {{
                background: {WHITE};
                border: 1px solid {BORDER};
                border-radius: 10px;
                padding: 1.1rem 1.25rem;
                box-shadow: 0 1px 2px rgba(11,31,58,0.04);
                height: 100%;
                box-sizing: border-box;
            }}
            .id-card-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 1rem;
            }}
            .id-card-title {{
                font-size: 0.72rem;
                font-weight: 700;
                letter-spacing: 0.06em;
                color: {TEXT_MUTED};
                text-transform: uppercase;
            }}
            .id-card-link {{
                font-size: 0.78rem;
                color: {ACCENT};
                font-weight: 600;
                text-decoration: none;
            }}
            .id-card-subtitle {{
                font-size: 0.72rem;
                color: {TEXT_MUTED};
            }}

            /* Cluster rows */
            .id-cluster-row {{
                display: grid;
                grid-template-columns: 1.5rem 1fr auto auto;
                align-items: center;
                gap: 0.5rem;
                padding: 0.55rem 0;
                border-bottom: 1px solid {BORDER};
            }}
            .id-cluster-row:last-child {{
                border-bottom: none;
            }}
            .id-cluster-num {{
                color: {TEXT_MUTED};
                font-size: 0.82rem;
                font-weight: 600;
            }}
            .id-cluster-name {{
                color: {TEXT_DARK};
                font-size: 0.88rem;
                font-weight: 500;
            }}
            .id-cluster-count {{
                color: {TEXT_MUTED};
                font-size: 0.82rem;
                font-weight: 600;
            }}

            /* Badges */
            .id-badge {{
                display: inline-block;
                padding: 0.15rem 0.55rem;
                border-radius: 999px;
                font-size: 0.68rem;
                font-weight: 700;
                letter-spacing: 0.02em;
                text-transform: lowercase;
            }}
            .id-badge-spike {{
                background: {RED_SOFT};
                color: {RED_TEXT};
            }}
            .id-badge-stable {{
                background: #E7F6EC;
                color: {GREEN};
            }}
            .id-badge-critical {{
                background: {RED_SOFT};
                color: {RED_TEXT};
            }}

            /* Post-mortem blocks */
            .id-pm-tag {{
                margin-left: 0.5rem;
            }}
            .id-pm-block {{
                background: #EFF6FF;
                border-radius: 8px;
                padding: 0.85rem 1rem;
                margin-bottom: 0.75rem;
            }}
            .id-pm-block-label {{
                font-size: 0.68rem;
                font-weight: 700;
                letter-spacing: 0.06em;
                color: {TEXT_MUTED};
                text-transform: uppercase;
                margin-bottom: 0.35rem;
            }}
            .id-pm-block-text {{
                font-size: 0.88rem;
                color: {TEXT_DARK};
                line-height: 1.45;
                margin: 0;
            }}

            /* Benchmark rows */
            .id-bench-row {{
                display: flex;
                justify-content: space-between;
                padding: 0.5rem 0;
                border-bottom: 1px solid {BORDER};
                font-size: 0.88rem;
            }}
            .id-bench-row:last-child {{
                border-bottom: none;
            }}
            .id-bench-key {{
                color: {TEXT_MUTED};
            }}
            .id-bench-val {{
                color: {TEXT_DARK};
                font-weight: 600;
            }}
            .id-bench-val.danger {{
                color: {RED_TEXT};
            }}

            /* Sidebar spike alert */
            .id-sidebar-alert {{
                background: {RED_SOFT};
                border: 1px solid #FECACA;
                border-radius: 10px;
                padding: 0.85rem 1rem;
                margin-top: 1.5rem;
            }}
            .id-sidebar-alert-title {{
                color: {RED_TEXT};
                font-size: 0.82rem;
                font-weight: 700;
                margin-bottom: 0.2rem;
            }}
            .id-sidebar-alert-title::before {{
                content: '● ';
                font-size: 0.6rem;
            }}
            .id-sidebar-alert-headline {{
                color: {RED_TEXT};
                font-size: 0.78rem;
                font-weight: 600;
            }}
            .id-sidebar-alert-meta {{
                color: {TEXT_MUTED};
                font-size: 0.72rem;
                margin-top: 0.15rem;
            }}

            /* Two-column dashboard grid helpers */
            .id-row-2 {{
                display: grid;
                grid-template-columns: 1.6fr 1fr;
                gap: 1rem;
                margin-bottom: 1rem;
            }}
            @media (max-width: 900px) {{
                .id-row-2 {{
                    grid-template-columns: 1fr;
                }}
            }}

            /* Streamlit button overrides for primary actions */
            div[data-testid="stButton"] > button[kind="primary"] {{
                background: {NAVY};
                color: {WHITE};
                border: none;
                border-radius: 8px;
                font-weight: 600;
            }}
            div[data-testid="stButton"] > button[kind="secondary"] {{
                background: {WHITE};
                color: {TEXT_DARK};
                border: 1px solid {BORDER};
                border-radius: 8px;
                font-weight: 600;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
