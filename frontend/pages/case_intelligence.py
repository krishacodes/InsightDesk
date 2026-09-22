import streamlit as st

from services.case_api import (
    get_cases,
    get_case_intelligence,
)


# ============================================================
# SMALL SAFE COSMETIC STYLING
# ============================================================

def _load_case_styles():

    st.markdown(
        """
        <style>

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 10px;
        }

        div[data-testid="stMetricLabel"] {
            font-size: 0.8rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.02em;
            opacity: 0.75;
        }

        div[data-testid="stMetricValue"] {
            font-weight: 700;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# HELPERS
# ============================================================

def format_confidence(value):

    if value is None:
        return "N/A"

    return f"{value * 100:.1f}%"


# ============================================================
# CASE SUMMARY
# ============================================================

def render_case_summary(case):

    case_id = case.get("case_id")

    st.subheader(
        f"Case #{case_id}"
    )

    with st.container(border=True):

        st.markdown(
            f"### {case.get('representative_text', 'No representative text')}"
        )

        st.caption(
            "Representative complaint for this deduplicated case."
        )

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric(
                "Reports",
                case.get("report_count", 0)
            )

        with c2:
            st.metric(
                "Users",
                len(case.get("user_ids") or [])
            )

        with c3:
            sentiment = case.get(
                "sentiment"
            )

            st.metric(
                "Sentiment",
                sentiment.title()
                if sentiment
                else "Unknown"
            )

        with c4:
            st.metric(
                "Confidence",
                format_confidence(
                    case.get(
                        "confidence_score"
                    )
                )
            )


# ============================================================
# CLASSIFICATION
# ============================================================

def render_classification(case, topic):

    st.subheader(
        "Classification"
    )

    with st.container(border=True):

        c1, c2 = st.columns(2)

        with c1:

            st.markdown(
                "**Topic**"
            )

            st.write(
                topic.get(
                    "topic_name",
                    "Unassigned"
                )
                if topic
                else "Unassigned"
            )

            st.markdown(
                "**Department**"
            )

            st.write(
                topic.get(
                    "department",
                    "Unassigned"
                )
                if topic
                else "Unassigned"
            )

        with c2:

            st.markdown(
                "**Sentiment Model**"
            )

            model = case.get(
                "sentiment_model"
            )

            st.write(
                model.upper()
                if model
                else "N/A"
            )

            st.markdown(
                "**Last Reported**"
            )

            st.write(
                case.get(
                    "last_reported_at",
                    "N/A"
                )
            )

    if topic and topic.get("description"):

        st.caption(
            topic["description"]
        )


# ============================================================
# SPIKE ANALYSIS
# ============================================================

def render_spike(spike):

    st.subheader(
        "Spike Analysis"
    )

    with st.container(border=True):

        if not spike:

            st.info(
                "Spike information unavailable."
            )

            return

        if spike.get("critical"):

            st.error(
                "Critical complaint spike detected."
            )

        elif spike.get("spike"):

            st.warning(
                "Complaint spike detected."
            )

        else:

            st.success(
                "No active complaint spike detected."
            )

        c1, c2 = st.columns(2)

        with c1:

            st.metric(
                "Current Window Reports",
                spike.get(
                    "current",
                    0
                )
            )

        with c2:

            st.metric(
                "Z-Score",
                spike.get(
                    "z_score",
                    "N/A"
                )
            )


# ============================================================
# COMPLAINT EVIDENCE
# ============================================================

def render_complaints(complaints):

    st.subheader(
        "Linked Complaint Evidence"
    )

    st.caption(
        "Individual complaints grouped into this case by the deduplication pipeline."
    )

    if not complaints:

        st.info(
            "No linked complaints found."
        )

        return

    for complaint in complaints:

        complaint_id = complaint.get(
            "complaint_id"
        )

        duplicate = complaint.get(
            "is_duplicate",
            False
        )

        label = (
            "Duplicate"
            if duplicate
            else "Original"
        )

        with st.container(border=True):

            top_left, top_right = st.columns(
                [4, 1]
            )

            with top_left:

                st.markdown(
                    f"**Complaint #{complaint_id}**"
                )

            with top_right:

                if duplicate:
                    st.warning(label)
                else:
                    st.success(label)

            st.write(
                complaint.get(
                    "complaint_text",
                    ""
                )
            )

            st.caption(
                f"User: {complaint.get('user_id', 'Unknown')}  |  "
                f"Created: {complaint.get('created_at', 'N/A')}"
            )


# ============================================================
# RCA
# ============================================================

def render_rca(rca):

    st.subheader(
        "Root Cause Analysis"
    )

    if not rca:

        with st.container(border=True):

            st.info(
                "No RCA has been generated for this case."
            )

        return

    with st.container(border=True):

        c1, c2 = st.columns(2)

        with c1:

            st.markdown(
                "**Probable Cause**"
            )

            st.write(
                rca.get(
                    "probable_cause",
                    "N/A"
                )
            )

            st.markdown(
                "**Affected Segment**"
            )

            st.write(
                rca.get(
                    "affected_segment",
                    "N/A"
                )
            )

        with c2:

            st.markdown(
                "**Severity**"
            )

            severity = rca.get(
                "severity",
                "N/A"
            )

            if severity == "HIGH":
                st.error(severity)

            elif severity == "MEDIUM":
                st.warning(severity)

            else:
                st.info(severity)

            st.markdown(
                "**Confidence**"
            )

            st.write(
                format_confidence(
                    rca.get(
                        "confidence"
                    )
                )
            )

        st.markdown(
            "**Recommended Action**"
        )

        st.write(
            rca.get(
                "recommended_action",
                "N/A"
            )
        )

        evidence = rca.get(
            "source_evidence",
            []
        )

        if evidence:

            with st.expander(
                "View RCA source evidence"
            ):

                for item in evidence:

                    st.write(
                        f"• {item}"
                    )

        st.caption(
            f"Analysis model: "
            f"{rca.get('model_used', 'Unknown')}"
        )


# ============================================================
# MAIN PAGE
# ============================================================

def render_case_intelligence():

    _load_case_styles()

    st.title(
        "Case Intelligence"
    )

    st.caption(
        "Investigate consolidated complaint cases, evidence, "
        "classification, spike activity, and root cause analysis."
    )

    # ----------------------------------
    # LOAD CASE LIST
    # ----------------------------------

    try:

        cases = get_cases()

    except Exception as e:

        st.error(
            "Unable to load cases."
        )

        st.code(
            str(e)
        )

        return

    if not cases:

        st.info(
            "No cases available."
        )

        return

    # ----------------------------------
    # CASE SELECTOR
    # ----------------------------------

    case_map = {
        case["case_id"]:
            case.get(
                "representative_text",
                ""
            )
        for case in cases
    }

    case_ids = sorted(
        case_map.keys(),
        reverse=True,
    )

    selected_case = st.selectbox(
        "Select a case",
        case_ids,
        format_func=lambda case_id:
            f"Case #{case_id} — "
            f"{case_map[case_id][:70]}"
    )

    # ----------------------------------
    # LOAD INTELLIGENCE
    # ----------------------------------

    try:

        with st.spinner(
            "Loading case intelligence..."
        ):

            data = get_case_intelligence(
                selected_case
            )

    except Exception as e:

        st.error(
            "Unable to load case intelligence."
        )

        st.code(
            str(e)
        )

        return

    case = data.get(
        "case",
        {}
    )

    topic = data.get(
        "topic"
    )

    complaints = data.get(
        "complaints",
        []
    )

    spike = data.get(
        "spike"
    )

    rca = data.get(
        "rca"
    )

    # ----------------------------------
    # PAGE CONTENT
    # ----------------------------------

    render_case_summary(
        case
    )

    st.divider()

    left, right = st.columns(
        [2, 1]
    )

    with left:

        render_classification(
            case,
            topic
        )

    with right:

        render_spike(
            spike
        )

    st.divider()

    render_complaints(
        complaints
    )

    st.divider()

    render_rca(
        rca
    )