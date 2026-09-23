import streamlit as st

from services.dashboard_api import (
    get_spikes,
    escalate_case,
)

from services.case_api import (
    get_case_intelligence,
)


def render_incident(case_id):

    try:
        data = get_case_intelligence(case_id)

    except Exception as e:
        st.error(f"Unable to load Case #{case_id}.")
        st.code(str(e))
        return

    case = data.get("case", {})
    topic = data.get("topic") or {}
    spike = data.get("spike") or {}
    rca = data.get("rca") or {}

    # --------------------------------------------------------
    # CASE
    # --------------------------------------------------------

    with st.container(border=True):

        st.markdown(
            f"### Case #{case_id}"
        )

        st.write(
            case.get(
                "representative_text",
                "No representative complaint available."
            )
        )

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric(
                "Reports",
                case.get("report_count", 0)
            )

        with c2:
            sentiment = case.get("sentiment")

            st.metric(
                "Sentiment",
                sentiment.title()
                if sentiment
                else "Unknown"
            )

        with c3:
            st.metric(
                "Department",
                topic.get(
                    "department",
                    "Unknown"
                )
            )

        with c4:

            if spike.get("critical"):
                status = "Critical"

            elif spike.get("spike"):
                status = "Spike"

            else:
                status = "Normal"

            st.metric(
                "Spike Status",
                status
            )

    # --------------------------------------------------------
    # SPIKE ANALYSIS
    # --------------------------------------------------------

    st.subheader("Spike Analysis")

    with st.container(border=True):

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

        s1, s2 = st.columns(2)

        with s1:
            st.metric(
                "Current Window Reports",
                spike.get(
                    "current",
                    0
                )
            )

        with s2:
            st.metric(
                "Z-Score",
                spike.get(
                    "z_score",
                    "N/A"
                )
            )

    # --------------------------------------------------------
    # RCA
    # --------------------------------------------------------

    st.subheader(
        "Root Cause Analysis"
    )

    if not rca:

        with st.container(border=True):

            st.info(
                "No RCA is currently available "
                "for this case."
            )

        return

    with st.container(border=True):

        st.markdown(
            "**Probable Cause**"
        )

        st.write(
            rca.get(
                "probable_cause",
                "Unavailable"
            )
        )

        r1, r2, r3 = st.columns(3)

        with r1:
            st.metric(
                "Severity",
                rca.get(
                    "severity",
                    "Unknown"
                )
            )

        with r2:

            confidence = rca.get(
                "confidence"
            )

            st.metric(
                "RCA Confidence",
                (
                    f"{confidence * 100:.0f}%"
                    if confidence is not None
                    else "N/A"
                )
            )

        with r3:
            st.metric(
                "Affected Segment",
                rca.get(
                    "affected_segment",
                    "Unknown"
                )
            )

        st.markdown(
            "**Recommended Action**"
        )

        st.write(
            rca.get(
                "recommended_action",
                "No recommendation available."
            )
        )

        evidence = rca.get(
            "source_evidence",
            []
        )

        if evidence:

            with st.expander(
                "View source evidence"
            ):

                for item in evidence:
                    st.write(f"• {item}")

        st.caption(
            f"Analysis model: "
            f"{rca.get('model_used', 'Unknown')}"
        )

    # --------------------------------------------------------
    # ESCALATION
    # --------------------------------------------------------

    st.subheader(
        "Email Escalation"
    )

    with st.container(border=True):

        severity = rca.get(
            "severity",
            "UNKNOWN"
        )

        st.write(
            "Send this incident and its RCA to "
            "the responsible department."
        )

        st.caption(
            f"Department: "
            f"{topic.get('department', 'Unknown')} "
            f"• Severity: {severity}"
        )

        if severity in (
            "MEDIUM",
            "HIGH",
            "CRITICAL",
        ):

            if st.button(
                "Escalate via Email",
                key=f"escalate_{case_id}",
                type="primary",
                use_container_width=True,
            ):

                try:

                    with st.spinner(
                        "Sending escalation..."
                    ):

                        result = escalate_case(
                            case_id
                        )

                    st.success(
                        result.get(
                            "message",
                            "Escalation sent successfully."
                        )
                    )

                except Exception as e:

                    st.error(
                        "Email escalation failed."
                    )

                    st.code(str(e))

        else:

            st.info(
                "This case is below the "
                "configured escalation threshold."
            )


def render_incident_response():

    st.title(
        "Incident Response"
    )

    st.caption(
        "Monitor complaint spikes, investigate root causes, "
        "and escalate significant incidents."
    )

    # --------------------------------------------------------
    # ACTIVE SPIKES
    # --------------------------------------------------------

    try:

        spikes = get_spikes()

    except Exception as e:

        st.error(
            "Unable to load spike information."
        )

        st.code(str(e))
        return

    critical_count = sum(
        1
        for spike in spikes
        if spike.get("critical")
    )

    c1, c2 = st.columns(2)

    with c1:
        st.metric(
            "Active Spikes",
            len(spikes)
        )

    with c2:
        st.metric(
            "Critical Spikes",
            critical_count
        )

    st.divider()

    # --------------------------------------------------------
    # CASE SELECTION
    # --------------------------------------------------------

    st.subheader(
        "Incident Investigation"
    )

    if not spikes:

        st.info(
            "No active complaint spikes are currently detected. "
            "You can still investigate a case manually."
        )

    manual_case_id = st.number_input(
        "Case ID",
        min_value=1,
        step=1,
        value=None,
        placeholder="Enter a case ID"
    )

    if manual_case_id is not None:

        st.divider()

        render_incident(
            int(manual_case_id)
        )