from dataclasses import dataclass
from datetime import datetime, timezone

from backend.database.supabase import (
    get_case,
    get_rca,
    get_topic,
    get_complaints_by_case,
    save_rca,
)

from backend.services.spike_detection import detect_spike

from backend.llm.provider import (
    generate_json,
    get_model_name,
    get_provider,
)


@dataclass
class RCAResult:
    probable_cause: str
    affected_segment: str
    recommended_action: str
    severity: str
    confidence: float
    source_evidence: list[str]
    generated_at: str
    model_used: str


def fetch_rca_context(case_id):
    case = get_case(case_id)

    if not case:
        raise ValueError(
            f"Case {case_id} not found."
        )

    complaints = get_complaints_by_case(
        case_id
    )

    spike = detect_spike(
        case_id
    )

    if case.get("topic_id") is not None:
        topic = get_topic(
            case["topic_id"]
        )
    else:
        topic = {
            "topic_name": "Unknown",
            "department": "General Support",
        }

    sentiment = {
        "sentiment": case.get(
            "sentiment"
        ),
        "confidence": case.get(
            "confidence_score"
        ),
        "model": case.get(
            "sentiment_model"
        ),
    }

    return {
        "case": case,
        "complaints": complaints[-5:],
        "topic": topic,
        "spike": spike,
        "sentiment": sentiment,
    }


def build_prompt(context):
    complaints = []

    for complaint in context["complaints"]:
        text = (
            complaint.get("text")
            or complaint.get("complaint_text")
            or complaint.get("content")
            or str(complaint)
        )

        complaints.append(
            f"- {text}"
        )

    complaint_text = "\n".join(
        complaints
    )

    return f"""
You are the Root Cause Analysis component of InsightDesk,
a SaaS complaint intelligence system.

Analyze ONLY the evidence supplied below.

Do not invent outages, infrastructure failures, geographic
effects, affected products, or technical causes that are not
supported by the evidence.

If the evidence is insufficient to identify an exact technical
cause, state the most plausible complaint-level cause and keep
confidence appropriately low.

Severity must be exactly one of:
LOW, MEDIUM, HIGH, CRITICAL

Confidence must be a NUMBER between 0.0 and 1.0.

Return ONLY one valid JSON object.
Do not use markdown.
Do not include reasoning outside the JSON.

Required schema:

{{
    "probable_cause": "string",
    "affected_segment": "string",
    "recommended_action": "string",
    "severity": "LOW|MEDIUM|HIGH|CRITICAL",
    "confidence": 0.0,
    "source_evidence": ["string"]
}}

CASE EVIDENCE

Representative Complaint:
{context["case"]["representative_text"]}

Topic:
{context["topic"]["topic_name"]}

Department:
{context["topic"]["department"]}

Sentiment:
{context["sentiment"]["sentiment"]}

Sentiment Confidence:
{context["sentiment"]["confidence"]}

Current Complaint Count:
{context["spike"]["current"]}

Z Score:
{context["spike"]["z_score"]}

Recent Complaints:
{complaint_text}
""".strip()


def _normalize_confidence(value):
    if isinstance(value, (int, float)):
        return max(
            0.0,
            min(1.0, float(value))
        )

    if isinstance(value, str):
        value = value.strip().lower()

        mapping = {
            "low": 0.35,
            "medium": 0.60,
            "moderate": 0.60,
            "high": 0.80,
            "very high": 0.90,
        }

        if value in mapping:
            return mapping[value]

        try:
            numeric = float(value)

            if numeric > 1:
                numeric = numeric / 100

            return max(
                0.0,
                min(1.0, numeric)
            )

        except ValueError:
            pass

    return 0.50


def _normalize_severity(value):
    severity = str(
        value or "MEDIUM"
    ).strip().upper()

    allowed = {
        "LOW",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }

    if severity not in allowed:
        return "MEDIUM"

    return severity


def _normalize_evidence(value):
    if value is None:
        return []

    if isinstance(value, list):
        return [
            str(item)
            for item in value
        ][:5]

    return [str(value)]


def generate_rca(
    case_id,
    force_regenerate=False,
    save_result=True,
):
    # Keep existing cache behaviour by default.
    existing = get_rca(
        case_id
    )

    if existing and not force_regenerate:
        return existing

    context = fetch_rca_context(
        case_id
    )

    prompt = build_prompt(
        context
    )

    data = generate_json(
        prompt
    )

    required_fields = [
        "probable_cause",
        "affected_segment",
        "recommended_action",
    ]

    missing = [
        field
        for field in required_fields
        if not data.get(field)
    ]

    if missing:
        raise RuntimeError(
            "LLM response missing required "
            f"fields: {missing}"
        )

    rca = RCAResult(
        probable_cause=str(
            data["probable_cause"]
        ),

        affected_segment=str(
            data["affected_segment"]
        ),

        recommended_action=str(
            data["recommended_action"]
        ),

        severity=_normalize_severity(
            data.get("severity")
        ),

        confidence=_normalize_confidence(
            data.get("confidence")
        ),

        source_evidence=_normalize_evidence(
            data.get("source_evidence")
        ),

        generated_at=datetime.now(
            timezone.utc
        ).isoformat(),

        model_used=(
            f"{get_provider()}:"
            f"{get_model_name()}"
        ),
    )
    if save_result:
            save_rca(
              case_id,
              rca
            )

    return rca


if __name__ == "__main__":
    print(
        "Provider:",
        get_provider()
    )

    print(
        "Model:",
        get_model_name()
    )

    result = generate_rca(
        71,
        force_regenerate=True,
    )

    print(result)