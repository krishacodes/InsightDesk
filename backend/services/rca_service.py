import sys
import os
from xmlrpc import client
from groq import Groq
from dotenv import load_dotenv
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../.."
        )
    )
)
load_dotenv()

client = Groq(
    api_key=os.getenv(
        "GROQ_API_KEY"
    )
)
from dataclasses import dataclass
from datetime import datetime
from backend.database.supabase import (
    get_case,
    get_rca,
    get_topic,
    get_complaints_by_case,
    save_rca
)


from backend.services.spike_detection import (
    detect_spike
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
if __name__ == "__main__":

    result = RCAResult(

        probable_cause="ISP outage",

        affected_segment="Mumbai",

        recommended_action="Escalate",

        severity="CRITICAL",

        confidence=0.95,

        source_evidence=[
            "Internet down"
        ],

        generated_at=
        datetime.utcnow().isoformat(),

        model_used=
        "llama-3"
    )

    print(result)
def fetch_rca_context(
    case_id
):

    case = get_case(
        case_id
    )

    complaints = get_complaints_by_case(
        case_id
    )

    spike = detect_spike(
        case_id
    )
    if case["topic_id"] is not None:

        topic = get_topic(
            case["topic_id"]
        )

    else:

        topic = {
            "topic_name":
            "Unknown",

            "department":
            "General Support"
        }

    sentiment = {

        "sentiment":
        case["sentiment"],

        "confidence":
        case["confidence_score"],

        "model":
        case["sentiment_model"]
    }

    return {

        "case":
        case,

        "complaints":
        complaints[-5:],

        "topic":
        topic,

        "spike":
        spike,

        "sentiment":
        sentiment
    }



def build_prompt(
    context
):
    prompt = f"""
You are an AI Root Cause Analysis assistant for InsightDesk.

Your task is to identify the most probable root cause of a complaint spike.

Example 1:

INPUT:

Topic:
Payments

Sentiment:
negative

Current Complaint Count:
25

Z Score:
4.8

Recent Complaints:
- Payment failed during checkout.
- Money debited but order not placed.
- Unable to complete transaction.

OUTPUT:

{{
    "probable_cause":"Payment gateway outage.",
    "affected_segment":"Customers performing online transactions.",
    "recommended_action":"Escalate immediately to Payments Team.",
    "severity":"CRITICAL",
    "confidence":0.96,
    "source_evidence":[
        "Payment failed",
        "Money debited",
        "Transaction unsuccessful"
    ]
}}

------------------------------------------------

Example 2:

INPUT:

Topic:
Authentication

Sentiment:
negative

Current Complaint Count:
12

Z Score:
3.1

Recent Complaints:
- Unable to login.
- OTP not received.
- Login page keeps refreshing.

OUTPUT:

{{
    "probable_cause":"Authentication service degradation.",
    "affected_segment":"Users attempting to login.",
    "recommended_action":"Investigate OTP and authentication services.",
    "severity":"HIGH",
    "confidence":0.91,
    "source_evidence":[
        "Unable to login",
        "OTP not received"
    ]
}}

------------------------------------------------

NOW ANALYZE THE FOLLOWING CASE:

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
{context["complaints"]}

Return ONLY valid JSON.

{{
    "probable_cause":"",
    "affected_segment":"",
    "recommended_action":"",
    "severity":"",
    "confidence":0.0,
    "source_evidence":[]
}}
"""
    return prompt
if __name__ == "__main__":
    context = fetch_rca_context(
    71
    )

    print(
    build_prompt(
        context
    )
    )
def generate_rca(
    case_id
):

    context = fetch_rca_context(
        case_id
    )

    prompt = build_prompt(
        context
    )

    response = client.chat.completions.create(
        ...
    )

    content = (
        response
        .choices[0]
        .message
        .content
    )

    content = (
        content
        .replace(
            "```json",
            ""
        )
        .replace(
            "```",
            ""
        )
        .strip()
    )
    import json
    data = json.loads(
        content
    )

    return RCAResult(
        probable_cause=
        data["probable_cause"],

        affected_segment=
        data["affected_segment"],

        recommended_action=
        data["recommended_action"],

        severity=
        data["severity"],

        confidence=
        data["confidence"],

        source_evidence=
        data["source_evidence"],

        generated_at=
        datetime.utcnow().isoformat(),

        model_used=
        "llama-3"

    )
def generate_rca(
    case_id
):

    existing = get_rca(
        case_id
    )

    if existing:

        return existing

    context = fetch_rca_context(
        case_id
    )

    prompt = build_prompt(
        context
    )

    response = client.chat.completions.create(

        model=
        "llama-3.3-70b-versatile",

        messages=[
            {
                "role":
                "user",

                "content":
                prompt
            }
        ],

        temperature=0
    )

    content = (
    response
    .choices[0]
    .message
    .content
    )

    content = (
    content
    .replace(
        "```json",
        ""
    )
    .replace(
        "```",
        ""
    )
    .strip()
    )
    print("RAW RESPONSE:")
    print(content)
    print("-" * 50)

    import json
    data = json.loads(
    content
    )

    rca = RCAResult(

    probable_cause=
    data["probable_cause"],

    affected_segment=
    data["affected_segment"],

    recommended_action=
    data["recommended_action"],

    severity=
    data["severity"],

    confidence=
    data["confidence"],

    source_evidence=
    data["source_evidence"],

    generated_at=
    datetime.utcnow().isoformat(),

    model_used=
    "llama-3.3-70b-versatile"
)

    save_rca(
    case_id,
    rca
)

    return rca
if __name__ == "__main__":

    result = generate_rca(
        71
    )

    print(
        result
    )

    print(
        get_rca(
            71
        )
    )