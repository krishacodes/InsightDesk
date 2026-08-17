from langchain_core.tools import tool

from backend.services.rca_service import generate_rca


@tool
def get_case_rca(case_id: int) -> dict:
    """
    Retrieve or generate Root Cause Analysis for an InsightDesk case.

    Use this when the user asks about:
    - root cause
    - RCA
    - probable cause
    - affected segment
    - recommended action
    - severity
    - confidence
    - evidence
    """

    try:
        rca = generate_rca(case_id)

        if not rca:
            return {
                "found": False,
                "case_id": case_id,
                "message": f"No RCA found for case {case_id}."
            }

        return {
            "found": True,
            "case_id": case_id,
            "rca": {
                "probable_cause": rca.get("probable_cause"),
                "affected_segment": rca.get("affected_segment"),
                "recommended_action": rca.get("recommended_action"),
                "severity": rca.get("severity"),
                "confidence": rca.get("confidence"),
                "source_evidence": rca.get("source_evidence", []),
                "generated_at": rca.get("generated_at"),
                "model_used": rca.get("model_used"),
            }
        }

    except Exception as e:
        return {
            "found": False,
            "case_id": case_id,
            "message": f"Unable to retrieve RCA for case {case_id}.",
            "error": str(e)
        }