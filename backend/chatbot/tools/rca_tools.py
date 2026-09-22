from langchain_core.tools import tool

from backend.database.supabase import get_rca


@tool
def get_case_rca(case_id: int) -> dict:
    """
    Retrieve the existing Root Cause Analysis for an InsightDesk case.

    Use this when the user asks about:
    - root cause
    - RCA
    - probable cause
    - affected segment
    - recommended action
    - severity
    - confidence
    - evidence

    This tool is read-only.
    It does not generate or modify RCA data.
    """

    try:
        rca = get_rca(case_id)

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
                "probable_cause": rca.get(
                    "probable_cause"
                ),
                "affected_segment": rca.get(
                    "affected_segment"
                ),
                "recommended_action": rca.get(
                    "recommended_action"
                ),
                "severity": rca.get(
                    "severity"
                ),
                "confidence": rca.get(
                    "confidence"
                ),
                "source_evidence": rca.get(
                    "source_evidence",
                    []
                ),
                "generated_at": rca.get(
                    "generated_at"
                ),
                "model_used": rca.get(
                    "model_used"
                ),
            }
        }

    except Exception as e:
        return {
            "found": False,
            "case_id": case_id,
            "message": (
                f"Unable to retrieve RCA "
                f"for case {case_id}."
            ),
            "error": str(e)
        }