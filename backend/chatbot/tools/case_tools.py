from langchain_core.tools import tool

from backend.database.supabase import (
    get_case,
    get_complaints_by_case,
)


@tool
def get_case_details(case_id: int) -> dict:
    """
    Retrieve details for an InsightDesk case.

    Use this when the user asks about a specific case.
    """
    try:
        case = get_case(case_id)

        if not case:
            return {
                "found": False,
                "case_id": case_id,
                "message": f"Case {case_id} was not found."
            }

        return {
            "found": True,
            "case": case
        }

    except Exception as e:
        return {
            "found": False,
            "case_id": case_id,
            "message": f"Unable to retrieve case {case_id}.",
            "error": str(e)
        }


@tool
def get_complaints_for_case(case_id: int) -> dict:
    """
    Retrieve complaints associated with an InsightDesk case.

    Use this when the user wants to understand the complaints
    belonging to a specific case.
    """
    try:
        complaints = get_complaints_by_case(case_id)

        if not complaints:
            return {
                "found": False,
                "case_id": case_id,
                "complaints": [],
                "message": f"No complaints found for case {case_id}."
            }

        return {
            "found": True,
            "case_id": case_id,
            "complaint_count": len(complaints),
            "complaints": complaints
        }

    except Exception as e:
        return {
            "found": False,
            "case_id": case_id,
            "complaints": [],
            "message": f"Unable to retrieve complaints for case {case_id}.",
            "error": str(e)
        }