from fastapi import FastAPI, HTTPException

from backend.models.complaint import ComplaintCreate

from backend.services.duplicate_service import process_complaint

app = FastAPI(
    title="InsightDesk API",
    version="1.0.0"
)


@app.get("/")
def home():
    return {
        "message": "InsightDesk API is running 🚀"
    }


@app.post("/analyze")
def analyze_complaint(
    complaint: ComplaintCreate
):
    """
    Main API endpoint.

    Receives a complaint,
    passes it to the duplicate detection pipeline,
    and returns the processing result.
    """

    try:

        result = process_complaint(
            complaint
        )

        return result

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )