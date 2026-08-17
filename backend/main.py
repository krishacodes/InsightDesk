from fastapi import FastAPI, HTTPException

from backend.chatbot.router import router as chatbot_router
from backend.routers.dashboard import router as dashboard_router

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


app.include_router(
    chatbot_router
)

app.include_router(
    dashboard_router
)