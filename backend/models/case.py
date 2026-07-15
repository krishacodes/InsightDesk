from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class CaseCreate(BaseModel):
    representative_text: str
    user_ids: List[str]


class CaseDB(CaseCreate):
    case_id: int
    report_count: int = 1

    last_reported_at: datetime

    topic_id: Optional[int] = None

    emotion: Optional[str] = None
    emotion_confidence: Optional[float] = None

    spike_status: bool = False
    spike_score: Optional[float] = None
    spike_detected_at: Optional[datetime] = None

    created_at: datetime