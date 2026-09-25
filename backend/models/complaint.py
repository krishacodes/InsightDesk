from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ComplaintCreate(BaseModel):
    complaint_text: str
    user_id: str
    rating: Optional[int] = None
    source: Optional[str] = None
    product: Optional[str] = None
    company_size: Optional[str] = None
    is_synthetic: bool = False
    created_at: Optional[datetime] = None


class ComplaintDB(ComplaintCreate):
    complaint_id: int
    case_id: Optional[int] = None
    cleaned_text: Optional[str] = None
    is_duplicate: bool = False
    created_at: datetime