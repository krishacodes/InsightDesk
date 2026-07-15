from pydantic import BaseModel
from typing import Optional


class TopicCreate(BaseModel):
    topic_name: str
    department: str
    description: Optional[str] = None


class TopicDB(TopicCreate):
    topic_id: int