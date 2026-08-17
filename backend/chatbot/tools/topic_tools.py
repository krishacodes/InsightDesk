from langchain_core.tools import tool

from backend.database.supabase import (
    get_topic as db_get_topic,
    get_topics as db_get_topics,
)


@tool
def get_topic(topic_id: int) -> dict:
    """
    Retrieve information about a specific InsightDesk topic.
    """
    try:
        topic = db_get_topic(topic_id)

        if not topic:
            return {
                "found": False,
                "topic_id": topic_id,
                "message": f"Topic {topic_id} was not found."
            }

        return {
            "found": True,
            "topic": {
                "topic_id": topic.get("topic_id"),
                "topic_name": topic.get("topic_name"),
                "topic_slug": topic.get("topic_slug"),
                "department": topic.get("department"),
                "description": topic.get("description")
            }
        }

    except Exception as e:
        return {
            "found": False,
            "topic_id": topic_id,
            "message": f"Unable to retrieve topic {topic_id}.",
            "error": str(e)
        }


@tool
def list_topics() -> dict:
    """
    Retrieve all available InsightDesk topics.
    """
    try:
        topics = db_get_topics()

        return {
            "found": True,
            "topic_count": len(topics),
            "topics": topics
        }

    except Exception as e:
        return {
            "found": False,
            "topics": [],
            "message": "Unable to retrieve topics.",
            "error": str(e)
        }