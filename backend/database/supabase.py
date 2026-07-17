from datetime import datetime, timezone, timedelta
import os

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

# ==========================================================
# Complaint Operations
# ==========================================================

def create_complaint(data: dict):
    """
    Insert a new complaint into the complaints table.
    """
    response = (
        supabase.table("complaints")
        .insert(data)
        .execute()
    )

    return response.data[0]


def update_complaint(
    complaint_id: int,
    updates: dict
):
    """
    Update an existing complaint.
    """
    response = (
        supabase.table("complaints")
        .update(updates)
        .eq("complaint_id", complaint_id)
        .execute()
    )

    return response.data


def assign_case_to_complaint(
    complaint_id: int,
    case_id: int,
    is_duplicate: bool
):
    """
    Assign a complaint to a case.
    """
    response = (
        supabase.table("complaints")
        .update(
            {
                "case_id": case_id,
                "is_duplicate": is_duplicate
            }
        )
        .eq("complaint_id", complaint_id)
        .execute()
    )

    return response.data


def get_complaint(
    complaint_id: int
):
    """
    Retrieve a complaint.
    """
    response = (
        supabase.table("complaints")
        .select("*")
        .eq("complaint_id", complaint_id)
        .single()
        .execute()
    )

    return response.data


# ==========================================================
# Case Operations
# ==========================================================

def create_case(
    representative_text: str,
    user_id: str
):
    """
    Create a brand-new case.
    """
    payload = {
        "representative_text": representative_text,
        "report_count": 1,
        "user_ids": [user_id],
        "last_reported_at": datetime.now(timezone.utc).isoformat()
    }

    response = (
        supabase.table("cases")
        .insert(payload)
        .execute()
    )
    print("CREATE CASE RESPONSE:")
    print(response.data)

    return response.data[0]


def get_case(
    case_id: int
):
    """
    Retrieve a case.
    """
    response = (
        supabase.table("cases")
        .select("*")
        .eq("case_id", case_id)
        .single()
        .execute()
    )

    return response.data


import time

def update_case(case_id, payload):

    for i in range(3):
        try:
            response = (
                supabase.table("cases")
                .update(payload)
                .eq("case_id", case_id)
                .execute()
            )

            return response.data

        except Exception:
            print(f"Retry {i+1} for case {case_id}")
            time.sleep(2)

    raise Exception("Supabase update failed.")

# ==========================================================
# Duplicate Optimization
# ==========================================================

def user_reported_case_recently(
    user_id: str,
    case_id: int,
    hours: int = 24
):
    """
    Check whether this user has already reported
    the same case within the last 'hours' hours.
    """

    cutoff = (
        datetime.now(timezone.utc) -
        timedelta(hours=hours)
    ).isoformat()

    response = (
        supabase.table("complaints")
        .select("complaint_id")
        .eq("user_id", user_id)
        .eq("case_id", case_id)
        .gte("created_at", cutoff)
        .limit(1)
        .execute()
    )

    return len(response.data) > 0




# ==========================================================
# Topic Operations
# ==========================================================

def get_topic(
    topic_id: int
):
    """
    Retrieve a topic.
    """
    response = (
        supabase.table("topics")
        .select("*")
        .eq("topic_id", topic_id)
        .single()
        .execute()
    )

    return response.data


def create_topic(
    topic_name: str,
    topic_slug: str,
    department: str,
    description: str,
    clustering_run_id: int,
):
    """
    Create a new topic.

    Returns
    -------
    dict
        Newly created topic record.
    """

    data = {
        "topic_name": topic_name,
        "topic_slug": topic_slug,
        "department": department,
        "description": description,
        "clustering_run_id": clustering_run_id,
    }

    response = (
        supabase.table("topics")
        .insert(data)
        .execute()
    )

    return response.data[0]
def get_topic_by_slug(topic_slug: str):
    """
    Fetch a topic using its unique topic_slug.

    Parameters
    ----------
    topic_slug : str

    Returns
    -------
    dict | None
    """

    response = (
        supabase.table("topics")
        .select("*")
        .eq("topic_slug", topic_slug)
        .execute()
    )

    if response.data:
        return response.data[0]

    return None


def update_topic(
    topic_id: int,
    topic_name: str = None,
    topic_slug: str = None,
    department: str = None,
    department_override: str = None,
    description: str = None,
    clustering_run_id: int = None,
    updated_at: str = None,
):
    """
    Update an existing topic.
    """

    update_data = {}

    if topic_name is not None:
        update_data["topic_name"] = topic_name

    if topic_slug is not None:
        update_data["topic_slug"] = topic_slug

    if department is not None:
        update_data["department"] = department

    if department_override is not None:
        update_data["department_override"] = department_override

    if description is not None:
        update_data["description"] = description

    if clustering_run_id is not None:
        update_data["clustering_run_id"] = clustering_run_id

    if updated_at is not None:
        update_data["updated_at"] = updated_at

    response = (
        supabase.table("topics")
        .update(update_data)
        .eq("topic_id", topic_id)
        .execute()
    )

    return response.data[0]
def get_all_topics():
    """
    Fetch all topics.
    """

    response = (
        supabase.table("topics")
        .select("*")
        .execute()
    )

    return response.data
def get_clustering_metadata():

    response = (
        supabase.table("clustering_metadata")
        .select("*")
        .order("metadata_id", desc=True)
        .limit(1)
        .execute()
    )

    if len(response.data) == 0:
        return {
            "clustering_version": 0,
            "last_case_count": 0,
            "total_topics": 0,
            "outlier_rate": 0
        }

    return response.data[0]
from datetime import datetime


def update_clustering_metadata(
    last_case_count: int = None,
    total_topics: int = None,
    outlier_rate: float = None,
    clustering_version: int = None,
):
    """
    Update clustering metadata after a successful BERTopic run.
    """

    update_data = {
        "last_clustered_at": datetime.utcnow().isoformat()
    }

    if last_case_count is not None:
        update_data["last_case_count"] = last_case_count

    if total_topics is not None:
        update_data["total_topics"] = total_topics

    if outlier_rate is not None:
        update_data["outlier_rate"] = outlier_rate

    if clustering_version is not None:
        update_data["clustering_version"] = clustering_version

    response = (
        supabase.table("clustering_metadata")
        .update(update_data)
        .eq("metadata_id", 1)
        .execute()
    )

    return True
def get_all_cases():
    """
    Fetch all representative cases for BERTopic clustering.
    """

    response = (
        supabase.table("cases")
        .select("case_id, representative_text")
        .execute()
    )

    return response.data
def update_case_topic(
    case_id: int,
    topic_id: int,
):
    """
    Assign a topic to a case.
    """

    response = (
        supabase.table("cases")
        .update(
            {
                "topic_id": topic_id
            }
        )
        .eq("case_id", case_id)
        .execute()
    )

    return response.data
def get_clustering_statistics():
    """
    Returns clustering statistics required by the Decision Engine.

    Returns:
    {
        "case_count": int,
        "outlier_count": int,
        "outlier_rate": float
    }
    """

    # Total number of cases
    total_response = (
        supabase.table("cases")
        .select("*", count="exact", head=True)
        .execute()
    )

    case_count = total_response.count or 0

    # Cases not assigned to any topic (BERTopic outliers)
    outlier_response = (
        supabase.table("cases")
        .select("*", count="exact", head=True)
        .is_("topic_id", None)
        .execute()
    )

    outlier_count = outlier_response.count or 0

    outlier_rate = (
        outlier_count / case_count if case_count > 0 else 0.0
    )

    return {
        "case_count": case_count,
        "outlier_count": outlier_count,
        "outlier_rate": outlier_rate,
    }
def create_benchmark(
    model_name,
    complaints_processed,
    average_latency_ms,
    throughput,
    memory_mb
):

    response = (
        supabase
        .table(
            "model_benchmarks"
        )
        .insert(
            {
                "model_name":
                    model_name,

                "complaints_processed":
                    complaints_processed,

                "average_latency_ms":
                    average_latency_ms,

                "throughput":
                    throughput,

                "memory_mb":
                    memory_mb
            }
        )
        .execute()
    )

    return response.data[0]
def get_benchmarks():

    response = (
        supabase
        .table("model_benchmarks")
        .select("*")
        .execute()
    )

    return response.data
def get_sample_complaints(
    limit=100
):

    response = (
        supabase
        .table(
            "complaints"
        )
        .select(
            "complaint_text"
        )
        .limit(limit)
        .execute()
    )

    return [
        row["complaint_text"]
        for row in response.data
    ]
def create_case_history(
    case_id: int,
    report_count: int
):

    response = (
        supabase
        .table(
            "case_history"
        )
        .insert(
            {
                "case_id": case_id,
                "report_count": report_count
            }
        )
        .execute()
    )

    return response.data
def get_case_history(
    case_id: int
):

    response = (
        supabase
        .table(
            "case_history"
        )
        .select("*")
        .eq(
            "case_id",
            case_id
        )
        .order(
            "recorded_at",
            desc=True
        )
        .limit(2)
        .execute()
    )

    return response.data
