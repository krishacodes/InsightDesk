from backend.services.preprocess import clean_text
from datetime import datetime, timezone

from backend.services.embedding_service import (
    generate_embedding,
    retrieve_similar_cases,
    store_case_embedding
)

from backend.services.cross_encoder import (
    rerank_candidate_cases
)

from backend.database.supabase import (
    create_complaint,
    update_complaint,
    assign_case_to_complaint,
    create_case,
    update_case,
    create_case_history,
    user_reported_case_recently,
    get_case,
    upsert_complaint_window
    
)
from backend.services.sentiment.sentiment_service import (
    enrich_case_with_sentiment
)
# STS-B Cross-Encoder duplicate-decision threshold.
#
# Calibrated on 150 generator-labelled complaint pairs:
# 75 duplicate and 75 non-duplicate.
#
# At this operating point on the calibration set:
# Precision = 92.59%
# Recall    = 33.33%
#
# The STS-B output is a bounded semantic similarity score,
# NOT a duplicate probability.

SIMILARITY_THRESHOLD = 0.5858

def _ensure_utc(dt):
    """
    Normalize a naive datetime (e.g. parsed from a plain CSV date)
    to UTC-aware, so all stored timestamps are consistent whether
    they originate from historical ingestion or live submission.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_case_timestamp(value):
    """
    Parse a case's stored created_at/last_reported_at (ISO string
    or datetime) back into a UTC-aware datetime for min/max
    comparison. Returns None if missing/unparseable.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(dt)


def increment_report_count(
    case_id: int,
    user_id: str,
    complaint_event_time: datetime = None
):
    """
    Increment report count and maintain
    case history for spike detection.

    complaint_event_time:
        None (default) - live submission. Unchanged from before:
        last_reported_at = current UTC time, created_at untouched,
        case_history recorded_at uses the DB default.
        a datetime - historical ingestion. created_at/last_reported_at
        are widened via min/max against the case's existing values,
        independent of ingestion order, and case_history records
        this same event time.
    """

    case = get_case(case_id)

    current_count = case["report_count"]

    users = case.get("user_ids", [])

    if user_id not in users:
        users.append(user_id)

    new_count = current_count + 1

    updates = {
        "report_count": new_count,
        "user_ids": users,
    }

    if complaint_event_time is not None:
        event_time = _ensure_utc(complaint_event_time)

        existing_created_at = _parse_case_timestamp(case.get("created_at"))
        existing_last_reported_at = _parse_case_timestamp(case.get("last_reported_at"))

        new_created_at = (
            min(existing_created_at, event_time)
            if existing_created_at is not None
            else event_time
        )
        new_last_reported_at = (
            max(existing_last_reported_at, event_time)
            if existing_last_reported_at is not None
            else event_time
        )

        updates["created_at"] = new_created_at.isoformat()
        updates["last_reported_at"] = new_last_reported_at.isoformat()
    else:
        updates["last_reported_at"] = datetime.now(timezone.utc).isoformat()

    response = update_case(
        case_id,
        updates
    )
    print("Increment called!")
    print(new_count)

    create_case_history(
        case_id=case_id,
        report_count=new_count,
        recorded_at=complaint_event_time
    )
    print("History created!")

    return response
def merge_duplicate_report(
    complaint_id: int,
    case_id: int
):
    """
    Mark complaint as duplicate and
    update the case timestamp.
    """

    assign_case_to_complaint(
        complaint_id,
        case_id,
        True
    )

    update_case(
        case_id,
        {
            "last_reported_at":
            datetime.now(timezone.utc).isoformat()
        }
    )

def process_complaint(complaint):
    """
    Main complaint processing pipeline.

    Steps
    -----
    1. Store complaint
    2. Preprocess text
    3. Generate MiniLM embedding
    4. Retrieve Top-K candidate cases
    5. Re-rank candidates using Cross Encoder
    6. Duplicate or New Case decision
    """

    # ----------------------------------------------------
    # Step 1 : Store Complaint
    # ----------------------------------------------------

    complaint_record = create_complaint(
        complaint.model_dump(exclude_none=True, mode="json")
    )

    complaint_id = complaint_record["complaint_id"]

    # ----------------------------------------------------
    # Step 2 : Preprocess
    # ----------------------------------------------------

    cleaned_text = clean_text(
        complaint.complaint_text
    )

    update_complaint(
        complaint_id,
        {
            "cleaned_text": cleaned_text
        }
    )

    # ----------------------------------------------------
    # Step 3 : Generate Embedding
    # ----------------------------------------------------

    embedding = generate_embedding(
        cleaned_text
    )

    # ----------------------------------------------------
    # Step 4 : Retrieve Candidate Cases
    # ----------------------------------------------------

    candidate_cases = retrieve_similar_cases(
        embedding
    )

    # ----------------------------------------------------
    # Step 5 : Cross Encoder Re-ranking
    # ----------------------------------------------------

    best_case_id, similarity_score = rerank_candidate_cases(
        complaint_text=cleaned_text,
        candidate_cases=candidate_cases
    )

    # ----------------------------------------------------
    # Step 6 : Duplicate Case Found
    # ----------------------------------------------------

    if (
        best_case_id is not None
        and similarity_score >= SIMILARITY_THRESHOLD
    ):

        # Recalibration policy: duplicate = f(complaint semantics) only.
        # user_id/time-based suppression (user_reported_case_recently /
        # merge_duplicate_report) is intentionally bypassed - every
        # complaint that semantically matches an existing case is
        # counted and windowed, regardless of who submitted it or when.
        increment_report_count(
            best_case_id,
            complaint.user_id,
            complaint_event_time=complaint.created_at
        )
        upsert_complaint_window(
            best_case_id,
            reference_time=complaint.created_at
        )
        assign_case_to_complaint(
            complaint_id,
            best_case_id,
            True
        )

        return {
            "complaint_id": complaint_id,
            "case_id": best_case_id,
            "is_duplicate": True,
            "status": "Existing case assigned"
        }

    # ----------------------------------------------------
    # Step 7 : Create New Case
    # ----------------------------------------------------

    # Create a new case record in the database. Populate basic fields
    # such as representative text, initial report count and user ids,
    # and timestamps.
    #
    # complaint.created_at is None for live submissions (falls back
    # to current time, unchanged from before) or a historical event
    # time for CSV ingestion (both created_at and last_reported_at
    # are initialized from that same value).
    if complaint.created_at is not None:
        case_created_at = _ensure_utc(complaint.created_at)
        case_last_reported_at = case_created_at
    else:
        case_created_at = datetime.now(timezone.utc)
        case_last_reported_at = case_created_at

    new_case = create_case(
        {
            "representative_text": cleaned_text,
            "report_count": 1,
            "user_ids": [complaint.user_id],
            "created_at": case_created_at.isoformat(),
            "last_reported_at": case_last_reported_at.isoformat(),
        }
    )

    case_id = new_case["case_id"]

    # Ensure complaint window/upsert is created for this case
    upsert_complaint_window(
        case_id,
        reference_time=complaint.created_at
    )
    # Store representative embedding in Pinecone
    store_case_embedding(
        case_id=case_id,
        embedding=embedding,
        representative_text=cleaned_text
    )
    enrich_case_with_sentiment(
    case_id
    )


    assign_case_to_complaint(
        complaint_id,
        case_id,
        False
    )

    return {
        "complaint_id": complaint_id,
        "case_id": case_id,
        "is_duplicate": False,
        "status": "New case created"
    }