from backend.services.preprocess import clean_text

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
    increment_report_count,
    user_reported_case_recently,
    merge_duplicate_report
)
from backend.services.sentiment.sentiment_service import (
    enrich_case_with_sentiment
)

SIMILARITY_THRESHOLD = 0.85


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
        complaint.model_dump()
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

        # Same user has already reported this case recently
        if user_reported_case_recently(
            complaint.user_id,
            best_case_id
        ):

            merge_duplicate_report(
                complaint_id,
                best_case_id
            )
            enrich_case_with_sentiment(
            case_id
            )


            return {
                "complaint_id": complaint_id,
                "case_id": best_case_id,
                "is_duplicate": True,
                "status": "Merged with recent report"
            }

        # Genuine new report for an existing case
        increment_report_count(
            best_case_id,
            complaint.user_id
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

    new_case = create_case(
        representative_text=cleaned_text,
        user_id=complaint.user_id
    )

    case_id = new_case["case_id"]

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