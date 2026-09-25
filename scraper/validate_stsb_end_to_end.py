"""
Read-only end-to-end diagnostic for STS-B duplicate detection.

Purpose
-------
Evaluate the proposed STS-B CrossEncoder inside the CURRENT retrieval
environment without modifying Supabase or Pinecone.

Important
---------
The existing case pool was created using the old MS-MARCO + -2.21
configuration. Therefore this is a diagnostic for threshold/model
selection, NOT a final evaluation of the future STS-B case structure.

Ground truth
------------
Synthetic generator issue_id is used as the independent reference.

Existing Supabase case_id assignments are NOT treated as ground truth.
"""

from collections import Counter, defaultdict

import numpy as np
import pandas as pd
from sentence_transformers import CrossEncoder

from backend.database.supabase import supabase
from backend.services.embedding_service import (
    generate_embedding,
    retrieve_similar_cases,
)
from backend.services.preprocess import clean_text


# ==========================================================
# Configuration
# ==========================================================

GROUND_TRUTH_FILE = "data/synthetic_ground_truth.csv"

OUTPUT_DETAIL_FILE = "data/stsb_end_to_end_diagnostic.csv"
OUTPUT_SUMMARY_FILE = "data/stsb_end_to_end_summary.csv"

MODEL_NAME = "cross-encoder/stsb-distilroberta-base"

THRESHOLDS = [
    0.5858,
    0.6469,
]

TOP_K = 5


# ==========================================================
# Helpers
# ==========================================================

def normalize_text(text):
    """
    Normalize only for exact mapping between CSV and Supabase.

    This is NOT semantic preprocessing.
    """
    if text is None:
        return ""

    return str(text).strip()


def fetch_all_synthetic_complaints_from_db():
    """
    Fetch synthetic complaints from Supabase with pagination.

    We deliberately avoid get_complaints() because Supabase/PostgREST
    may cap a response at 1000 rows.
    """

    rows = []

    page_size = 500
    start = 0

    while True:

        response = (
            supabase
            .table("complaints")
            .select(
                "complaint_id,"
                "case_id,"
                "complaint_text,"
                "cleaned_text,"
                "is_duplicate,"
                "is_synthetic"
            )
            .eq("is_synthetic", True)
            .range(
                start,
                start + page_size - 1
            )
            .execute()
        )

        batch = response.data or []

        rows.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return rows


def build_text_to_ground_truth(gt_df):
    """
    Map exact complaint text -> generator issue metadata.

    Duplicate text values are marked ambiguous and excluded later.
    """

    mapping = defaultdict(list)

    for _, row in gt_df.iterrows():

        text = normalize_text(
            row["complaint_text"]
        )

        mapping[text].append(
            {
                "issue_id": row["issue_id"],
                "department": row["department"],
                "complaint_ref": row["complaint_ref"],
            }
        )

    return mapping


def build_case_issue_map(
    db_rows,
    gt_text_map,
):
    """
    Map existing Supabase cases back to synthetic issue_ids.

    A case is considered CLEAN only when all synthetic complaints
    belonging to that case map to exactly one generator issue_id.

    This prevents us from pretending the old MS-MARCO case_id itself
    is ground truth.
    """

    case_issues = defaultdict(list)

    mapped_rows = {}

    ambiguous_text_count = 0
    unmatched_count = 0

    for row in db_rows:

        text = normalize_text(
            row.get("complaint_text")
        )

        matches = gt_text_map.get(
            text,
            []
        )

        if len(matches) == 0:
            unmatched_count += 1
            continue

        if len(matches) > 1:
            ambiguous_text_count += 1
            continue

        gt = matches[0]

        complaint_id = row["complaint_id"]
        case_id = row.get("case_id")

        mapped_rows[complaint_id] = {
            **row,
            "issue_id": gt["issue_id"],
            "department": gt["department"],
            "complaint_ref": gt["complaint_ref"],
        }

        if case_id is not None:
            case_issues[int(case_id)].append(
                gt["issue_id"]
            )

    clean_case_issue = {}
    contaminated_cases = {}

    for case_id, issues in case_issues.items():

        unique_issues = sorted(
            set(issues)
        )

        if len(unique_issues) == 1:

            clean_case_issue[case_id] = (
                unique_issues[0]
            )

        else:

            contaminated_cases[case_id] = (
                unique_issues
            )

    return (
        mapped_rows,
        clean_case_issue,
        contaminated_cases,
        unmatched_count,
        ambiguous_text_count,
    )


def score_candidates(
    model,
    complaint_text,
    candidates,
):
    """
    Score Pinecone candidates using STS-B.

    Returns candidates sorted by STS-B score descending.
    """

    pairs = []
    valid_candidates = []

    for candidate in candidates:

        metadata = candidate.get(
            "metadata",
            {}
        )

        representative_text = metadata.get(
            "representative_text"
        )

        case_id = metadata.get(
            "case_id"
        )

        if (
            representative_text is None
            or case_id is None
        ):
            continue

        pairs.append(
            (
                complaint_text,
                representative_text,
            )
        )

        valid_candidates.append(
            candidate
        )

    if not pairs:
        return []

    scores = model.predict(
        pairs,
        show_progress_bar=False,
    )

    scored = []

    for candidate, score in zip(
        valid_candidates,
        scores,
    ):

        metadata = candidate[
            "metadata"
        ]

        scored.append(
            {
                "case_id": int(
                    metadata["case_id"]
                ),
                "representative_text":
                    metadata[
                        "representative_text"
                    ],
                "pinecone_score":
                    candidate.get(
                        "score"
                    ),
                "stsb_score":
                    float(score),
            }
        )

    scored.sort(
        key=lambda x: x["stsb_score"],
        reverse=True,
    )

    return scored


# ==========================================================
# Main
# ==========================================================

def main():

    print("=" * 70)
    print("STS-B END-TO-END READ-ONLY DIAGNOSTIC")
    print("=" * 70)

    # ------------------------------------------------------
    # 1. Load independent generator ground truth
    # ------------------------------------------------------

    gt_df = pd.read_csv(
        GROUND_TRUTH_FILE
    )

    print(
        f"\nGround-truth rows: "
        f"{len(gt_df)}"
    )

    print(
        f"Unique issue IDs: "
        f"{gt_df['issue_id'].nunique()}"
    )

    gt_text_map = (
        build_text_to_ground_truth(
            gt_df
        )
    )

    # ------------------------------------------------------
    # 2. Fetch synthetic Supabase complaints
    # ------------------------------------------------------

    print(
        "\nFetching synthetic complaints "
        "from Supabase..."
    )

    db_rows = (
        fetch_all_synthetic_complaints_from_db()
    )

    print(
        f"Synthetic DB rows fetched: "
        f"{len(db_rows)}"
    )

    # ------------------------------------------------------
    # 3. Build independent case -> issue mapping
    # ------------------------------------------------------

    (
        mapped_rows,
        clean_case_issue,
        contaminated_cases,
        unmatched_count,
        ambiguous_text_count,
    ) = build_case_issue_map(
        db_rows,
        gt_text_map,
    )

    print(
        f"Mapped synthetic complaints: "
        f"{len(mapped_rows)}"
    )

    print(
        f"Unmatched synthetic DB rows: "
        f"{unmatched_count}"
    )

    print(
        f"Ambiguous exact-text rows: "
        f"{ambiguous_text_count}"
    )

    print(
        f"Cleanly mapped existing cases: "
        f"{len(clean_case_issue)}"
    )

    print(
        f"Old cases containing >1 synthetic "
        f"issue_id: {len(contaminated_cases)}"
    )

    if contaminated_cases:

        print(
            "\nNOTE: contaminated old cases are "
            "not treated as valid reference cases."
        )

    # ------------------------------------------------------
    # 4. Load STS-B separately
    # ------------------------------------------------------

    print(
        f"\nLoading {MODEL_NAME}..."
    )

    stsb_model = CrossEncoder(
        MODEL_NAME
    )

    print("Model loaded.")

    # ------------------------------------------------------
    # 5. Evaluate each mapped synthetic complaint
    # ------------------------------------------------------

    results = []

    total = len(mapped_rows)

    for index, (
        complaint_id,
        row,
    ) in enumerate(
        mapped_rows.items(),
        start=1,
    ):

        if (
            index == 1
            or index % 25 == 0
            or index == total
        ):
            print(
                f"Processing "
                f"{index}/{total}"
            )

        issue_id = row["issue_id"]

        cleaned_text = row.get(
            "cleaned_text"
        )

        if not cleaned_text:

            cleaned_text = clean_text(
                row["complaint_text"]
            )

        # --------------------------------------------------
        # MiniLM -> Pinecone Top-K
        # --------------------------------------------------

        embedding = generate_embedding(
            cleaned_text
        )

        candidates = (
            retrieve_similar_cases(
                embedding,
                top_k=TOP_K,
            )
        )

        # --------------------------------------------------
        # STS-B reranking
        # --------------------------------------------------

        scored_candidates = (
            score_candidates(
                stsb_model,
                cleaned_text,
                candidates,
            )
        )

        candidate_case_ids = [
            c["case_id"]
            for c in scored_candidates
        ]

        candidate_issue_ids = [
            clean_case_issue.get(
                c["case_id"]
            )
            for c in scored_candidates
        ]

        # --------------------------------------------------
        # Does Top-K contain a CLEAN case for this issue?
        # --------------------------------------------------

        correct_candidate_positions = []

        for position, candidate in enumerate(
            scored_candidates,
            start=1,
        ):

            candidate_issue = (
                clean_case_issue.get(
                    candidate["case_id"]
                )
            )

            if candidate_issue == issue_id:

                correct_candidate_positions.append(
                    position
                )

        correct_issue_retrieved = (
            len(
                correct_candidate_positions
            ) > 0
        )

        # --------------------------------------------------
        # Does an appropriate existing case actually exist?
        # --------------------------------------------------

        issue_clean_cases = [
            case_id
            for case_id, mapped_issue
            in clean_case_issue.items()
            if mapped_issue == issue_id
        ]

        valid_existing_case_exists = (
            len(issue_clean_cases) > 0
        )

        # --------------------------------------------------
        # STS-B winner
        # --------------------------------------------------

        if scored_candidates:

            winner = scored_candidates[0]

            winner_case_id = (
                winner["case_id"]
            )

            winner_score = (
                winner["stsb_score"]
            )

            winner_issue_id = (
                clean_case_issue.get(
                    winner_case_id
                )
            )

        else:

            winner_case_id = None
            winner_score = np.nan
            winner_issue_id = None

        rerank_correct = (
            correct_issue_retrieved
            and winner_issue_id == issue_id
        )

        # --------------------------------------------------
        # Threshold decisions
        # --------------------------------------------------

        threshold_results = {}

        for threshold in THRESHOLDS:

            key = str(threshold)

            predicted_merge = (
                winner_case_id is not None
                and winner_score >= threshold
            )

            predicted_correct_merge = (
                predicted_merge
                and winner_issue_id == issue_id
            )

            false_merge = (
                predicted_merge
                and winner_issue_id is not None
                and winner_issue_id != issue_id
            )

            # If winner case cannot be cleanly mapped,
            # don't call it a proven false merge.
            uncertain_merge = (
                predicted_merge
                and winner_issue_id is None
            )

            threshold_results[key] = {
                "predicted_merge":
                    predicted_merge,
                "correct_merge":
                    predicted_correct_merge,
                "false_merge":
                    false_merge,
                "uncertain_merge":
                    uncertain_merge,
            }

        # --------------------------------------------------
        # Save detailed result
        # --------------------------------------------------

        result = {
            "complaint_id":
                complaint_id,

            "complaint_ref":
                row["complaint_ref"],

            "issue_id":
                issue_id,

            "department":
                row["department"],

            "old_case_id":
                row.get("case_id"),

            "old_is_duplicate":
                row.get("is_duplicate"),

            "valid_existing_case_exists":
                valid_existing_case_exists,

            "correct_issue_retrieved_top5":
                correct_issue_retrieved,

            "correct_candidate_positions":
                ",".join(
                    map(
                        str,
                        correct_candidate_positions
                    )
                ),

            "stsb_winner_case_id":
                winner_case_id,

            "stsb_winner_issue_id":
                winner_issue_id,

            "stsb_winner_score":
                winner_score,

            "rerank_correct":
                rerank_correct,

            "candidate_case_ids":
                ",".join(
                    map(
                        str,
                        candidate_case_ids
                    )
                ),

            "candidate_issue_ids":
                ",".join(
                    str(x)
                    for x in candidate_issue_ids
                ),
        }

        for threshold in THRESHOLDS:

            key = str(threshold)

            values = (
                threshold_results[key]
            )

            prefix = (
                f"threshold_{threshold}"
            )

            result[
                f"{prefix}_merge"
            ] = values[
                "predicted_merge"
            ]

            result[
                f"{prefix}_correct_merge"
            ] = values[
                "correct_merge"
            ]

            result[
                f"{prefix}_false_merge"
            ] = values[
                "false_merge"
            ]

            result[
                f"{prefix}_uncertain_merge"
            ] = values[
                "uncertain_merge"
            ]

        results.append(result)

    # ------------------------------------------------------
    # 6. Detailed output
    # ------------------------------------------------------

    result_df = pd.DataFrame(
        results
    )

    result_df.to_csv(
        OUTPUT_DETAIL_FILE,
        index=False,
    )

    # ======================================================
    # 7. Separate diagnostics
    # ======================================================

    existing_df = result_df[
        result_df[
            "valid_existing_case_exists"
        ]
    ].copy()

    no_case_df = result_df[
        ~result_df[
            "valid_existing_case_exists"
        ]
    ].copy()

    summary_rows = []

    # ------------------------------------------------------
    # Retrieval + reranking
    # Only meaningful where a clean appropriate case exists.
    # ------------------------------------------------------

    if len(existing_df) > 0:

        retrieval_success = (
            existing_df[
                "correct_issue_retrieved_top5"
            ].mean()
        )

        retrieved_df = existing_df[
            existing_df[
                "correct_issue_retrieved_top5"
            ]
        ]

        if len(retrieved_df) > 0:

            rerank_success_given_retrieval = (
                retrieved_df[
                    "rerank_correct"
                ].mean()
            )

        else:

            rerank_success_given_retrieval = (
                np.nan
            )

    else:

        retrieval_success = np.nan

        rerank_success_given_retrieval = (
            np.nan
        )

    summary_rows.append(
        {
            "metric":
                "existing_case_test_count",
            "value":
                len(existing_df),
        }
    )

    summary_rows.append(
        {
            "metric":
                "retrieval_success_rate",
            "value":
                retrieval_success,
        }
    )

    summary_rows.append(
        {
            "metric":
                "rerank_success_given_retrieval",
            "value":
                rerank_success_given_retrieval,
        }
    )

    summary_rows.append(
        {
            "metric":
                "no_valid_case_test_count",
            "value":
                len(no_case_df),
        }
    )

    # ------------------------------------------------------
    # Threshold comparison
    # ------------------------------------------------------

    for threshold in THRESHOLDS:

        prefix = (
            f"threshold_{threshold}"
        )

        # Existing-case side:
        # Did we successfully merge to the correct clean issue?
        if len(existing_df) > 0:

            correct_merge_rate = (
                existing_df[
                    f"{prefix}_correct_merge"
                ].mean()
            )

            false_merge_count_existing = int(
                existing_df[
                    f"{prefix}_false_merge"
                ].sum()
            )

            uncertain_merge_count_existing = int(
                existing_df[
                    f"{prefix}_uncertain_merge"
                ].sum()
            )

        else:

            correct_merge_rate = np.nan
            false_merge_count_existing = 0
            uncertain_merge_count_existing = 0

        # No-valid-case side:
        # Correct behaviour is rejection/new case.
        if len(no_case_df) > 0:

            no_case_merge_count = int(
                no_case_df[
                    f"{prefix}_merge"
                ].sum()
            )

            correct_new_case_rate = (
                1
                - no_case_df[
                    f"{prefix}_merge"
                ].mean()
            )

        else:

            no_case_merge_count = 0
            correct_new_case_rate = np.nan

        summary_rows.extend(
            [
                {
                    "metric":
                        f"{threshold}_existing_correct_merge_rate",
                    "value":
                        correct_merge_rate,
                },
                {
                    "metric":
                        f"{threshold}_existing_false_merge_count",
                    "value":
                        false_merge_count_existing,
                },
                {
                    "metric":
                        f"{threshold}_existing_uncertain_merge_count",
                    "value":
                        uncertain_merge_count_existing,
                },
                {
                    "metric":
                        f"{threshold}_no_case_correct_rejection_rate",
                    "value":
                        correct_new_case_rate,
                },
                {
                    "metric":
                        f"{threshold}_no_case_merge_count",
                    "value":
                        no_case_merge_count,
                },
            ]
        )

    summary_df = pd.DataFrame(
        summary_rows
    )

    summary_df.to_csv(
        OUTPUT_SUMMARY_FILE,
        index=False,
    )

    # ======================================================
    # 8. Console report
    # ======================================================

    print("\n" + "=" * 70)
    print("DIAGNOSTIC RESULTS")
    print("=" * 70)

    print(
        f"\nExisting-case tests: "
        f"{len(existing_df)}"
    )

    print(
        f"No-valid-case tests: "
        f"{len(no_case_df)}"
    )

    if len(existing_df) > 0:

        print(
            "\nRETRIEVAL"
        )

        print(
            "Correct issue present in "
            f"Top-{TOP_K}: "
            f"{retrieval_success:.2%}"
        )

        if not np.isnan(
            rerank_success_given_retrieval
        ):

            print(
                "STS-B ranks correct issue #1 "
                "when retrieved: "
                f"{rerank_success_given_retrieval:.2%}"
            )

    print(
        "\nTHRESHOLD COMPARISON"
    )

    for threshold in THRESHOLDS:

        prefix = (
            f"threshold_{threshold}"
        )

        print(
            f"\nThreshold = {threshold}"
        )

        if len(existing_df) > 0:

            correct = (
                existing_df[
                    f"{prefix}_correct_merge"
                ].sum()
            )

            false_merges = (
                existing_df[
                    f"{prefix}_false_merge"
                ].sum()
            )

            uncertain = (
                existing_df[
                    f"{prefix}_uncertain_merge"
                ].sum()
            )

            print(
                "  Existing cases:"
            )

            print(
                f"    Correct merges: "
                f"{int(correct)}/"
                f"{len(existing_df)} "
                f"({correct / len(existing_df):.2%})"
            )

            print(
                f"    Proven false merges: "
                f"{int(false_merges)}"
            )

            print(
                f"    Uncertain merges: "
                f"{int(uncertain)}"
            )

        if len(no_case_df) > 0:

            merges = (
                no_case_df[
                    f"{prefix}_merge"
                ].sum()
            )

            rejected = (
                len(no_case_df)
                - merges
            )

            print(
                "  No-valid-case tests:"
            )

            print(
                f"    Correct rejections: "
                f"{int(rejected)}/"
                f"{len(no_case_df)} "
                f"({rejected / len(no_case_df):.2%})"
            )

            print(
                f"    Merge decisions: "
                f"{int(merges)}"
            )

    print(
        "\nFiles written:"
    )

    print(
        f"  {OUTPUT_DETAIL_FILE}"
    )

    print(
        f"  {OUTPUT_SUMMARY_FILE}"
    )

    print(
        "\nREAD-ONLY diagnostic complete."
    )

    print(
        "No Supabase or Pinecone records "
        "were modified."
    )


if __name__ == "__main__":
    main()