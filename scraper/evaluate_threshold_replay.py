import os
import sys
import pandas as pd
import numpy as np

# Allow imports from project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sentence_transformers import SentenceTransformer, CrossEncoder
from backend.services.preprocess import clean_text


# ============================================================
# CONFIG
# ============================================================

GROUND_TRUTH_PATH = "data/synthetic_ground_truth.csv"

THRESHOLDS = [0.30, 0.40, 0.50, 0.5858, 0.70]

TOP_K = 5

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
CROSS_ENCODER_MODEL_NAME = "cross-encoder/stsb-distilroberta-base"

OUTPUT_SUMMARY = "data/threshold_replay_summary.csv"
OUTPUT_ASSIGNMENTS = "data/threshold_replay_assignments.csv"


# ============================================================
# LOAD MODELS
# ============================================================

print("Loading MiniLM...")
embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)

print("Loading STS-B CrossEncoder...")
cross_encoder = CrossEncoder(CROSS_ENCODER_MODEL_NAME)

print("Models loaded.\n")


# ============================================================
# HELPERS
# ============================================================

def cosine_similarity_matrix(query_embedding, case_embeddings):
    """
    Compute cosine similarity between one complaint embedding
    and all current case embeddings.
    """

    query = np.asarray(query_embedding, dtype=np.float32)
    cases = np.asarray(case_embeddings, dtype=np.float32)

    query_norm = np.linalg.norm(query)
    case_norms = np.linalg.norm(cases, axis=1)

    denominator = case_norms * query_norm

    # Prevent division by zero
    denominator = np.where(denominator == 0, 1e-12, denominator)

    return np.dot(cases, query) / denominator


def get_top_k_candidates(complaint_embedding, cases, k=5):
    """
    Mimics Pinecone cosine Top-K retrieval using the current
    in-memory case pool.
    """

    if not cases:
        return []

    case_embeddings = [case["embedding"] for case in cases]

    similarities = cosine_similarity_matrix(
        complaint_embedding,
        case_embeddings
    )

    top_indices = np.argsort(similarities)[::-1][:k]

    candidates = []

    for idx in top_indices:
        candidates.append(
            {
                "case_index": int(idx),
                "cosine_score": float(similarities[idx]),
                "representative_text": cases[idx]["representative_text"],
            }
        )

    return candidates


def cross_encoder_best_candidate(complaint_text, candidates):
    """
    Score the cleaned complaint against the representative text
    of each retrieved candidate case.
    """

    if not candidates:
        return None, 0.0

    pairs = [
        [complaint_text, candidate["representative_text"]]
        for candidate in candidates
    ]

    scores = cross_encoder.predict(pairs)

    best_local_index = int(np.argmax(scores))
    best_candidate = candidates[best_local_index]

    return best_candidate["case_index"], float(scores[best_local_index])


# ============================================================
# REPLAY
# ============================================================

def replay_threshold(df, threshold):
    """
    Sequentially replay complaints from an empty case pool.

    Important:
    - Representative text is the FIRST complaint that created
      the case, matching current Dedup v1 behaviour.
    - Representative embedding is frozen.
    - Every threshold starts from scratch.
    """

    cases = []
    assignments = []

    for row_number, row in df.iterrows():

        raw_text = str(row["complaint_text"])
        issue_id = str(row["issue_id"])
        complaint_ref = row["complaint_ref"]

        cleaned_text = clean_text(raw_text)

        complaint_embedding = embedding_model.encode(
            cleaned_text
        )

        # ----------------------------------------------------
        # No cases yet -> create first case
        # ----------------------------------------------------

        if not cases:
            new_case_index = 0

            cases.append(
                {
                    "case_index": new_case_index,
                    "representative_text": cleaned_text,
                    "embedding": complaint_embedding,
                    "members": [
                        {
                            "complaint_ref": complaint_ref,
                            "issue_id": issue_id,
                        }
                    ],
                }
            )

            assignments.append(
                {
                    "threshold": threshold,
                    "complaint_ref": complaint_ref,
                    "issue_id": issue_id,
                    "assigned_case": new_case_index,
                    "decision": "new_case",
                    "cross_encoder_score": None,
                }
            )

            continue

        # ----------------------------------------------------
        # MiniLM + cosine Top-K retrieval
        # ----------------------------------------------------

        candidates = get_top_k_candidates(
            complaint_embedding,
            cases,
            TOP_K
        )

        # ----------------------------------------------------
        # STS-B CrossEncoder
        # ----------------------------------------------------

        best_case_index, best_score = cross_encoder_best_candidate(
            cleaned_text,
            candidates
        )

        # ----------------------------------------------------
        # Merge / create decision
        # ----------------------------------------------------

        if best_score >= threshold:

            cases[best_case_index]["members"].append(
                {
                    "complaint_ref": complaint_ref,
                    "issue_id": issue_id,
                }
            )

            assigned_case = best_case_index
            decision = "merge"

        else:

            assigned_case = len(cases)

            cases.append(
                {
                    "case_index": assigned_case,
                    "representative_text": cleaned_text,
                    "embedding": complaint_embedding,
                    "members": [
                        {
                            "complaint_ref": complaint_ref,
                            "issue_id": issue_id,
                        }
                    ],
                }
            )

            decision = "new_case"

        assignments.append(
            {
                "threshold": threshold,
                "complaint_ref": complaint_ref,
                "issue_id": issue_id,
                "assigned_case": assigned_case,
                "decision": decision,
                "cross_encoder_score": best_score,
            }
        )

    return cases, assignments


# ============================================================
# CASE-LEVEL METRICS
# ============================================================

def calculate_case_metrics(cases, df):
    """
    Calculate fragmentation and contamination metrics.
    """

    # --------------------------------------------------------
    # issue_id -> set(case IDs)
    # --------------------------------------------------------

    issue_to_cases = {}

    # issue_id -> case -> complaint count
    issue_case_counts = {}

    contaminated_cases = 0
    pure_cases = 0
    cross_issue_assignments = 0

    for case in cases:

        case_index = case["case_index"]
        members = case["members"]

        issue_counts = {}

        for member in members:
            issue = member["issue_id"]

            issue_counts[issue] = issue_counts.get(issue, 0) + 1

            issue_to_cases.setdefault(issue, set()).add(case_index)

            issue_case_counts.setdefault(issue, {})
            issue_case_counts[issue][case_index] = (
                issue_case_counts[issue].get(case_index, 0) + 1
            )

        unique_issues = len(issue_counts)

        if unique_issues > 1:
            contaminated_cases += 1

            # Number of complaints not belonging to the
            # majority issue in this case.
            majority_count = max(issue_counts.values())

            cross_issue_assignments += len(members) - majority_count

        else:
            pure_cases += 1

    # --------------------------------------------------------
    # Fragmentation
    # --------------------------------------------------------

    cases_per_issue = [
        len(case_ids)
        for case_ids in issue_to_cases.values()
    ]

    mean_cases_per_issue = float(np.mean(cases_per_issue))
    median_cases_per_issue = float(np.median(cases_per_issue))
    min_cases_per_issue = int(np.min(cases_per_issue))
    max_cases_per_issue = int(np.max(cases_per_issue))

    # --------------------------------------------------------
    # Largest-case coverage
    # --------------------------------------------------------

    issue_total_counts = (
        df.groupby("issue_id")
        .size()
        .to_dict()
    )

    largest_case_coverages = []

    for issue, case_counts in issue_case_counts.items():

        largest_case = max(case_counts.values())
        total_issue_complaints = issue_total_counts[issue]

        coverage = largest_case / total_issue_complaints

        largest_case_coverages.append(coverage)

    mean_largest_case_coverage = float(
        np.mean(largest_case_coverages)
    )

    total_cases = len(cases)

    contaminated_case_rate = (
        contaminated_cases / total_cases
        if total_cases
        else 0
    )

    pure_case_rate = (
        pure_cases / total_cases
        if total_cases
        else 0
    )

    return {
        "final_cases": total_cases,

        "mean_cases_per_issue": mean_cases_per_issue,
        "median_cases_per_issue": median_cases_per_issue,
        "min_cases_per_issue": min_cases_per_issue,
        "max_cases_per_issue": max_cases_per_issue,

        "mean_largest_case_coverage": mean_largest_case_coverage,

        "contaminated_cases": contaminated_cases,
        "contaminated_case_rate": contaminated_case_rate,

        "pure_cases": pure_cases,
        "pure_case_rate": pure_case_rate,

        "cross_issue_assignments": cross_issue_assignments,
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print(f"Reading: {GROUND_TRUTH_PATH}")

    df = pd.read_csv(GROUND_TRUTH_PATH)

    required_columns = {
        "complaint_ref",
        "issue_id",
        "complaint_text",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    # Stable deterministic order.
    #
    # complaint_ref was assigned by the synthetic generator,
    # so we use it as the replay order.
    df = df.sort_values(
        by="complaint_ref",
        kind="stable"
    ).reset_index(drop=True)

    print(f"Synthetic complaints: {len(df)}")
    print(f"Ground-truth issues: {df['issue_id'].nunique()}")
    print(f"Thresholds: {THRESHOLDS}\n")

    all_summaries = []
    all_assignments = []

    for threshold in THRESHOLDS:

        print("=" * 70)
        print(f"REPLAYING THRESHOLD = {threshold}")
        print("=" * 70)

        cases, assignments = replay_threshold(
            df,
            threshold
        )

        metrics = calculate_case_metrics(
            cases,
            df
        )

        summary = {
            "threshold": threshold,
            **metrics,
        }

        all_summaries.append(summary)
        all_assignments.extend(assignments)

        print(f"Final cases: {metrics['final_cases']}")
        print(
            "Mean cases/issue:",
            round(metrics["mean_cases_per_issue"], 4)
        )
        print(
            "Median cases/issue:",
            round(metrics["median_cases_per_issue"], 4)
        )
        print(
            "Largest-case coverage:",
            round(
                metrics["mean_largest_case_coverage"] * 100,
                2
            ),
            "%"
        )
        print(
            "Contaminated cases:",
            metrics["contaminated_cases"]
        )
        print(
            "Contaminated case rate:",
            round(
                metrics["contaminated_case_rate"] * 100,
                2
            ),
            "%"
        )
        print(
            "Cross-issue assignments:",
            metrics["cross_issue_assignments"]
        )
        print(
            "Pure case rate:",
            round(
                metrics["pure_case_rate"] * 100,
                2
            ),
            "%"
        )

        print()

    # ========================================================
    # SAVE
    # ========================================================

    summary_df = pd.DataFrame(all_summaries)

    assignments_df = pd.DataFrame(all_assignments)

    summary_df.to_csv(
        OUTPUT_SUMMARY,
        index=False
    )

    assignments_df.to_csv(
        OUTPUT_ASSIGNMENTS,
        index=False
    )

    print("\n" + "=" * 70)
    print("FINAL THRESHOLD COMPARISON")
    print("=" * 70)

    display_columns = [
        "threshold",
        "final_cases",
        "mean_cases_per_issue",
        "mean_largest_case_coverage",
        "contaminated_cases",
        "contaminated_case_rate",
        "cross_issue_assignments",
        "pure_case_rate",
    ]

    print(
        summary_df[display_columns].to_string(
            index=False
        )
    )

    print("\nSaved:")
    print(OUTPUT_SUMMARY)
    print(OUTPUT_ASSIGNMENTS)


if __name__ == "__main__":
    main()