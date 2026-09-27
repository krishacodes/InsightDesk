"""
Independent held-out retrieval diagnostic for InsightDesk.

Purpose:
    Measure whether the current Pinecone retrieval stage can retrieve
    the correct existing case for a complaint WITHOUT using the
    production pipeline's previous case assignment as ground truth.

Important:
    - The held-out complaint must NOT have been used to create/update
      the target case representation.
    - Ground-truth target_case_id must come from an independent source
      (e.g. synthetic issue/case mapping or manually audited pairs).
    - This script tests RETRIEVAL only.
    - It does not apply the CrossEncoder threshold.
    - It evaluates Top-1 / Top-5 / Top-10 / Top-20 / Top-50.
"""

import os
import random
from collections import Counter

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from backend.services.embedding_service import (
    embedding_model,
    retrieve_similar_cases,
)
from backend.services.text_cleaner import clean_text


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

WIDE_TOP_K = 50
RANDOM_SEED = 42

# This file MUST contain independently established ground truth.
#
# Required columns:
#   complaint_id
#   target_case_id
#
# Example:
#
# complaint_id,target_case_id
# 1832,1481
# 1945,1520
#
GROUND_TRUTH_FILE = "data/heldout_retrieval_pairs.csv"


# ============================================================
# LOAD INDEPENDENT GROUND TRUTH
# ============================================================

def load_ground_truth():
    if not os.path.exists(GROUND_TRUTH_FILE):
        raise FileNotFoundError(
            f"\nMissing {GROUND_TRUTH_FILE}\n"
            "\n"
            "Create this file using an INDEPENDENT ground-truth mapping.\n"
            "Do not generate target_case_id from the production dedup result."
        )

    df = pd.read_csv(GROUND_TRUTH_FILE)

    required = {"complaint_id", "target_case_id"}

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    df = df.dropna(subset=["complaint_id", "target_case_id"]).copy()

    df["complaint_id"] = df["complaint_id"].astype(int)
    df["target_case_id"] = df["target_case_id"].astype(int)

    df = df.drop_duplicates(
        subset=["complaint_id", "target_case_id"]
    )

    return df


# ============================================================
# FETCH COMPLAINTS
# ============================================================

def fetch_complaints(complaint_ids):
    """
    Fetch the actual complaint text for the held-out complaints.
    """

    rows = []

    # Keep requests reasonably sized.
    chunk_size = 100

    for i in range(0, len(complaint_ids), chunk_size):
        chunk = complaint_ids[i:i + chunk_size]

        response = (
            supabase
            .table("complaints")
            .select(
                "complaint_id,complaint_text,case_id,"
                "created_at,is_synthetic"
            )
            .in_("complaint_id", chunk)
            .execute()
        )

        rows.extend(response.data or [])

    return pd.DataFrame(rows)


# ============================================================
# FETCH CURRENT CASE REPRESENTATIONS
# ============================================================

def fetch_cases(case_ids):
    """
    Fetch the current case representations.

    This is intentionally separated from complaint retrieval so
    we can verify exactly what representation Pinecone is expected
    to retrieve.
    """

    rows = []

    chunk_size = 100

    for i in range(0, len(case_ids), chunk_size):
        chunk = case_ids[i:i + chunk_size]

        response = (
            supabase
            .table("cases")
            .select("case_id,representative_text")
            .in_("case_id", chunk)
            .execute()
        )

        rows.extend(response.data or [])

    return pd.DataFrame(rows)


# ============================================================
# RETRIEVAL TEST
# ============================================================

def evaluate_retrieval(test_df):

    results = []

    print("\n" + "=" * 70)
    print("INDEPENDENT HELD-OUT RETRIEVAL DIAGNOSTIC")
    print("=" * 70)

    print(f"\nTesting complaints: {len(test_df)}")
    print(f"Wide retrieval depth: Top-{WIDE_TOP_K}")

    for idx, row in test_df.iterrows():

        complaint_id = int(row["complaint_id"])
        target_case_id = int(row["target_case_id"])
        complaint_text = str(row["complaint_text"])

        cleaned = clean_text(complaint_text)

        if not cleaned:
            print(
                f"Skipping complaint {complaint_id}: "
                "empty after cleaning"
            )
            continue

        embedding = embedding_model.encode(cleaned).tolist()

        matches = retrieve_similar_cases(
            embedding,
            top_k=WIDE_TOP_K
        )

        ranked_case_ids = []

        for match in matches:
            case_id = match.get("id")

            if case_id is None:
                metadata = match.get("metadata", {})
                case_id = metadata.get("case_id")

            if case_id is not None:
                try:
                    ranked_case_ids.append(int(case_id))
                except (TypeError, ValueError):
                    pass

        rank = None

        for position, case_id in enumerate(ranked_case_ids, start=1):
            if case_id == target_case_id:
                rank = position
                break

        results.append({
            "complaint_id": complaint_id,
            "target_case_id": target_case_id,
            "retrieved_rank": rank,
            "found_top1": rank is not None and rank <= 1,
            "found_top5": rank is not None and rank <= 5,
            "found_top10": rank is not None and rank <= 10,
            "found_top20": rank is not None and rank <= 20,
            "found_top50": rank is not None and rank <= 50,
        })

    return pd.DataFrame(results)


# ============================================================
# REPORT
# ============================================================

def report(results):

    total = len(results)

    if total == 0:
        print("\nNo valid test cases.")
        return

    print("\n" + "=" * 70)
    print("RETRIEVAL RESULTS")
    print("=" * 70)

    for k in [1, 5, 10, 20, 50]:

        column = f"found_top{k}"

        count = int(results[column].sum())
        percentage = count / total * 100

        print(
            f"Top-{k:2d}: "
            f"{count:3d}/{total} "
            f"({percentage:5.1f}%)"
        )

    not_found = results["retrieved_rank"].isna().sum()

    print(
        f"\nNot found within Top-{WIDE_TOP_K}: "
        f"{not_found}/{total} "
        f"({not_found / total * 100:.1f}%)"
    )

    # Rank distribution
    print("\nRANK DISTRIBUTION")

    rank_values = results["retrieved_rank"].dropna().astype(int)

    distribution = Counter()

    for rank in rank_values:

        if rank == 1:
            distribution["rank 1"] += 1

        elif rank <= 5:
            distribution["rank 2-5"] += 1

        elif rank <= 10:
            distribution["rank 6-10"] += 1

        elif rank <= 20:
            distribution["rank 11-20"] += 1

        elif rank <= 50:
            distribution["rank 21-50"] += 1

    for bucket in [
        "rank 1",
        "rank 2-5",
        "rank 6-10",
        "rank 11-20",
        "rank 21-50",
    ]:
        print(f"{bucket:12s}: {distribution[bucket]}")

    # Actual misses
    misses = results[results["retrieved_rank"].isna()]

    if len(misses):

        print("\n" + "=" * 70)
        print("TARGET CASES NOT FOUND IN TOP-50")
        print("=" * 70)

        print(
            misses[
                ["complaint_id", "target_case_id"]
            ].to_string(index=False)
        )


# ============================================================
# MAIN
# ============================================================

def main():

    truth = load_ground_truth()

    print(
        f"Loaded {len(truth)} independently labelled "
        "held-out relationships."
    )

    complaint_ids = truth["complaint_id"].tolist()

    complaints = fetch_complaints(complaint_ids)

    if complaints.empty:
        raise RuntimeError(
            "No complaints were returned from Supabase."
        )

    print(
        f"Fetched {len(complaints)} / "
        f"{len(complaint_ids)} complaints."
    )

    # --------------------------------------------------------
    # Verify that every ground-truth complaint was retrieved.
    # --------------------------------------------------------

    merged = truth.merge(
        complaints,
        on="complaint_id",
        how="left",
        suffixes=("_truth", "_db")
    )

    missing = merged[
        merged["complaint_text"].isna()
    ]

    if len(missing):

        print(
            f"\nWARNING: {len(missing)} ground-truth complaints "
            "were not found in Supabase."
        )

        merged = merged[
            merged["complaint_text"].notna()
        ]

    # --------------------------------------------------------
    # Critical sanity check:
    #
    # The held-out complaint's CURRENT production assignment
    # must not be used as the target.
    # --------------------------------------------------------

    leakage = merged[
        merged["case_id"].notna()
        & (
            merged["case_id"].astype(int)
            == merged["target_case_id"].astype(int)
        )
    ]

    print(
        f"\nComplaints whose CURRENT DB case_id equals "
        f"ground-truth target: {len(leakage)}"
    )

    print(
        "NOTE: This is not automatically leakage because the "
        "target case may have existed before the complaint. "
        "The important requirement is that the complaint was "
        "NOT used to construct/update the target case vector."
    )

    # --------------------------------------------------------
    # Randomize only after ground truth is established.
    # --------------------------------------------------------

    random.seed(RANDOM_SEED)

    test_df = merged.sample(
        frac=1,
        random_state=RANDOM_SEED
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # Run retrieval.
    # --------------------------------------------------------

    results = evaluate_retrieval(test_df)

    # --------------------------------------------------------
    # Save detailed results.
    # --------------------------------------------------------

    output_file = "data/heldout_retrieval_results.csv"

    results.to_csv(
        output_file,
        index=False
    )

    print(
        f"\nDetailed results saved to: {output_file}"
    )

    report(results)


if __name__ == "__main__":
    main()