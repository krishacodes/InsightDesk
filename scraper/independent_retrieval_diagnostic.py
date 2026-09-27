"""
Independent held-out retrieval diagnostic (v3)

Purpose
-------
Measure retrieval quality independently of the production deduplication
pipeline.

Ground truth:
    synthetic_ground_truth.csv -> issue_id

NOT used:
    - production Supabase case_id
    - production case formation
    - production threshold
    - CrossEncoder scoring
    - production Pinecone namespace

Experiment
----------
For every issue_id with >= 2 complaints:

    1. Select one complaint as the founder.
    2. Embed the founder.
    3. Put exactly one founder vector into a temporary Pinecone namespace.
    4. Hold out every other complaint for that issue.
    5. Query the temporary index with each held-out complaint.
    6. Record the rank of the correct issue_id.

This isolates the retrieval stage.

Run from project root:

    python -m scraper.independent_retrieval_diagnostic
"""

import csv
from collections import defaultdict, Counter

from backend.services.preprocess import clean_text
from backend.services.embedding_service import model as embedding_model
from backend.services.embedding_service import index as pinecone_index


# ============================================================
# CONFIG
# ============================================================

GROUND_TRUTH_PATH = "data/synthetic_ground_truth.csv"

TEMP_NAMESPACE = "retrieval-replay-test"

WIDE_TOP_K = 50


# ============================================================
# PINECONE RESPONSE HELPERS
# ============================================================

def get_matches(result):
    """Handle different Pinecone response shapes."""

    if isinstance(result, list):
        return result

    if hasattr(result, "matches"):
        return result.matches

    return result["matches"]


def get_match_id(match):
    """Extract vector ID from a Pinecone match."""

    if hasattr(match, "id"):
        return match.id

    return match["id"]


# ============================================================
# LOAD INDEPENDENT GROUND TRUTH
# ============================================================

def load_ground_truth():

    with open(
        GROUND_TRUTH_PATH,
        encoding="utf-8"
    ) as f:

        rows = list(csv.DictReader(f))

    required_columns = {
        "issue_id",
        "complaint_text",
    }

    missing = required_columns - set(rows[0].keys())

    if missing:
        raise ValueError(
            f"Ground truth is missing columns: {sorted(missing)}"
        )

    by_issue = defaultdict(list)

    for row in rows:

        issue_id = row["issue_id"]
        text = row["complaint_text"]

        if not issue_id or not text:
            continue

        by_issue[issue_id].append(row)

    return by_issue


# ============================================================
# BUILD INDEPENDENT FOUNDERS + HOLDOUTS
# ============================================================

def build_founders_and_holdout(by_issue):

    founders = {}
    holdout = []

    for issue_id, complaints in by_issue.items():

        # Need at least two complaints:
        # one founder + one independent query.
        if len(complaints) < 2:
            continue

        # Founder selection is determined entirely by the
        # ground-truth file and BEFORE any retrieval/scoring.
        founder = complaints[0]

        founders[issue_id] = founder

        for complaint in complaints[1:]:

            holdout.append({
                "issue_id": issue_id,
                "complaint_text": complaint["complaint_text"],
            })

    return founders, holdout


# ============================================================
# BUILD TEMPORARY INDEX
# ============================================================

def build_temp_index(founders):

    print(
        f"Embedding {len(founders)} independent founder complaints..."
    )

    vectors = []

    for issue_id, founder in founders.items():

        cleaned = clean_text(
            founder["complaint_text"]
        )

        if not cleaned:
            print(
                f"WARNING: empty founder text for issue_id={issue_id}"
            )
            continue

        embedding = embedding_model.encode(
            cleaned
        ).tolist()

        vectors.append({
            "id": issue_id,
            "values": embedding,
            "metadata": {
                "issue_id": issue_id
            },
        })

    print(
        f"Upserting {len(vectors)} founder vectors into "
        f"temporary namespace '{TEMP_NAMESPACE}'..."
    )

    pinecone_index.upsert(
        vectors=vectors,
        namespace=TEMP_NAMESPACE
    )

    return len(vectors)


# ============================================================
# CLEANUP
# ============================================================

def cleanup_temp_index():

    print(
        f"\nCleaning temporary namespace "
        f"'{TEMP_NAMESPACE}'..."
    )

    try:

        pinecone_index.delete(
            delete_all=True,
            namespace=TEMP_NAMESPACE
        )

        print(
            "Temporary namespace cleared. "
            "Production namespace untouched."
        )

    except Exception as e:

        print(
            f"WARNING: cleanup failed: {e}"
        )


# ============================================================
# RETRIEVAL TEST
# ============================================================

def run_retrieval_test(holdout):

    results = []

    for number, item in enumerate(
        holdout,
        start=1
    ):

        target_issue = item["issue_id"]

        cleaned = clean_text(
            item["complaint_text"]
        )

        if not cleaned:

            results.append({
                "issue_id": target_issue,
                "complaint_text": item["complaint_text"],
                "rank": None,
                "status": "empty_after_cleaning",
            })

            continue

        embedding = embedding_model.encode(
            cleaned
        ).tolist()

        result = pinecone_index.query(
            vector=embedding,
            top_k=WIDE_TOP_K,
            include_metadata=True,
            namespace=TEMP_NAMESPACE,
        )

        matches = get_matches(result)

        matched_issue_ids = [
            get_match_id(match)
            for match in matches
        ]

        if target_issue in matched_issue_ids:

            rank = (
                matched_issue_ids.index(target_issue)
                + 1
            )

            status = "found"

        else:

            rank = None
            status = "not_found"

        results.append({
            "issue_id": target_issue,
            "complaint_text": item["complaint_text"],
            "rank": rank,
            "status": status,
        })

        if number % 50 == 0:
            print(
                f"Processed {number}/{len(holdout)} "
                "held-out complaints..."
            )

    return results


# ============================================================
# REPORT
# ============================================================

def report(results, number_of_founders):

    valid = [
        r for r in results
        if r["status"] != "empty_after_cleaning"
    ]

    total = len(valid)

    ranks = [
        r["rank"]
        for r in valid
        if r["rank"] is not None
    ]

    print("\n")
    print("=" * 70)
    print("INDEPENDENT HELD-OUT RETRIEVAL RESULTS")
    print("=" * 70)

    print(
        f"Independent issue representations: "
        f"{number_of_founders}"
    )

    print(
        f"Held-out complaints tested: {total}"
    )

    print(
        f"Maximum possible candidate rank: "
        f"{number_of_founders}"
    )

    print()

    for k in [1, 5, 10, 20, 50]:

        effective_k = min(
            k,
            number_of_founders
        )

        count = sum(
            1
            for rank in ranks
            if rank <= effective_k
        )

        percentage = (
            count / total * 100
            if total
            else 0
        )

        print(
            f"Top-{effective_k:2d}: "
            f"{count:3d}/{total} "
            f"({percentage:5.1f}%)"
        )

    not_found = sum(
        1
        for r in valid
        if r["rank"] is None
    )

    print(
        f"\nNot retrieved at all: "
        f"{not_found}/{total} "
        f"({not_found / total * 100:.1f}%)"
    )

    # --------------------------------------------------------
    # Rank distribution
    # --------------------------------------------------------

    buckets = Counter()

    for rank in ranks:

        if rank == 1:
            buckets["rank 1"] += 1

        elif rank <= 5:
            buckets["rank 2-5"] += 1

        elif rank <= 10:
            buckets["rank 6-10"] += 1

        elif rank <= 20:
            buckets["rank 11-20"] += 1

        else:
            buckets["rank 21+"] += 1

    print("\nRank distribution:")

    for label in [
        "rank 1",
        "rank 2-5",
        "rank 6-10",
        "rank 11-20",
        "rank 21+",
    ]:

        print(
            f"  {label:12s}: "
            f"{buckets[label]}"
        )

    # --------------------------------------------------------
    # Genuine retrieval failures
    # --------------------------------------------------------

    failures = [
        r
        for r in valid
        if r["rank"] is None
    ]

    if failures:

        print("\n")
        print("=" * 70)
        print(
            "GENUINE RETRIEVAL FAILURES"
        )
        print("=" * 70)

        for failure in failures:

            print(
                f"\nissue_id = {failure['issue_id']}"
            )

            print(
                f"text = {failure['complaint_text']}"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("INDEPENDENT RETRIEVAL DIAGNOSTIC")
    print("=" * 70)

    by_issue = load_ground_truth()

    founders, holdout = (
        build_founders_and_holdout(
            by_issue
        )
    )

    print(
        f"\nLoaded {len(by_issue)} issue IDs "
        "from independent ground truth."
    )

    print(
        f"Issues with >=2 complaints: "
        f"{len(founders)}"
    )

    print(
        f"Held-out complaints available: "
        f"{len(holdout)}"
    )

    if not holdout:

        raise RuntimeError(
            "No eligible held-out complaints."
        )

    # --------------------------------------------------------
    # IMPORTANT:
    # The temporary namespace contains ONLY the independent
    # founder representations.
    # --------------------------------------------------------

    build_temp_index(founders)

    try:

        print(
            f"\nQuerying {len(holdout)} held-out complaints "
            f"against temporary index..."
        )

        results = run_retrieval_test(
            holdout
        )

        report(
            results,
            len(founders)
        )

        # ----------------------------------------------------
        # Save detailed results.
        # ----------------------------------------------------

        output_path = (
            "data/"
            "independent_retrieval_results.csv"
        )

        with open(
            output_path,
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "issue_id",
                    "complaint_text",
                    "rank",
                    "status",
                ]
            )

            writer.writeheader()
            writer.writerows(results)

        print(
            f"\nDetailed results saved to: "
            f"{output_path}"
        )

    finally:

        cleanup_temp_index()


if __name__ == "__main__":
    main()