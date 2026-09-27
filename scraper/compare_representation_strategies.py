"""
Three-way case-representation comparison:
Founder vs Centroid vs Medoid.

Uses the same independent synthetic ground truth as the
independent retrieval diagnostic.

Leave-one-out:
    For the held-out complaint's own issue, its embedding is
    excluded when constructing the centroid/medoid.

The experiment is entirely local:
    - no Supabase
    - no Pinecone
    - same MiniLM embeddings
    - exact cosine ranking over the 32 issue representations

Run:
    python -m scraper.compare_representation_strategies
"""

import csv
import random
from collections import defaultdict

import numpy as np

from backend.services.preprocess import clean_text
from backend.services.embedding_service import model as embedding_model


# ============================================================
# CONFIG
# ============================================================

GROUND_TRUTH_PATH = "data/synthetic_ground_truth.csv"

# None = test all eligible held-out complaints
SAMPLE_SIZE = None

RANDOM_SEED = 42


# ============================================================
# LOAD GROUND TRUTH
# ============================================================

def load_ground_truth():

    with open(
        GROUND_TRUTH_PATH,
        encoding="utf-8"
    ) as f:

        rows = list(
            csv.DictReader(f)
        )

    by_issue = defaultdict(list)

    for row in rows:

        issue_id = row["issue_id"]
        complaint_text = row["complaint_text"]

        if not issue_id or not complaint_text:
            continue

        by_issue[issue_id].append(
            complaint_text
        )

    return by_issue


# ============================================================
# EMBEDDING
# ============================================================

def embed_all(by_issue):

    embedded = {}

    total = sum(
        len(texts)
        for texts in by_issue.values()
    )

    done = 0

    print(
        f"\nEmbedding {total} complaints..."
    )

    for issue_id, texts in by_issue.items():

        pairs = []

        for text in texts:

            cleaned = clean_text(text)

            if not cleaned:
                raise ValueError(
                    f"Complaint became empty after cleaning "
                    f"(issue_id={issue_id})"
                )

            embedding = np.asarray(
                embedding_model.encode(cleaned),
                dtype=np.float32
            )

            pairs.append(
                (text, embedding)
            )

            done += 1

            if done % 100 == 0:

                print(
                    f"  embedded {done}/{total}..."
                )

        embedded[issue_id] = pairs

    return embedded


# ============================================================
# COSINE SIMILARITY
# ============================================================

def cosine_sim(a, b):

    denominator = (
        np.linalg.norm(a)
        * np.linalg.norm(b)
    )

    if denominator == 0:
        return 0.0

    return float(
        np.dot(a, b) / denominator
    )


# ============================================================
# REPRESENTATION STRATEGIES
# ============================================================

def founder_vector(
    pairs,
    exclude_index=None
):
    """
    Current architecture.

    The first complaint in ground-truth file order
    is treated as the founder.

    Because all holdout queries begin at index 1,
    the founder itself is never the query.
    """

    for i, (text, embedding) in enumerate(pairs):

        if i != exclude_index:

            return embedding

    raise ValueError(
        "No available complaint for founder representation."
    )


def centroid_vector(
    pairs,
    exclude_index=None
):
    """
    Mean embedding of all available complaints.

    The held-out complaint is excluded when calculating
    the target issue's representation.
    """

    vectors = [
        embedding
        for i, (text, embedding) in enumerate(pairs)
        if i != exclude_index
    ]

    if not vectors:

        raise ValueError(
            "No vectors available for centroid."
        )

    return np.mean(
        vectors,
        axis=0
    )


def medoid_vector(
    pairs,
    exclude_index=None
):
    """
    Select the actual complaint embedding closest to the
    leave-one-out centroid.

    Unlike centroid, the resulting representation is always
    a real complaint embedding.
    """

    candidates = [
        (i, embedding)
        for i, (text, embedding)
        in enumerate(pairs)
        if i != exclude_index
    ]

    if not candidates:

        raise ValueError(
            "No vectors available for medoid."
        )

    centroid = np.mean(
        [
            embedding
            for i, embedding in candidates
        ],
        axis=0
    )

    best_index, best_embedding = max(
        candidates,
        key=lambda item: cosine_sim(
            item[1],
            centroid
        )
    )

    return best_embedding


# ============================================================
# STRATEGIES
# ============================================================

STRATEGIES = {
    "founder": founder_vector,
    "centroid": centroid_vector,
    "medoid": medoid_vector,
}


# ============================================================
# COMPARISON
# ============================================================

def run_comparison(
    embedded,
    sample_size=None
):

    issue_ids = list(
        embedded.keys()
    )

    # --------------------------------------------------------
    # Precompute representations for OTHER issues.
    #
    # The held-out complaint belongs to only one issue, so
    # there is no leakage into the other 31 issues.
    # --------------------------------------------------------

    full_reps = {
        name: {}
        for name in STRATEGIES
    }

    for name, fn in STRATEGIES.items():

        for issue_id, pairs in embedded.items():

            full_reps[name][issue_id] = fn(
                pairs,
                exclude_index=None
            )

    # --------------------------------------------------------
    # Build holdout.
    #
    # index 0 = founder
    # index 1 onward = held-out complaints
    # --------------------------------------------------------

    holdout = []

    for issue_id, pairs in embedded.items():

        for i in range(
            1,
            len(pairs)
        ):

            holdout.append(
                (issue_id, i)
            )

    if sample_size is not None:

        random.seed(
            RANDOM_SEED
        )

        holdout = random.sample(
            holdout,
            min(
                sample_size,
                len(holdout)
            )
        )

    print(
        f"\nTesting {len(holdout)} held-out complaints "
        "across 3 strategies..."
    )

    results = []

    for n, (target_issue, idx) in enumerate(
        holdout,
        start=1
    ):

        query_text, query_embedding = (
            embedded[target_issue][idx]
        )

        row = {
            "query_index": n,
            "issue_id": target_issue,
            "complaint_text": query_text,
        }

        for strategy_name, strategy_fn in STRATEGIES.items():

            # ------------------------------------------------
            # LEAVE-ONE-OUT:
            # exclude this query from its own issue's
            # representation.
            # ------------------------------------------------

            target_representation = strategy_fn(
                embedded[target_issue],
                exclude_index=idx
            )

            scored = []

            for issue_id in issue_ids:

                if issue_id == target_issue:

                    representation = (
                        target_representation
                    )

                else:

                    representation = (
                        full_reps[
                            strategy_name
                        ][issue_id]
                    )

                score = cosine_sim(
                    query_embedding,
                    representation
                )

                scored.append(
                    (
                        issue_id,
                        score
                    )
                )

            # Highest similarity first.
            scored.sort(
                key=lambda x: x[1],
                reverse=True
            )

            ranked_ids = [
                issue_id
                for issue_id, score in scored
            ]

            target_rank = (
                ranked_ids.index(
                    target_issue
                ) + 1
            )

            target_score = next(
                score
                for issue_id, score in scored
                if issue_id == target_issue
            )

            row[
                f"{strategy_name}_rank"
            ] = target_rank

            row[
                f"{strategy_name}_score"
            ] = target_score

        results.append(row)

        if n % 50 == 0:

            print(
                f"  processed "
                f"{n}/{len(holdout)}..."
            )

    return results, len(issue_ids)


# ============================================================
# REPORT
# ============================================================

def report(
    results,
    n_issues
):

    total = len(results)

    print("\n")
    print("=" * 80)
    print(
        "REPRESENTATION STRATEGY COMPARISON"
    )
    print(
        "Leave-one-out evaluation"
    )
    print("=" * 80)

    print(
        f"\nQueries tested: {total}"
    )

    print(
        f"Candidate issues: {n_issues}"
    )

    print()

    header = (
        f"{'Strategy':<10}"
        f"{'Top-1':>10}"
        f"{'Top-5':>10}"
        f"{'Top-10':>10}"
        f"{'Top-20':>10}"
        f"{'MeanRank':>12}"
    )

    print(header)
    print("-" * len(header))

    strategy_ranks = {}

    for strategy in STRATEGIES:

        ranks = [
            row[
                f"{strategy}_rank"
            ]
            for row in results
        ]

        strategy_ranks[
            strategy
        ] = ranks

        top1 = (
            sum(r <= 1 for r in ranks)
            / total
            * 100
        )

        top5 = (
            sum(r <= 5 for r in ranks)
            / total
            * 100
        )

        top10 = (
            sum(r <= 10 for r in ranks)
            / total
            * 100
        )

        top20 = (
            sum(r <= 20 for r in ranks)
            / total
            * 100
        )

        mean_rank = (
            sum(ranks)
            / total
        )

        print(
            f"{strategy:<10}"
            f"{top1:>9.1f}%"
            f"{top5:>9.1f}%"
            f"{top10:>9.1f}%"
            f"{top20:>9.1f}%"
            f"{mean_rank:>12.2f}"
        )

    # --------------------------------------------------------
    # Pairwise comparison
    # --------------------------------------------------------

    founder = strategy_ranks[
        "founder"
    ]

    centroid = strategy_ranks[
        "centroid"
    ]

    medoid = strategy_ranks[
        "medoid"
    ]

    print("\n")
    print("=" * 80)
    print(
        "PAIRWISE COMPARISON AGAINST FOUNDER"
    )
    print("=" * 80)

    for name, alternative in [
        ("centroid", centroid),
        ("medoid", medoid),
    ]:

        better = sum(
            a < f
            for a, f in zip(
                alternative,
                founder
            )
        )

        same = sum(
            a == f
            for a, f in zip(
                alternative,
                founder
            )
        )

        worse = sum(
            a > f
            for a, f in zip(
                alternative,
                founder
            )
        )

        print(
            f"\n{name.capitalize()} vs founder:"
        )

        print(
            f"  Better rank : "
            f"{better} "
            f"({better / total * 100:.1f}%)"
        )

        print(
            f"  Same rank   : "
            f"{same} "
            f"({same / total * 100:.1f}%)"
        )

        print(
            f"  Worse rank  : "
            f"{worse} "
            f"({worse / total * 100:.1f}%)"
        )

    # --------------------------------------------------------
    # Top-K comparison
    # --------------------------------------------------------

    print("\n")
    print("=" * 80)
    print(
        "RETRIEVAL PERFORMANCE BY TOP-K"
    )
    print("=" * 80)

    for k in [
        1,
        5,
        10,
        20,
    ]:

        print(
            f"\nTop-{k}:"
        )

        for strategy in STRATEGIES:

            ranks = strategy_ranks[
                strategy
            ]

            hits = sum(
                r <= k
                for r in ranks
            )

            print(
                f"  {strategy:<8}: "
                f"{hits}/{total} "
                f"({hits / total * 100:.1f}%)"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 80)
    print(
        "INDEPENDENT REPRESENTATION COMPARISON"
    )
    print("=" * 80)

    by_issue = load_ground_truth()

    print(
        f"\nLoaded {len(by_issue)} issue IDs."
    )

    print(
        "Embedding all complaints once..."
    )

    embedded = embed_all(
        by_issue
    )

    results, n_issues = run_comparison(
        embedded,
        sample_size=SAMPLE_SIZE
    )

    # --------------------------------------------------------
    # Save detailed per-query results
    # --------------------------------------------------------

    output_path = (
        "data/"
        "representation_comparison_results.csv"
    )

    fieldnames = [
        "query_index",
        "issue_id",
        "complaint_text",

        "founder_rank",
        "founder_score",

        "centroid_rank",
        "centroid_score",

        "medoid_rank",
        "medoid_score",
    ]

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(
            results
        )

    print(
        f"\nPer-query results saved to: "
        f"{output_path}"
    )

    report(
        results,
        n_issues
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()