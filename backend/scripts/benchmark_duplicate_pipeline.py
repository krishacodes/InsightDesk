"""
Benchmark InsightDesk's duplicate-detection pipeline.

Pipeline measured:
    Preprocessing
        ↓
    MiniLM embedding generation
        ↓
    Pinecone candidate retrieval
        ↓
    Cross-Encoder reranking

Cross-Encoder:
    cross-encoder/ms-marco-MiniLM-L-6-v2

Production decision threshold:
    -2.21

IMPORTANT:
This benchmark intentionally stops before complaint creation,
case assignment, case creation, or any other database write.

It is therefore safe to run repeatedly.

Usage:
    python backend/scripts/benchmark_duplicate_pipeline.py
"""

import time
import statistics

from backend.services.preprocess import clean_text

from backend.services.embedding_service import (
    generate_embedding,
    retrieve_similar_cases,
)

from backend.services.cross_encoder import (
    rerank_candidate_cases,
)

from backend.database.supabase import (
    get_sample_complaints,
)


# ============================================================
# CONFIGURATION
# ============================================================

SAMPLE_SIZE = 100

SIMILARITY_THRESHOLD = -2.21


# ============================================================
# HELPERS
# ============================================================

def average(values):
    return (
        sum(values) / len(values)
        if values
        else 0
    )


def median(values):
    return (
        statistics.median(values)
        if values
        else 0
    )


def percentile(values, percentile_value):
    """
    Calculate percentile without requiring NumPy.
    """

    if not values:
        return 0

    sorted_values = sorted(values)

    index = (
        percentile_value / 100
    ) * (len(sorted_values) - 1)

    lower = int(index)
    upper = min(
        lower + 1,
        len(sorted_values) - 1
    )

    weight = index - lower

    return (
        sorted_values[lower] * (1 - weight)
        +
        sorted_values[upper] * weight
    )


def print_latency_stats(
    name,
    values
):
    """
    Print mean, median and P95 latency.
    """

    print(
        f"{name:<24}"
        f"Mean: {average(values):>8.2f} ms | "
        f"Median: {median(values):>8.2f} ms | "
        f"P95: {percentile(values, 95):>8.2f} ms"
    )


# ============================================================
# BENCHMARK
# ============================================================

def benchmark():

    print(
        "\n"
        "========== DUPLICATE PIPELINE BENCHMARK =========="
        "\n"
    )

    print(
        f"Sample size requested: {SAMPLE_SIZE}"
    )

    print(
        f"Decision threshold:    {SIMILARITY_THRESHOLD}"
    )

    print(
        "\nLoading sample complaints..."
    )

    # --------------------------------------------------------
    # Load existing complaints
    # --------------------------------------------------------

    complaints = get_sample_complaints(
        limit=SAMPLE_SIZE
    )

    if not complaints:

        print(
            "No sample complaints found."
        )

        return

    # --------------------------------------------------------
    # Timing containers
    # --------------------------------------------------------

    preprocessing_times = []

    embedding_times = []

    retrieval_times = []

    rerank_times = []

    total_times = []

    candidate_counts = []

    rerank_scores = []

    predicted_duplicates = 0

    predicted_non_duplicates = 0

    no_candidate_count = 0

    failed_count = 0

    # --------------------------------------------------------
    # Process sample
    # --------------------------------------------------------

    for index, complaint in enumerate(
        complaints,
        start=1
    ):

        text = (

            complaint.get(
                "complaint_text"
            )

            if isinstance(
                complaint,
                dict
            )

            else complaint
        )

        if not text:

            continue

        try:

            # =================================================
            # TOTAL START
            # =================================================

            total_start = time.perf_counter()

            # =================================================
            # STEP 1: PREPROCESSING
            # =================================================

            start = time.perf_counter()

            cleaned = clean_text(
                text
            )

            end = time.perf_counter()

            preprocessing_times.append(
                (end - start) * 1000
            )

            # =================================================
            # STEP 2: EMBEDDING
            # =================================================

            start = time.perf_counter()

            embedding = generate_embedding(
                cleaned
            )

            end = time.perf_counter()

            embedding_times.append(
                (end - start) * 1000
            )

            # =================================================
            # STEP 3: CANDIDATE RETRIEVAL
            # =================================================

            start = time.perf_counter()

            candidates = retrieve_similar_cases(
                embedding
            )

            end = time.perf_counter()

            retrieval_times.append(
                (end - start) * 1000
            )

            candidate_count = (
                len(candidates)
                if candidates
                else 0
            )

            candidate_counts.append(
                candidate_count
            )

            # =================================================
            # STEP 4: CROSS-ENCODER RERANKING
            # =================================================

            start = time.perf_counter()

            best_case_id, score = (
                rerank_candidate_cases(
                    complaint_text=cleaned,
                    candidate_cases=candidates
                )
            )

            end = time.perf_counter()

            rerank_times.append(
                (end - start) * 1000
            )

            # =================================================
            # TOTAL END
            # =================================================

            total_end = time.perf_counter()

            total_times.append(
                (
                    total_end
                    - total_start
                ) * 1000
            )

            # =================================================
            # DECISION STATISTICS
            # =================================================

            if (
                best_case_id is None
                or score is None
            ):

                no_candidate_count += 1

                predicted_non_duplicates += 1

            else:

                score = float(
                    score
                )

                rerank_scores.append(
                    score
                )

                if (
                    score
                    >= SIMILARITY_THRESHOLD
                ):

                    predicted_duplicates += 1

                else:

                    predicted_non_duplicates += 1

            # -------------------------------------------------
            # Progress
            # -------------------------------------------------

            if (
                index % 10 == 0
                or index == len(complaints)
            ):

                print(
                    f"Processed "
                    f"{index}/"
                    f"{len(complaints)}"
                )

        except Exception as error:

            failed_count += 1

            print(
                f"Error processing "
                f"sample {index}: "
                f"{error}"
            )

    # --------------------------------------------------------
    # Number successfully benchmarked
    # --------------------------------------------------------

    n = len(
        total_times
    )

    if n == 0:

        print(
            "\nNo complaints were "
            "successfully benchmarked."
        )

        return

    # ========================================================
    # RESULTS
    # ========================================================

    print(
        "\n"
        "========== PERFORMANCE RESULTS =========="
        "\n"
    )

    print(
        f"Complaints processed:   {n}"
    )

    print(
        f"Failed samples:         {failed_count}"
    )

    print()

    print_latency_stats(
        "Preprocessing",
        preprocessing_times
    )

    print_latency_stats(
        "Embedding",
        embedding_times
    )

    print_latency_stats(
        "Candidate retrieval",
        retrieval_times
    )

    print_latency_stats(
        "Cross-Encoder rerank",
        rerank_times
    )

    print_latency_stats(
        "End-to-end",
        total_times
    )

    # ========================================================
    # THROUGHPUT
    # ========================================================

    mean_total = average(
        total_times
    )

    throughput = (

        1000 / mean_total

        if mean_total > 0

        else 0
    )

    print(
        "\n"
        "---------- THROUGHPUT ----------"
    )

    print(
        f"Average throughput: "
        f"{throughput:.2f} complaints/sec"
    )

    # ========================================================
    # CANDIDATE RETRIEVAL
    # ========================================================

    print(
        "\n"
        "---------- CANDIDATE RETRIEVAL ----------"
    )

    print(
        f"Average candidates returned: "
        f"{average(candidate_counts):.2f}"
    )

    print(
        f"Median candidates returned:  "
        f"{median(candidate_counts):.2f}"
    )

    print(
        f"No-candidate results:         "
        f"{no_candidate_count}"
    )

    # ========================================================
    # CROSS-ENCODER SCORES
    # ========================================================

    print(
        "\n"
        "---------- CROSS-ENCODER SCORES ----------"
    )

    if rerank_scores:

        print(
            f"Mean best score:   "
            f"{average(rerank_scores):.4f}"
        )

        print(
            f"Median best score: "
            f"{median(rerank_scores):.4f}"
        )

        print(
            f"Minimum score:     "
            f"{min(rerank_scores):.4f}"
        )

        print(
            f"Maximum score:     "
            f"{max(rerank_scores):.4f}"
        )

    else:

        print(
            "No reranking scores available."
        )

    # ========================================================
    # DECISION DISTRIBUTION
    # ========================================================

    print(
        "\n"
        "---------- DECISION DISTRIBUTION ----------"
    )

    print(
        f"Threshold:                 "
        f"{SIMILARITY_THRESHOLD}"
    )

    print(
        f"Predicted duplicate:       "
        f"{predicted_duplicates}"
    )

    print(
        f"Predicted non-duplicate:   "
        f"{predicted_non_duplicates}"
    )

    if n:

        duplicate_rate = (
            predicted_duplicates
            / n
            * 100
        )

        print(
            f"Predicted duplicate rate:  "
            f"{duplicate_rate:.2f}%"
        )

    print(
        "\n"
        "============================================"
        "\n"
    )

    print(
        "NOTE:"
    )

    print(
        "The duplicate/non-duplicate counts above "
        "are model decisions, NOT ground-truth "
        "accuracy measurements."
    )

    print(
        "Precision, recall and F1 should be reported "
        "from the manually labelled calibration set."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    benchmark()