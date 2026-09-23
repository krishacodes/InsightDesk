"""
Final production clustering dry run.

Uses:
- Frozen 71-case snapshot
- Production embedding function
- Production _run_bertopic()

NO DATABASE WRITES.

Run:
    python -m backend.scripts.dry_run_production_clustering
"""

from collections import Counter

from backend.services.clustering_service import (
    _generate_embeddings,
    _run_bertopic,
)

from backend.scripts.evaluate_clustering import (
    load_case_snapshot,
)


def main():

    print("\n==========================================")
    print("PRODUCTION CLUSTERING VERIFICATION")
    print("==========================================\n")

    # --------------------------------------------------------
    # Load same frozen snapshot used during evaluation
    # --------------------------------------------------------

    _, documents = load_case_snapshot(
        refresh=False
    )

    print(f"Cases loaded: {len(documents)}")

    if not documents:
        print("No cases found.")
        return

    # --------------------------------------------------------
    # Generate embeddings using production function
    # --------------------------------------------------------

    print("\nGenerating embeddings...")

    embeddings = _generate_embeddings(
        documents
    )

    print(
        f"Embedding shape: {embeddings.shape}"
    )

    # --------------------------------------------------------
    # Run PRODUCTION clustering
    # --------------------------------------------------------

    print("\nRunning production BERTopic...")

    topics, probabilities, topic_model = (
        _run_bertopic(
            documents,
            embeddings
        )
    )

    # --------------------------------------------------------
    # Counts
    # --------------------------------------------------------

    counts = Counter(topics)

    normal_topics = sorted(
        topic
        for topic in counts
        if topic != -1
    )

    outlier_count = counts.get(
        -1,
        0
    )

    outlier_rate = (
        outlier_count / len(topics)
        if topics
        else 0
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print("\n==========================================")
    print("PRODUCTION RESULT")
    print("==========================================\n")

    print(
        f"Cases:        {len(documents)}"
    )

    print(
        f"Topics:       {len(normal_topics)}"
    )

    print(
        f"Outliers:     {outlier_count}"
    )

    print(
        f"Outlier rate: {outlier_rate:.2%}"
    )

    # --------------------------------------------------------
    # Topic sizes
    # --------------------------------------------------------

    print("\nTopic sizes:")

    for topic_number in normal_topics:

        print(
            f"  Topic {topic_number}: "
            f"{counts[topic_number]} cases"
        )

    if outlier_count:

        print(
            f"  Outliers: {outlier_count}"
        )

    # --------------------------------------------------------
    # Keywords
    # --------------------------------------------------------

    print("\n==========================================")
    print("TOPIC KEYWORDS")
    print("==========================================")

    for topic_number in normal_topics:

        words = topic_model.get_topic(
            topic_number
        )

        keywords = [
            word
            for word, _
            in words[:8]
        ]

        print(
            f"\nTopic {topic_number} "
            f"({counts[topic_number]} cases)"
        )

        print(
            ", ".join(keywords)
        )

    # --------------------------------------------------------
    # Expected result
    # --------------------------------------------------------

    print("\n==========================================")
    print("FINAL VERIFICATION")
    print("==========================================\n")

    expected_topics = 6
    expected_outliers = 1

    if (
        len(normal_topics) == expected_topics
        and
        outlier_count == expected_outliers
    ):

        print("PASS")

        print(
            "Production clustering reproduces "
            "final Config 8."
        )

        print(
            "Expected and actual: "
            "6 topics / 1 outlier."
        )

    else:

        print("FAIL")

        print(
            f"Expected: "
            f"{expected_topics} topics / "
            f"{expected_outliers} outlier"
        )

        print(
            f"Actual: "
            f"{len(normal_topics)} topics / "
            f"{outlier_count} outliers"
        )

    print("\n==========================================")
    print("NO DATABASE CHANGES WERE MADE")
    print("==========================================\n")


if __name__ == "__main__":
    main()