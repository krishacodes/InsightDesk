"""
Inspect the final InsightDesk clustering candidate.

FINAL EXPERIMENT:
1. Load frozen 71-case snapshot.
2. Remove known synthetic/template boilerplate.
3. Generate MiniLM embeddings from CLEANED text.
4. Run BERTopic using the selected clustering parameters.
5. Print metrics and every case assignment.

Nothing is written to Supabase.

Usage:
    python -m backend.scripts.inspect_final_clustering
"""

from collections import Counter

from backend.scripts.evaluate_clustering import (
    load_case_snapshot,
)

from backend.scripts.tune_clustering import (
    run_configuration,
    clean_for_clustering,
)

from backend.services.clustering_service import (
    _generate_embeddings,
)


# ============================================================
# FINAL CANDIDATE CONFIGURATION
# ============================================================

FINAL_CONFIG = {
    "config_id": 8,

    # Keep True so BERTopic topic representation also
    # uses the cleaned text.
    "clean_text": True,

    "domain_stopwords": True,

    "n_neighbors": 12,

    "min_cluster_size": 4,

    "min_samples": 1,
}


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "\n"
        "========== FINAL CLUSTERING INSPECTION =========="
        "\n"
    )

    # --------------------------------------------------------
    # Load frozen snapshot
    # --------------------------------------------------------

    case_ids, documents = load_case_snapshot(
        refresh=False
    )

    if not documents:
        print("No frozen clustering cases found.")
        return

    if len(case_ids) != len(documents):
        raise ValueError(
            "Number of case IDs does not match "
            "number of documents."
        )

    print(
        f"Cases loaded: {len(documents)}"
    )

    # ========================================================
    # CLEAN DOCUMENTS BEFORE EMBEDDING
    # ========================================================

    print(
        "\nCleaning clustering-specific boilerplate..."
    )

    cleaned_documents = [

        clean_for_clustering(document)

        for document in documents
    ]

    # --------------------------------------------------------
    # Show how many documents actually changed
    # --------------------------------------------------------

    changed_count = sum(

        1

        for original, cleaned
        in zip(
            documents,
            cleaned_documents
        )

        if original != cleaned
    )

    print(
        f"Documents modified by cleaning: "
        f"{changed_count}/{len(documents)}"
    )

    # --------------------------------------------------------
    # Safety check
    # --------------------------------------------------------

    empty_documents = [

        case_ids[index]

        for index, document
        in enumerate(cleaned_documents)

        if not document.strip()
    ]

    if empty_documents:

        print(
            "\nWARNING: Cleaning produced empty "
            "documents for case IDs:"
        )

        print(
            empty_documents
        )

        print(
            "\nStopping clustering."
        )

        return

    # ========================================================
    # EMBEDDINGS FROM CLEANED TEXT
    # ========================================================

    print(
        "\nGenerating MiniLM embeddings "
        "from CLEANED complaint text..."
    )

    embeddings = _generate_embeddings(
        cleaned_documents
    )

    # ========================================================
    # RUN CLUSTERING
    # ========================================================

    print(
        "\nRunning final clustering candidate...\n"
    )

    # IMPORTANT:
    #
    # We pass cleaned_documents here.
    #
    # run_configuration() will call clean_for_clustering()
    # again because clean_text=True, but the operation is
    # effectively idempotent after the phrases have already
    # been removed.
    #
    # More importantly, embeddings were generated from the
    # cleaned text BEFORE clustering.

    result = run_configuration(
        cleaned_documents,
        embeddings,
        FINAL_CONFIG,
    )

    topic_model = result["_model"]

    topics = result["_topics"]

    topic_counts = Counter(
        topics
    )

    # ========================================================
    # SUMMARY
    # ========================================================

    print(
        "\n"
        "========== SUMMARY =========="
        "\n"
    )

    print(
        f"Cases:             "
        f"{len(documents)}"
    )

    print(
        f"Topics:            "
        f"{result['topics']}"
    )

    print(
        f"Outliers:          "
        f"{result['outliers']}"
    )

    print(
        f"Outlier rate:      "
        f"{result['outlier_rate']:.2%}"
    )

    if result[
        "coherence_cv"
    ] is not None:

        print(
            f"Coherence c_v:     "
            f"{result['coherence_cv']:.4f}"
        )

    else:

        print(
            "Coherence c_v:     N/A"
        )

    if result[
        "topic_diversity"
    ] is not None:

        print(
            f"Diversity@5:       "
            f"{result['topic_diversity']:.4f}"
        )

    else:

        print(
            "Diversity@5:       N/A"
        )

    print(
        f"Largest cluster:    "
        f"{result['largest_cluster']}"
    )

    print(
        f"Largest share:      "
        f"{result['largest_cluster_share']:.2%}"
    )

    print(
        f"Documents cleaned:  "
        f"{changed_count}"
    )

    # ========================================================
    # TOPIC MEMBERSHIPS
    # ========================================================

    print(
        "\n"
        "========== TOPIC MEMBERSHIPS =========="
    )

    normal_topics = sorted(

        topic

        for topic in set(topics)

        if topic != -1
    )

    for topic_num in normal_topics:

        print(
            "\n"
            + "=" * 80
        )

        print(
            f"TOPIC {topic_num} | "
            f"{topic_counts[topic_num]} CASES"
        )

        print(
            "=" * 80
        )

        # ----------------------------------------------------
        # Keywords
        # ----------------------------------------------------

        topic_words = topic_model.get_topic(
            topic_num
        )

        if topic_words:

            keywords = [

                word

                for word, _
                in topic_words[:10]
            ]

        else:

            keywords = []

        print(
            "\nKeywords:"
        )

        print(
            ", ".join(keywords)
            if keywords
            else "No keywords available"
        )

        # ----------------------------------------------------
        # Cases
        # ----------------------------------------------------

        print(
            "\nCases:"
        )

        for index, assigned_topic in enumerate(
            topics
        ):

            if assigned_topic != topic_num:
                continue

            case_id = case_ids[index]

            # Print ORIGINAL text for human inspection.
            #
            # Clustering used cleaned text, but seeing the
            # original complaint makes semantic inspection
            # easier.

            original_document = documents[
                index
            ]

            print(
                f"\nCase {case_id}:"
            )

            print(
                f"  {original_document}"
            )

    # ========================================================
    # OUTLIERS
    # ========================================================

    print(
        "\n\n"
        "========== OUTLIERS =========="
        "\n"
    )

    outlier_indices = [

        index

        for index, assigned_topic
        in enumerate(topics)

        if assigned_topic == -1
    ]

    print(
        f"Total outliers: "
        f"{len(outlier_indices)}"
    )

    if not outlier_indices:

        print(
            "\nNo outlier cases."
        )

    else:

        for index in outlier_indices:

            case_id = case_ids[
                index
            ]

            original_document = documents[
                index
            ]

            print(
                f"\nCase {case_id}:"
            )

            print(
                f"  {original_document}"
            )

    # ========================================================
    # TOPIC SIZE DISTRIBUTION
    # ========================================================

    print(
        "\n\n"
        "========== TOPIC SIZE DISTRIBUTION =========="
        "\n"
    )

    for topic_num in sorted(
        topic_counts
    ):

        label = (

            "OUTLIER"

            if topic_num == -1

            else f"Topic {topic_num}"
        )

        count = topic_counts[
            topic_num
        ]

        percentage = (

            count
            / len(documents)
            * 100
        )

        print(
            f"{label:<12} "
            f"{count:>3} cases "
            f"({percentage:.2f}%)"
        )

    # ========================================================
    # CLEANING EXAMPLES
    # ========================================================

    print(
        "\n\n"
        "========== CLEANING EXAMPLES =========="
        "\n"
    )

    shown = 0

    for index, (
        original,
        cleaned
    ) in enumerate(
        zip(
            documents,
            cleaned_documents
        )
    ):

        if original == cleaned:
            continue

        print(
            f"Case {case_ids[index]}"
        )

        print(
            f"Original: {original}"
        )

        print(
            f"Cleaned:  {cleaned}"
        )

        print()

        shown += 1

        # Only print a few examples
        if shown >= 5:
            break

    # ========================================================
    # END
    # ========================================================

    print(
        "\n"
        "=============================================="
    )

    print(
        "Final candidate inspection complete."
    )

    print(
        "No changes were made to Supabase."
    )

    print(
        "=============================================="
        "\n"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()