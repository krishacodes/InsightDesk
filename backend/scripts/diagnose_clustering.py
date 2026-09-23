"""
Diagnose the V1 BERTopic clustering result.

Uses the SAME frozen case snapshot and SAME reproducible V1
configuration as evaluate_clustering.py.

This script is READ-ONLY.
Nothing is written to Supabase.

Outputs:
    - Topic number
    - Cluster size
    - Topic coherence
    - Top keywords
    - Cases assigned to each topic
    - All BERTopic outliers
    - clustering_v1_diagnostics.txt

Usage:
    python -m backend.scripts.diagnose_clustering
"""

import os

from backend.scripts.evaluate_clustering import (
    SNAPSHOT_FILE,
    load_case_snapshot,
    run_reproducible_bertopic,
    compute_coherence,
)

from backend.services.clustering_service import (
    _generate_embeddings,
)


# ============================================================
# CONFIGURATION
# ============================================================

TOP_KEYWORDS = 10

SCRIPT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

OUTPUT_FILE = os.path.join(
    SCRIPT_DIR,
    "clustering_v1_diagnostics.txt"
)


# ============================================================
# HELPERS
# ============================================================

def clean_display_text(text):
    """
    Make long/multiline complaint text easier to inspect.
    """

    if text is None:
        return ""

    return " ".join(
        str(text).split()
    )


def get_topic_keywords(
    topic_model,
    topic_num,
    top_k=TOP_KEYWORDS,
):
    """
    Extract top-k BERTopic keywords.
    """

    terms = topic_model.get_topic(
        topic_num
    )

    if not terms:
        return []

    return [
        word
        for word, _
        in terms[:top_k]
    ]


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "\n========== V1 CLUSTERING DIAGNOSTICS ==========\n"
    )

    # --------------------------------------------------------
    # Load EXACT SAME frozen V1 case set
    # --------------------------------------------------------

    if not os.path.exists(SNAPSHOT_FILE):

        raise RuntimeError(
            "clustering_case_snapshot.json was not found.\n"
            "Run evaluate_clustering.py first."
        )

    case_ids, documents = load_case_snapshot(
        refresh=False
    )

    print(
        f"\nFrozen cases loaded: {len(documents)}"
    )

    # --------------------------------------------------------
    # Generate embeddings
    # --------------------------------------------------------

    print(
        "\nGenerating embeddings..."
    )

    embeddings = _generate_embeddings(
        documents
    )

    # --------------------------------------------------------
    # Reproduce V1 BERTopic
    # --------------------------------------------------------

    print(
        "\nRunning V1 BERTopic..."
    )

    (
        topics,
        probabilities,
        topic_model,
    ) = run_reproducible_bertopic(
        documents,
        embeddings
    )

    # --------------------------------------------------------
    # Compute coherence again
    # --------------------------------------------------------

    (
        overall_coherence,
        per_topic_coherence,
        coherence_topic_numbers,
    ) = compute_coherence(
        topic_model,
        documents,
        topics,
    )

    coherence_lookup = {}

    if per_topic_coherence:

        coherence_lookup = {

            topic_num: score

            for topic_num, score
            in zip(
                coherence_topic_numbers,
                per_topic_coherence
            )
        }

    # --------------------------------------------------------
    # Discover topics
    # --------------------------------------------------------

    normal_topics = sorted(
        topic
        for topic in set(topics)
        if topic != -1
    )

    # --------------------------------------------------------
    # Build report
    # --------------------------------------------------------

    lines = []

    lines.append(
        "INSIGHTDESK V1 CLUSTERING DIAGNOSTICS"
    )

    lines.append(
        "=" * 70
    )

    lines.append(
        f"Cases evaluated: {len(documents)}"
    )

    lines.append(
        f"Topics discovered: {len(normal_topics)}"
    )

    lines.append(
        f"Overall c_v coherence: "
        f"{overall_coherence:.4f}"
        if overall_coherence is not None
        else "Overall c_v coherence: N/A"
    )

    outlier_count = sum(
        1
        for topic in topics
        if topic == -1
    )

    lines.append(
        f"Outliers: {outlier_count}"
    )

    lines.append(
        f"Outlier rate: "
        f"{outlier_count / len(documents):.2%}"
    )

    lines.append("")

    # ========================================================
    # NORMAL TOPICS
    # ========================================================

    for topic_num in normal_topics:

        indices = [

            index

            for index, assigned_topic
            in enumerate(topics)

            if assigned_topic == topic_num
        ]

        keywords = get_topic_keywords(
            topic_model,
            topic_num
        )

        coherence = coherence_lookup.get(
            topic_num
        )

        lines.append("")
        lines.append("=" * 70)

        lines.append(
            f"TOPIC {topic_num}"
        )

        lines.append("-" * 70)

        lines.append(
            f"Cluster size: {len(indices)}"
        )

        if coherence is not None:

            lines.append(
                f"c_v coherence: {coherence:.4f}"
            )

        else:

            lines.append(
                "c_v coherence: N/A"
            )

        lines.append(
            "Top keywords: "
            + ", ".join(keywords)
        )

        lines.append("")

        lines.append(
            "Assigned cases:"
        )

        for position, index in enumerate(
            indices,
            start=1
        ):

            case_id = case_ids[
                index
            ]

            text = clean_display_text(
                documents[index]
            )

            lines.append(
                f"\n  {position}. "
                f"Case ID: {case_id}"
            )

            lines.append(
                f"     {text}"
            )

    # ========================================================
    # OUTLIERS
    # ========================================================

    lines.append("")
    lines.append("")
    lines.append("=" * 70)

    lines.append(
        "OUTLIERS (TOPIC -1)"
    )

    lines.append("-" * 70)

    outlier_indices = [

        index

        for index, assigned_topic
        in enumerate(topics)

        if assigned_topic == -1
    ]

    lines.append(
        f"Total outliers: "
        f"{len(outlier_indices)}"
    )

    lines.append("")

    for position, index in enumerate(
        outlier_indices,
        start=1
    ):

        case_id = case_ids[
            index
        ]

        text = clean_display_text(
            documents[index]
        )

        lines.append(
            f"{position}. Case ID: "
            f"{case_id}"
        )

        lines.append(
            f"   {text}"
        )

        lines.append("")

    # ========================================================
    # WRITE REPORT
    # ========================================================

    report = "\n".join(
        lines
    )

    print(
        "\n" + report
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(
            report
        )

    print(
        "\n"
        "========== DIAGNOSTICS SAVED =========="
    )

    print(
        OUTPUT_FILE
    )

    print(
        "\nDo NOT modify clustering_service.py yet."
    )

    print(
        "Use this report to decide which V2 changes "
        "are actually justified."
    )

    print(
        "\n========================================="
    )


if __name__ == "__main__":

    main()