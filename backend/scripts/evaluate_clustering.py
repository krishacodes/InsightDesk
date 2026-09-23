"""
Evaluate InsightDesk's BERTopic clustering without requiring
ground-truth topic labels.

This script is intended for reproducible V1 vs V2 clustering
experiments.

Metrics:
    1. Topic coherence (c_v)
    2. In-memory BERTopic outlier count/rate
    3. Current DB outlier count/rate
    4. Cluster-size distribution
    5. Topic diversity

Reproducibility:
    - UMAP uses a fixed random_state.
    - On the first run, the current set of representative cases is
      saved to a static JSON snapshot.
    - Future runs reuse that exact snapshot so V1 and V2 are evaluated
      on identical input data.

IMPORTANT:
    This script is READ-ONLY with respect to Supabase.
    It does NOT call:
        _update_database()
        _update_metadata()
        update_case_topic()
        create_topic()
        update_topic()

Usage:

    pip install gensim

    # First run: automatically creates the case snapshot
    python -m backend.scripts.evaluate_clustering

    # Explicitly recreate the snapshot if ever required
    python -m backend.scripts.evaluate_clustering --refresh-snapshot

Do NOT refresh the snapshot between V1 and V2 if you want a valid
before/after comparison.
"""

import argparse
import json
import os
from collections import Counter

import numpy as np

from bertopic import BERTopic
from gensim.corpora.dictionary import Dictionary
from gensim.models.coherencemodel import CoherenceModel
from sklearn.feature_extraction.text import CountVectorizer
from umap import UMAP

from backend.services.clustering_service import (
    _load_cases,
    _generate_embeddings,
)

from backend.database.supabase import (
    get_clustering_statistics,
)


# ============================================================
# CONFIGURATION
# ============================================================

RANDOM_STATE = 42

TOP_K_WORDS = 5

SCRIPT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

SNAPSHOT_FILE = os.path.join(
    SCRIPT_DIR,
    "clustering_case_snapshot.json"
)

RESULT_FILE = os.path.join(
    SCRIPT_DIR,
    "clustering_evaluation_results.json"
)


# ============================================================
# CASE SNAPSHOTTING
# ============================================================

def create_case_snapshot():
    """
    Read the current representative cases from Supabase and save
    them locally.

    This snapshot freezes the evaluation input so V1 and V2 can
    be compared using exactly the same cases.
    """

    print(
        "\nCreating clustering case snapshot from Supabase..."
    )

    case_ids, documents = _load_cases()

    if not documents:
        raise RuntimeError(
            "No representative cases available for snapshot."
        )

    snapshot = {
        "case_count": len(documents),
        "case_ids": case_ids,
        "documents": documents,
    }

    with open(
        SNAPSHOT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            snapshot,
            file,
            indent=2,
            ensure_ascii=False
        )

    print(
        f"Snapshot saved to:\n{SNAPSHOT_FILE}"
    )

    print(
        f"Cases frozen in snapshot: {len(documents)}"
    )

    return case_ids, documents


def load_case_snapshot(
    refresh=False
):
    """
    Load the frozen case set.

    If no snapshot exists, create one automatically.

    refresh=True deliberately replaces the snapshot.
    """

    if refresh or not os.path.exists(
        SNAPSHOT_FILE
    ):

        return create_case_snapshot()

    print(
        "\nLoading frozen clustering case snapshot..."
    )

    with open(
        SNAPSHOT_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        snapshot = json.load(
            file
        )

    case_ids = snapshot[
        "case_ids"
    ]

    documents = snapshot[
        "documents"
    ]

    if len(case_ids) != len(documents):

        raise RuntimeError(
            "Snapshot is invalid: case_ids and documents "
            "have different lengths."
        )

    print(
        f"Snapshot cases loaded: {len(documents)}"
    )

    print(
        f"Snapshot file:\n{SNAPSHOT_FILE}"
    )

    return case_ids, documents


# ============================================================
# REPRODUCIBLE BERTOPIC
# ============================================================

def run_reproducible_bertopic(
    documents,
    embeddings
):
    """
    Reproduce the CURRENT V1 clustering configuration while fixing
    UMAP's random seed.

    Current production configuration being mirrored:

        all-MiniLM-L6-v2 embeddings
        CountVectorizer:
            stop_words="english"
            ngram_range=(1, 2)
            min_df=2
        BERTopic:
            min_topic_size=3
            calculate_probabilities=True

    IMPORTANT:
    When we build V2, update this function to mirror the refined
    BERTopic/UMAP/HDBSCAN configuration while keeping the SAME
    case snapshot.
    """

    vectorizer_model = CountVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        min_df=2,
    )

    # Fixed seed removes UMAP randomness between evaluation runs.
    #
    # Other parameters are intentionally left close to UMAP's
    # standard BERTopic behaviour for the V1 baseline.
    umap_model = UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        random_state=RANDOM_STATE,
    )

    topic_model = BERTopic(
        embedding_model=None,
        vectorizer_model=vectorizer_model,
        umap_model=umap_model,
        calculate_probabilities=True,
        verbose=True,
        min_topic_size=3,
    )

    topics, probabilities = (
        topic_model.fit_transform(
            documents,
            embeddings
        )
    )

    return (
        topics,
        probabilities,
        topic_model
    )


# ============================================================
# TOPIC COHERENCE
# ============================================================

def compute_coherence(
    topic_model,
    documents,
    topics,
):
    """
    Compute c_v topic coherence.

    Returns:
        overall_coherence
        per_topic_coherence
        unique_topics
    """

    vectorizer = (
        topic_model.vectorizer_model
    )

    analyzer = (
        vectorizer.build_analyzer()
    )

    tokens = [
        analyzer(document)
        for document in documents
    ]

    dictionary = Dictionary(
        tokens
    )

    corpus = [
        dictionary.doc2bow(token_list)
        for token_list in tokens
    ]

    vocabulary = set(
        vectorizer
        .get_feature_names_out()
    )

    unique_topics = sorted(
        topic
        for topic in set(topics)
        if topic != -1
    )

    topic_words = []

    valid_topic_numbers = []

    for topic_num in unique_topics:

        topic_terms = (
            topic_model.get_topic(
                topic_num
            )
        )

        words = [

            word

            for word, _ in topic_terms[
                :TOP_K_WORDS
            ]

            if word in vocabulary
        ]

        if not words:
            continue

        topic_words.append(
            words
        )

        valid_topic_numbers.append(
            topic_num
        )

    if not topic_words:

        return (
            None,
            None,
            []
        )

    coherence_model = CoherenceModel(
        topics=topic_words,
        texts=tokens,
        corpus=corpus,
        dictionary=dictionary,
        coherence="c_v",
        processes=1,
    )

    overall_coherence = (
        coherence_model
        .get_coherence()
    )

    per_topic_coherence = (
        coherence_model
        .get_coherence_per_topic()
    )

    return (
        float(overall_coherence),
        [
            float(score)
            for score
            in per_topic_coherence
        ],
        valid_topic_numbers,
    )


# ============================================================
# TOPIC DIVERSITY
# ============================================================

def compute_topic_diversity(
    topic_model,
    topics,
    top_k=TOP_K_WORDS,
):
    """
    Topic diversity =

        number of UNIQUE top-k topic words
        ----------------------------------
        total number of top-k topic words

    Example:

        10 topics × 5 keywords = 50 total keyword slots

        if only 32 words are unique:

            topic diversity = 32 / 50 = 0.64

    Higher values indicate less keyword repetition between topics.

    Range:
        0 -> highly repetitive topics
        1 -> completely distinct top-k vocabularies
    """

    unique_topics = sorted(
        topic
        for topic in set(topics)
        if topic != -1
    )

    all_topic_words = []

    for topic_num in unique_topics:

        terms = (
            topic_model.get_topic(
                topic_num
            )
        )

        if not terms:
            continue

        words = [
            word
            for word, _
            in terms[:top_k]
        ]

        all_topic_words.extend(
            words
        )

    if not all_topic_words:

        return None

    unique_words = set(
        all_topic_words
    )

    diversity = (
        len(unique_words)
        / len(all_topic_words)
    )

    return float(
        diversity
    )


# ============================================================
# CLUSTER-SIZE DISTRIBUTION
# ============================================================

def compute_cluster_sizes(
    topics
):
    """
    Return document count per BERTopic topic.

    Topic -1 represents BERTopic outliers.
    """

    counts = Counter(
        int(topic)
        for topic in topics
    )

    return dict(
        sorted(
            counts.items(),
            key=lambda item: item[0]
        )
    )


# ============================================================
# IN-MEMORY OUTLIERS
# ============================================================

def compute_in_memory_outliers(
    topics
):
    """
    Calculate outliers from THIS exact BERTopic evaluation run.

    This is more appropriate for V1/V2 comparison than relying only
    on DB state, because the DB may reflect a previous clustering run.
    """

    total = len(
        topics
    )

    outliers = sum(
        1
        for topic in topics
        if topic == -1
    )

    rate = (
        outliers / total
        if total > 0
        else 0
    )

    return (
        outliers,
        rate
    )


# ============================================================
# DISPLAY TOPICS
# ============================================================

def print_topic_details(
    topic_model,
    cluster_sizes,
):
    """
    Print all discovered topics with their size and top keywords.
    """

    normal_topics = [
        topic_num
        for topic_num in cluster_sizes
        if topic_num != -1
    ]

    normal_topics.sort(
        key=lambda topic_num:
            cluster_sizes[topic_num],
        reverse=True,
    )

    print(
        "\n---------- CLUSTER SIZE DISTRIBUTION ----------"
    )

    if -1 in cluster_sizes:

        print(
            f"Outliers (-1): "
            f"{cluster_sizes[-1]} cases"
        )

    for topic_num in normal_topics:

        terms = (
            topic_model.get_topic(
                topic_num
            )
        )

        keywords = [
            word
            for word, _
            in terms[:TOP_K_WORDS]
        ]

        print(
            f"Topic {topic_num:>2}: "
            f"{cluster_sizes[topic_num]:>3} cases | "
            f"{', '.join(keywords)}"
        )


# ============================================================
# MAIN EVALUATION
# ============================================================

def main(
    refresh_snapshot=False
):

    print(
        "\n"
        "========== INSIGHTDESK CLUSTERING EVALUATION =========="
        "\n"
    )

    print(
        f"UMAP random_state: {RANDOM_STATE}"
    )

    print(
        f"Topic diversity top-k: {TOP_K_WORDS}"
    )

    # --------------------------------------------------------
    # STEP 1: Load frozen case set
    # --------------------------------------------------------

    case_ids, documents = (
        load_case_snapshot(
            refresh=refresh_snapshot
        )
    )

    if not documents:

        print(
            "No cases available to cluster."
        )

        return

    # --------------------------------------------------------
    # STEP 2: Generate embeddings
    # --------------------------------------------------------

    print(
        "\nGenerating MiniLM embeddings..."
    )

    embeddings = (
        _generate_embeddings(
            documents
        )
    )

    # --------------------------------------------------------
    # STEP 3: Run reproducible BERTopic
    # --------------------------------------------------------

    print(
        "\nRunning reproducible BERTopic in memory..."
    )

    topics, probabilities, topic_model = (
        run_reproducible_bertopic(
            documents,
            embeddings
        )
    )

    if topic_model is None:

        print(
            "BERTopic did not produce a model."
        )

        return

    # --------------------------------------------------------
    # STEP 4: Number of discovered topics
    # --------------------------------------------------------

    unique_topics = sorted(
        topic
        for topic in set(topics)
        if topic != -1
    )

    number_of_topics = len(
        unique_topics
    )

    # --------------------------------------------------------
    # STEP 5: In-memory outliers
    # --------------------------------------------------------

    (
        in_memory_outlier_count,
        in_memory_outlier_rate,
    ) = compute_in_memory_outliers(
        topics
    )

    # --------------------------------------------------------
    # STEP 6: Cluster size distribution
    # --------------------------------------------------------

    cluster_sizes = (
        compute_cluster_sizes(
            topics
        )
    )

    # --------------------------------------------------------
    # STEP 7: Topic coherence
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

    # --------------------------------------------------------
    # STEP 8: Topic diversity
    # --------------------------------------------------------

    topic_diversity = (
        compute_topic_diversity(
            topic_model,
            topics,
            TOP_K_WORDS,
        )
    )

    # --------------------------------------------------------
    # STEP 9: Current DB clustering statistics
    #
    # These are printed for reference only.
    # V1/V2 comparisons should primarily use the in-memory
    # results generated from the frozen snapshot.
    # --------------------------------------------------------

    try:

        db_stats = (
            get_clustering_statistics()
        )

    except Exception as error:

        print(
            f"\nWarning: Could not retrieve "
            f"DB clustering statistics: {error}"
        )

        db_stats = {}

    # ========================================================
    # RESULTS
    # ========================================================

    print(
        "\n"
        "========== CLUSTERING RESULTS =========="
        "\n"
    )

    print(
        f"Cases evaluated:                 "
        f"{len(documents)}"
    )

    print(
        f"Topics discovered:               "
        f"{number_of_topics}"
    )

    print(
        f"In-memory outlier count:         "
        f"{in_memory_outlier_count}"
    )

    print(
        f"In-memory outlier rate:          "
        f"{in_memory_outlier_rate:.2%}"
    )

    # --------------------------------------------------------
    # DB reference
    # --------------------------------------------------------

    db_outlier_count = (
        db_stats.get(
            "outlier_count"
        )
    )

    db_outlier_rate = (
        db_stats.get(
            "outlier_rate"
        )
    )

    print(
        "\n"
        "Current DB reference:"
    )

    print(
        f"DB outlier count:                "
        f"{db_outlier_count}"
    )

    if (
        db_outlier_rate
        is not None
    ):

        print(
            f"DB outlier rate:                 "
            f"{db_outlier_rate:.2%}"
        )

    else:

        print(
            "DB outlier rate:                 N/A"
        )

    # --------------------------------------------------------
    # Coherence
    # --------------------------------------------------------

    print(
        "\n"
        "---------- TOPIC QUALITY ----------"
    )

    if overall_coherence is not None:

        print(
            f"Overall topic coherence (c_v):   "
            f"{overall_coherence:.4f}"
        )

    else:

        print(
            "Overall topic coherence (c_v):   N/A"
        )

    if topic_diversity is not None:

        print(
            f"Topic diversity @"
            f"{TOP_K_WORDS}:                 "
            f"{topic_diversity:.4f}"
        )

    else:

        print(
            f"Topic diversity @"
            f"{TOP_K_WORDS}:                 N/A"
        )

    # --------------------------------------------------------
    # Per-topic coherence
    # --------------------------------------------------------

    if (
        overall_coherence is not None
        and per_topic_coherence
    ):

        print(
            "\n"
            "Lowest 5 coherence topics:"
        )

        topic_scores = list(
            zip(
                coherence_topic_numbers,
                per_topic_coherence
            )
        )

        topic_scores.sort(
            key=lambda item:
                item[1]
        )

        for (
            topic_num,
            score
        ) in topic_scores[:5]:

            terms = (
                topic_model.get_topic(
                    topic_num
                )
            )

            keywords = [
                word
                for word, _
                in terms[:TOP_K_WORDS]
            ]

            print(
                f"  Topic {topic_num}: "
                f"coherence={score:.4f} | "
                f"{', '.join(keywords)}"
            )

    # --------------------------------------------------------
    # Cluster distribution
    # --------------------------------------------------------

    print_topic_details(
        topic_model,
        cluster_sizes
    )

    # ========================================================
    # SAVE RESULT SNAPSHOT
    # ========================================================

    result_data = {

        "evaluation_version":
            "V1_BASELINE",

        "random_state":
            RANDOM_STATE,

        "case_snapshot":
            os.path.basename(
                SNAPSHOT_FILE
            ),

        "cases_evaluated":
            len(documents),

        "topics_discovered":
            number_of_topics,

        "in_memory_outlier_count":
            in_memory_outlier_count,

        "in_memory_outlier_rate":
            in_memory_outlier_rate,

        "db_outlier_count":
            db_outlier_count,

        "db_outlier_rate":
            db_outlier_rate,

        "overall_coherence_cv":
            overall_coherence,

        "topic_diversity_top_k":
            TOP_K_WORDS,

        "topic_diversity":
            topic_diversity,

        "cluster_sizes":
            {
                str(topic):
                    count
                for topic, count
                in cluster_sizes.items()
            },
    }

    if per_topic_coherence:

        result_data[
            "per_topic_coherence"
        ] = {

            str(topic_num):
                score

            for (
                topic_num,
                score
            ) in zip(
                coherence_topic_numbers,
                per_topic_coherence
            )
        }

    with open(
        RESULT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            result_data,
            file,
            indent=2
        )

    print(
        "\n"
        "========== EVALUATION SAVED =========="
    )

    print(
        f"Case snapshot:\n{SNAPSHOT_FILE}"
    )

    print(
        f"\nEvaluation results:\n{RESULT_FILE}"
    )

    print(
        "\nIMPORTANT:"
    )

    print(
        "Do NOT refresh the case snapshot before V2."
    )

    print(
        "V1 and V2 must use the exact same case set "
        "for a defensible comparison."
    )

    print(
        "\n========================================"
    )


# ============================================================
# COMMAND LINE
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--refresh-snapshot",
        action="store_true",
        help=(
            "Replace the frozen case snapshot "
            "with the current Supabase case set. "
            "Do NOT use this between V1 and V2."
        )
    )

    args = parser.parse_args()

    main(
        refresh_snapshot=(
            args.refresh_snapshot
        )
    )