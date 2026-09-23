"""
Tune InsightDesk BERTopic clustering in-memory.

This script uses the SAME frozen case snapshot created by
evaluate_clustering.py so V1 and V2 are evaluated on identical input.

Nothing is written to Supabase.

Metrics:
- number of topics
- outlier count/rate
- c_v coherence
- topic diversity@5
- largest cluster size/share
- simple ranking score

The goal is NOT exhaustive hyperparameter search.
This is a fast, controlled refinement experiment for the current
small dataset (~71 representative cases).

Usage:
    python -m backend.scripts.tune_clustering
"""

import os
import re
import csv

from collections import Counter

from bertopic import BERTopic

from sklearn.feature_extraction.text import (
    CountVectorizer,
    ENGLISH_STOP_WORDS,
)

from umap import UMAP
from hdbscan import HDBSCAN

from backend.scripts.evaluate_clustering import (
    load_case_snapshot,
    compute_coherence,
    compute_topic_diversity,
)

from backend.services.clustering_service import (
    _generate_embeddings,
)


# ============================================================
# CONFIGURATION
# ============================================================

RANDOM_STATE = 42

TOP_K_WORDS = 5

SCRIPT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

RESULT_FILE = os.path.join(
    SCRIPT_DIR,
    "clustering_tuning_results.csv"
)


# ============================================================
# CLUSTERING-SPECIFIC NOISE
# ============================================================

NOISE_PHRASES = [

    "vision helpdesk",

    "vision help desk",

    "expected much better reliability",

    "this has impacted our daily operations",

    "impacted our daily operations",

    "it s very frustrating to deal with",

    "very frustrating to deal with",

    "overall experience",

    "overall rating",
]


DOMAIN_STOPWORDS = [

    "vision",

    "helpdesk",

    "software",

    "product",

    "overall",
]


# ============================================================
# CLUSTERING-SPECIFIC CLEANING
# ============================================================

def clean_for_clustering(text):
    """
    Remove known product/domain boilerplate and repeated
    synthetic/template phrases.

    IMPORTANT:
    This is only for topic modelling experiments.
    It does not modify the duplicate-detection preprocessing.
    """

    if not text:
        return ""

    cleaned = text.lower()

    # --------------------------------------------------------
    # Remove repeated phrases
    # --------------------------------------------------------

    for phrase in NOISE_PHRASES:

        cleaned = cleaned.replace(
            phrase,
            " "
        )

    # --------------------------------------------------------
    # Normalize whitespace
    # --------------------------------------------------------

    cleaned = re.sub(
        r"\s+",
        " ",
        cleaned
    )

    return cleaned.strip()


# ============================================================
# STOPWORD BUILDER
# ============================================================

def build_stopwords(
    use_domain_stopwords
):
    """
    Use ordinary English stopwords or English + InsightDesk
    domain-specific stopwords.
    """

    if not use_domain_stopwords:

        return "english"

    return list(

        set(
            ENGLISH_STOP_WORDS
        )

        | set(
            DOMAIN_STOPWORDS
        )
    )


# ============================================================
# RUN ONE CONFIGURATION
# ============================================================

def run_configuration(
    documents,
    embeddings,
    config
):

    # --------------------------------------------------------
    # Optional clustering-specific cleaning
    # --------------------------------------------------------

    if config[
        "clean_text"
    ]:

        working_documents = [

            clean_for_clustering(
                document
            )

            for document
            in documents
        ]

    else:

        working_documents = list(
            documents
        )

    # --------------------------------------------------------
    # Stopwords
    # --------------------------------------------------------

    stop_words = build_stopwords(
        config[
            "domain_stopwords"
        ]
    )

    # --------------------------------------------------------
    # Vectorizer
    # --------------------------------------------------------

    vectorizer_model = CountVectorizer(

        stop_words=stop_words,

        ngram_range=(
            1,
            2
        ),

        min_df=2,
    )

    # --------------------------------------------------------
    # UMAP
    # --------------------------------------------------------

    umap_model = UMAP(

        n_neighbors=
            config[
                "n_neighbors"
            ],

        n_components=5,

        min_dist=0.0,

        metric="cosine",

        random_state=
            RANDOM_STATE,
    )

    # --------------------------------------------------------
    # HDBSCAN
    # --------------------------------------------------------

    hdbscan_model = HDBSCAN(

        min_cluster_size=
            config[
                "min_cluster_size"
            ],

        min_samples=
            config[
                "min_samples"
            ],

        metric="euclidean",

        cluster_selection_method=
            "eom",

        prediction_data=True,
    )

    # --------------------------------------------------------
    # BERTopic
    # --------------------------------------------------------

    topic_model = BERTopic(

        embedding_model=None,

        vectorizer_model=
            vectorizer_model,

        umap_model=
            umap_model,

        hdbscan_model=
            hdbscan_model,

        calculate_probabilities=True,

        verbose=False,
    )

    # --------------------------------------------------------
    # Fit
    # --------------------------------------------------------

    topics, probabilities = (
        topic_model.fit_transform(
            working_documents,
            embeddings
        )
    )

    # --------------------------------------------------------
    # Topic count
    # --------------------------------------------------------

    normal_topics = [

        topic

        for topic
        in set(topics)

        if topic != -1
    ]

    topic_count = len(
        normal_topics
    )

    # --------------------------------------------------------
    # Outliers
    # --------------------------------------------------------

    outlier_count = sum(

        1

        for topic
        in topics

        if topic == -1
    )

    outlier_rate = (

        outlier_count
        / len(topics)

        if topics

        else 0
    )

    # --------------------------------------------------------
    # Cluster-size distribution
    # --------------------------------------------------------

    cluster_sizes = Counter(

        topic

        for topic
        in topics

        if topic != -1
    )

    largest_cluster = (

        max(
            cluster_sizes.values()
        )

        if cluster_sizes

        else 0
    )

    largest_cluster_share = (

        largest_cluster
        / len(topics)

        if topics

        else 0
    )

    # --------------------------------------------------------
    # Coherence
    # --------------------------------------------------------

    try:

        (
            coherence,
            _,
            _
        ) = compute_coherence(

            topic_model,
            working_documents,
            topics
        )

    except Exception as error:

        print(
            f"  Coherence failed: "
            f"{error}"
        )

        coherence = None

    # --------------------------------------------------------
    # Topic diversity
    # --------------------------------------------------------

    try:

        diversity = (
            compute_topic_diversity(
                topic_model,
                topics,
                top_k=TOP_K_WORDS
            )
        )

    except Exception as error:

        print(
            f"  Diversity failed: "
            f"{error}"
        )

        diversity = None

    # --------------------------------------------------------
    # Result
    # --------------------------------------------------------

    return {

        **config,

        "topics":
            topic_count,

        "outliers":
            outlier_count,

        "outlier_rate":
            outlier_rate,

        "coherence_cv":
            coherence,

        "topic_diversity":
            diversity,

        "largest_cluster":
            largest_cluster,

        "largest_cluster_share":
            largest_cluster_share,

        "_model":
            topic_model,

        "_topics":
            topics,

        "_documents":
            working_documents,
    }


# ============================================================
# SIMPLE SHORTLISTING SCORE
# ============================================================

def calculate_score(
    result
):
    """
    Shortlisting heuristic.

    Rewards:
    - coherence
    - topic diversity

    Penalizes:
    - high outlier rate
    - one oversized cluster

    IMPORTANT:
    This score is only used to shortlist configurations.
    Final V2 selection still requires semantic inspection.
    """

    coherence = (
        result[
            "coherence_cv"
        ]
        or 0
    )

    diversity = (
        result[
            "topic_diversity"
        ]
        or 0
    )

    outlier_penalty = (
        result[
            "outlier_rate"
        ]
    )

    largest_share = (
        result[
            "largest_cluster_share"
        ]
    )

    cluster_penalty = max(

        0,

        largest_share
        - 0.40
    )

    score = (

        0.45
        * coherence

        + 0.25
        * diversity

        - 0.20
        * outlier_penalty

        - 0.10
        * cluster_penalty
    )

    return score


# ============================================================
# FAST CONTROLLED CONFIGURATIONS
# ============================================================

def get_configs():
    """
    Eight strategically chosen configurations.

    1-6:
        phrase cleaning + domain stopwords,
        while varying UMAP/HDBSCAN parameters.

    7:
        phrase cleaning only.

    8:
        domain stopwords only.

    This also gives a small preprocessing ablation study.
    """

    return [

        # ----------------------------------------------------
        # Config 1
        # ----------------------------------------------------

        {
            "config_id": 1,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 15,

            "min_cluster_size": 3,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 2
        # ----------------------------------------------------

        {
            "config_id": 2,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 15,

            "min_cluster_size": 3,

            "min_samples": 2,
        },

        # ----------------------------------------------------
        # Config 3
        # ----------------------------------------------------

        {
            "config_id": 3,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 12,

            "min_cluster_size": 3,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 4
        # ----------------------------------------------------

        {
            "config_id": 4,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 12,

            "min_cluster_size": 4,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 5
        # ----------------------------------------------------

        {
            "config_id": 5,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 8,

            "min_cluster_size": 3,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 6
        # ----------------------------------------------------

        {
            "config_id": 6,

            "clean_text": True,

            "domain_stopwords": True,

            "n_neighbors": 8,

            "min_cluster_size": 4,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 7
        #
        # Cleaning only
        # ----------------------------------------------------

        {
            "config_id": 7,

            "clean_text": True,

            "domain_stopwords": False,

            "n_neighbors": 12,

            "min_cluster_size": 3,

            "min_samples": 1,
        },

        # ----------------------------------------------------
        # Config 8
        #
        # Domain stopwords only
        # ----------------------------------------------------

        {
            "config_id": 8,

            "clean_text": False,

            "domain_stopwords": True,

            "n_neighbors": 12,

            "min_cluster_size": 3,

            "min_samples": 1,
        },
    ]


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "\n"
        "========== INSIGHTDESK FAST CLUSTERING TUNING =========="
        "\n"
    )

    # --------------------------------------------------------
    # Load SAME frozen snapshot
    # --------------------------------------------------------

    _, documents = load_case_snapshot(
        refresh=False
    )

    if not documents:

        print(
            "No frozen clustering cases found."
        )

        return

    print(
        f"Frozen cases: "
        f"{len(documents)}"
    )

    # --------------------------------------------------------
    # Generate embeddings ONCE
    # --------------------------------------------------------

    print(
        "\nGenerating MiniLM embeddings once..."
    )

    embeddings = (
        _generate_embeddings(
            documents
        )
    )

    # --------------------------------------------------------
    # Get fast configurations
    # --------------------------------------------------------

    configs = get_configs()

    print(
        f"\nConfigurations to test: "
        f"{len(configs)}\n"
    )

    results = []

    # --------------------------------------------------------
    # Run configurations
    # --------------------------------------------------------

    for index, config in enumerate(
        configs,
        start=1
    ):

        print(
            f"[{index}/{len(configs)}] "
            f"Testing Config "
            f"{config['config_id']}..."
        )

        try:

            result = run_configuration(

                documents,
                embeddings,
                config
            )

            result[
                "ranking_score"
            ] = calculate_score(
                result
            )

            results.append(
                result
            )

            print(
                f"  Topics: "
                f"{result['topics']}"
            )

            print(
                f"  Outlier rate: "
                f"{result['outlier_rate']:.2%}"
            )

            print(
                f"  Coherence: "
                f"{result['coherence_cv']:.4f}"
                if result[
                    "coherence_cv"
                ] is not None
                else "  Coherence: N/A"
            )

            print(
                f"  Diversity: "
                f"{result['topic_diversity']:.4f}"
                if result[
                    "topic_diversity"
                ] is not None
                else "  Diversity: N/A"
            )

            print()

        except Exception as error:

            print(
                f"  FAILED: "
                f"{error}\n"
            )

    # --------------------------------------------------------
    # Sort results
    # --------------------------------------------------------

    results.sort(

        key=lambda item:
            item[
                "ranking_score"
            ],

        reverse=True
    )

    # ========================================================
    # PRINT ALL CONFIGURATIONS
    # ========================================================

    print(
        "\n"
        "========== CONFIGURATION RANKING =========="
        "\n"
    )

    for rank, result in enumerate(
        results,
        start=1
    ):

        print(
            f"Rank {rank} | "
            f"Config "
            f"{result['config_id']}"
        )

        print(
            f"  clean_text:          "
            f"{result['clean_text']}"
        )

        print(
            f"  domain_stopwords:    "
            f"{result['domain_stopwords']}"
        )

        print(
            f"  n_neighbors:         "
            f"{result['n_neighbors']}"
        )

        print(
            f"  min_cluster_size:    "
            f"{result['min_cluster_size']}"
        )

        print(
            f"  min_samples:         "
            f"{result['min_samples']}"
        )

        print(
            f"  topics:              "
            f"{result['topics']}"
        )

        print(
            f"  outliers:            "
            f"{result['outliers']}"
        )

        print(
            f"  outlier_rate:        "
            f"{result['outlier_rate']:.2%}"
        )

        if result[
            "coherence_cv"
        ] is not None:

            print(
                f"  coherence c_v:       "
                f"{result['coherence_cv']:.4f}"
            )

        if result[
            "topic_diversity"
        ] is not None:

            print(
                f"  diversity@5:         "
                f"{result['topic_diversity']:.4f}"
            )

        print(
            f"  largest cluster:     "
            f"{result['largest_cluster']}"
        )

        print(
            f"  largest share:       "
            f"{result['largest_cluster_share']:.2%}"
        )

        print(
            f"  ranking score:       "
            f"{result['ranking_score']:.4f}"
        )

        print()

    # ========================================================
    # SAVE CSV
    # ========================================================

    with open(

        RESULT_FILE,

        "w",

        encoding="utf-8",

        newline=""

    ) as file:

        fieldnames = [

            "rank",

            "config_id",

            "clean_text",

            "domain_stopwords",

            "n_neighbors",

            "min_cluster_size",

            "min_samples",

            "topics",

            "outliers",

            "outlier_rate",

            "coherence_cv",

            "topic_diversity",

            "largest_cluster",

            "largest_cluster_share",

            "ranking_score",
        ]

        writer = csv.DictWriter(

            file,

            fieldnames=
                fieldnames
        )

        writer.writeheader()

        for rank, result in enumerate(
            results,
            start=1
        ):

            writer.writerow(
                {

                    "rank":
                        rank,

                    "config_id":
                        result[
                            "config_id"
                        ],

                    "clean_text":
                        result[
                            "clean_text"
                        ],

                    "domain_stopwords":
                        result[
                            "domain_stopwords"
                        ],

                    "n_neighbors":
                        result[
                            "n_neighbors"
                        ],

                    "min_cluster_size":
                        result[
                            "min_cluster_size"
                        ],

                    "min_samples":
                        result[
                            "min_samples"
                        ],

                    "topics":
                        result[
                            "topics"
                        ],

                    "outliers":
                        result[
                            "outliers"
                        ],

                    "outlier_rate":
                        result[
                            "outlier_rate"
                        ],

                    "coherence_cv":
                        result[
                            "coherence_cv"
                        ],

                    "topic_diversity":
                        result[
                            "topic_diversity"
                        ],

                    "largest_cluster":
                        result[
                            "largest_cluster"
                        ],

                    "largest_cluster_share":
                        result[
                            "largest_cluster_share"
                        ],

                    "ranking_score":
                        result[
                            "ranking_score"
                        ],
                }
            )

    print(
        f"\nFull tuning results saved to:\n"
        f"{RESULT_FILE}"
    )

    # ========================================================
    # PRINT BEST CONFIGURATION TOPICS
    # ========================================================

    if not results:

        return

    best = results[
        0
    ]

    print(
        "\n"
        "========== BEST CONFIGURATION =========="
        "\n"
    )

    print(
        f"Config ID: "
        f"{best['config_id']}"
    )

    print(
        f"Topics: "
        f"{best['topics']}"
    )

    print(
        f"Outlier rate: "
        f"{best['outlier_rate']:.2%}"
    )

    print(
        f"Coherence: "
        f"{best['coherence_cv']:.4f}"
    )

    print(
        f"Diversity@5: "
        f"{best['topic_diversity']:.4f}"
    )

    print(
        "\n"
        "========== BEST CONFIG TOPICS =========="
        "\n"
    )

    topic_model = best[
        "_model"
    ]

    topics = best[
        "_topics"
    ]

    documents_used = best[
        "_documents"
    ]

    topic_counts = Counter(
        topics
    )

    # --------------------------------------------------------
    # Print each non-outlier topic
    # --------------------------------------------------------

    for topic_num in sorted(
        set(topics)
    ):

        if topic_num == -1:
            continue

        keywords = [

            word

            for word, _
            in topic_model.get_topic(
                topic_num
            )[:8]
        ]

        print(
            f"Topic {topic_num} | "
            f"{topic_counts[topic_num]} cases"
        )

        print(
            "Keywords: "
            + ", ".join(
                keywords
            )
        )

        # ----------------------------------------------------
        # Representative examples
        # ----------------------------------------------------

        examples = [

            documents_used[
                index
            ]

            for index, assigned_topic
            in enumerate(topics)

            if assigned_topic
            == topic_num

        ][:3]

        for example in examples:

            print(
                f"  - {example}"
            )

        print()

    # --------------------------------------------------------
    # Outlier examples
    # --------------------------------------------------------

    outlier_examples = [

        documents_used[
            index
        ]

        for index, assigned_topic
        in enumerate(topics)

        if assigned_topic
        == -1

    ]

    print(
        "========== BEST CONFIG OUTLIERS =========="
    )

    print(
        f"Total outliers: "
        f"{len(outlier_examples)}"
    )

    for example in outlier_examples[
        :10
    ]:

        print(
            f"  - {example}"
        )

    print(
        "\n============================================"
    )

    print(
        "\nIMPORTANT:"
    )

    print(
        "Do not modify production clustering yet."
    )

    print(
        "Compare the top configuration against V1 "
        "and inspect its topic semantics first."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()