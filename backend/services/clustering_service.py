from datetime import datetime, timezone
from typing import Dict

from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from umap import UMAP
from hdbscan import HDBSCAN

from backend.database.supabase import (
    get_all_cases,
    get_clustering_metadata,
    get_clustering_statistics,
    update_clustering_metadata,
    create_topic,
    get_topic_by_slug,
    update_topic,
    update_case_topic,
)

from backend.services.department_service import (
    assign_department,
    assign_department_by_keywords,
)


# ============================================================
# FINAL CLUSTERING CONFIGURATION
# ============================================================

CLUSTER_GROWTH_THRESHOLD = 0.20
OUTLIER_THRESHOLD = 0.15
MAX_CLUSTER_AGE_DAYS = 7

# Final frozen configuration from clustering evaluation
N_NEIGHBORS = 12
N_COMPONENTS = 5
MIN_DIST = 0.0

MIN_CLUSTER_SIZE = 3
MIN_SAMPLES = 1

RANDOM_STATE = 42


# ============================================================
# DOMAIN STOPWORDS
# ============================================================

DOMAIN_STOPWORDS = {
    "vision",
    "helpdesk",
    "software",
    "product",
    "overall",
}

STOP_WORDS = list(
    set(ENGLISH_STOP_WORDS).union(
        DOMAIN_STOPWORDS
    )
)


# ============================================================
# EMBEDDING MODEL
# ============================================================

embedding_model = SentenceTransformer(
    "all-MiniLM-L6-v2"
)


# ============================================================
# VECTORIZER
# ============================================================

vectorizer_model = CountVectorizer(
    stop_words=STOP_WORDS,
    ngram_range=(1, 2),
    min_df=2,
)


# ============================================================
# RECLUSTERING DECISION
# ============================================================

def _should_recluster(force: bool = False):
    """
    Decide whether BERTopic clustering should run.

    Returns
    -------
    tuple[bool, str]
    """

    if force:
        return True, "FORCE"

    metadata = get_clustering_metadata()
    stats = get_clustering_statistics()

    current_cases = stats["case_count"]
    current_outlier_rate = stats["outlier_rate"]

    last_case_count = metadata["last_case_count"]

    # First clustering
    if last_case_count == 0:
        return True, "FIRST_RUN"

    # Case growth
    growth = (
        current_cases - last_case_count
    ) / last_case_count

    if growth >= CLUSTER_GROWTH_THRESHOLD:
        return True, "CASE_GROWTH"

    # Outlier rate
    if current_outlier_rate >= OUTLIER_THRESHOLD:
        return True, "OUTLIER_RATE"

    # Scheduled refresh
    last_clustered = metadata["last_clustered_at"]

    if last_clustered:

        if isinstance(last_clustered, str):

            last_clustered = datetime.fromisoformat(
                last_clustered.replace(
                    "Z",
                    "+00:00"
                )
            )

        days_since_cluster = (
            datetime.now(timezone.utc)
            - last_clustered
        ).days

        if days_since_cluster >= MAX_CLUSTER_AGE_DAYS:
            return True, "SCHEDULED_REFRESH"

    return False, "SKIP"


# ============================================================
# LOAD CASES
# ============================================================

def _load_cases():
    """
    Load representative case texts from Supabase.

    Returns
    -------
    tuple[list[int], list[str]]
    """

    cases = get_all_cases()

    if not cases:
        return [], []

    case_ids = []
    documents = []

    for case in cases:

        case_id = case.get("case_id")
        text = case.get("representative_text")

        if case_id is None:
            continue

        if text is None:
            continue

        text = text.strip()

        if not text:
            continue

        case_ids.append(case_id)
        documents.append(text)

    return case_ids, documents


# ============================================================
# EMBEDDINGS
# ============================================================

def _generate_embeddings(documents):
    """
    Generate normalized MiniLM embeddings from ORIGINAL
    representative case text.

    Important:
    The final frozen clustering configuration does NOT
    perform template phrase cleaning before embedding.
    """

    if not documents:
        return []

    try:

        embeddings = embedding_model.encode(
            documents,
            batch_size=32,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        return embeddings

    except Exception as e:

        raise RuntimeError(
            f"Embedding generation failed: {e}"
        ) from e


# ============================================================
# BERTOPIC
# ============================================================

def _run_bertopic(
    documents,
    embeddings
):
    """
    Run the final frozen BERTopic Config 8.

    Final configuration:
        clean_text = False
        domain_stopwords = True
        n_neighbors = 12
        min_cluster_size = 3
        min_samples = 1
    """

    if not documents:
        return [], [], None

    # --------------------------------------------------------
    # Fresh stopword set
    # EXACTLY as used in tune_clustering.py
    # --------------------------------------------------------

    stop_words = list(
        set(ENGLISH_STOP_WORDS)
        | set(DOMAIN_STOPWORDS)
    )

    # --------------------------------------------------------
    # Fresh vectorizer
    # EXACTLY as used in Config 8
    # --------------------------------------------------------

    vectorizer_model = CountVectorizer(
        stop_words=stop_words,
        ngram_range=(1, 2),
        min_df=2,
    )

    # --------------------------------------------------------
    # UMAP
    # --------------------------------------------------------

    umap_model = UMAP(
        n_neighbors=12,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        random_state=42,
    )

    # --------------------------------------------------------
    # HDBSCAN
    # --------------------------------------------------------

    hdbscan_model = HDBSCAN(
        min_cluster_size=3,
        min_samples=1,
        metric="euclidean",
        cluster_selection_method="eom",
        prediction_data=True,
    )

    # --------------------------------------------------------
    # BERTopic
    # --------------------------------------------------------

    topic_model = BERTopic(
        embedding_model=None,
        vectorizer_model=vectorizer_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        calculate_probabilities=True,
        verbose=False,
    )

    # --------------------------------------------------------
    # Fit using ORIGINAL documents + original embeddings
    # Config 8 has clean_text=False
    # --------------------------------------------------------

    topics, probabilities = topic_model.fit_transform(
        documents,
        embeddings
    )

    return (
        topics,
        probabilities,
        topic_model,
    )
# ============================================================
# PROCESS TOPICS
# ============================================================

def _process_topics(
    topic_model,
    clustering_run_id: int
) -> Dict[int, int]:
    """
    Convert BERTopic topic numbers into database topic IDs.
    """

    topic_mapping = {}

    topic_info = (
        topic_model.get_topic_info()
    )

    for _, row in topic_info.iterrows():

        topic_number = int(
            row["Topic"]
        )

        # BERTopic noise/outlier
        if topic_number == -1:

            topic_mapping[-1] = None
            continue

        words = topic_model.get_topic(
            topic_number
        )

        if not words:
            topic_mapping[
                topic_number
            ] = None

            continue

        keywords = [
            word
            for word, _
            in words[:5]
            if word
        ]

        if not keywords:

            topic_mapping[
                topic_number
            ] = None

            continue

        # ----------------------------------------------------
        # Topic metadata
        # ----------------------------------------------------

        topic_name = " + ".join(
            keywords[:3]
        ).title()

        topic_slug = "_".join(
            keywords[:3]
        ).lower()

        description = ", ".join(
            keywords
        )

        # ----------------------------------------------------
        # Department assignment
        # ----------------------------------------------------

        try:

            department = (
                assign_department(
                    keywords
                )
            )

        except Exception:

            department = (
                assign_department_by_keywords(
                    keywords
                )
            )

        # ----------------------------------------------------
        # Existing topic
        # ----------------------------------------------------

        existing_topic = (
            get_topic_by_slug(
                topic_slug
            )
        )

        if existing_topic:

            topic_id = existing_topic[
                "topic_id"
            ]

            update_topic(
                topic_id=topic_id,
                topic_name=topic_name,
                description=description,
                department=department,
                clustering_run_id=clustering_run_id,
                updated_at=datetime.now(
                    timezone.utc
                ).isoformat(),
            )

        # ----------------------------------------------------
        # New topic
        # ----------------------------------------------------

        else:

            try:

                new_topic = create_topic(
                    topic_name=topic_name,
                    topic_slug=topic_slug,
                    department=department,
                    description=description,
                    clustering_run_id=clustering_run_id,
                )

                topic_id = new_topic[
                    "topic_id"
                ]

            except Exception as e:

                print(
                    f"Error creating topic "
                    f"{topic_number}: {e}"
                )

                topic_id = None

        topic_mapping[
            topic_number
        ] = topic_id

    return topic_mapping


# ============================================================
# UPDATE CASE TOPICS
# ============================================================

def _update_database(
    case_ids,
    topics,
    topic_mapping
):
    """
    Update each case with its database topic ID.

    BERTopic outliers receive topic_id=None.
    """

    updated_cases = 0

    for case_id, bertopic_topic in zip(
        case_ids,
        topics
    ):

        bertopic_topic = int(
            bertopic_topic
        )

        db_topic_id = (
            topic_mapping.get(
                bertopic_topic
            )
        )

        update_case_topic(
            case_id=case_id,
            topic_id=db_topic_id
        )

        updated_cases += 1

    return updated_cases


# ============================================================
# UPDATE METADATA
# ============================================================

def _update_metadata(
    topic_mapping
):
    """
    Update clustering metadata after successful clustering.
    """

    metadata = (
        get_clustering_metadata()
    )

    stats = (
        get_clustering_statistics()
    )

    current_version = (
        metadata["clustering_version"]
        or 0
    )

    total_topics = len(
        [
            topic
            for topic in topic_mapping
            if topic != -1
        ]
    )

    update_clustering_metadata(
        last_case_count=stats[
            "case_count"
        ],
        total_topics=total_topics,
        outlier_rate=stats[
            "outlier_rate"
        ],
        clustering_version=(
            current_version + 1
        ),
    )


# ============================================================
# MAIN CLUSTERING PIPELINE
# ============================================================

def cluster_cases(
    force: bool = False
):
    """
    Execute the complete production BERTopic pipeline.
    """

    # --------------------------------------------------------
    # Should clustering run?
    # --------------------------------------------------------

    should_cluster, reason = (
        _should_recluster(
            force
        )
    )

    if not should_cluster:

        return {
            "status": "skipped",
            "reason": reason,
        }

    # --------------------------------------------------------
    # Load cases
    # --------------------------------------------------------

    case_ids, documents = (
        _load_cases()
    )

    if not documents:

        return {
            "status": "skipped",
            "reason":
                "No representative cases found.",
        }

    # --------------------------------------------------------
    # Embeddings
    # --------------------------------------------------------

    embeddings = (
        _generate_embeddings(
            documents
        )
    )

    # --------------------------------------------------------
    # BERTopic
    # --------------------------------------------------------

    (
        topics,
        probabilities,
        topic_model,
    ) = _run_bertopic(
        documents,
        embeddings
    )

    # --------------------------------------------------------
    # Clustering version
    # --------------------------------------------------------

    metadata = (
        get_clustering_metadata()
    )

    clustering_run_id = (
        metadata[
            "clustering_version"
        ]
        or 0
    ) + 1

    # --------------------------------------------------------
    # Topics
    # --------------------------------------------------------

    topic_mapping = (
        _process_topics(
            topic_model,
            clustering_run_id
        )
    )

    # --------------------------------------------------------
    # Cases
    # --------------------------------------------------------

    updated_cases = (
        _update_database(
            case_ids,
            topics,
            topic_mapping
        )
    )

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    _update_metadata(
        topic_mapping
    )

    # --------------------------------------------------------
    # Result
    # --------------------------------------------------------

    total_topics = len(
        [
            topic
            for topic
            in topic_mapping
            if topic != -1
        ]
    )

    outlier_count = sum(
        1
        for topic in topics
        if topic == -1
    )

    return {
        "status": "success",
        "reason": reason,
        "cases_clustered":
            updated_cases,
        "topics_created":
            total_topics,
        "outliers":
            outlier_count,
        "clustering_version":
            clustering_run_id,
    }