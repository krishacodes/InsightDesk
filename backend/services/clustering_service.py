from datetime import datetime, timezone

from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

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
# -------------------------------
# Clustering Configuration
# -------------------------------

CLUSTER_GROWTH_THRESHOLD = 0.20      # 20%
OUTLIER_THRESHOLD = 0.15             # 15%
MAX_CLUSTER_AGE_DAYS = 7
# -------------------------------
# Embedding Model
# -------------------------------

embedding_model = SentenceTransformer(
    "all-MiniLM-L6-v2"
)
def _should_recluster(force: bool = False):
    """
    Decide whether BERTopic clustering should be executed.

    Returns
    -------
    (bool, str)

    Example

    (True, "CASE_GROWTH")

    (False, "SKIP")
    """

    # -----------------------------------
    # Manual override
    # -----------------------------------

    if force:
        return True, "FORCE"

    metadata = get_clustering_metadata()

    stats = get_clustering_statistics()

    current_cases = stats["case_count"]

    current_outlier_rate = stats["outlier_rate"]

    last_case_count = metadata["last_case_count"]

    # -----------------------------------
    # First clustering
    # -----------------------------------

    if last_case_count == 0:
        return True, "FIRST_RUN"

    # -----------------------------------
    # Case growth
    # -----------------------------------

    growth = (
        current_cases - last_case_count
    ) / last_case_count

    if growth >= CLUSTER_GROWTH_THRESHOLD:
        return True, "CASE_GROWTH"

    # -----------------------------------
    # Outlier rate
    # -----------------------------------

    if current_outlier_rate >= OUTLIER_THRESHOLD:
        return True, "OUTLIER_RATE"

    # -----------------------------------
    # Weekly refresh
    # -----------------------------------

    last_clustered = metadata["last_clustered_at"]

    if last_clustered:

        if isinstance(last_clustered, str):
            last_clustered = datetime.fromisoformat(
                last_clustered.replace("Z", "+00:00")
            )

        days_since_cluster = (
            datetime.now(timezone.utc) - last_clustered
        ).days

        if days_since_cluster >= MAX_CLUSTER_AGE_DAYS:
            return True, "SCHEDULED_REFRESH"

    return False, "SKIP"
def _load_cases():
    """
    Load representative cases from the database for BERTopic clustering.

    Returns
    -------
    tuple[list[int], list[str]]
        case_ids  : List of case IDs
        documents : List of representative complaint texts
    """

    cases = get_all_cases()

    if not cases:
        return [], []

    case_ids = []
    documents = []

    for case in cases:
        case_id = case.get("case_id")
        text = case.get("representative_text")

        # Skip invalid records
        if text is None:
            continue

        text = text.strip()

        if text == "":
            continue

        case_ids.append(case_id)
        documents.append(text)

    return case_ids, documents
def _generate_embeddings(documents):
    """
    Generate embeddings for representative case texts.
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
        )
def _run_bertopic(documents, embeddings):
    """
    Run BERTopic on representative case embeddings.

    Parameters
    ----------
    documents : list[str]

    embeddings : numpy.ndarray

    Returns
    -------
    topics
    probabilities
    topic_model
    """

    if not documents:
        return [], [], None

    topic_model = BERTopic(
        embedding_model=None,
        calculate_probabilities=True,
        verbose=True,
        min_topic_size=2,
    )

    topics, probabilities = topic_model.fit_transform(
        documents,
        embeddings
    )

    return topics, probabilities, topic_model
from typing import Dict
from datetime import datetime

from backend.database.supabase import (
    get_topic_by_slug,
    create_topic,
    update_topic,
)

from backend.services.department_service import (
    assign_department,
    assign_department_by_keywords,
)


def _process_topics(topic_model, clustering_run_id: int) -> Dict[int, int]:
    """
    Process BERTopic topics and synchronize them with the database.

    Parameters
    ----------
    topic_model : BERTopic
        Trained BERTopic model.

    clustering_run_id : int
        ID of the current clustering run.

    Returns
    -------
    dict
        Mapping:
        BERTopic Topic Number -> Database topic_id
    """

    topic_mapping = {}

    topic_info = topic_model.get_topic_info()

    for _, row in topic_info.iterrows():

        topic_number = row["Topic"]

        # -------------------------------------------------
        # Skip BERTopic outliers
        # -------------------------------------------------

        if topic_number == -1:
            topic_mapping[-1] = None
            continue

        # -------------------------------------------------
        # Extract keywords
        # -------------------------------------------------

        words = topic_model.get_topic(topic_number)

        keywords = [word for word, _ in words[:5]]

        # -------------------------------------------------
        # Generate metadata
        # -------------------------------------------------

        topic_name = " + ".join(keywords[:2]).title()

        topic_slug = "_".join(keywords[:3]).lower()

        description = ", ".join(keywords)

        # -------------------------------------------------
        # Department Assignment
        # -------------------------------------------------

        try:
            department = assign_department(keywords)

        except Exception:

            department = assign_department_by_keywords(keywords)

        # -------------------------------------------------
        # Check if topic already exists
        # -------------------------------------------------

        existing_topic = get_topic_by_slug(topic_slug)

        # -------------------------------------------------
        # Existing Topic
        # -------------------------------------------------

        if existing_topic:

            topic_id = existing_topic["topic_id"]

            update_topic(
                topic_id=topic_id,
                topic_name=topic_name,
                description=description,
                department=department,
                clustering_run_id=clustering_run_id,
                updated_at=datetime.utcnow().isoformat(),
            )

        # -------------------------------------------------
        # New Topic
        # -------------------------------------------------

        else:

            new_topic = create_topic(
                topic_name=topic_name,
                topic_slug=topic_slug,
                department=department,
                description=description,
                clustering_run_id=clustering_run_id,
            )

            topic_id = new_topic["topic_id"]

        # -------------------------------------------------
        # Store Mapping
        # -------------------------------------------------

        topic_mapping[topic_number] = topic_id

    return topic_mapping
def _update_database(case_ids, topics, topic_mapping):
    """
    Update each case with its assigned database topic_id.

    Parameters
    ----------
    case_ids : list[int]
    topics : list[int]
    topic_mapping : dict
    """

    updated_cases = 0

    for case_id, bertopic_topic in zip(case_ids, topics):

        db_topic_id = topic_mapping.get(bertopic_topic)

        update_case_topic(
            case_id=case_id,
            topic_id=db_topic_id
        )

        updated_cases += 1

    return updated_cases
def _update_metadata(topic_mapping):
    """
    Update clustering metadata after a successful clustering run.
    """

    metadata = get_clustering_metadata()
    stats = get_clustering_statistics()

    current_version = metadata["clustering_version"] or 0

    total_topics = len(
        [topic for topic in topic_mapping.keys() if topic != -1]
    )

    update_clustering_metadata(
        last_case_count=stats["case_count"],
        total_topics=total_topics,
        outlier_rate=stats["outlier_rate"],
        clustering_version=current_version + 1
    )
def cluster_cases(force: bool = False):
    """
    Execute the complete BERTopic clustering pipeline.
    """

    # ----------------------------------
    # Check whether clustering is needed
    # ----------------------------------

    should_cluster, reason = _should_recluster(force)

    if not should_cluster:
        return {
            "status": "skipped",
            "reason": reason
        }

    # ----------------------------------
    # Load representative cases
    # ----------------------------------

    case_ids, documents = _load_cases()

    if not documents:
        return {
            "status": "skipped",
            "reason": "No representative cases found."
        }

    # ----------------------------------
    # Generate embeddings
    # ----------------------------------

    embeddings = _generate_embeddings(documents)

    # ----------------------------------
    # Run BERTopic
    # ----------------------------------

    topics, probabilities, topic_model = _run_bertopic(
        documents,
        embeddings
    )

    # ----------------------------------
    # Determine clustering version
    # ----------------------------------

    metadata = get_clustering_metadata()

    clustering_run_id = (
        metadata["clustering_version"] or 0
    ) + 1

    # ----------------------------------
    # Process topics
    # ----------------------------------

    topic_mapping = _process_topics(
        topic_model,
        clustering_run_id
    )

    # ----------------------------------
    # Update cases
    # ----------------------------------

    updated_cases = _update_database(
        case_ids,
        topics,
        topic_mapping
    )

    # ----------------------------------
    # Update metadata
    # ----------------------------------

    _update_metadata(topic_mapping)

    # ----------------------------------
    # Return summary
    # ----------------------------------

    return {
        "status": "success",
        "reason": reason,
        "cases_clustered": updated_cases,
        "topics_created": len(
            [t for t in topic_mapping if t != -1]
        ),
        "clustering_version": clustering_run_id
    }