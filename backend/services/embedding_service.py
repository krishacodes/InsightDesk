import os

from dotenv import load_dotenv
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer

load_dotenv()

# ==========================================================
# Configuration
# ==========================================================

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 5

# ==========================================================
# Load Embedding Model (Singleton)
# ==========================================================

model = SentenceTransformer(MODEL_NAME)

# ==========================================================
# Pinecone Client
# ==========================================================

pc = Pinecone(
    api_key=os.getenv("PINECONE_API_KEY")
)

index = pc.Index(
    os.getenv("PINECONE_INDEX")
)

# ==========================================================
# Embedding Functions
# ==========================================================

def generate_embedding(text: str) -> list:
    """
    Convert cleaned complaint text into a MiniLM embedding.
    """

    return model.encode(text).tolist()


# ==========================================================
# Retrieval Functions
# ==========================================================

def retrieve_similar_cases(
    embedding: list,
    top_k: int = TOP_K
) -> list:
    """
    Retrieve Top-K semantically similar cases from Pinecone.
    """

    response = index.query(
        vector=embedding,
        top_k=top_k,
        include_metadata=True
    )

    return response.get("matches", [])


# ==========================================================
# Storage Functions
# ==========================================================

def store_case_embedding(
    case_id: int,
    embedding: list,
    representative_text: str,
    department: str = None,
    topic: str = None
):
    """
    Store embedding for a newly created case.
    """

    metadata = {
        "case_id": case_id,
        "representative_text": representative_text,
        "department": department,
        "topic": topic
    }

    # Pinecone does not allow None/null values
    metadata = {
        key: value
        for key, value in metadata.items()
        if value is not None
    }

    index.upsert(
        vectors=[
            {
                "id": str(case_id),
                "values": embedding,
                "metadata": metadata
            }
        ]
    )


def update_case_embedding(
    case_id: int,
    embedding: list,
    representative_text: str,
    department: str = None,
    topic: str = None
):
    """
    Update representative embedding of an existing case.

    Pinecone upsert automatically overwrites
    the previous embedding if it exists.
    """

    store_case_embedding(
        case_id=case_id,
        embedding=embedding,
        representative_text=representative_text,
        department=department,
        topic=topic
    )


# ==========================================================
# Utility Functions
# ==========================================================

def delete_case_embedding(case_id: int):
    """
    Delete a case embedding from Pinecone.
    Useful if a case is removed or merged.
    """

    index.delete(
        ids=[str(case_id)]
    )