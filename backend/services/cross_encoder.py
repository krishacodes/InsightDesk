from sentence_transformers import CrossEncoder

# --------------------------------------------------------
# Load Cross Encoder Model
# --------------------------------------------------------

MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"

model = CrossEncoder(MODEL_NAME)

# --------------------------------------------------------
# Main Function
# --------------------------------------------------------

def rerank_candidate_cases(
    complaint_text: str,
    candidate_cases: list
):
    """
    Compare a complaint against all candidate cases.

    Returns
    -------
    best_case_id
    best_similarity
    """

    if len(candidate_cases) == 0:
        return None, 0.0

    sentence_pairs = []
    valid_cases = []

    for case in candidate_cases:

        metadata = case.get(
            "metadata",
            {}
        )

        representative_text = metadata.get(
            "representative_text"
        )

        # Skip malformed Pinecone entries
        if representative_text is None:
            continue

        sentence_pairs.append(
            (
                complaint_text,
                representative_text
            )
        )

        valid_cases.append(
            case
        )

    # No valid candidate cases found
    if len(sentence_pairs) == 0:
        return None, 0.0

    scores = model.predict(
        sentence_pairs
    )

    best_index = scores.argmax()

    best_case = valid_cases[
        best_index
    ]

    return (
        best_case["metadata"]["case_id"],
        float(
            scores[best_index]
        )
    )