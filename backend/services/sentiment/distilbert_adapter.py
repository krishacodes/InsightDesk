from transformers import pipeline

from backend.services.sentiment.base_model import (
    BaseSentimentModel
)

from backend.services.sentiment.sentiment_result import (
    SentimentResult
)


class DistilBERTAdapter(
    BaseSentimentModel
):
    """
    Lightweight sentiment model implementation
    using DistilBERT.

    Since the SST-2 model only predicts
    POSITIVE and NEGATIVE labels, a confidence-
    based threshold is used to infer Neutral
    sentiment.
    """

    NEUTRAL_THRESHOLD = 0.60

    def __init__(self):

        self.model = pipeline(
            "sentiment-analysis",
            model=(
                "distilbert-base-uncased-"
                "finetuned-sst-2-english"
            )
        )

    def predict(
        self,
        text: str
    ) -> SentimentResult:

        result = self.model(text)[0]

        score = float(
            result["score"]
        )

        label = result[
            "label"
        ].upper()

        # ---------------------------------
        # Neutral Mapping Logic
        # ---------------------------------

        if score < self.NEUTRAL_THRESHOLD:

            sentiment = "Neutral"

        elif label == "POSITIVE":

            sentiment = "Positive"

        else:

            sentiment = "Negative"

        # ---------------------------------
        # Return Standardized Result
        # ---------------------------------

        return SentimentResult(
            sentiment=sentiment,
            confidence=score,
            model_name="distilbert"
        )