from transformers import pipeline

from backend.services.sentiment.base_model import (
    BaseSentimentModel
)

from backend.services.sentiment.sentiment_result import (
    SentimentResult
)


class RoBERTaAdapter(
    BaseSentimentModel
):

    def __init__(self):

        self.model = pipeline(
            "sentiment-analysis",
            model=
            "cardiffnlp/twitter-roberta-base-sentiment-latest"
        )

    def predict(
        self,
        text: str
    ) -> SentimentResult:

        result = self.model(
            text
        )[0]

        return SentimentResult(
            sentiment=result["label"],
            confidence=float(
                result["score"]
            ),
            model_name="roberta"
        )