#Dynamically instantiate the appropriate sentiment model at runtime without exposing model selection logic to the rest of the application.
#implements the Factory Pattern to dynamically instantiate and cache sentiment models, enabling seamless runtime model switching while 
#maintaining a consistent interface across the InsightDesk sentiment pipeline.
from functools import lru_cache

from backend.services.sentiment.roberta_adapter import (
    RoBERTaAdapter
)

from backend.services.sentiment.distilbert_adapter import (
    DistilBERTAdapter
)


@lru_cache(maxsize=2)
def get_sentiment_model(
    model_name: str
):

    model_name = model_name.lower()

    if model_name == "roberta":

        return RoBERTaAdapter()

    if model_name == "distilbert":

        return DistilBERTAdapter()

    raise ValueError(
        f"Unsupported sentiment model: "
        f"{model_name}"
    )