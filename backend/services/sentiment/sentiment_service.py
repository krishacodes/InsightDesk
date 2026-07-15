#Provide a single public entry point for sentiment analysis 
# by abstracting model selection, prediction, and result handling from the rest of the application.
#serves as the public interface for the InsightDesk sentiment subsystem, encapsulating model retrieval,
#prediction, and result standardization behind a single analyze_sentiment() function.
from backend.services.sentiment.factory import (
    get_sentiment_model
)
from backend.database.supabase import (
    get_case,
    update_case
)
from backend.services.sentiment.sentiment_result import (
    SentimentResult
)


def analyze_sentiment(
    text: str,
    model_name: str = "roberta"
) -> SentimentResult:

    model = get_sentiment_model(
        model_name
    )

    return model.predict(
        text
    )
def enrich_case_with_sentiment(

    case_id: int,

    model_name: str = "roberta"
):

    case = get_case(
        case_id
    )

    result = analyze_sentiment(

        text=
        case[
            "representative_text"
        ],

        model_name=
        model_name
    )

    update_case(

        case_id,

        {

            "sentiment":
            result.sentiment,

            "confidence_score":
            result.confidence,

            "sentiment_model":
            result.model_name
        }
    )

    return result