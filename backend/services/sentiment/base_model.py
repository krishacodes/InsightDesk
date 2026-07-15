from abc import ABC, abstractmethod

from backend.services.sentiment.sentiment_result import (
    SentimentResult
)


class BaseSentimentModel(ABC):

    """
    Abstract base class for all
    sentiment models.
    """

    @abstractmethod
    def predict(
        self,
        text: str
    ) -> SentimentResult:
        """
        Predict sentiment for input text.
        """
        pass
#To ensure every sentiment model implements the same method.
#ABC = Abstract Base Class.Think of it as a contract:"If you inherit from me, you MUST implement predict()."