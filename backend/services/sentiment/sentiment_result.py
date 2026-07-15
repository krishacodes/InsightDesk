from dataclasses import dataclass


@dataclass
class SentimentResult:

    sentiment: str
    confidence: float
    model_name: str

#defines a standardized output structure for all sentiment models,
   #ensuring that RoBERTa, DistilBERT, and future models return sentiment predictions in a consistent format across the InsightDesk pipeline.'''