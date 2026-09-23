"""
Evaluate a sentiment model's predictions against rating-derived
ground-truth labels, and print/save accuracy, precision, recall,
and macro F1 — the numbers your model_benchmarks table doesn't have.

Ground truth is DERIVED from the `rating` column (1-5 stars):
    1-2 stars -> Negative
    3   stars -> Neutral
    4-5 stars -> Positive

This is a proxy for true sentiment, not hand-labeled ground truth.
State that clearly in the paper (it's a reasonable, common shortcut,
but ratings and sentiment don't always perfectly agree).

Usage:
    python evaluate_sentiment_model.py
"""

from collections import Counter
import csv
import sys
import random
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
)

from backend.services.sentiment.sentiment_service import analyze_sentiment

DATA_FILE = "data/master_complaints.csv"   # adjust path if running from elsewhere
SAMPLE_SIZE = 100                         # keep in sync with your benchmark sample size
MODEL_NAME = "distilbert"                 # change to "roberta" for the other run


def rating_to_label(rating: int) -> str:
    rating = int(rating)
    if rating <= 2:
        return "Negative"
    if rating == 3:
        return "Neutral"
    return "Positive"


import random   # add this import at the top of the file, with the other imports

def load_samples(path: str, limit: int, seed: int = 42):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("complaint_text") or not row.get("rating"):
                continue
            rows.append(row)

    random.seed(seed)
    return random.sample(rows, min(limit, len(rows)))


def main():
    rows = load_samples(DATA_FILE, SAMPLE_SIZE)
    if not rows:
        print(f"No usable rows found in {DATA_FILE}")
        sys.exit(1)

    y_true = []
    y_pred = []

    for row in rows:
        text = row["complaint_text"]
        true_label = rating_to_label(row["rating"])

        result = analyze_sentiment(text, model_name=MODEL_NAME)
        pred_label = result.sentiment

        y_true.append(true_label)
        y_pred.append(pred_label)
    print("True label distribution:", Counter(y_true))
    print("Pred label distribution:", Counter(y_pred))
    accuracy = accuracy_score(y_true, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="macro", zero_division=0
    )

    print(f"\n========== EVAL REPORT ({MODEL_NAME}) ==========\n")
    print(f"Samples evaluated: {len(rows)}")
    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision (macro): {precision:.4f}")
    print(f"Recall (macro):    {recall:.4f}")
    print(f"Macro F1:  {f1:.4f}")
    print("\n=================================================\n")

    # Print this dict so you can paste it straight into
    # EXTRA_METRICS in export_benchmarks.py
    print("Paste into EXTRA_METRICS in export_benchmarks.py:")
    print(
        f'    "{MODEL_NAME}": {{"accuracy": {accuracy:.4f}, '
        f'"precision": {precision:.4f}, "recall": {recall:.4f}, '
        f'"macro_f1": {f1:.4f}}},'
    )


if __name__ == "__main__":
    main()