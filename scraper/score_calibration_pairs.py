import pandas as pd
from sentence_transformers import CrossEncoder
from pathlib import Path

INPUT_PATH = Path("data/calibration_pairs.csv")
OUTPUT_PATH = Path("data/calibration_pairs_scored.csv")

MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"


def main():
    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded pairs: {len(df)}")
    print(f"Loading cross-encoder: {MODEL_NAME}")

    model = CrossEncoder(MODEL_NAME)

    pairs = list(zip(
        df["text_a"].astype(str),
        df["text_b"].astype(str)
    ))

    print("Scoring calibration pairs...")

    scores = model.predict(
        pairs,
        batch_size=32,
        show_progress_bar=True
    )

    # Raw cross-encoder logits — NOT probabilities.
    df["cross_encoder_score"] = scores

    df.to_csv(
        OUTPUT_PATH,
        index=False,
        encoding="utf-8"
    )

    duplicate_scores = df.loc[
        df["true_label"] == "duplicate",
        "cross_encoder_score"
    ]

    nonduplicate_scores = df.loc[
        df["true_label"] == "not_duplicate",
        "cross_encoder_score"
    ]

    print("\nScoring complete.")
    print("-----------------------------------------")
    print(f"Total pairs          : {len(df)}")
    print(f"Duplicate pairs      : {len(duplicate_scores)}")
    print(f"Non-duplicate pairs  : {len(nonduplicate_scores)}")

    print("\nDuplicate score range:")
    print(f"  Min  : {duplicate_scores.min():.4f}")
    print(f"  Mean : {duplicate_scores.mean():.4f}")
    print(f"  Max  : {duplicate_scores.max():.4f}")

    print("\nNon-duplicate score range:")
    print(f"  Min  : {nonduplicate_scores.min():.4f}")
    print(f"  Mean : {nonduplicate_scores.mean():.4f}")
    print(f"  Max  : {nonduplicate_scores.max():.4f}")

    print(f"\nSaved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()