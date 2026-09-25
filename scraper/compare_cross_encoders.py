"""
Compare CrossEncoder models on the SAME frozen 150-pair
InsightDesk duplicate-detection calibration dataset.

This script DOES NOT modify the production pipeline.

Models:
1. MS-MARCO relevance CrossEncoder — current baseline
2. STS-B semantic-similarity CrossEncoder
3. Quora duplicate-detection CrossEncoder

Input:
    data/calibration_pairs.csv

Output:
    data/cross_encoder_comparison_scored.csv

Run:
    python scraper/compare_cross_encoders.py
"""

from pathlib import Path

import pandas as pd
from sentence_transformers import CrossEncoder


INPUT_PATH = Path("data/calibration_pairs.csv")

OUTPUT_PATH = Path(
    "data/cross_encoder_comparison_scored.csv"
)


MODELS = {
    "msmarco": "cross-encoder/ms-marco-MiniLM-L-6-v2",

    "stsb": "cross-encoder/stsb-distilroberta-base",

    "quora": "cross-encoder/quora-distilroberta-base",
}


def validate_data(df):

    required = {
        "pair_id",
        "pair_type",
        "text_a",
        "text_b",
        "true_label",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing columns: {sorted(missing)}"
        )

    valid_labels = {
        "duplicate",
        "not_duplicate",
    }

    labels = (
        df["true_label"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    invalid = set(labels) - valid_labels

    if invalid:
        raise ValueError(
            f"Invalid labels: {invalid}"
        )

    df["true_label"] = labels

    return df


def score_model(df, short_name, model_name):

    print("\n" + "=" * 70)
    print(f"MODEL: {short_name}")
    print(f"NAME : {model_name}")
    print("=" * 70)

    print("Loading model...")

    model = CrossEncoder(model_name)

    pairs = list(
        zip(
            df["text_a"].astype(str),
            df["text_b"].astype(str),
        )
    )

    print(f"Scoring {len(pairs)} pairs...")

    scores = model.predict(
        pairs,
        batch_size=32,
        show_progress_bar=True,
    )

    column = f"{short_name}_score"

    df[column] = scores

    print("\nScore distribution by pair type:")

    summary = (
        df.groupby("pair_type")[column]
        .agg(
            [
                "count",
                "min",
                "mean",
                "median",
                "max",
            ]
        )
        .round(4)
    )

    print(summary.to_string())

    return df


def print_known_examples(df):

    print("\n" + "=" * 70)
    print("LOWEST-SCORING POSITIVES")
    print("=" * 70)

    positives = df[
        df["true_label"] == "duplicate"
    ]

    for model in MODELS:

        score_col = f"{model}_score"

        print(f"\n--- {model.upper()} ---")

        examples = positives.nsmallest(
            5,
            score_col,
        )

        for _, row in examples.iterrows():

            print(
                f"\nScore: "
                f"{row[score_col]:.4f}"
            )

            print("A:", row["text_a"])
            print("B:", row["text_b"])


def main():

    if not INPUT_PATH.exists():

        raise FileNotFoundError(
            f"{INPUT_PATH} not found."
        )

    df = pd.read_csv(INPUT_PATH)

    df = validate_data(df)

    print("CROSS-ENCODER COMPARISON")
    print("-----------------------------------------")
    print(f"Pairs       : {len(df)}")

    print(
        "Duplicates  :",
        (df["true_label"] == "duplicate").sum(),
    )

    print(
        "Nonduplicates:",
        (df["true_label"] == "not_duplicate").sum(),
    )

    print("\nPair types:")

    print(
        df["pair_type"]
        .value_counts()
        .to_string()
    )

    # -----------------------------------------------------
    # Score all models
    # -----------------------------------------------------

    for short_name, model_name in MODELS.items():

        df = score_model(
            df,
            short_name,
            model_name,
        )

    # -----------------------------------------------------
    # Save raw scores
    # -----------------------------------------------------

    df.to_csv(
        OUTPUT_PATH,
        index=False,
        encoding="utf-8",
    )

    # -----------------------------------------------------
    # Overall distributions
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("DUPLICATE VS NON-DUPLICATE DISTRIBUTIONS")
    print("=" * 70)

    for model in MODELS:

        score_col = f"{model}_score"

        print(f"\n{model.upper()}")

        summary = (
            df.groupby("true_label")[score_col]
            .agg(
                [
                    "count",
                    "min",
                    "mean",
                    "median",
                    "max",
                ]
            )
            .round(4)
        )

        print(summary.to_string())

    # -----------------------------------------------------
    # Known difficult examples
    # -----------------------------------------------------

    print_known_examples(df)

    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)

    print(f"Saved to: {OUTPUT_PATH}")

    print(
        "\nNo production model or threshold "
        "has been changed."
    )


if __name__ == "__main__":
    main()