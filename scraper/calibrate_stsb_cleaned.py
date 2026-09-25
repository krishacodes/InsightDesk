"""
STS-B cleaned-text calibration.

Purpose
-------
The original STS-B threshold was calibrated using raw complaint text,
while the production duplicate-detection pipeline applies clean_text()
before CrossEncoder scoring.

This script recalibrates STS-B using the EXACT production preprocessing.

IMPORTANT
---------
- Does NOT modify production code.
- Does NOT modify Supabase.
- Does NOT modify Pinecone.
- Does NOT change SIMILARITY_THRESHOLD.
- Does NOT overwrite previous raw-text calibration artifacts.

Input
-----
data/calibration_pairs.csv

Expected labels
---------------
true_label:
    duplicate
    not_duplicate

Outputs
-------
data/stsb_cleaned_calibration_scored.csv
data/stsb_cleaned_threshold_sweep.csv
data/stsb_cleaned_operating_points.csv
"""

import numpy as np
import pandas as pd

from sentence_transformers import CrossEncoder

from backend.services.preprocess import clean_text


# ==========================================================
# CONFIGURATION
# ==========================================================

INPUT_PATH = "data/calibration_pairs.csv"

SCORED_OUTPUT_PATH = (
    "data/stsb_cleaned_calibration_scored.csv"
)

SWEEP_OUTPUT_PATH = (
    "data/stsb_cleaned_threshold_sweep.csv"
)

OPERATING_POINTS_OUTPUT_PATH = (
    "data/stsb_cleaned_operating_points.csv"
)

MODEL_NAME = (
    "cross-encoder/stsb-distilroberta-base"
)

CURRENT_PRODUCTION_THRESHOLD = 0.5858


# ==========================================================
# METRICS
# ==========================================================

def calculate_metrics(
    labels,
    scores,
    threshold,
):
    """
    Calculate binary classification metrics using:

        score >= threshold -> duplicate
        score < threshold  -> not duplicate
    """

    predictions = (
        scores >= threshold
    ).astype(int)

    tp = int(
        np.sum(
            (predictions == 1)
            & (labels == 1)
        )
    )

    fp = int(
        np.sum(
            (predictions == 1)
            & (labels == 0)
        )
    )

    tn = int(
        np.sum(
            (predictions == 0)
            & (labels == 0)
        )
    )

    fn = int(
        np.sum(
            (predictions == 0)
            & (labels == 1)
        )
    )

    precision = (
        tp / (tp + fp)
        if (tp + fp) > 0
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0.0
    )

    f1 = (
        2 * precision * recall
        / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    accuracy = (
        (tp + tn)
        / len(labels)
        if len(labels) > 0
        else 0.0
    )

    return {
        "threshold": float(threshold),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "accuracy": float(accuracy),
    }


# ==========================================================
# OPERATING-POINT SELECTION
# ==========================================================

def get_best_f1_row(
    sweep_df,
):
    """
    Select the threshold with maximum F1.

    Tie-breaking:
    1. Higher precision
    2. Higher recall
    3. Higher threshold
    """

    return (
        sweep_df
        .sort_values(
            [
                "f1",
                "precision",
                "recall",
                "threshold",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )
        .iloc[0]
    )


def get_precision_floor_row(
    sweep_df,
    precision_floor,
):
    """
    Select the highest-recall operating point satisfying
    the requested precision floor.

    This preserves the previous calibration policy:
    prioritize false-merge control, then maximize recall.
    """

    candidates = sweep_df[
        sweep_df["precision"]
        >= precision_floor
    ].copy()

    # Remove thresholds predicting no positive examples.
    candidates = candidates[
        (
            candidates["tp"]
            + candidates["fp"]
        ) > 0
    ]

    if candidates.empty:
        return None

    return (
        candidates
        .sort_values(
            [
                "recall",
                "f1",
                "precision",
                "threshold",
            ],
            ascending=[
                False,
                False,
                False,
                False,
            ],
        )
        .iloc[0]
    )


# ==========================================================
# PRINTING
# ==========================================================

def print_metrics(
    title,
    row,
):
    print(f"\n{title}")
    print("-" * 70)

    if row is None:
        print(
            "No valid operating point found."
        )
        return

    print(
        f"Threshold : "
        f"{float(row['threshold']):.6f}"
    )

    print(
        f"TP        : "
        f"{int(row['tp'])}"
    )

    print(
        f"FP        : "
        f"{int(row['fp'])}"
    )

    print(
        f"TN        : "
        f"{int(row['tn'])}"
    )

    print(
        f"FN        : "
        f"{int(row['fn'])}"
    )

    print(
        f"Precision : "
        f"{float(row['precision']):.4f}"
    )

    print(
        f"Recall    : "
        f"{float(row['recall']):.4f}"
    )

    print(
        f"F1        : "
        f"{float(row['f1']):.4f}"
    )

    print(
        f"Accuracy  : "
        f"{float(row['accuracy']):.4f}"
    )


# ==========================================================
# MAIN
# ==========================================================

def main():

    print("=" * 70)
    print("STS-B CLEANED-TEXT CALIBRATION")
    print("=" * 70)

    # ------------------------------------------------------
    # Load calibration pairs
    # ------------------------------------------------------

    df = pd.read_csv(
        INPUT_PATH
    )

    required_columns = {
        "text_a",
        "text_b",
        "true_label",
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    # ------------------------------------------------------
    # Convert existing string labels to numeric labels
    # ------------------------------------------------------

    label_map = {
        "duplicate": 1,
        "not_duplicate": 0,
    }

    normalized_labels = (
        df["true_label"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["label"] = (
        normalized_labels
        .map(label_map)
    )

    if df["label"].isna().any():

        invalid_labels = (
            normalized_labels[
                df["label"].isna()
            ]
            .unique()
            .tolist()
        )

        raise RuntimeError(
            "Unexpected true_label values: "
            f"{invalid_labels}"
        )

    df["label"] = (
        df["label"]
        .astype(int)
    )

    print(
        "Calibration pairs:",
        len(df),
    )

    print(
        "Positive pairs:",
        int(
            (df["label"] == 1)
            .sum()
        ),
    )

    print(
        "Negative pairs:",
        int(
            (df["label"] == 0)
            .sum()
        ),
    )

    # Basic integrity check.
    if len(df) == 0:
        raise RuntimeError(
            "Calibration dataset is empty."
        )

    # ------------------------------------------------------
    # Apply EXACT production preprocessing
    # ------------------------------------------------------

    print(
        "\nApplying production clean_text()..."
    )

    df["clean_text_a"] = (
        df["text_a"]
        .fillna("")
        .astype(str)
        .apply(clean_text)
    )

    df["clean_text_b"] = (
        df["text_b"]
        .fillna("")
        .astype(str)
        .apply(clean_text)
    )

    empty_a = int(
        (
            df["clean_text_a"]
            .str.len()
            == 0
        ).sum()
    )

    empty_b = int(
        (
            df["clean_text_b"]
            .str.len()
            == 0
        ).sum()
    )

    print(
        "Empty cleaned text_a:",
        empty_a,
    )

    print(
        "Empty cleaned text_b:",
        empty_b,
    )

    if (
        empty_a > 0
        or empty_b > 0
    ):
        raise RuntimeError(
            "Production clean_text() produced "
            "one or more empty calibration strings. "
            "Inspect these pairs before calibration."
        )

    # ------------------------------------------------------
    # Load STS-B CrossEncoder
    # ------------------------------------------------------

    print(
        "\nLoading model:",
        MODEL_NAME,
    )

    model = CrossEncoder(
        MODEL_NAME
    )

    sentence_pairs = list(
        zip(
            df["clean_text_a"],
            df["clean_text_b"],
        )
    )

    print(
        "Scoring cleaned calibration pairs..."
    )

    scores = model.predict(
        sentence_pairs
    )

    df["stsb_cleaned_score"] = (
        np.asarray(
            scores,
            dtype=float,
        )
    )

    # ------------------------------------------------------
    # Save pair-level scores
    # ------------------------------------------------------

    df.to_csv(
        SCORED_OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Score distributions
    # ------------------------------------------------------

    duplicate_scores = (
        df.loc[
            df["label"] == 1,
            "stsb_cleaned_score",
        ]
    )

    nonduplicate_scores = (
        df.loc[
            df["label"] == 0,
            "stsb_cleaned_score",
        ]
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "CLEANED SCORE DISTRIBUTIONS"
    )

    print(
        "=" * 70
    )

    print(
        "\nDuplicate pairs"
    )

    print(
        f"Count  : "
        f"{len(duplicate_scores)}"
    )

    print(
        f"Min    : "
        f"{duplicate_scores.min():.4f}"
    )

    print(
        f"Mean   : "
        f"{duplicate_scores.mean():.4f}"
    )

    print(
        f"Median : "
        f"{duplicate_scores.median():.4f}"
    )

    print(
        f"Max    : "
        f"{duplicate_scores.max():.4f}"
    )

    print(
        "\nNon-duplicate pairs"
    )

    print(
        f"Count  : "
        f"{len(nonduplicate_scores)}"
    )

    print(
        f"Min    : "
        f"{nonduplicate_scores.min():.4f}"
    )

    print(
        f"Mean   : "
        f"{nonduplicate_scores.mean():.4f}"
    )

    print(
        f"Median : "
        f"{nonduplicate_scores.median():.4f}"
    )

    print(
        f"Max    : "
        f"{nonduplicate_scores.max():.4f}"
    )

    # ------------------------------------------------------
    # Threshold sweep
    # ------------------------------------------------------

    labels = (
        df["label"]
        .to_numpy(
            dtype=int
        )
    )

    score_array = (
        df["stsb_cleaned_score"]
        .to_numpy(
            dtype=float
        )
    )

    # Predictions only change when the threshold crosses
    # an observed score. Therefore observed scores are
    # sufficient candidate thresholds.
    thresholds = np.sort(
        np.unique(
            score_array
        )
    )

    sweep_rows = []

    for threshold in thresholds:

        metrics = calculate_metrics(
            labels=labels,
            scores=score_array,
            threshold=threshold,
        )

        sweep_rows.append(
            metrics
        )

    sweep_df = pd.DataFrame(
        sweep_rows
    )

    sweep_df = (
        sweep_df
        .sort_values(
            "threshold"
        )
        .reset_index(
            drop=True
        )
    )

    sweep_df.to_csv(
        SWEEP_OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Select operating points
    # ------------------------------------------------------

    max_f1 = (
        get_best_f1_row(
            sweep_df
        )
    )

    precision_90 = (
        get_precision_floor_row(
            sweep_df,
            0.90,
        )
    )

    precision_95 = (
        get_precision_floor_row(
            sweep_df,
            0.95,
        )
    )

    precision_97 = (
        get_precision_floor_row(
            sweep_df,
            0.97,
        )
    )

    # ------------------------------------------------------
    # Evaluate CURRENT production threshold separately
    # ------------------------------------------------------

    current_metrics = (
        calculate_metrics(
            labels=labels,
            scores=score_array,
            threshold=(
                CURRENT_PRODUCTION_THRESHOLD
            ),
        )
    )

    # ------------------------------------------------------
    # Save operating points
    # ------------------------------------------------------

    operating_rows = []

    def add_operating_point(
        name,
        row,
    ):
        if row is None:
            return

        operating_rows.append(
            {
                "operating_point":
                    name,

                "threshold":
                    float(
                        row["threshold"]
                    ),

                "tp":
                    int(
                        row["tp"]
                    ),

                "fp":
                    int(
                        row["fp"]
                    ),

                "tn":
                    int(
                        row["tn"]
                    ),

                "fn":
                    int(
                        row["fn"]
                    ),

                "precision":
                    float(
                        row["precision"]
                    ),

                "recall":
                    float(
                        row["recall"]
                    ),

                "f1":
                    float(
                        row["f1"]
                    ),

                "accuracy":
                    float(
                        row["accuracy"]
                    ),
            }
        )

    add_operating_point(
        "maximum_f1",
        max_f1,
    )

    add_operating_point(
        "precision_ge_0.90",
        precision_90,
    )

    add_operating_point(
        "precision_ge_0.95",
        precision_95,
    )

    add_operating_point(
        "precision_ge_0.97",
        precision_97,
    )

    add_operating_point(
        "current_production_0.5858",
        current_metrics,
    )

    operating_df = pd.DataFrame(
        operating_rows
    )

    operating_df.to_csv(
        OPERATING_POINTS_OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Print current threshold
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "CURRENT PRODUCTION THRESHOLD "
        "ON CLEANED TEXT"
    )

    print(
        "=" * 70
    )

    print_metrics(
        "Threshold 0.5858",
        current_metrics,
    )

    # ------------------------------------------------------
    # Print cleaned operating points
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "CLEANED-TEXT OPERATING POINTS"
    )

    print(
        "=" * 70
    )

    print_metrics(
        "Maximum F1",
        max_f1,
    )

    print_metrics(
        "Precision >= 0.90",
        precision_90,
    )

    print_metrics(
        "Precision >= 0.95",
        precision_95,
    )

    print_metrics(
        "Precision >= 0.97",
        precision_97,
    )

    # ------------------------------------------------------
    # Compare old threshold vs corrected calibration
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "OLD VS CLEANED-CALIBRATION THRESHOLD"
    )

    print(
        "=" * 70
    )

    print(
        "Current production threshold:",
        CURRENT_PRODUCTION_THRESHOLD,
    )

    if precision_90 is not None:

        corrected_threshold = float(
            precision_90[
                "threshold"
            ]
        )

        difference = (
            corrected_threshold
            - CURRENT_PRODUCTION_THRESHOLD
        )

        print(
            "Cleaned-text P>=0.90 candidate:",
            f"{corrected_threshold:.6f}",
        )

        print(
            "Threshold difference:",
            f"{difference:+.6f}",
        )

    # ------------------------------------------------------
    # Output files
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "OUTPUT FILES"
    )

    print(
        "=" * 70
    )

    print(
        SCORED_OUTPUT_PATH
    )

    print(
        SWEEP_OUTPUT_PATH
    )

    print(
        OPERATING_POINTS_OUTPUT_PATH
    )

    print(
        "\nCalibration complete."
    )

    print(
        "Production threshold was NOT changed."
    )


if __name__ == "__main__":
    main()