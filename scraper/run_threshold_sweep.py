"""
Threshold calibration for InsightDesk duplicate detection.

Uses the already-scored independent synthetic calibration set:

    data/calibration_pairs_scored.csv

Ground truth:
    75 duplicate pairs
    75 non-duplicate pairs
        - 50 hard negatives
        - 25 easy negatives

Scores were produced using the SAME cross-encoder as the live pipeline:
    cross-encoder/ms-marco-MiniLM-L-6-v2

This script:
1. Evaluates the existing threshold (-2.21).
2. Sweeps the complete observed score decision space.
3. Computes TP, FP, TN, FN, Precision, Recall and F1.
4. Finds the maximum-F1 operating point.
5. Finds precision-constrained operating points.
6. Saves the full sweep and a compact report table.

IMPORTANT:
The maximum-F1 threshold is NOT automatically selected for production.
InsightDesk gives greater importance to false-positive control because
a false merge can contaminate downstream case-level analytics.

Run from project root:

    python scraper/run_threshold_sweep.py
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

INPUT_PATH = Path("data/calibration_pairs_scored.csv")

FULL_OUTPUT_PATH = Path("data/threshold_sweep_results.csv")
SUMMARY_OUTPUT_PATH = Path("data/threshold_candidates.csv")
MARKDOWN_OUTPUT_PATH = Path("data/threshold_sweep_results.md")

OLD_THRESHOLD = -2.21

PRECISION_FLOORS = [
    0.90,
    0.95,
    0.97,
    0.98,
    0.99,
    1.00,
]


# ---------------------------------------------------------
# Metrics
# ---------------------------------------------------------

def evaluate(scores, labels, threshold):
    """
    Predict duplicate when:

        cross_encoder_score >= threshold
    """

    predictions = scores >= threshold

    tp = int(((predictions == 1) & (labels == 1)).sum())
    fp = int(((predictions == 1) & (labels == 0)).sum())
    tn = int(((predictions == 0) & (labels == 0)).sum())
    fn = int(((predictions == 0) & (labels == 1)).sum())

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
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    return {
        "threshold": float(threshold),
        "TP": tp,
        "FP": fp,
        "TN": tn,
        "FN": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


# ---------------------------------------------------------
# Loading
# ---------------------------------------------------------

def load_data():

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"{INPUT_PATH} not found. "
            "Run score_calibration_pairs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    required = {
        "true_label",
        "cross_encoder_score",
        "pair_type",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    valid_labels = {
        "duplicate",
        "not_duplicate",
    }

    actual_labels = set(
        df["true_label"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    invalid = actual_labels - valid_labels

    if invalid:
        raise ValueError(
            f"Invalid true_label values: {invalid}"
        )

    if df["cross_encoder_score"].isna().any():
        raise ValueError(
            "Some cross_encoder_score values are missing."
        )

    df["true_binary"] = (
        df["true_label"]
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("duplicate")
        .astype(int)
    )

    return df


# ---------------------------------------------------------
# Exact threshold sweep
# ---------------------------------------------------------

def build_thresholds(scores):
    """
    Classification changes only when a threshold crosses an observed
    score.

    We therefore evaluate every unique observed score, plus values
    immediately below the minimum and above the maximum.

    OLD_THRESHOLD is explicitly included for direct comparison.
    """

    unique_scores = np.sort(np.unique(scores))

    epsilon = 1e-6

    thresholds = np.concatenate([
        [unique_scores[0] - epsilon],
        unique_scores,
        [unique_scores[-1] + epsilon],
        [OLD_THRESHOLD],
    ])

    return np.sort(np.unique(thresholds))


# ---------------------------------------------------------
# Precision-constrained candidates
# ---------------------------------------------------------

def find_precision_candidate(sweep, floor):
    """
    Among thresholds satisfying the requested precision floor,
    choose maximum recall.

    Tie-breaking:
        1. higher recall
        2. higher F1
        3. fewer false positives
        4. higher threshold
    """

    eligible = sweep[
        sweep["precision"] >= floor
    ].copy()

    # Ignore the trivial classifier that predicts zero duplicates.
    eligible = eligible[
        (eligible["TP"] + eligible["FP"]) > 0
    ]

    if eligible.empty:
        return None

    eligible = eligible.sort_values(
        by=[
            "recall",
            "f1",
            "FP",
            "threshold",
        ],
        ascending=[
            False,
            False,
            True,
            False,
        ],
    )

    return eligible.iloc[0]


# ---------------------------------------------------------
# Formatting
# ---------------------------------------------------------

def format_row(name, row):

    return {
        "operating_point": name,
        "threshold": round(float(row["threshold"]), 6),
        "TP": int(row["TP"]),
        "FP": int(row["FP"]),
        "TN": int(row["TN"]),
        "FN": int(row["FN"]),
        "precision": round(float(row["precision"]), 4),
        "recall": round(float(row["recall"]), 4),
        "f1": round(float(row["f1"]), 4),
    }


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    df = load_data()

    scores = df["cross_encoder_score"].to_numpy(dtype=float)
    labels = df["true_binary"].to_numpy(dtype=int)

    n_total = len(df)
    n_duplicate = int(labels.sum())
    n_nonduplicate = n_total - n_duplicate

    print("\nCALIBRATION DATASET")
    print("-----------------------------------------")
    print(f"Total pairs          : {n_total}")
    print(f"Duplicate            : {n_duplicate}")
    print(f"Not duplicate        : {n_nonduplicate}")

    print("\nPAIR TYPES")
    print("-----------------------------------------")
    print(df["pair_type"].value_counts().to_string())

    duplicate_scores = scores[labels == 1]
    nonduplicate_scores = scores[labels == 0]

    print("\nSCORE DISTRIBUTIONS")
    print("-----------------------------------------")

    print("Duplicate:")
    print(f"  min                : {duplicate_scores.min():.4f}")
    print(f"  mean               : {duplicate_scores.mean():.4f}")
    print(f"  max                : {duplicate_scores.max():.4f}")

    print("\nNot duplicate:")
    print(f"  min                : {nonduplicate_scores.min():.4f}")
    print(f"  mean               : {nonduplicate_scores.mean():.4f}")
    print(f"  max                : {nonduplicate_scores.max():.4f}")

    # -----------------------------------------------------
    # Sweep
    # -----------------------------------------------------

    thresholds = build_thresholds(scores)

    results = [
        evaluate(scores, labels, threshold)
        for threshold in thresholds
    ]

    sweep = pd.DataFrame(results)

    sweep = sweep.sort_values(
        "threshold"
    ).reset_index(drop=True)

    # -----------------------------------------------------
    # Existing threshold
    # -----------------------------------------------------

    old_result = evaluate(
        scores,
        labels,
        OLD_THRESHOLD,
    )

    # -----------------------------------------------------
    # Maximum F1
    # -----------------------------------------------------

    best_f1_row = (
        sweep
        .sort_values(
            by=[
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

    # -----------------------------------------------------
    # Precision-oriented candidates
    # -----------------------------------------------------

    summary_rows = []

    summary_rows.append(
        format_row(
            "Existing threshold",
            old_result,
        )
    )

    summary_rows.append(
        format_row(
            "Maximum F1",
            best_f1_row,
        )
    )

    for floor in PRECISION_FLOORS:

        candidate = find_precision_candidate(
            sweep,
            floor,
        )

        if candidate is not None:

            summary_rows.append(
                format_row(
                    f"Precision >= {floor:.2f}",
                    candidate,
                )
            )

    summary = pd.DataFrame(summary_rows)

    # Remove identical operating points that may satisfy
    # several precision floors, while preserving labels in
    # the full console output.
    summary.to_csv(
        SUMMARY_OUTPUT_PATH,
        index=False,
    )

    sweep.to_csv(
        FULL_OUTPUT_PATH,
        index=False,
    )

    # -----------------------------------------------------
    # Console output
    # -----------------------------------------------------

    print("\nEXISTING THRESHOLD")
    print("-----------------------------------------")

    old_display = format_row(
        "Existing threshold",
        old_result,
    )

    for key, value in old_display.items():
        if key != "operating_point":
            print(f"{key:<20}: {value}")

    print("\nCANDIDATE OPERATING POINTS")
    print("-----------------------------------------")

    print(
        summary.to_string(
            index=False
        )
    )

    # -----------------------------------------------------
    # Markdown report
    # -----------------------------------------------------

    with open(
        MARKDOWN_OUTPUT_PATH,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "# Duplicate-Detection Threshold Calibration\n\n"
        )

        f.write(
            f"Calibration dataset: **{n_total} independently "
            f"labelled synthetic pairs** "
            f"({n_duplicate} duplicate, "
            f"{n_nonduplicate} non-duplicate).\n\n"
        )

        f.write(
            "Ground-truth labels were derived from "
            "generator-assigned issue identifiers established "
            "before duplicate-detection inference.\n\n"
        )

        f.write(
            "Model: "
            "`cross-encoder/ms-marco-MiniLM-L-6-v2`\n\n"
        )

        f.write(
            "A pair is predicted as duplicate when "
            "`cross_encoder_score >= threshold`.\n\n"
        )

        f.write(
            "Raw cross-encoder outputs are ranking logits, "
            "not calibrated probabilities.\n\n"
        )

        f.write(
            "## Candidate operating points\n\n"
        )

        f.write(
            "| Operating point | Threshold | TP | FP | TN | FN | "
            "Precision | Recall | F1 |\n"
        )

        f.write(
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n"
        )

        for _, row in summary.iterrows():

            f.write(
                f"| {row['operating_point']} "
                f"| {row['threshold']} "
                f"| {int(row['TP'])} "
                f"| {int(row['FP'])} "
                f"| {int(row['TN'])} "
                f"| {int(row['FN'])} "
                f"| {row['precision']:.4f} "
                f"| {row['recall']:.4f} "
                f"| {row['f1']:.4f} |\n"
            )

        f.write(
            "\n## Threshold-selection principle\n\n"
        )

        f.write(
            "The maximum-F1 operating point is reported as a "
            "reference rather than automatically selected. "
            "InsightDesk treats false-positive duplicate merges "
            "as particularly costly because an incorrect merge "
            "can propagate into case counts, sentiment analysis, "
            "spike detection and root-cause analysis. "
            "Precision-constrained operating points are therefore "
            "reported separately for threshold selection.\n"
        )

    print("\nFILES SAVED")
    print("-----------------------------------------")
    print(f"Full sweep : {FULL_OUTPUT_PATH}")
    print(f"Candidates : {SUMMARY_OUTPUT_PATH}")
    print(f"Report     : {MARKDOWN_OUTPUT_PATH}")

    print(
        "\nNOTE: No new production threshold has been "
        "automatically selected."
    )


if __name__ == "__main__":
    main()