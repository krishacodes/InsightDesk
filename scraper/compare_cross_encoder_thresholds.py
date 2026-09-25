"""
Threshold comparison for the three CrossEncoder models.

Uses:
    data/cross_encoder_comparison_scored.csv

IMPORTANT:
Each model is swept over its OWN observed score range.
Threshold values are NOT compared directly across models.

Output:
    data/cross_encoder_threshold_comparison.csv
    data/cross_encoder_best_operating_points.csv

Run:
    python scraper/compare_cross_encoder_thresholds.py
"""

from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/cross_encoder_comparison_scored.csv"
)

FULL_OUTPUT = Path(
    "data/cross_encoder_threshold_comparison.csv"
)

SUMMARY_OUTPUT = Path(
    "data/cross_encoder_best_operating_points.csv"
)


MODELS = {
    "MS-MARCO": "msmarco_score",
    "STS-B": "stsb_score",
    "Quora": "quora_score",
}

PRECISION_FLOORS = [
    0.90,
    0.95,
    0.97,
    1.00,
]


def evaluate(scores, labels, threshold):

    pred = scores >= threshold

    tp = int(((pred == 1) & (labels == 1)).sum())
    fp = int(((pred == 1) & (labels == 0)).sum())
    tn = int(((pred == 0) & (labels == 0)).sum())
    fn = int(((pred == 0) & (labels == 1)).sum())

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
        if precision + recall > 0
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


def build_thresholds(scores):

    unique = np.sort(
        np.unique(scores)
    )

    epsilon = 1e-8

    return np.concatenate([
        [unique[0] - epsilon],
        unique,
        [unique[-1] + epsilon],
    ])


def best_f1(sweep):

    return (
        sweep.sort_values(
            [
                "f1",
                "precision",
                "recall",
            ],
            ascending=[
                False,
                False,
                False,
            ],
        )
        .iloc[0]
    )


def precision_candidate(sweep, floor):

    eligible = sweep[
        (sweep["precision"] >= floor)
        &
        ((sweep["TP"] + sweep["FP"]) > 0)
    ].copy()

    if eligible.empty:
        return None

    # Among thresholds satisfying the precision requirement:
    # maximize recall, then F1, then minimize FP.
    return (
        eligible.sort_values(
            [
                "recall",
                "f1",
                "FP",
            ],
            ascending=[
                False,
                False,
                True,
            ],
        )
        .iloc[0]
    )


def make_summary_row(
    model,
    operating_point,
    row,
):

    return {
        "model": model,
        "operating_point": operating_point,
        "threshold": row["threshold"],
        "TP": int(row["TP"]),
        "FP": int(row["FP"]),
        "TN": int(row["TN"]),
        "FN": int(row["FN"]),
        "precision": row["precision"],
        "recall": row["recall"],
        "f1": row["f1"],
    }


def main():

    df = pd.read_csv(INPUT_PATH)

    labels = (
        df["true_label"]
        .str.strip()
        .str.lower()
        .eq("duplicate")
        .astype(int)
        .to_numpy()
    )

    all_sweeps = []
    summary_rows = []

    for model_name, score_column in MODELS.items():

        print("\n" + "=" * 75)
        print(model_name)
        print("=" * 75)

        scores = df[
            score_column
        ].astype(float).to_numpy()

        thresholds = build_thresholds(
            scores
        )

        results = [
            evaluate(
                scores,
                labels,
                threshold,
            )
            for threshold in thresholds
        ]

        sweep = pd.DataFrame(results)

        sweep.insert(
            0,
            "model",
            model_name,
        )

        all_sweeps.append(
            sweep
        )

        # -----------------------------
        # Maximum F1
        # -----------------------------

        best = best_f1(sweep)

        summary_rows.append(
            make_summary_row(
                model_name,
                "Maximum F1",
                best,
            )
        )

        # -----------------------------
        # Precision floors
        # -----------------------------

        for floor in PRECISION_FLOORS:

            candidate = precision_candidate(
                sweep,
                floor,
            )

            if candidate is None:
                continue

            summary_rows.append(
                make_summary_row(
                    model_name,
                    f"Precision >= {floor:.2f}",
                    candidate,
                )
            )

    # ---------------------------------
    # Save
    # ---------------------------------

    full = pd.concat(
        all_sweeps,
        ignore_index=True,
    )

    summary = pd.DataFrame(
        summary_rows
    )

    full.to_csv(
        FULL_OUTPUT,
        index=False,
    )

    summary.to_csv(
        SUMMARY_OUTPUT,
        index=False,
    )

    # ---------------------------------
    # Display
    # ---------------------------------

    pd.set_option(
        "display.max_columns",
        None,
    )

    pd.set_option(
        "display.width",
        160,
    )

    print("\n")
    print("=" * 100)
    print("CROSS-ENCODER OPERATING POINT COMPARISON")
    print("=" * 100)

    display = summary.copy()

    for column in [
        "threshold",
        "precision",
        "recall",
        "f1",
    ]:

        display[column] = (
            display[column]
            .astype(float)
            .round(4)
        )

    print(
        display.to_string(
            index=False
        )
    )

    print("\nSaved:")
    print(FULL_OUTPUT)
    print(SUMMARY_OUTPUT)


if __name__ == "__main__":
    main()