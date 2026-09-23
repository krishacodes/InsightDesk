"""
Calibrate the Cross-Encoder decision threshold used by InsightDesk's
duplicate-detection pipeline.

Model:
    cross-encoder/ms-marco-MiniLM-L-6-v2

Input:
    backend/scripts/calibration_data.csv

The calibration dataset contains manually reviewed complaint pairs:

    label = 1  -> same underlying issue / should be merged
    label = 0  -> different underlying issue / should remain separate

The script evaluates:

1. Cross-Encoder score distributions
2. Existing production threshold (0.85)
3. F1-optimal threshold
4. Precision-constrained operating thresholds:
       >= 90% precision
       >= 95% precision
       >= 97% precision
5. Pair-level predictions
6. Full threshold sweep saved as CSV

IMPORTANT:
This script is read-only with respect to the InsightDesk production
pipeline. It does NOT modify Supabase, complaints, cases, or the
production threshold.

Usage:
    python backend/scripts/calibrate_duplicate_threshold.py
"""

import os
import csv
import numpy as np

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)

from backend.services.cross_encoder import model


# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

CALIBRATION_FILE = os.path.join(
    SCRIPT_DIR,
    "calibration_data.csv"
)

RESULT_FILE = os.path.join(
    SCRIPT_DIR,
    "threshold_calibration_results.csv"
)

CURRENT_THRESHOLD = 0.85

PRECISION_TARGETS = [
    0.90,
    0.95,
    0.97,
]

NUM_THRESHOLD_POINTS = 500


# ============================================================
# LOAD CALIBRATION DATA
# ============================================================

def load_calibration_data():
    """
    Load manually labelled complaint pairs.

    Expected CSV columns:

        text_a,text_b,label

    label:
        1 = same underlying issue
        0 = different underlying issue
    """

    pairs = []
    labels = []

    with open(
        CALIBRATION_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        required_columns = {
            "text_a",
            "text_b",
            "label"
        }

        if not required_columns.issubset(
            reader.fieldnames or []
        ):
            raise ValueError(
                "CSV must contain columns: "
                "text_a,text_b,label"
            )

        for row_number, row in enumerate(
            reader,
            start=2
        ):

            text_a = row["text_a"].strip()
            text_b = row["text_b"].strip()

            if not text_a or not text_b:
                print(
                    f"Skipping row {row_number}: "
                    "empty complaint text."
                )
                continue

            try:
                label = int(
                    row["label"]
                )

            except ValueError:
                raise ValueError(
                    f"Invalid label on row "
                    f"{row_number}: "
                    f"{row['label']}"
                )

            if label not in (0, 1):
                raise ValueError(
                    f"Invalid label on row "
                    f"{row_number}: {label}. "
                    "Labels must be 0 or 1."
                )

            pairs.append(
                (
                    text_a,
                    text_b
                )
            )

            labels.append(
                label
            )

    return (
        pairs,
        np.array(
            labels,
            dtype=int
        )
    )


# ============================================================
# METRIC CALCULATION
# ============================================================

def evaluate_threshold(
    scores,
    labels,
    threshold
):
    """
    Evaluate one decision threshold.

    A pair is classified as duplicate when:

        cross_encoder_score >= threshold
    """

    predictions = (
        scores >= threshold
    ).astype(int)

    accuracy = accuracy_score(
        labels,
        predictions
    )

    precision = precision_score(
        labels,
        predictions,
        zero_division=0
    )

    recall = recall_score(
        labels,
        predictions,
        zero_division=0
    )

    f1 = f1_score(
        labels,
        predictions,
        zero_division=0
    )

    tn, fp, fn, tp = confusion_matrix(
        labels,
        predictions,
        labels=[0, 1]
    ).ravel()

    return {

        "threshold":
            float(threshold),

        "accuracy":
            float(accuracy),

        "precision":
            float(precision),

        "recall":
            float(recall),

        "f1":
            float(f1),

        "tp":
            int(tp),

        "tn":
            int(tn),

        "fp":
            int(fp),

        "fn":
            int(fn),
    }


# ============================================================
# RESULT DISPLAY
# ============================================================

def print_result(
    result,
    title=None
):
    """
    Print evaluation metrics in a consistent format.
    """

    if title:

        print(
            f"\n---------- {title} ----------"
        )

    print(
        f"Threshold: {result['threshold']:.4f}"
    )

    print(
        f"Accuracy:  {result['accuracy']:.4f}"
    )

    print(
        f"Precision: {result['precision']:.4f}"
    )

    print(
        f"Recall:    {result['recall']:.4f}"
    )

    print(
        f"F1 Score:  {result['f1']:.4f}"
    )

    print(
        f"TP: {result['tp']} | "
        f"TN: {result['tn']} | "
        f"FP: {result['fp']} | "
        f"FN: {result['fn']}"
    )


# ============================================================
# MAIN CALIBRATION
# ============================================================

def calibrate():

    print(
        "\n"
        "========== DUPLICATE THRESHOLD CALIBRATION =========="
        "\n"
    )

    # --------------------------------------------------------
    # STEP 1: Load manually reviewed pairs
    # --------------------------------------------------------

    pairs, labels = load_calibration_data()

    if len(pairs) == 0:

        print(
            "No calibration pairs found."
        )

        return

    positive_count = int(
        np.sum(
            labels == 1
        )
    )

    negative_count = int(
        np.sum(
            labels == 0
        )
    )

    print(
        f"Calibration pairs:     {len(pairs)}"
    )

    print(
        f"Duplicate pairs:       {positive_count}"
    )

    print(
        f"Non-duplicate pairs:   {negative_count}"
    )

    # --------------------------------------------------------
    # Basic validation
    # --------------------------------------------------------

    if positive_count == 0:
        raise ValueError(
            "Calibration dataset contains "
            "no duplicate examples."
        )

    if negative_count == 0:
        raise ValueError(
            "Calibration dataset contains "
            "no non-duplicate examples."
        )

    # --------------------------------------------------------
    # STEP 2: Generate Cross-Encoder scores
    # --------------------------------------------------------

    print(
        "\nGenerating Cross-Encoder scores..."
    )

    scores = np.asarray(
        model.predict(
            pairs,
            show_progress_bar=True
        ),
        dtype=float
    )

    if len(scores) != len(labels):

        raise RuntimeError(
            "Number of Cross-Encoder scores "
            "does not match number of labels."
        )

    # --------------------------------------------------------
    # STEP 3: Score distributions
    # --------------------------------------------------------

    positive_scores = scores[
        labels == 1
    ]

    negative_scores = scores[
        labels == 0
    ]

    print(
        "\n"
        "---------- SCORE DISTRIBUTION ----------"
    )

    print(
        f"Overall min score:       "
        f"{scores.min():.4f}"
    )

    print(
        f"Overall max score:       "
        f"{scores.max():.4f}"
    )

    print(
        f"Duplicate mean score:    "
        f"{positive_scores.mean():.4f}"
    )

    print(
        f"Duplicate median score:  "
        f"{np.median(positive_scores):.4f}"
    )

    print(
        f"Duplicate min score:     "
        f"{positive_scores.min():.4f}"
    )

    print(
        f"Duplicate max score:     "
        f"{positive_scores.max():.4f}"
    )

    print(
        f"Non-duplicate mean:      "
        f"{negative_scores.mean():.4f}"
    )

    print(
        f"Non-duplicate median:    "
        f"{np.median(negative_scores):.4f}"
    )

    print(
        f"Non-duplicate min:       "
        f"{negative_scores.min():.4f}"
    )

    print(
        f"Non-duplicate max:       "
        f"{negative_scores.max():.4f}"
    )

    # --------------------------------------------------------
    # STEP 4: Threshold sweep
    #
    # IMPORTANT:
    # MS-MARCO Cross-Encoder outputs raw ranking scores.
    # They are NOT probabilities constrained to [0, 1].
    #
    # Therefore we sweep across the actual observed score
    # range rather than assuming thresholds such as 0.5.
    # --------------------------------------------------------

    thresholds = np.linspace(

        scores.min() - 0.01,

        scores.max() + 0.01,

        NUM_THRESHOLD_POINTS
    )

    results = []

    for threshold in thresholds:

        result = evaluate_threshold(
            scores,
            labels,
            threshold
        )

        results.append(
            result
        )

    # ========================================================
    # STEP 5: PRECISION-CONSTRAINED OPERATING POINTS
    # ========================================================

    print(
        "\n"
        "========== PRECISION-CONSTRAINED THRESHOLDS =========="
    )

    precision_results = {}

    for required_precision in PRECISION_TARGETS:

        eligible = [

            result

            for result in results

            if result["precision"]
            >= required_precision
        ]

        if not eligible:

            print(
                f"\nNo threshold achieved "
                f"{required_precision:.0%} precision."
            )

            continue

        # ----------------------------------------------------
        # Among all thresholds meeting the precision target:
        #
        # 1. Maximize recall
        # 2. Then maximize F1
        # 3. Then maximize accuracy
        #
        # This gives us the highest duplicate coverage while
        # maintaining the required precision.
        # ----------------------------------------------------

        selected = max(

            eligible,

            key=lambda x: (

                x["recall"],

                x["f1"],

                x["accuracy"]
            )
        )

        precision_results[
            required_precision
        ] = selected

        print(
            f"\nMinimum Precision: "
            f"{required_precision:.0%}"
        )

        print_result(
            selected
        )

    # ========================================================
    # STEP 6: F1-OPTIMAL THRESHOLD
    # ========================================================

    best_result = max(

        results,

        key=lambda x: (

            x["f1"],

            x["precision"],

            x["accuracy"]
        )
    )

    best_threshold = best_result[
        "threshold"
    ]

    # ========================================================
    # STEP 7: CURRENT PRODUCTION THRESHOLD
    # ========================================================

    current_result = evaluate_threshold(

        scores,

        labels,

        CURRENT_THRESHOLD
    )

    print_result(

        current_result,

        title=(
            f"CURRENT THRESHOLD: "
            f"{CURRENT_THRESHOLD}"
        )
    )

    # ========================================================
    # STEP 8: DISPLAY F1-OPTIMAL THRESHOLD
    # ========================================================

    print(
        "\n"
        "========== F1-OPTIMAL CALIBRATED THRESHOLD =========="
    )

    print_result(
        best_result
    )

    # ========================================================
    # STEP 9: PAIR-LEVEL RESULTS USING F1 THRESHOLD
    # ========================================================

    print(
        "\n"
        "========== PAIR SCORES AT F1-OPTIMAL THRESHOLD =========="
        "\n"
    )

    wrong_count = 0

    for index, (
        pair,
        label,
        score
    ) in enumerate(

        zip(
            pairs,
            labels,
            scores
        ),

        start=1
    ):

        predicted = int(
            score >= best_threshold
        )

        correct = (
            predicted == label
        )

        status = (
            "CORRECT"
            if correct
            else "WRONG"
        )

        if not correct:
            wrong_count += 1

        print(
            f"Pair {index:02d} | "
            f"Label={label} | "
            f"Score={score:.4f} | "
            f"Pred={predicted} | "
            f"{status}"
        )

        print(
            f"  A: {pair[0]}"
        )

        print(
            f"  B: {pair[1]}"
        )

        print()

    print(
        f"Total misclassified pairs "
        f"at F1 threshold: {wrong_count}"
    )

    # ========================================================
    # STEP 10: SAVE COMPLETE THRESHOLD SWEEP
    # ========================================================

    with open(

        RESULT_FILE,

        "w",

        encoding="utf-8",

        newline=""

    ) as file:

        fieldnames = [

            "threshold",

            "accuracy",

            "precision",

            "recall",

            "f1",

            "tp",

            "tn",

            "fp",

            "fn",
        ]

        writer = csv.DictWriter(

            file,

            fieldnames=fieldnames
        )

        writer.writeheader()

        for result in results:

            writer.writerow(
                result
            )

    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    print(
        "\n"
        "========== CALIBRATION SUMMARY =========="
    )

    print(
        f"\nCurrent production threshold:"
        f" {CURRENT_THRESHOLD:.4f}"
    )

    print(
        f"Current Precision: "
        f"{current_result['precision']:.4f}"
    )

    print(
        f"Current Recall:    "
        f"{current_result['recall']:.4f}"
    )

    print(
        f"Current F1:        "
        f"{current_result['f1']:.4f}"
    )

    print(
        "\nF1-optimal operating point:"
    )

    print(
        f"Threshold: "
        f"{best_result['threshold']:.4f}"
    )

    print(
        f"Precision: "
        f"{best_result['precision']:.4f}"
    )

    print(
        f"Recall:    "
        f"{best_result['recall']:.4f}"
    )

    print(
        f"F1:        "
        f"{best_result['f1']:.4f}"
    )

    # --------------------------------------------------------
    # Print precision-oriented choices again for easy copying
    # --------------------------------------------------------

    for target in PRECISION_TARGETS:

        selected = precision_results.get(
            target
        )

        if selected is None:
            continue

        print(
            f"\nPrecision >= {target:.0%}:"
        )

        print(
            f"  Threshold: "
            f"{selected['threshold']:.4f}"
        )

        print(
            f"  Precision: "
            f"{selected['precision']:.4f}"
        )

        print(
            f"  Recall:    "
            f"{selected['recall']:.4f}"
        )

        print(
            f"  F1:        "
            f"{selected['f1']:.4f}"
        )

        print(
            f"  FP: {selected['fp']} | "
            f"FN: {selected['fn']}"
        )

    print(
        f"\nThreshold sweep saved to:\n"
        f"{RESULT_FILE}"
    )

    print(
        "\n=========================================="
    )

    print(
        "\nIMPORTANT:"
    )

    print(
        "Do NOT update duplicate_service.py yet."
    )

    print(
        "Choose the production operating point "
        "after comparing the 90%, 95%, and 97% "
        "precision-constrained thresholds."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    calibrate()