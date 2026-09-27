"""
Scores every pair in data/calibration_pairs.csv using the SAME
cross-encoder model your live pipeline uses (ms-marco-MiniLM-L-6-v2),
then sweeps a range of candidate thresholds and reports Precision,
Recall, F1, False Positives and False Negatives at each one.

This produces the exact table needed to justify the new operating
threshold in your report, replacing the old -2.21 (which was
calibrated on a single-product corpus - see earlier session notes).

Run from the project root, AFTER filling in every blank true_label
in data/calibration_pairs.csv:
    python scraper/run_threshold_sweep.py
"""

import csv
from pathlib import Path
from sentence_transformers import CrossEncoder

CALIBRATION_PATH = Path("data/calibration_pairs.csv")
OUTPUT_TABLE_PATH = Path("data/threshold_sweep_results.csv")
OUTPUT_MARKDOWN_PATH = Path("data/threshold_sweep_results.md")

# Same model as backend/services/cross_encoder.py - must match the
# live pipeline exactly, or this calibration doesn't transfer.
MODEL_NAME = "cross-encoder/stsb-distilroberta-base"

# STS-B outputs a bounded 0-1 similarity score, unlike MS-MARCO's
# unbounded raw logit - the sweep range must match. Includes your
# three already-decided candidate operating points (0.5858, 0.6469,
# 0.7377) plus the max-F1 point (0.1963) for the full comparison
# table.
CANDIDATE_THRESHOLDS = [round(t, 4) for t in
    [0.05, 0.10, 0.15, 0.1963, 0.25, 0.30, 0.35, 0.40, 0.45,
     0.50, 0.5858, 0.60, 0.6469, 0.70, 0.7377, 0.80, 0.90]
]


def load_pairs():
    with open(CALIBRATION_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    missing_labels = [r for r in rows if not r["true_label"].strip()]
    if missing_labels:
        raise SystemExit(
            f"{len(missing_labels)} rows still have an empty true_label. "
            f"Fill in every 'real' source row (duplicate / not_duplicate) "
            f"in {CALIBRATION_PATH} before running this script."
        )

    for r in rows:
        label = r["true_label"].strip().lower()
        if label not in ("duplicate", "not_duplicate"):
            raise SystemExit(
                f"Pair {r['pair_id']} has invalid true_label: {r['true_label']!r}. "
                f"Must be exactly 'duplicate' or 'not_duplicate'."
            )
        r["true_is_duplicate"] = (label == "duplicate")

    return rows


def score_pairs(rows):
    print(f"Loading {MODEL_NAME} ...")
    model = CrossEncoder(MODEL_NAME)

    sentence_pairs = [(r["text_a"], r["text_b"]) for r in rows]
    print(f"Scoring {len(sentence_pairs)} pairs ...")
    scores = model.predict(sentence_pairs)

    for r, score in zip(rows, scores):
        r["score"] = float(score)

    return rows


def evaluate_at_threshold(rows, threshold):
    tp = fp = tn = fn = 0
    for r in rows:
        predicted_duplicate = r["score"] >= threshold
        actual_duplicate = r["true_is_duplicate"]

        if predicted_duplicate and actual_duplicate:
            tp += 1
        elif predicted_duplicate and not actual_duplicate:
            fp += 1
        elif not predicted_duplicate and not actual_duplicate:
            tn += 1
        else:
            fn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) > 0 else 0.0)

    return {
        "threshold": threshold,
        "TP": tp, "FP": fp, "TN": tn, "FN": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def main():
    rows = load_pairs()
    n_dup = sum(1 for r in rows if r["true_is_duplicate"])
    n_nondup = len(rows) - n_dup
    print(f"Loaded {len(rows)} labeled pairs ({n_dup} duplicate, {n_nondup} not_duplicate)")

    rows = score_pairs(rows)

    # Save per-pair scores so future ground-truth corrections can
    # recompute metrics WITHOUT re-running the model (see
    # recompute_metrics_with_corrections.py). This is the one thing
    # this script was previously missing - only the aggregate sweep
    # table was being saved, not individual pair scores.
    scored_pairs_path = Path("data/calibration_pairs_scored.csv")
    with open(scored_pairs_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "pair_id", "text_a", "text_b", "true_label", "score"
        ])
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "pair_id": r["pair_id"],
                "text_a": r["text_a"],
                "text_b": r["text_b"],
                "true_label": r["true_label"],
                "score": r["score"],
            })
    print(f"Saved per-pair scores to {scored_pairs_path}")

    scores = [r["score"] for r in rows]
    print(f"\nScore distribution: min={min(scores):.2f}  max={max(scores):.2f}  "
          f"mean={sum(scores)/len(scores):.2f}")

    # Two separate grids, for two separate purposes:
    #
    # 1. CANDIDATE_THRESHOLDS (the curated, round-number list) - for
    #    the printed/paper-facing summary table. Readable, but each
    #    value is an approximation and may not land exactly on a
    #    true decision boundary in the data.
    #
    # 2. observed_thresholds (every unique score actually produced
    #    by the model) - guarantees hitting every real decision
    #    boundary exactly, with no rounding gap. Used to find the
    #    TRUE best-F1 and best-precision-oriented operating points,
    #    which may differ from what the curated grid finds by a
    #    pair or two (exactly the kind of discrepancy that showed
    #    up comparing this run to an earlier, differently-derived
    #    "0.5858" report).
    observed_thresholds = sorted(set(r["score"] for r in rows))

    results = [evaluate_at_threshold(rows, t) for t in CANDIDATE_THRESHOLDS]
    results_full_grid = [evaluate_at_threshold(rows, t) for t in observed_thresholds]

    results_sorted_by_f1 = sorted(results_full_grid, key=lambda r: -r["f1"])
    best_f1 = results_sorted_by_f1[0]

    # precision-oriented pick: highest recall among thresholds with
    # precision >= 0.90, matching the ORIGINAL -2.21 calibration's
    # own stated policy (see backend/services/duplicate_service.py
    # comment: "Selected as a precision-oriented operating point")
    precision_oriented = [r for r in results_full_grid if r["precision"] >= 0.90]
    best_precision_oriented = (
        max(precision_oriented, key=lambda r: r["recall"])
        if precision_oriented else None
    )

    with open(OUTPUT_TABLE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "threshold", "TP", "FP", "TN", "FN", "precision", "recall", "f1"
        ])
        writer.writeheader()
        writer.writerows(results)

    with open(OUTPUT_MARKDOWN_PATH, "w", encoding="utf-8") as f:
        f.write(f"# Duplicate-Detection Threshold Sweep\n\n")
        f.write(f"Calibration set: {len(rows)} pairs "
                f"({n_dup} duplicate, {n_nondup} not_duplicate)\n\n")
        f.write(f"Model: `{MODEL_NAME}`\n\n")
        f.write("| Threshold | TP | FP | TN | FN | Precision | Recall | F1 |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for r in results:
            marker = ""
            if r["threshold"] == best_f1["threshold"]:
                marker += " **(best F1)**"
            if best_precision_oriented and r["threshold"] == best_precision_oriented["threshold"]:
                marker += " **(chosen: precision-oriented)**"
            f.write(f"| {r['threshold']} | {r['TP']} | {r['FP']} | {r['TN']} | {r['FN']} "
                    f"| {r['precision']} | {r['recall']} | {r['f1']}{marker} |\n")

    print("\n" + "=" * 70)
    print(f"{'Threshold':>10} {'TP':>4} {'FP':>4} {'TN':>4} {'FN':>4} "
          f"{'Precision':>10} {'Recall':>8} {'F1':>8}")
    print("-" * 70)
    for r in results:
        print(f"{r['threshold']:>10} {r['TP']:>4} {r['FP']:>4} {r['TN']:>4} {r['FN']:>4} "
              f"{r['precision']:>10} {r['recall']:>8} {r['f1']:>8}")
    print("=" * 70)

    print(f"\nBest F1: threshold={best_f1['threshold']}  "
          f"P={best_f1['precision']}  R={best_f1['recall']}  F1={best_f1['f1']}")

    if best_precision_oriented:
        print(f"Best precision-oriented (P>=0.90, max recall): "
              f"threshold={best_precision_oriented['threshold']}  "
              f"P={best_precision_oriented['precision']}  "
              f"R={best_precision_oriented['recall']}  "
              f"F1={best_precision_oriented['f1']}")
    else:
        print("No threshold in the sweep reaches P>=0.90 - "
              "widen CANDIDATE_THRESHOLDS or review calibration set quality.")

    print(f"\nSaved: {OUTPUT_TABLE_PATH}")
    print(f"Saved: {OUTPUT_MARKDOWN_PATH} (paste directly into your report)")


if __name__ == "__main__":
    main()