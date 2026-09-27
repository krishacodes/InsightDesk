"""
Recomputes Precision/Recall/F1 using ALREADY-COMPUTED STS-B scores,
after applying ground-truth corrections identified in the manual
audit. No model re-inference - pure relabeling + recomputation.

Requires:
  data/calibration_pairs_scored.csv
      columns: pair_id, text_a, text_b, true_label, score
      (true_label = 'duplicate' / 'not_duplicate', the ORIGINAL
      generator-assigned label; score = the STS-B score already
      computed by run_threshold_sweep.py)

  data/ground_truth_corrections.csv
      columns: pair_id, original_label, corrected_label, reason

Outputs corrected P/R/F1 at each threshold in CANDIDATE_THRESHOLDS,
alongside the ORIGINAL (uncorrected) numbers for direct comparison -
so the paper can report the delta attributable to ground-truth
correction, separate from the delta attributable to model/threshold
choice.

Run from the project root:
    python scraper/recompute_metrics_with_corrections.py
"""

import csv
from pathlib import Path

SCORED_PAIRS_PATH = Path("data/calibration_pairs_scored.csv")
CORRECTIONS_PATH = Path("data/ground_truth_corrections.csv")

CANDIDATE_THRESHOLDS = [0.1963, 0.5858, 0.6469, 0.7377]


def load_scored_pairs():
    with open(SCORED_PAIRS_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["score"] = float(r["score"])
        r["true_is_duplicate_original"] = (
            r["true_label"].strip().lower() == "duplicate"
        )
        # default: corrected == original unless overridden below
        r["true_is_duplicate_corrected"] = r["true_is_duplicate_original"]
    return {r["pair_id"]: r for r in rows}


def apply_corrections(pairs_by_id):
    if not CORRECTIONS_PATH.exists():
        print(f"No corrections file found at {CORRECTIONS_PATH} - "
              f"reporting original labels only.")
        return pairs_by_id, 0

    with open(CORRECTIONS_PATH, encoding="utf-8") as f:
        corrections = list(csv.DictReader(f))

    applied = 0
    for c in corrections:
        pid = c["pair_id"]
        if pid not in pairs_by_id:
            print(f"WARNING: correction for pair_id={pid} not found in scored pairs - skipped")
            continue
        corrected_label = c["corrected_label"].strip().lower()
        if corrected_label not in ("duplicate", "not_duplicate"):
            print(f"WARNING: pair_id={pid} has invalid corrected_label "
                  f"{c['corrected_label']!r} - skipped")
            continue
        pairs_by_id[pid]["true_is_duplicate_corrected"] = (corrected_label == "duplicate")
        applied += 1

    return pairs_by_id, applied


def evaluate(rows, threshold, label_key):
    tp = fp = tn = fn = 0
    for r in rows:
        predicted = r["score"] >= threshold
        actual = r[label_key]
        if predicted and actual: tp += 1
        elif predicted and not actual: fp += 1
        elif not predicted and not actual: tn += 1
        else: fn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"threshold": threshold, "TP": tp, "FP": fp, "TN": tn, "FN": fn,
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4)}


def main():
    pairs_by_id = load_scored_pairs()
    pairs_by_id, n_applied = apply_corrections(pairs_by_id)
    rows = list(pairs_by_id.values())

    print(f"Loaded {len(rows)} scored pairs, applied {n_applied} corrections\n")
    print(f"{'Threshold':>10} | {'--- ORIGINAL ---':^32} | {'--- CORRECTED ---':^32}")
    print(f"{'':>10} | {'P':>6} {'R':>6} {'F1':>6} {'TP':>4} {'FP':>4} {'FN':>4} | "
          f"{'P':>6} {'R':>6} {'F1':>6} {'TP':>4} {'FP':>4} {'FN':>4}")

    for t in CANDIDATE_THRESHOLDS:
        orig = evaluate(rows, t, "true_is_duplicate_original")
        corr = evaluate(rows, t, "true_is_duplicate_corrected")
        print(f"{t:>10} | {orig['precision']:>6} {orig['recall']:>6} {orig['f1']:>6} "
              f"{orig['TP']:>4} {orig['FP']:>4} {orig['FN']:>4} | "
              f"{corr['precision']:>6} {corr['recall']:>6} {corr['f1']:>6} "
              f"{corr['TP']:>4} {corr['FP']:>4} {corr['FN']:>4}")

    print("\nReport BOTH columns in your paper - the delta between them is "
          "exactly the effect of the ground-truth audit, isolated from any "
          "model or threshold change.")


if __name__ == "__main__":
    main()