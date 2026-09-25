# Duplicate-Detection Threshold Calibration

Calibration dataset: **150 independently labelled synthetic pairs** (75 duplicate, 75 non-duplicate).

Ground-truth labels were derived from generator-assigned issue identifiers established before duplicate-detection inference.

Model: `cross-encoder/ms-marco-MiniLM-L-6-v2`

A pair is predicted as duplicate when `cross_encoder_score >= threshold`.

Raw cross-encoder outputs are ranking logits, not calibrated probabilities.

## Candidate operating points

| Operating point | Threshold | TP | FP | TN | FN | Precision | Recall | F1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Existing threshold | -2.21 | 32 | 7 | 68 | 43 | 0.8205 | 0.4267 | 0.5614 |
| Maximum F1 | -8.698521 | 67 | 38 | 37 | 8 | 0.6381 | 0.8933 | 0.7444 |
| Precision >= 0.90 | 1.348029 | 22 | 2 | 73 | 53 | 0.9167 | 0.2933 | 0.4444 |
| Precision >= 0.95 | 2.420434 | 20 | 1 | 74 | 55 | 0.9524 | 0.2667 | 0.4167 |
| Precision >= 0.97 | 4.671704 | 17 | 0 | 75 | 58 | 1.0000 | 0.2267 | 0.3696 |
| Precision >= 0.98 | 4.671704 | 17 | 0 | 75 | 58 | 1.0000 | 0.2267 | 0.3696 |
| Precision >= 0.99 | 4.671704 | 17 | 0 | 75 | 58 | 1.0000 | 0.2267 | 0.3696 |
| Precision >= 1.00 | 4.671704 | 17 | 0 | 75 | 58 | 1.0000 | 0.2267 | 0.3696 |

## Threshold-selection principle

The maximum-F1 operating point is reported as a reference rather than automatically selected. InsightDesk treats false-positive duplicate merges as particularly costly because an incorrect merge can propagate into case counts, sentiment analysis, spike detection and root-cause analysis. Precision-constrained operating points are therefore reported separately for threshold selection.
