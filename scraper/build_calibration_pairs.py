"""
Build a synthetic duplicate-detection calibration dataset.

Ground truth comes ONLY from synthetic_ground_truth.csv:
- issue_id was assigned during synthetic-data generation, before the
  duplicate-detection pipeline processed the complaints.
- Therefore these labels are independent of the current MiniLM /
  Pinecone / cross-encoder case assignments.

Pair definitions
----------------
Positive:
    Same issue_id.

Hard negative:
    Different issue_id, same department.
    These test whether the cross-encoder can distinguish different
    underlying issues within the same broad helpdesk category.

Easy negative:
    Different issue_id, different department.

Final calibration set
---------------------
75 duplicate pairs
50 hard-negative pairs
25 easy-negative pairs
----------------------
150 total pairs

Run from project root:
    python scraper/build_calibration_pairs.py

Input:
    data/synthetic_ground_truth.csv

Output:
    data/calibration_pairs.csv
"""

import csv
import random
from itertools import combinations
from pathlib import Path


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

RANDOM_SEED = 42

INPUT_PATH = Path("data/synthetic_ground_truth.csv")
OUTPUT_PATH = Path("data/calibration_pairs.csv")

N_POSITIVE = 75
N_HARD_NEGATIVE = 50
N_EASY_NEGATIVE = 25

EXPECTED_TOTAL = (
    N_POSITIVE
    + N_HARD_NEGATIVE
    + N_EASY_NEGATIVE
)

random.seed(RANDOM_SEED)


# ---------------------------------------------------------
# Loading / validation
# ---------------------------------------------------------

def load_ground_truth():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Ground-truth file not found: {INPUT_PATH}"
        )

    with open(INPUT_PATH, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    required_columns = {
        "complaint_text",
        "issue_id",
        "department",
    }

    if not rows:
        raise ValueError("Synthetic ground-truth file is empty.")

    missing = required_columns - set(rows[0].keys())

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    # Remove unusable rows.
    cleaned_rows = []

    for row in rows:
        text = (row.get("complaint_text") or "").strip()
        issue_id = (row.get("issue_id") or "").strip()
        department = (row.get("department") or "").strip()

        if not text or not issue_id or not department:
            continue

        row["complaint_text"] = text
        row["issue_id"] = issue_id
        row["department"] = department

        cleaned_rows.append(row)

    if not cleaned_rows:
        raise ValueError(
            "No valid rows remained after validation."
        )

    return cleaned_rows


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def pair_key(a, b):
    """
    Canonical text-pair key used to prevent the same pair from
    appearing twice in reversed order.
    """

    text_a = a["complaint_text"].strip()
    text_b = b["complaint_text"].strip()

    return tuple(sorted((text_a, text_b)))


def sample_balanced_candidates(
    grouped_candidates,
    target_count,
):
    """
    Round-robin sampling across groups.

    This prevents one issue or department from dominating the
    calibration set while still allowing enough pairs to reach
    the requested target.
    """

    group_names = list(grouped_candidates.keys())
    random.shuffle(group_names)

    for group in group_names:
        random.shuffle(grouped_candidates[group])

    selected = []
    selected_keys = set()

    made_progress = True

    while (
        len(selected) < target_count
        and made_progress
    ):
        made_progress = False

        for group in group_names:
            candidates = grouped_candidates[group]

            while candidates:
                pair = candidates.pop()
                key = pair_key(*pair)

                if key in selected_keys:
                    continue

                selected.append(pair)
                selected_keys.add(key)
                made_progress = True
                break

            if len(selected) >= target_count:
                break

    return selected


# ---------------------------------------------------------
# Positive pairs
# ---------------------------------------------------------

def build_positive_pairs(rows):
    """
    Same issue_id = duplicate.
    """

    by_issue = {}

    for row in rows:
        by_issue.setdefault(
            row["issue_id"], []
        ).append(row)

    candidates_by_issue = {}

    for issue_id, items in by_issue.items():

        if len(items) < 2:
            continue

        candidates_by_issue[issue_id] = list(
            combinations(items, 2)
        )

    selected = sample_balanced_candidates(
        candidates_by_issue,
        N_POSITIVE,
    )

    if len(selected) < N_POSITIVE:
        raise ValueError(
            f"Could generate only {len(selected)} "
            f"positive pairs; {N_POSITIVE} required."
        )

    return selected


# ---------------------------------------------------------
# Hard negatives
# ---------------------------------------------------------

def build_hard_negative_pairs(rows):
    """
    Different issue_id + same department = hard negative.
    """

    by_department = {}

    for row in rows:
        by_department.setdefault(
            row["department"], []
        ).append(row)

    candidates_by_department = {}

    for department, items in by_department.items():

        candidates = []

        for a, b in combinations(items, 2):

            if a["issue_id"] == b["issue_id"]:
                continue

            candidates.append((a, b))

        if candidates:
            candidates_by_department[
                department
            ] = candidates

    selected = sample_balanced_candidates(
        candidates_by_department,
        N_HARD_NEGATIVE,
    )

    if len(selected) < N_HARD_NEGATIVE:
        raise ValueError(
            f"Could generate only {len(selected)} "
            f"hard-negative pairs; "
            f"{N_HARD_NEGATIVE} required."
        )

    return selected


# ---------------------------------------------------------
# Easy negatives
# ---------------------------------------------------------

def build_easy_negative_pairs(rows):
    """
    Different department = easy negative.
    """

    by_department = {}

    for row in rows:
        by_department.setdefault(
            row["department"], []
        ).append(row)

    departments = list(by_department.keys())

    if len(departments) < 2:
        raise ValueError(
            "At least two departments are required "
            "to generate easy negatives."
        )

    selected = []
    selected_keys = set()

    max_attempts = 10000
    attempts = 0

    while (
        len(selected) < N_EASY_NEGATIVE
        and attempts < max_attempts
    ):
        attempts += 1

        dept_a, dept_b = random.sample(
            departments,
            2,
        )

        a = random.choice(
            by_department[dept_a]
        )

        b = random.choice(
            by_department[dept_b]
        )

        key = pair_key(a, b)

        if key in selected_keys:
            continue

        selected.append((a, b))
        selected_keys.add(key)

    if len(selected) < N_EASY_NEGATIVE:
        raise ValueError(
            f"Could generate only {len(selected)} "
            f"easy-negative pairs; "
            f"{N_EASY_NEGATIVE} required."
        )

    return selected


# ---------------------------------------------------------
# Output construction
# ---------------------------------------------------------

def make_output_row(
    pair_id,
    a,
    b,
    pair_type,
    true_label,
):
    """
    Preserve ground-truth metadata for auditability.

    issue_id and department are NOT model inputs.
    They only document how synthetic labels were obtained.
    """

    return {
        "pair_id": pair_id,
        "source": "synthetic",
        "pair_type": pair_type,

        "text_a": a["complaint_text"],
        "text_b": b["complaint_text"],

        "issue_id_a": a["issue_id"],
        "issue_id_b": b["issue_id"],

        "department_a": a["department"],
        "department_b": b["department"],

        "true_label": true_label,
    }


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    rows = load_ground_truth()

    print(
        f"Loaded {len(rows)} valid synthetic "
        f"ground-truth complaints."
    )

    positive_pairs = build_positive_pairs(rows)

    hard_negative_pairs = (
        build_hard_negative_pairs(rows)
    )

    easy_negative_pairs = (
        build_easy_negative_pairs(rows)
    )

    output_rows = []

    # -------------------------
    # Positive
    # -------------------------

    for a, b in positive_pairs:

        output_rows.append(
            make_output_row(
                pair_id=0,
                a=a,
                b=b,
                pair_type="positive",
                true_label="duplicate",
            )
        )

    # -------------------------
    # Hard negative
    # -------------------------

    for a, b in hard_negative_pairs:

        output_rows.append(
            make_output_row(
                pair_id=0,
                a=a,
                b=b,
                pair_type="hard_negative",
                true_label="not_duplicate",
            )
        )

    # -------------------------
    # Easy negative
    # -------------------------

    for a, b in easy_negative_pairs:

        output_rows.append(
            make_output_row(
                pair_id=0,
                a=a,
                b=b,
                pair_type="easy_negative",
                true_label="not_duplicate",
            )
        )

    # Randomize order so pair type is not grouped.
    random.shuffle(output_rows)

    # Assign final reproducible IDs.
    for pair_id, row in enumerate(
        output_rows,
        start=1,
    ):
        row["pair_id"] = pair_id

    # Final integrity checks.
    if len(output_rows) != EXPECTED_TOTAL:
        raise RuntimeError(
            f"Expected {EXPECTED_TOTAL} pairs, "
            f"but generated {len(output_rows)}."
        )

    positive_count = sum(
        row["true_label"] == "duplicate"
        for row in output_rows
    )

    negative_count = sum(
        row["true_label"] == "not_duplicate"
        for row in output_rows
    )

    if positive_count != N_POSITIVE:
        raise RuntimeError(
            "Positive pair count mismatch."
        )

    if negative_count != (
        N_HARD_NEGATIVE
        + N_EASY_NEGATIVE
    ):
        raise RuntimeError(
            "Negative pair count mismatch."
        )

    # -------------------------
    # Save CSV
    # -------------------------

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fieldnames = [
        "pair_id",
        "source",
        "pair_type",
        "text_a",
        "text_b",
        "issue_id_a",
        "issue_id_b",
        "department_a",
        "department_b",
        "true_label",
    ]

    with open(
        OUTPUT_PATH,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(output_rows)

    # -------------------------
    # Summary
    # -------------------------

    print("\nCalibration dataset created successfully.")
    print("-----------------------------------------")
    print(f"Positive pairs      : {len(positive_pairs)}")
    print(f"Hard negatives      : {len(hard_negative_pairs)}")
    print(f"Easy negatives      : {len(easy_negative_pairs)}")
    print(f"Total negatives     : {negative_count}")
    print(f"Total pairs         : {len(output_rows)}")
    print(f"Random seed         : {RANDOM_SEED}")
    print(f"Saved to            : {OUTPUT_PATH}")

    print("\nGround-truth balance:")
    print(
        f"  Duplicate         : "
        f"{positive_count} "
        f"({positive_count / len(output_rows):.1%})"
    )
    print(
        f"  Not duplicate     : "
        f"{negative_count} "
        f"({negative_count / len(output_rows):.1%})"
    )


if __name__ == "__main__":
    main()