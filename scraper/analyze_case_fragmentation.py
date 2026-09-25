"""
Analyze case fragmentation and cross-issue contamination
for the final InsightDesk STS-B deduplication run.

Ground truth:
    data/synthetic_ground_truth.csv

Output:
    data/synthetic_case_fragmentation.csv
    data/synthetic_cross_issue_cases.csv

This script:
- DOES NOT modify Supabase.
- DOES NOT modify Pinecone.
- DOES NOT run model inference.
- Maps the 488 synthetic complaints to their final case IDs.
- Handles repeated identical complaint texts using occurrence numbers.
"""

import pandas as pd

from backend.database.supabase import supabase


# ==========================================================
# Configuration
# ==========================================================

GROUND_TRUTH_PATH = (
    "data/synthetic_ground_truth.csv"
)

FRAGMENTATION_OUTPUT_PATH = (
    "data/synthetic_case_fragmentation.csv"
)

CONTAMINATION_OUTPUT_PATH = (
    "data/synthetic_cross_issue_cases.csv"
)


# ==========================================================
# Supabase Helper
# ==========================================================

def fetch_all_synthetic_complaints(
    batch_size=500,
):
    """
    Fetch all synthetic complaints from Supabase using
    pagination so PostgREST limits do not truncate results.
    """

    rows = []
    start = 0

    while True:

        batch = (
            supabase
            .table("complaints")
            .select(
                "complaint_id,"
                "complaint_text,"
                "case_id,"
                "is_duplicate,"
                "is_synthetic,"
                "created_at"
            )
            .eq(
                "is_synthetic",
                True,
            )
            .order(
                "complaint_id"
            )
            .range(
                start,
                start + batch_size - 1,
            )
            .execute()
            .data
        )

        rows.extend(batch)

        if len(batch) < batch_size:
            break

        start += batch_size

    return rows


# ==========================================================
# Text Preparation
# ==========================================================

def normalize_text_for_mapping(series):
    """
    Apply only minimal normalization required for deterministic
    CSV-to-database mapping.

    IMPORTANT:
    This is NOT the semantic preprocessing used by InsightDesk.
    """

    return (
        series
        .astype(str)
        .str.strip()
    )


# ==========================================================
# Mapping
# ==========================================================

def map_ground_truth_to_database(
    gt,
    db,
):
    """
    Map generator ground-truth complaints to their final
    Supabase complaint/case records.

    complaint_text is not unique because some synthetic
    complaints intentionally contain identical text.

    Therefore mapping uses:

        complaint_text + occurrence number

    Occurrence numbers are assigned deterministically:
    - ground truth ordered by complaint_ref
    - database ordered by complaint_id

    Before mapping, text multiplicities must match exactly.
    """

    print("\n" + "=" * 70)
    print("MAPPING VALIDATION")
    print("=" * 70)

    # ------------------------------------------------------
    # Normalize text
    # ------------------------------------------------------

    gt = gt.copy()
    db = db.copy()

    gt["complaint_text"] = (
        normalize_text_for_mapping(
            gt["complaint_text"]
        )
    )

    db["complaint_text"] = (
        normalize_text_for_mapping(
            db["complaint_text"]
        )
    )

    # ------------------------------------------------------
    # Deterministic ordering
    # ------------------------------------------------------

    gt = (
        gt
        .sort_values(
            "complaint_ref"
        )
        .reset_index(drop=True)
    )

    db = (
        db
        .sort_values(
            "complaint_id"
        )
        .reset_index(drop=True)
    )

    # ------------------------------------------------------
    # Compare text multiplicities
    # ------------------------------------------------------

    gt_counts = (
        gt["complaint_text"]
        .value_counts()
        .sort_index()
    )

    db_counts = (
        db["complaint_text"]
        .value_counts()
        .sort_index()
    )

    repeated_gt_texts = int(
        (gt_counts > 1).sum()
    )

    repeated_db_texts = int(
        (db_counts > 1).sum()
    )

    print(
        "Repeated texts in ground truth:",
        repeated_gt_texts,
    )

    print(
        "Repeated texts in database:",
        repeated_db_texts,
    )

    all_texts = (
        set(gt_counts.index)
        | set(db_counts.index)
    )

    mismatches = []

    for text in all_texts:

        gt_count = int(
            gt_counts.get(
                text,
                0,
            )
        )

        db_count = int(
            db_counts.get(
                text,
                0,
            )
        )

        if gt_count != db_count:

            mismatches.append(
                {
                    "complaint_text": text,
                    "ground_truth_count":
                        gt_count,
                    "database_count":
                        db_count,
                }
            )

    print(
        "Texts with count mismatch:",
        len(mismatches),
    )

    if mismatches:

        mismatch_df = pd.DataFrame(
            mismatches
        )

        print(
            "\nTEXT MULTIPLICITY MISMATCHES"
        )

        print("-" * 70)

        print(
            mismatch_df
            .head(20)
            .to_string(index=False)
        )

        raise RuntimeError(
            "Ground-truth and database text "
            "multiplicities do not match. "
            "Stopping to avoid ambiguous mapping."
        )

    # ------------------------------------------------------
    # Occurrence number
    # ------------------------------------------------------

    gt["text_occurrence"] = (
        gt
        .groupby(
            "complaint_text"
        )
        .cumcount()
    )

    db["text_occurrence"] = (
        db
        .groupby(
            "complaint_text"
        )
        .cumcount()
    )

    # ------------------------------------------------------
    # One-to-one merge
    # ------------------------------------------------------

    mapped = gt.merge(
        db[
            [
                "complaint_id",
                "complaint_text",
                "text_occurrence",
                "case_id",
                "is_duplicate",
                "created_at",
            ]
        ],
        on=[
            "complaint_text",
            "text_occurrence",
        ],
        how="left",
        validate="one_to_one",
    )

    mapped_count = int(
        mapped["case_id"]
        .notna()
        .sum()
    )

    missing = mapped[
        mapped["case_id"].isna()
    ]

    print(
        "Mapped synthetic complaints:",
        mapped_count,
    )

    print(
        "Unmapped synthetic complaints:",
        len(missing),
    )

    # ------------------------------------------------------
    # Integrity checks
    # ------------------------------------------------------

    if len(mapped) != len(gt):

        raise RuntimeError(
            "Mapped row count does not equal "
            "ground-truth row count."
        )

    if len(missing) > 0:

        print(
            "\nUNMAPPED COMPLAINTS"
        )

        print("-" * 70)

        print(
            missing[
                [
                    "complaint_ref",
                    "issue_id",
                    "complaint_text",
                    "text_occurrence",
                ]
            ]
            .head(20)
            .to_string(index=False)
        )

        raise RuntimeError(
            "Synthetic complaint mapping "
            "is incomplete."
        )

    if mapped[
        "complaint_id"
    ].duplicated().any():

        raise RuntimeError(
            "A database complaint was mapped "
            "more than once."
        )

    mapped["case_id"] = (
        mapped["case_id"]
        .astype(int)
    )

    print(
        "Mapping integrity: PASS"
    )

    return mapped


# ==========================================================
# Fragmentation Analysis
# ==========================================================

def analyze_fragmentation(mapped):
    """
    Measure how many final cases each generator issue_id
    was split across.
    """

    results = []

    for issue_id, group in (
        mapped.groupby("issue_id")
    ):

        complaint_count = len(group)

        case_counts = (
            group["case_id"]
            .value_counts()
        )

        final_cases = int(
            group["case_id"]
            .nunique()
        )

        largest_case_size = int(
            case_counts.iloc[0]
        )

        largest_case_coverage = (
            largest_case_size
            / complaint_count
        )

        fragmentation_ratio = (
            final_cases
            / complaint_count
        )

        duplicate_count = int(
            group["is_duplicate"]
            .eq(True)
            .sum()
        )

        new_case_count = int(
            group["is_duplicate"]
            .eq(False)
            .sum()
        )

        results.append(
            {
                "issue_id":
                    issue_id,

                "department":
                    group[
                        "department"
                    ].iloc[0],

                "complaints":
                    complaint_count,

                "final_cases":
                    final_cases,

                "new_case_complaints":
                    new_case_count,

                "duplicate_complaints":
                    duplicate_count,

                "largest_case_size":
                    largest_case_size,

                "largest_case_coverage":
                    largest_case_coverage,

                "fragmentation_ratio":
                    fragmentation_ratio,
            }
        )

    results_df = pd.DataFrame(
        results
    )

    results_df = (
        results_df
        .sort_values(
            [
                "fragmentation_ratio",
                "final_cases",
                "complaints",
            ],
            ascending=[
                False,
                False,
                False,
            ],
        )
        .reset_index(drop=True)
    )

    return results_df


# ==========================================================
# Contamination Analysis
# ==========================================================

def analyze_cross_issue_cases(mapped):
    """
    Identify final cases containing synthetic complaints
    from more than one generator issue_id.

    These are potential false-merge / contamination cases.
    """

    issues_per_case = (
        mapped
        .groupby("case_id")[
            "issue_id"
        ]
        .nunique()
    )

    contaminated_case_ids = (
        issues_per_case[
            issues_per_case > 1
        ]
        .index
        .tolist()
    )

    rows = []

    for case_id in (
        contaminated_case_ids
    ):

        group = mapped[
            mapped["case_id"]
            == case_id
        ]

        issue_ids = sorted(
            group[
                "issue_id"
            ]
            .unique()
            .tolist()
        )

        departments = sorted(
            group[
                "department"
            ]
            .astype(str)
            .unique()
            .tolist()
        )

        rows.append(
            {
                "case_id":
                    int(case_id),

                "complaints":
                    len(group),

                "issue_count":
                    len(issue_ids),

                "issue_ids":
                    " | ".join(
                        issue_ids
                    ),

                "departments":
                    " | ".join(
                        departments
                    ),
            }
        )

    contamination_df = (
        pd.DataFrame(rows)
    )

    if not contamination_df.empty:

        contamination_df = (
            contamination_df
            .sort_values(
                [
                    "issue_count",
                    "complaints",
                ],
                ascending=[
                    False,
                    False,
                ],
            )
            .reset_index(drop=True)
        )

    return (
        contaminated_case_ids,
        contamination_df,
    )


# ==========================================================
# Main
# ==========================================================

def main():

    print(
        "=" * 70
    )

    print(
        "SYNTHETIC CASE FRAGMENTATION ANALYSIS"
    )

    print(
        "=" * 70
    )

    # ------------------------------------------------------
    # Load ground truth
    # ------------------------------------------------------

    gt = pd.read_csv(
        GROUND_TRUTH_PATH
    )

    required_columns = {
        "complaint_ref",
        "department",
        "issue_id",
        "complaint_text",
    }

    missing_columns = (
        required_columns
        - set(gt.columns)
    )

    if missing_columns:

        raise RuntimeError(
            "Ground-truth CSV is missing "
            f"columns: {sorted(missing_columns)}"
        )

    print(
        "Ground-truth rows:",
        len(gt),
    )

    print(
        "Ground-truth issues:",
        gt[
            "issue_id"
        ].nunique(),
    )

    # ------------------------------------------------------
    # Fetch final DB assignments
    # ------------------------------------------------------

    db_rows = (
        fetch_all_synthetic_complaints()
    )

    db = pd.DataFrame(
        db_rows
    )

    print(
        "Synthetic DB complaints:",
        len(db),
    )

    if len(gt) != len(db):

        raise RuntimeError(
            "\nSynthetic row-count mismatch.\n"
            f"Ground truth: {len(gt)}\n"
            f"Database:     {len(db)}"
        )

    # ------------------------------------------------------
    # Map
    # ------------------------------------------------------

    mapped = (
        map_ground_truth_to_database(
            gt,
            db,
        )
    )

    # ------------------------------------------------------
    # Fragmentation
    # ------------------------------------------------------

    results_df = (
        analyze_fragmentation(
            mapped
        )
    )

    # ------------------------------------------------------
    # Contamination
    # ------------------------------------------------------

    (
        contaminated_case_ids,
        contamination_df,
    ) = analyze_cross_issue_cases(
        mapped
    )

    # ------------------------------------------------------
    # Global statistics
    # ------------------------------------------------------

    synthetic_complaints = len(
        mapped
    )

    ground_truth_issues = int(
        mapped[
            "issue_id"
        ].nunique()
    )

    synthetic_cases = int(
        mapped[
            "case_id"
        ].nunique()
    )

    avg_cases_per_issue = float(
        results_df[
            "final_cases"
        ].mean()
    )

    median_cases_per_issue = float(
        results_df[
            "final_cases"
        ].median()
    )

    min_cases_per_issue = int(
        results_df[
            "final_cases"
        ].min()
    )

    max_cases_per_issue = int(
        results_df[
            "final_cases"
        ].max()
    )

    overall_case_complaint_ratio = (
        synthetic_cases
        / synthetic_complaints
    )

    mean_largest_case_coverage = float(
        results_df[
            "largest_case_coverage"
        ].mean()
    )

    # ------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------

    results_df.to_csv(
        FRAGMENTATION_OUTPUT_PATH,
        index=False,
    )

    contamination_df.to_csv(
        CONTAMINATION_OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Summary
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "FRAGMENTATION SUMMARY"
    )

    print(
        "=" * 70
    )

    print(
        "Synthetic complaints:",
        synthetic_complaints,
    )

    print(
        "Ground-truth issues:",
        ground_truth_issues,
    )

    print(
        "Final cases containing "
        "synthetic complaints:",
        synthetic_cases,
    )

    print(
        "Average cases per issue:",
        round(
            avg_cases_per_issue,
            2,
        ),
    )

    print(
        "Median cases per issue:",
        round(
            median_cases_per_issue,
            2,
        ),
    )

    print(
        "Minimum cases per issue:",
        min_cases_per_issue,
    )

    print(
        "Maximum cases per issue:",
        max_cases_per_issue,
    )

    print(
        "Overall case/complaint ratio:",
        round(
            overall_case_complaint_ratio,
            4,
        ),
    )

    print(
        "Mean largest-case coverage:",
        f"{mean_largest_case_coverage:.2%}",
    )

    print(
        "Cases containing >1 issue_id:",
        len(
            contaminated_case_ids
        ),
    )

    # ------------------------------------------------------
    # Per-issue table
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "PER-ISSUE FRAGMENTATION"
    )

    print(
        "=" * 70
    )

    display_df = (
        results_df.copy()
    )

    display_df[
        "largest_case_coverage"
    ] = (
        display_df[
            "largest_case_coverage"
        ]
        .map(
            lambda x: f"{x:.2%}"
        )
    )

    display_df[
        "fragmentation_ratio"
    ] = (
        display_df[
            "fragmentation_ratio"
        ]
        .map(
            lambda x: f"{x:.2%}"
        )
    )

    print(
        display_df.to_string(
            index=False
        )
    )

    # ------------------------------------------------------
    # Cross-issue contamination
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "CROSS-ISSUE CASES"
    )

    print(
        "=" * 70
    )

    if contamination_df.empty:

        print(
            "None"
        )

    else:

        print(
            contamination_df
            .to_string(
                index=False
            )
        )

        print(
            "\nDETAILS"
        )

        print(
            "-" * 70
        )

        for case_id in (
            contaminated_case_ids
        ):

            group = mapped[
                mapped["case_id"]
                == case_id
            ]

            print(
                f"\nCASE {case_id}"
            )

            print(
                "Issue IDs:",
                sorted(
                    group[
                        "issue_id"
                    ]
                    .unique()
                    .tolist()
                ),
            )

            for _, row in (
                group.iterrows()
            ):

                print(
                    f"  [{row['issue_id']}] "
                    f"{row['complaint_text']}"
                )

    # ------------------------------------------------------
    # Output paths
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
        FRAGMENTATION_OUTPUT_PATH
    )

    print(
        CONTAMINATION_OUTPUT_PATH
    )

    print(
        "\nAnalysis complete."
    )


if __name__ == "__main__":
    main()