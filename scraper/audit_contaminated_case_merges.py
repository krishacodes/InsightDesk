"""
Final diagnostic for contaminated synthetic cases.

For every final case containing synthetic complaints from multiple
generator issue_ids:

1. Map generator ground truth to the final Supabase complaints.
2. Fetch the final case representative_text.
3. Identify which synthetic complaint corresponds to that representative,
   when possible.
4. Score representative_text <-> each attached synthetic complaint
   using the SAME production preprocessing and STS-B CrossEncoder.
5. Compare scores against production threshold 0.5858.
6. Save detailed results.

IMPORTANT
---------
This is a present-state consistency diagnostic.

It does NOT perfectly replay historical ingestion because:
- the final Pinecone candidate pool differs from the pool available
  when each complaint originally arrived;
- this script does not reconstruct Top-K retrieval at each timestamp;
- it does not claim that every current representative/complaint score
  was the exact historical score responsible for the merge.

However, because production cases keep a frozen representative_text,
the representative-to-complaint comparison directly audits the semantic
decision surface used by the production CrossEncoder.

READ-ONLY:
- Does not modify Supabase
- Does not modify Pinecone
- Does not modify threshold
"""

import pandas as pd
from sentence_transformers import CrossEncoder

from backend.database.supabase import supabase
from backend.services.preprocess import clean_text


# ==========================================================
# Configuration
# ==========================================================

GROUND_TRUTH_PATH = (
    "data/synthetic_ground_truth.csv"
)

OUTPUT_PATH = (
    "data/stsb_contaminated_case_merge_audit.csv"
)

CASE_SUMMARY_OUTPUT_PATH = (
    "data/stsb_contaminated_case_merge_audit_summary.csv"
)

MODEL_NAME = (
    "cross-encoder/stsb-distilroberta-base"
)

THRESHOLD = 0.5858


# ==========================================================
# Supabase fetch helpers
# ==========================================================

def fetch_all_synthetic_complaints(
    batch_size=500,
):
    rows = []
    start = 0

    while True:

        batch = (
            supabase
            .table("complaints")
            .select(
                "complaint_id,"
                "complaint_text,"
                "cleaned_text,"
                "case_id,"
                "is_duplicate,"
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

    return pd.DataFrame(rows)


def fetch_cases_by_ids(
    case_ids,
    batch_size=100,
):
    """
    Fetch only the cases required for this diagnostic.
    """

    rows = []

    case_ids = [
        int(case_id)
        for case_id in case_ids
    ]

    for start in range(
        0,
        len(case_ids),
        batch_size,
    ):

        batch_ids = case_ids[
            start:
            start + batch_size
        ]

        batch = (
            supabase
            .table("cases")
            .select(
                "case_id,"
                "representative_text,"
                "created_at,"
                "last_reported_at,"
                "report_count"
            )
            .in_(
                "case_id",
                batch_ids,
            )
            .execute()
            .data
        )

        rows.extend(batch)

    return pd.DataFrame(rows)


# ==========================================================
# Ground-truth mapping
# ==========================================================

def map_ground_truth(
    gt,
    db,
):
    """
    Reuse the validated deterministic mapping strategy from the
    previous fragmentation/false-merge analysis.

    Repeated identical complaint texts are aligned using occurrence
    order after sorting GT by complaint_ref and DB by complaint_id.
    """

    gt = gt.copy()
    db = db.copy()

    gt["complaint_text"] = (
        gt["complaint_text"]
        .astype(str)
        .str.strip()
    )

    db["complaint_text"] = (
        db["complaint_text"]
        .astype(str)
        .str.strip()
    )

    gt = (
        gt
        .sort_values(
            "complaint_ref"
        )
        .reset_index(
            drop=True
        )
    )

    db = (
        db
        .sort_values(
            "complaint_id"
        )
        .reset_index(
            drop=True
        )
    )

    # ------------------------------------------------------
    # Multiplicity integrity check
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
                    "complaint_text":
                        text,
                    "gt_count":
                        gt_count,
                    "db_count":
                        db_count,
                }
            )

    if mismatches:

        mismatch_df = pd.DataFrame(
            mismatches
        )

        print(
            mismatch_df.head(
                20
            ).to_string(
                index=False
            )
        )

        raise RuntimeError(
            "Ground-truth/database text "
            "multiplicity mismatch."
        )

    # ------------------------------------------------------
    # Repeated identical texts
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

    mapped = gt.merge(
        db[
            [
                "complaint_id",
                "complaint_text",
                "text_occurrence",
                "cleaned_text",
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

    if mapped["complaint_id"].isna().any():

        raise RuntimeError(
            "Incomplete synthetic complaint mapping."
        )

    if mapped["case_id"].isna().any():

        raise RuntimeError(
            "Mapped synthetic complaint has "
            "no case_id."
        )

    mapped["complaint_id"] = (
        mapped["complaint_id"]
        .astype(int)
    )

    mapped["case_id"] = (
        mapped["case_id"]
        .astype(int)
    )

    return mapped


# ==========================================================
# Representative mapping
# ==========================================================

def normalize_for_comparison(
    text,
):
    """
    Apply the exact production preprocessing.

    representative_text is expected to already be cleaned in the
    current pipeline, but running clean_text() again makes comparison
    explicit and protects against legacy/raw rows.
    """

    if pd.isna(text):
        return ""

    return clean_text(
        str(text)
    )


def identify_representative_issue(
    representative_text,
    case_group,
):
    """
    Attempt to map the frozen case representative back to one of the
    synthetic complaints attached to the case.

    Returns:
        representative_issue_id
        representative_complaint_id
        representative_match_count
    """

    rep_clean = (
        normalize_for_comparison(
            representative_text
        )
    )

    matches = []

    for _, row in case_group.iterrows():

        complaint_clean = (
            normalize_for_comparison(
                row["complaint_text"]
            )
        )

        if complaint_clean == rep_clean:

            matches.append(
                row
            )

    if len(matches) == 1:

        match = matches[0]

        return (
            match["issue_id"],
            int(
                match["complaint_id"]
            ),
            1,
        )

    if len(matches) > 1:

        issue_ids = {
            row["issue_id"]
            for row in matches
        }

        # If repeated identical complaints all belong to the
        # same issue, the issue mapping is still unambiguous.
        if len(issue_ids) == 1:

            first = matches[0]

            return (
                first["issue_id"],
                int(
                    first["complaint_id"]
                ),
                len(matches),
            )

        # Same representative text maps to multiple generator
        # issues, so do not invent an issue identity.
        return (
            None,
            None,
            len(matches),
        )

    return (
        None,
        None,
        0,
    )


# ==========================================================
# Main
# ==========================================================

def main():

    print("=" * 70)
    print("FINAL CONTAMINATED-CASE MERGE AUDIT")
    print("=" * 70)

    # ------------------------------------------------------
    # Load ground truth + synthetic DB complaints
    # ------------------------------------------------------

    gt = pd.read_csv(
        GROUND_TRUTH_PATH
    )

    db = (
        fetch_all_synthetic_complaints()
    )

    print(
        "Ground-truth complaints:",
        len(gt),
    )

    print(
        "Database synthetic complaints:",
        len(db),
    )

    mapped = map_ground_truth(
        gt,
        db,
    )

    print(
        "Mapped synthetic complaints:",
        len(mapped),
    )

    # ------------------------------------------------------
    # Identify contaminated cases
    # ------------------------------------------------------

    issue_counts = (
        mapped
        .groupby(
            "case_id"
        )["issue_id"]
        .nunique()
    )

    contaminated_ids = (
        issue_counts[
            issue_counts > 1
        ]
        .index
        .tolist()
    )

    print(
        "Cross-issue cases:",
        len(contaminated_ids),
    )

    if not contaminated_ids:

        print(
            "No contaminated synthetic cases found."
        )
        return

    # ------------------------------------------------------
    # Fetch final case representatives
    # ------------------------------------------------------

    cases = fetch_cases_by_ids(
        contaminated_ids
    )

    if len(cases) != len(
        contaminated_ids
    ):

        fetched_ids = set(
            cases["case_id"]
            .astype(int)
            .tolist()
        )

        missing_ids = (
            set(contaminated_ids)
            - fetched_ids
        )

        raise RuntimeError(
            "Could not fetch all contaminated "
            f"cases. Missing: {sorted(missing_ids)}"
        )

    cases["case_id"] = (
        cases["case_id"]
        .astype(int)
    )

    case_lookup = (
        cases
        .set_index(
            "case_id"
        )
        .to_dict(
            orient="index"
        )
    )

    # ------------------------------------------------------
    # Load production CrossEncoder
    # ------------------------------------------------------

    print(
        "\nLoading:",
        MODEL_NAME,
    )

    model = CrossEncoder(
        MODEL_NAME
    )

    # ------------------------------------------------------
    # Build representative -> complaint comparisons
    # ------------------------------------------------------

    audit_rows = []

    for case_id in contaminated_ids:

        case_group = (
            mapped[
                mapped["case_id"]
                == case_id
            ]
            .copy()
            .sort_values(
                [
                    "created_at",
                    "complaint_id",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        case_data = (
            case_lookup[
                case_id
            ]
        )

        representative_text = (
            case_data.get(
                "representative_text"
            )
        )

        representative_clean = (
            normalize_for_comparison(
                representative_text
            )
        )

        if not representative_clean:

            raise RuntimeError(
                f"Case {case_id} has empty "
                "representative_text."
            )

        (
            representative_issue_id,
            representative_complaint_id,
            representative_match_count,
        ) = identify_representative_issue(
            representative_text,
            case_group,
        )

        issue_ids_in_case = sorted(
            case_group[
                "issue_id"
            ]
            .astype(str)
            .unique()
            .tolist()
        )

        for _, complaint in (
            case_group.iterrows()
        ):

            complaint_clean = (
                normalize_for_comparison(
                    complaint[
                        "complaint_text"
                    ]
                )
            )

            # Score using the exact semantic objects:
            #
            # incoming cleaned complaint
            #        <->
            # frozen cleaned representative
            #
            # STS-B is symmetric in intended semantics,
            # but we keep production ordering here.
            score = float(
                model.predict(
                    [
                        (
                            complaint_clean,
                            representative_clean,
                        )
                    ]
                )[0]
            )

            if (
                representative_issue_id
                is None
            ):
                relation = (
                    "representative_issue_unknown"
                )

            elif (
                str(
                    complaint["issue_id"]
                )
                == str(
                    representative_issue_id
                )
            ):
                relation = (
                    "same_issue_as_representative"
                )

            else:
                relation = (
                    "cross_issue_vs_representative"
                )

            audit_rows.append(
                {
                    "case_id":
                        case_id,

                    "case_created_at":
                        case_data.get(
                            "created_at"
                        ),

                    "case_report_count":
                        case_data.get(
                            "report_count"
                        ),

                    "issues_in_case":
                        " | ".join(
                            issue_ids_in_case
                        ),

                    "representative_text":
                        representative_text,

                    "representative_cleaned_text":
                        representative_clean,

                    "representative_issue_id":
                        representative_issue_id,

                    "representative_complaint_id":
                        representative_complaint_id,

                    "representative_match_count":
                        representative_match_count,

                    "complaint_id":
                        int(
                            complaint[
                                "complaint_id"
                            ]
                        ),

                    "complaint_created_at":
                        complaint[
                            "created_at"
                        ],

                    "complaint_issue_id":
                        complaint[
                            "issue_id"
                        ],

                    "complaint_is_duplicate":
                        complaint[
                            "is_duplicate"
                        ],

                    "complaint_text":
                        complaint[
                            "complaint_text"
                        ],

                    "complaint_cleaned_text":
                        complaint_clean,

                    "relation_to_representative":
                        relation,

                    "stsb_score":
                        score,

                    "threshold":
                        THRESHOLD,

                    "above_threshold":
                        bool(
                            score
                            >= THRESHOLD
                        ),
                }
            )

    audit = pd.DataFrame(
        audit_rows
    )

    audit.to_csv(
        OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Cross-issue representative comparisons only
    # ------------------------------------------------------

    cross_issue = (
        audit[
            audit[
                "relation_to_representative"
            ]
            == "cross_issue_vs_representative"
        ]
        .copy()
    )

    unknown_rep = (
        audit[
            audit[
                "relation_to_representative"
            ]
            == "representative_issue_unknown"
        ]
        .copy()
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "REPRESENTATIVE MAPPING"
    )

    print(
        "=" * 70
    )

    mapped_rep_cases = (
        audit[
            audit[
                "representative_issue_id"
            ].notna()
        ][
            "case_id"
        ]
        .nunique()
    )

    unknown_rep_cases = (
        audit[
            audit[
                "representative_issue_id"
            ].isna()
        ][
            "case_id"
        ]
        .nunique()
    )

    print(
        "Contaminated cases:",
        len(contaminated_ids),
    )

    print(
        "Representative issue mapped:",
        mapped_rep_cases,
    )

    print(
        "Representative issue unknown:",
        unknown_rep_cases,
    )

    # ------------------------------------------------------
    # Main result
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "CROSS-ISSUE REPRESENTATIVE SCORE SUMMARY"
    )

    print(
        "=" * 70
    )

    print(
        "Threshold:",
        THRESHOLD,
    )

    print(
        "Cross-issue representative comparisons:",
        len(cross_issue),
    )

    if len(cross_issue) > 0:

        above = int(
            cross_issue[
                "above_threshold"
            ].sum()
        )

        below = (
            len(cross_issue)
            - above
        )

        print(
            ">= threshold:",
            above,
        )

        print(
            "< threshold:",
            below,
        )

        print(
            "Percent >= threshold:",
            f"{above / len(cross_issue):.2%}",
        )

        print(
            "Minimum score:",
            round(
                float(
                    cross_issue[
                        "stsb_score"
                    ].min()
                ),
                4,
            ),
        )

        print(
            "Mean score:",
            round(
                float(
                    cross_issue[
                        "stsb_score"
                    ].mean()
                ),
                4,
            ),
        )

        print(
            "Maximum score:",
            round(
                float(
                    cross_issue[
                        "stsb_score"
                    ].max()
                ),
                4,
            ),
        )

    print(
        "Comparisons with unknown "
        "representative issue:",
        len(unknown_rep),
    )

    # ------------------------------------------------------
    # Cases containing below-threshold cross-issue members
    # ------------------------------------------------------

    if len(cross_issue) > 0:

        below_threshold = (
            cross_issue[
                ~cross_issue[
                    "above_threshold"
                ]
            ]
        )

        below_case_ids = sorted(
            below_threshold[
                "case_id"
            ]
            .unique()
            .tolist()
        )

        print(
            "\nCases with at least one "
            "cross-issue complaint currently "
            "below threshold vs representative:",
            len(below_case_ids),
        )

        print(
            "Case IDs:",
            below_case_ids,
        )

    # ------------------------------------------------------
    # Detailed cross-issue results
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "CROSS-ISSUE REPRESENTATIVE COMPARISONS"
    )

    print(
        "=" * 70
    )

    if cross_issue.empty:

        print(
            "No cross-issue representative "
            "comparisons available."
        )

    else:

        display_rows = (
            cross_issue
            .sort_values(
                "stsb_score",
                ascending=False,
            )
        )

        for _, row in (
            display_rows.iterrows()
        ):

            print(
                f"\nCase {row['case_id']}"
            )

            print(
                "Representative issue:",
                row[
                    "representative_issue_id"
                ],
            )

            print(
                "Complaint issue:",
                row[
                    "complaint_issue_id"
                ],
            )

            print(
                "Complaint ID:",
                row[
                    "complaint_id"
                ],
            )

            print(
                "Complaint marked duplicate:",
                row[
                    "complaint_is_duplicate"
                ],
            )

            print(
                "Score:",
                round(
                    float(
                        row[
                            "stsb_score"
                        ]
                    ),
                    4,
                ),
            )

            print(
                "Above threshold:",
                row[
                    "above_threshold"
                ],
            )

            print(
                "Representative:",
                row[
                    "representative_cleaned_text"
                ],
            )

            print(
                "Complaint:",
                row[
                    "complaint_cleaned_text"
                ],
            )

    # ------------------------------------------------------
    # Per-case summary
    # ------------------------------------------------------

    if not cross_issue.empty:

        case_summary = (
            cross_issue
            .groupby(
                "case_id"
            )
            .agg(
                representative_issue_id=(
                    "representative_issue_id",
                    "first",
                ),

                cross_issue_complaints=(
                    "complaint_id",
                    "size",
                ),

                cross_issue_above_threshold=(
                    "above_threshold",
                    "sum",
                ),

                min_score=(
                    "stsb_score",
                    "min",
                ),

                mean_score=(
                    "stsb_score",
                    "mean",
                ),

                max_score=(
                    "stsb_score",
                    "max",
                ),
            )
            .reset_index()
        )

        case_summary[
            "cross_issue_below_threshold"
        ] = (
            case_summary[
                "cross_issue_complaints"
            ]
            - case_summary[
                "cross_issue_above_threshold"
            ]
        )

        case_summary.to_csv(
            CASE_SUMMARY_OUTPUT_PATH,
            index=False,
        )

        print(
            "\n" + "=" * 70
        )

        print(
            "PER-CASE SUMMARY"
        )

        print(
            "=" * 70
        )

        print(
            case_summary.to_string(
                index=False,
                formatters={
                    "min_score":
                        lambda x: f"{x:.4f}",
                    "mean_score":
                        lambda x: f"{x:.4f}",
                    "max_score":
                        lambda x: f"{x:.4f}",
                },
            )
        )

    # ------------------------------------------------------
    # Final reminder
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "INTERPRETATION NOTE"
    )

    print(
        "=" * 70
    )

    print(
        "These scores compare each final complaint "
        "with the final frozen case representative."
    )

    print(
        "They are a production-aligned present-state "
        "diagnostic, not an exact sequential replay."
    )

    print(
        "A below-threshold score must therefore NOT "
        "automatically be labelled an implementation bug."
    )

    print(
        "\nSaved:"
    )

    print(
        OUTPUT_PATH
    )

    if not cross_issue.empty:

        print(
            CASE_SUMMARY_OUTPUT_PATH
        )


if __name__ == "__main__":
    main()