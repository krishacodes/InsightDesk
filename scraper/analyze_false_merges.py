"""
Diagnose cross-issue merges produced by the final STS-B dedup run.

For every final case containing synthetic complaints from multiple
generator issue_ids:

1. Reconstruct the synthetic complaints in that case.
2. Score every cross-issue complaint pair using the SAME production
   STS-B CrossEncoder.
3. Compare scores against the production threshold 0.5858.
4. Save detailed pair-level results.

READ-ONLY:
- Does not modify Supabase
- Does not modify Pinecone
"""

import pandas as pd
from sentence_transformers import CrossEncoder

from backend.database.supabase import supabase


GROUND_TRUTH_PATH = "data/synthetic_ground_truth.csv"

OUTPUT_PATH = (
    "data/stsb_false_merge_analysis.csv"
)

MODEL_NAME = (
    "cross-encoder/stsb-distilroberta-base"
)

THRESHOLD = 0.5858


# ==========================================================
# Supabase
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
            .eq("is_synthetic", True)
            .order("complaint_id")
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


# ==========================================================
# Mapping
# ==========================================================

def map_ground_truth(gt, db):

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
        .sort_values("complaint_ref")
        .reset_index(drop=True)
    )

    db = (
        db
        .sort_values("complaint_id")
        .reset_index(drop=True)
    )

    # Ensure identical-text multiplicities match.
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

    for text in all_texts:

        if (
            int(gt_counts.get(text, 0))
            != int(db_counts.get(text, 0))
        ):
            raise RuntimeError(
                "Ground-truth/database text "
                "multiplicity mismatch."
            )

    # Handle repeated identical complaint text.
    gt["text_occurrence"] = (
        gt.groupby("complaint_text")
        .cumcount()
    )

    db["text_occurrence"] = (
        db.groupby("complaint_text")
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

    if mapped["case_id"].isna().any():
        raise RuntimeError(
            "Incomplete synthetic mapping."
        )

    mapped["case_id"] = (
        mapped["case_id"]
        .astype(int)
    )

    return mapped


# ==========================================================
# Main
# ==========================================================

def main():

    print("=" * 70)
    print("STS-B FALSE-MERGE DIAGNOSTIC")
    print("=" * 70)

    # ------------------------------------------------------
    # Load data
    # ------------------------------------------------------

    gt = pd.read_csv(
        GROUND_TRUTH_PATH
    )

    db = (
        fetch_all_synthetic_complaints()
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
        .groupby("case_id")[
            "issue_id"
        ]
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

    # ------------------------------------------------------
    # Build all cross-issue pairs
    # ------------------------------------------------------

    pair_rows = []

    for case_id in contaminated_ids:

        group = (
            mapped[
                mapped["case_id"]
                == case_id
            ]
            .reset_index(drop=True)
        )

        for i in range(len(group)):

            for j in range(
                i + 1,
                len(group),
            ):

                a = group.iloc[i]
                b = group.iloc[j]

                # Only different ground-truth issues.
                if (
                    a["issue_id"]
                    == b["issue_id"]
                ):
                    continue

                text_a = a["cleaned_text"]
                text_b = b["cleaned_text"]

                # Fallback if cleaned_text is missing.
                if (
                    pd.isna(text_a)
                    or not str(text_a).strip()
                ):
                    text_a = a[
                        "complaint_text"
                    ]

                if (
                    pd.isna(text_b)
                    or not str(text_b).strip()
                ):
                    text_b = b[
                        "complaint_text"
                    ]

                pair_rows.append(
                    {
                        "case_id":
                            case_id,

                        "issue_id_a":
                            a["issue_id"],

                        "issue_id_b":
                            b["issue_id"],

                        "complaint_id_a":
                            a["complaint_id"],

                        "complaint_id_b":
                            b["complaint_id"],

                        "text_a":
                            text_a,

                        "text_b":
                            text_b,
                    }
                )

    pairs = pd.DataFrame(
        pair_rows
    )

    print(
        "Cross-issue pairs:",
        len(pairs),
    )

    if pairs.empty:

        print(
            "No cross-issue pairs found."
        )
        return

    # ------------------------------------------------------
    # Load SAME production CrossEncoder
    # ------------------------------------------------------

    print(
        "\nLoading:",
        MODEL_NAME,
    )

    model = CrossEncoder(
        MODEL_NAME
    )

    sentence_pairs = list(
        zip(
            pairs["text_a"],
            pairs["text_b"],
        )
    )

    print(
        "Scoring pairs..."
    )

    scores = model.predict(
        sentence_pairs
    )

    pairs[
        "stsb_score"
    ] = scores

    pairs[
        "above_threshold"
    ] = (
        pairs["stsb_score"]
        >= THRESHOLD
    )

    # ------------------------------------------------------
    # Save
    # ------------------------------------------------------

    pairs.to_csv(
        OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------
    # Summary
    # ------------------------------------------------------

    total = len(pairs)

    above = int(
        pairs[
            "above_threshold"
        ].sum()
    )

    below = (
        total - above
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "FALSE-MERGE SCORE SUMMARY"
    )

    print(
        "=" * 70
    )

    print(
        "Cross-issue pairs:",
        total,
    )

    print(
        f"Threshold: {THRESHOLD}"
    )

    print(
        "Pairs >= threshold:",
        above,
    )

    print(
        "Pairs < threshold:",
        below,
    )

    print(
        "Percent >= threshold:",
        f"{above / total:.2%}",
    )

    print(
        "Minimum score:",
        round(
            float(
                pairs[
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
                pairs[
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
                pairs[
                    "stsb_score"
                ].max()
            ),
            4,
        ),
    )

    # ------------------------------------------------------
    # Highest scoring false pairs
    # ------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "HIGHEST-SCORING CROSS-ISSUE PAIRS"
    )

    print(
        "=" * 70
    )

    top = (
        pairs
        .sort_values(
            "stsb_score",
            ascending=False,
        )
        .head(20)
    )

    for _, row in top.iterrows():

        print(
            f"\nCase {row['case_id']}"
        )

        print(
            f"{row['issue_id_a']} "
            f"<-> "
            f"{row['issue_id_b']}"
        )

        print(
            "Score:",
            round(
                row["stsb_score"],
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
            "A:",
            row["text_a"],
        )

        print(
            "B:",
            row["text_b"],
        )

    # ------------------------------------------------------
    # Per-case maximum cross-issue score
    # ------------------------------------------------------

    case_summary = (
        pairs
        .groupby("case_id")
        .agg(
            max_cross_issue_score=(
                "stsb_score",
                "max",
            ),
            mean_cross_issue_score=(
                "stsb_score",
                "mean",
            ),
            cross_issue_pairs=(
                "stsb_score",
                "size",
            ),
            pairs_above_threshold=(
                "above_threshold",
                "sum",
            ),
        )
        .reset_index()
        .sort_values(
            "max_cross_issue_score",
            ascending=False,
        )
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "PER-CASE CROSS-ISSUE SCORE SUMMARY"
    )

    print(
        "=" * 70
    )

    print(
        case_summary.to_string(
            index=False,
            formatters={
                "max_cross_issue_score":
                    lambda x: f"{x:.4f}",
                "mean_cross_issue_score":
                    lambda x: f"{x:.4f}",
            },
        )
    )

    print(
        "\nSaved:",
        OUTPUT_PATH,
    )


if __name__ == "__main__":
    main()