"""
Export qualitative examples from InsightDesk's duplicate-detection
pipeline.

This script retrieves existing cases containing multiple complaints
and prints representative examples suitable for qualitative analysis
in the project report/paper.

It is READ-ONLY:
    - Does not create complaints
    - Does not create cases
    - Does not modify case assignments
    - Does not modify Supabase

IMPORTANT:
Historical case assignments may have been generated under earlier
pipeline configurations. Therefore these examples demonstrate the
system's semantic grouping behaviour, but should not be described as
an evaluation performed specifically at the final -2.21 threshold.

Usage:
    python backend/scripts/export_duplicate_examples.py
"""

from backend.database.supabase import (
    get_cases,
    get_complaints_by_case,
)


# ============================================================
# CONFIGURATION
# ============================================================

NUM_EXAMPLES = 5

MIN_COMPLAINTS = 2

OUTPUT_FILE = "duplicate_pipeline_examples.txt"


# ============================================================
# HELPERS
# ============================================================

def normalize_text(text):
    """
    Normalize text only for checking whether two displayed
    complaints are exact textual duplicates.

    This does NOT affect any database data.
    """

    if not text:
        return ""

    return " ".join(
        str(text)
        .lower()
        .strip()
        .split()
    )


def analyze_case(complaints):
    """
    Calculate simple descriptive statistics for a grouped case.
    """

    texts = [
        complaint.get(
            "complaint_text",
            ""
        )
        for complaint in complaints
        if complaint.get(
            "complaint_text"
        )
    ]

    normalized = [
        normalize_text(text)
        for text in texts
    ]

    unique_texts = set(
        normalized
    )

    user_ids = {
        complaint.get("user_id")
        for complaint in complaints
        if complaint.get("user_id")
    }

    return {
        "complaint_count":
            len(texts),

        "unique_wording_count":
            len(unique_texts),

        "unique_user_count":
            len(user_ids),

        "has_wording_variation":
            len(unique_texts) > 1,
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "\n"
        "========== DUPLICATE PIPELINE "
        "QUALITATIVE EXAMPLES =========="
        "\n"
    )

    # --------------------------------------------------------
    # Load cases
    # --------------------------------------------------------

    cases = get_cases()

    if not cases:

        print(
            "No cases found."
        )

        return

    print(
        f"Total cases available: {len(cases)}"
    )

    print(
        f"Minimum complaints per example: "
        f"{MIN_COMPLAINTS}"
    )

    # --------------------------------------------------------
    # Collect eligible cases first
    # --------------------------------------------------------

    eligible_cases = []

    for case in cases:

        case_id = case.get(
            "case_id"
        )

        if case_id is None:
            continue

        complaints = get_complaints_by_case(
            case_id
        )

        if len(complaints) < MIN_COMPLAINTS:
            continue

        stats = analyze_case(
            complaints
        )

        eligible_cases.append(
            {
                "case":
                    case,

                "complaints":
                    complaints,

                "stats":
                    stats,
            }
        )

    if not eligible_cases:

        print(
            f"No cases found with "
            f"{MIN_COMPLAINTS}+ complaints."
        )

        return

    # --------------------------------------------------------
    # Ranking
    #
    # Prefer examples that:
    #
    # 1. contain wording variation
    # 2. contain multiple users
    # 3. contain several complaints
    #
    # This produces stronger qualitative examples than simply
    # selecting cases containing repeated identical sentences.
    # --------------------------------------------------------

    eligible_cases.sort(

        key=lambda item: (

            item["stats"][
                "has_wording_variation"
            ],

            item["stats"][
                "unique_user_count"
            ],

            item["stats"][
                "unique_wording_count"
            ],

            item["stats"][
                "complaint_count"
            ],
        ),

        reverse=True
    )

    selected_cases = (
        eligible_cases[
            :NUM_EXAMPLES
        ]
    )

    # --------------------------------------------------------
    # Generate report
    # --------------------------------------------------------

    output_lines = []

    output_lines.append(
        "INSIGHTDESK DUPLICATE-DETECTION "
        "QUALITATIVE EXAMPLES"
    )

    output_lines.append(
        "=" * 60
    )

    output_lines.append(
        ""
    )

    output_lines.append(
        f"Examples selected: "
        f"{len(selected_cases)}"
    )

    output_lines.append(
        ""
    )

    for example_number, item in enumerate(
        selected_cases,
        start=1
    ):

        case = item[
            "case"
        ]

        complaints = item[
            "complaints"
        ]

        stats = item[
            "stats"
        ]

        case_id = case.get(
            "case_id"
        )

        representative_text = case.get(
            "representative_text",
            ""
        )

        output_lines.append(
            ""
        )

        output_lines.append(
            f"EXAMPLE {example_number}"
        )

        output_lines.append(
            "-" * 60
        )

        output_lines.append(
            f"Case ID: {case_id}"
        )

        output_lines.append(
            f"Representative text: "
            f"{representative_text}"
        )

        output_lines.append(
            f"Grouped complaints: "
            f"{stats['complaint_count']}"
        )

        output_lines.append(
            f"Unique complaint wordings: "
            f"{stats['unique_wording_count']}"
        )

        output_lines.append(
            f"Unique users: "
            f"{stats['unique_user_count']}"
        )

        output_lines.append(
            ""
        )

        output_lines.append(
            "Complaints:"
        )

        # ----------------------------------------------------
        # Print complaints
        # ----------------------------------------------------

        for complaint_number, complaint in enumerate(
            complaints,
            start=1
        ):

            complaint_text = complaint.get(
                "complaint_text",
                ""
            )

            user_id = complaint.get(
                "user_id",
                "Unknown"
            )

            complaint_id = complaint.get(
                "complaint_id",
                "Unknown"
            )

            is_duplicate = complaint.get(
                "is_duplicate"
            )

            output_lines.append(
                f"  {complaint_number}. "
                f"{complaint_text}"
            )

            output_lines.append(
                f"     Complaint ID: "
                f"{complaint_id}"
            )

            output_lines.append(
                f"     User: "
                f"{user_id}"
            )

            output_lines.append(
                f"     Pipeline duplicate flag: "
                f"{is_duplicate}"
            )

        output_lines.append(
            ""
        )

    # --------------------------------------------------------
    # Print report
    # --------------------------------------------------------

    report = "\n".join(
        output_lines
    )

    print(
        "\n"
        + report
    )

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(
            report
        )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    varied_examples = sum(

        1

        for item in selected_cases

        if item["stats"][
            "has_wording_variation"
        ]
    )

    multi_user_examples = sum(

        1

        for item in selected_cases

        if item["stats"][
            "unique_user_count"
        ] > 1
    )

    print(
        "\n"
        "========== QUALITATIVE SUMMARY =========="
    )

    print(
        f"Examples exported:          "
        f"{len(selected_cases)}"
    )

    print(
        f"With wording variation:     "
        f"{varied_examples}"
    )

    print(
        f"With multiple users:        "
        f"{multi_user_examples}"
    )

    print(
        f"\nSaved to: {OUTPUT_FILE}"
    )

    print(
        "\nNOTE:"
    )

    print(
        "These are qualitative examples of existing "
        "case groupings. They are not ground-truth "
        "accuracy measurements."
    )

    print(
        "\n=========================================="
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()