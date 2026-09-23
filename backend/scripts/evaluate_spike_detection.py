"""
Evaluate InsightDesk's statistical spike-detection module.

READ-ONLY:
This script does not modify Supabase or trigger emails.

It evaluates existing case histories and reports:
- cases evaluated
- cases with sufficient history
- normal/spike/critical counts
- Z-score
- historical mean/std
- current complaint volume

Usage:
    python -m backend.scripts.evaluate_spike_detection
"""

import numpy as np

from backend.database.supabase import (
    get_cases,
    get_case_history_bulk,
)

from backend.services.spike_detection import (
    detect_spikes_bulk,
)


MIN_HISTORY_RECORDS = 7
SPIKE_THRESHOLD = 2
CRITICAL_THRESHOLD = 3


def main():

    print(
        "\n========== INSIGHTDESK SPIKE DETECTION RESULTS ==========\n"
    )

    # --------------------------------------------------
    # Load cases
    # --------------------------------------------------

    cases = get_cases()

    if not cases:
        print("No cases found.")
        return
    # --------------------------------------------------
    # Extract case IDs
    # --------------------------------------------------

    case_ids = [
        case["case_id"]
        for case in cases
    ]

    # --------------------------------------------------
    # Load history in bulk
    # --------------------------------------------------

    raw_history = get_case_history_bulk(
    case_ids
    )

    # Convert list -> {case_id: [history rows]}
    history_by_case = {}

    for row in raw_history:

        case_id = row["case_id"]

        if case_id not in history_by_case:
            history_by_case[case_id] = []

        history_by_case[case_id].append(
            row
        )



    # --------------------------------------------------
    # Run existing production detector
    # --------------------------------------------------

    results = detect_spikes_bulk(
        cases,
        history_by_case
    )

    sufficient = 0
    insufficient = 0

    normal_count = 0
    spike_count = 0
    critical_count = 0

    detailed_results = []

    # --------------------------------------------------
    # Calculate diagnostic information
    # --------------------------------------------------

    for case in cases:

        case_id = case["case_id"]

        history = history_by_case.get(
            case_id,
            []
        )

        result = results.get(
            case_id,
            {}
        )

        counts = [
            row["complaint_ct"]
            for row in history
        ]

        if len(history) < MIN_HISTORY_RECORDS:

            insufficient += 1

            continue

        sufficient += 1

        historical = counts[:-1]

        current = counts[-1]

        mean = float(
            np.mean(historical)
        )

        std = float(
            np.std(historical)
        )

        z_score = result.get(
            "z_score",
            0
        )

        is_spike = result.get(
            "spike",
            False
        )

        is_critical = result.get(
            "critical",
            False
        )

        if is_critical:

            critical_count += 1

        elif is_spike:

            spike_count += 1

        else:

            normal_count += 1

        detailed_results.append(
            {
                "case_id": case_id,
                "mean": mean,
                "std": std,
                "current": current,
                "z_score": z_score,
                "spike": is_spike,
                "critical": is_critical,
            }
        )

    # --------------------------------------------------
    # Summary
    # --------------------------------------------------

    print("Method: Z-score based statistical anomaly detection")
    print(
        f"Spike threshold:       Z > {SPIKE_THRESHOLD}"
    )
    print(
        f"Critical threshold:    Z > {CRITICAL_THRESHOLD}"
    )
    print(
        f"Minimum history rows:  {MIN_HISTORY_RECORDS}"
    )

    print("\n---------- DATA COVERAGE ----------")

    print(
        f"Cases evaluated:             {len(cases)}"
    )

    print(
        f"Cases with enough history:   {sufficient}"
    )

    print(
        f"Cases with insufficient data:{insufficient}"
    )

    print("\n---------- DETECTION SUMMARY ----------")

    print(
        f"Normal cases:                {normal_count}"
    )

    print(
        f"Spike cases (2 < Z <= 3):    {spike_count}"
    )

    print(
        f"Critical cases (Z > 3):      {critical_count}"
    )

    print(
        f"Total anomalies:             "
        f"{spike_count + critical_count}"
    )

    # --------------------------------------------------
    # Detected anomalies
    # --------------------------------------------------

    anomalies = [
        item
        for item in detailed_results
        if item["spike"]
    ]

    anomalies.sort(
        key=lambda item: item["z_score"],
        reverse=True
    )

    print("\n---------- DETECTED SPIKES ----------")

    if not anomalies:

        print(
            "No spikes detected in the current dataset."
        )

    else:

        for item in anomalies:

            if item["critical"]:
                classification = "CRITICAL SPIKE"
            else:
                classification = "SPIKE"

            print(
                f"\nCase ID:                 "
                f"{item['case_id']}"
            )

            print(
                f"Historical mean:          "
                f"{item['mean']:.2f}"
            )

            print(
                f"Historical std deviation: "
                f"{item['std']:.2f}"
            )

            print(
                f"Current complaint count:  "
                f"{item['current']}"
            )

            print(
                f"Z-score:                  "
                f"{item['z_score']:.2f}"
            )

            print(
                f"Classification:           "
                f"{classification}"
            )

    # --------------------------------------------------
    # Highest normal case for comparison
    # --------------------------------------------------

    normal_results = [
        item
        for item in detailed_results
        if not item["spike"]
    ]

    if normal_results:

        normal_results.sort(
            key=lambda item: item["z_score"],
            reverse=True
        )

        example = normal_results[0]

        print(
            "\n---------- NORMAL CASE EXAMPLE ----------"
        )

        print(
            f"Case ID:                 "
            f"{example['case_id']}"
        )

        print(
            f"Historical mean:          "
            f"{example['mean']:.2f}"
        )

        print(
            f"Historical std deviation: "
            f"{example['std']:.2f}"
        )

        print(
            f"Current complaint count:  "
            f"{example['current']}"
        )

        print(
            f"Z-score:                  "
            f"{example['z_score']:.2f}"
        )

        print(
            "Classification:           NORMAL"
        )

    print(
        "\n=========================================================\n"
    )


if __name__ == "__main__":
    main()