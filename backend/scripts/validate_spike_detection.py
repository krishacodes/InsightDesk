"""
Functional validation for InsightDesk spike detection.

This script does NOT read or write Supabase.
It validates the statistical decision logic using controlled
historical complaint-count scenarios.

Production rules:
    Z > 2  -> Spike
    Z > 3  -> Critical Spike

Usage:
    python -m backend.scripts.validate_spike_detection
"""

import numpy as np

from backend.services.spike_detection import (
    compute_z_score,
)


SPIKE_THRESHOLD = 2
CRITICAL_THRESHOLD = 3


def classify(history, current):
    """
    Apply the same classification rules used by InsightDesk.
    """

    z_score = compute_z_score(
        history,
        current
    )

    if z_score > CRITICAL_THRESHOLD:
        classification = "CRITICAL"

    elif z_score > SPIKE_THRESHOLD:
        classification = "SPIKE"

    else:
        classification = "NORMAL"

    return float(z_score), classification


def run_scenario(
    name,
    history,
    current,
    expected,
):
    """
    Run one controlled spike-detection scenario.
    """

    mean = (
        float(np.mean(history))
        if history
        else 0
    )

    std = (
        float(np.std(history))
        if history
        else 0
    )

    z_score, actual = classify(
        history,
        current
    )

    passed = (
        actual == expected
    )

    return {
        "name": name,
        "history": history,
        "history_rows": len(history),
        "mean": mean,
        "std": std,
        "current": current,
        "z_score": z_score,
        "expected": expected,
        "actual": actual,
        "passed": passed,
    }


def main():

    print(
        "\n========== INSIGHTDESK SPIKE DETECTION VALIDATION ==========\n"
    )

    print(
        "Method: Z-score based statistical anomaly detection"
    )

    print(
        f"Spike threshold:       Z > {SPIKE_THRESHOLD}"
    )

    print(
        f"Critical threshold:    Z > {CRITICAL_THRESHOLD}"
    )

    # ========================================================
    # CONTROLLED TEST SCENARIOS
    # ========================================================

    baseline_history = [
        10,
        11,
        9,
        10,
        12,
        9,
        10,
    ]

    scenarios = [

        # ----------------------------------------------------
        # 1. Stable complaint volume
        # ----------------------------------------------------

        {
            "name":
                "Stable complaint volume",

            "history":
                baseline_history,

            "current":
                11,

            "expected":
                "NORMAL",
        },

        # ----------------------------------------------------
        # 2. Moderate increase, still below spike threshold
        #
        # Expected Z ≈ 1.88
        # ----------------------------------------------------

        {
            "name":
                "Moderate increase",

            "history":
                baseline_history,

            "current":
                12,

            "expected":
                "NORMAL",
        },

        # ----------------------------------------------------
        # 3. Spike
        #
        # Expected Z ≈ 2.89
        #
        # 2 < Z <= 3
        # ----------------------------------------------------

        {
            "name":
                "Spike increase",

            "history":
                baseline_history,

            "current":
                13,

            "expected":
                "SPIKE",
        },

        # ----------------------------------------------------
        # 4. Critical spike
        #
        # Expected Z ≈ 5.92
        #
        # Z > 3
        # ----------------------------------------------------

        {
            "name":
                "Critical increase",

            "history":
                baseline_history,

            "current":
                16,

            "expected":
                "CRITICAL",
        },

        # ----------------------------------------------------
        # 5. Complaint volume decrease
        #
        # Negative Z-score should NOT be classified as spike.
        # ----------------------------------------------------

        {
            "name":
                "Volume decrease",

            "history":
                baseline_history,

            "current":
                5,

            "expected":
                "NORMAL",
        },
    ]

    results = []

    # ========================================================
    # RUN CONTROLLED SCENARIOS
    # ========================================================

    for scenario in scenarios:

        result = run_scenario(
            scenario["name"],
            scenario["history"],
            scenario["current"],
            scenario["expected"],
        )

        results.append(
            result
        )

    # ========================================================
    # PRINT CONTROLLED RESULTS
    # ========================================================

    for index, result in enumerate(
        results,
        start=1
    ):

        print(
            f"\n---------- SCENARIO {index} ----------"
        )

        print(
            f"Scenario:              "
            f"{result['name']}"
        )

        print(
            f"Historical counts:     "
            f"{result['history']}"
        )

        print(
            f"Historical mean:       "
            f"{result['mean']:.2f}"
        )

        print(
            f"Historical std:        "
            f"{result['std']:.2f}"
        )

        print(
            f"Current count:         "
            f"{result['current']}"
        )

        print(
            f"Z-score:               "
            f"{result['z_score']:.2f}"
        )

        print(
            f"Expected:              "
            f"{result['expected']}"
        )

        print(
            f"Actual:                "
            f"{result['actual']}"
        )

        print(
            f"Validation:            "
            f"{'PASS' if result['passed'] else 'FAIL'}"
        )

    # ========================================================
    # EDGE CASE 1: INSUFFICIENT HISTORY
    # ========================================================

    short_history = [
        10,
        11,
        9,
        10,
        12,
        9,
    ]

    short_current = 30

    short_z = compute_z_score(
        short_history,
        short_current
    )

    insufficient_pass = (
        short_z == 0
    )

    print(
        "\n---------- EDGE CASE: INSUFFICIENT HISTORY ----------"
    )

    print(
        f"Historical rows:       "
        f"{len(short_history)}"
    )

    print(
        "Required rows:         7"
    )

    print(
        f"Current count:         "
        f"{short_current}"
    )

    print(
        f"Returned Z-score:      "
        f"{short_z:.2f}"
    )

    print(
        f"Validation:            "
        f"{'PASS' if insufficient_pass else 'FAIL'}"
    )

    # ========================================================
    # EDGE CASE 2: ZERO VARIANCE
    # ========================================================

    zero_variance_history = [
        10,
        10,
        10,
        10,
        10,
        10,
        10,
    ]

    zero_variance_current = 50

    zero_z = compute_z_score(
        zero_variance_history,
        zero_variance_current
    )

    zero_variance_pass = (
        zero_z == 0
    )

    print(
        "\n---------- EDGE CASE: ZERO VARIANCE ----------"
    )

    print(
        f"Historical counts:     "
        f"{zero_variance_history}"
    )

    print(
        f"Current count:         "
        f"{zero_variance_current}"
    )

    print(
        f"Returned Z-score:      "
        f"{zero_z:.2f}"
    )

    print(
        f"Validation:            "
        f"{'PASS' if zero_variance_pass else 'FAIL'}"
    )

    # ========================================================
    # FINAL VALIDATION SUMMARY
    # ========================================================

    controlled_passed = sum(
        1
        for result in results
        if result["passed"]
    )

    controlled_total = len(
        results
    )

    edge_passed = (
        int(insufficient_pass)
        +
        int(zero_variance_pass)
    )

    edge_total = 2

    total_passed = (
        controlled_passed
        +
        edge_passed
    )

    total_tests = (
        controlled_total
        +
        edge_total
    )

    print(
        "\n========== VALIDATION SUMMARY ==========\n"
    )

    print(
        f"Controlled scenarios passed: "
        f"{controlled_passed}/{controlled_total}"
    )

    print(
        f"Edge cases passed:           "
        f"{edge_passed}/{edge_total}"
    )

    print(
        f"Total tests passed:          "
        f"{total_passed}/{total_tests}"
    )

    if total_passed == total_tests:

        print(
            "\nResult: ALL SPIKE-DETECTION "
            "VALIDATION TESTS PASSED"
        )

    else:

        print(
            "\nResult: SOME VALIDATION TESTS FAILED"
        )

    print(
        "\n=========================================\n"
    )


if __name__ == "__main__":

    main()