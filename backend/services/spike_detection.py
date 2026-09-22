import numpy as np

from backend.database.supabase import (
    get_case_history,
    get_case_history_bulk,
)


def compute_z_score(
    history,
    current
):

    if len(history) < 7:
        return 0

    mean = np.mean(history)

    std = np.std(history)

    if std == 0:
        return 0

    return (
        current - mean
    ) / std


def detect_spike(case_id):

    history = get_case_history(
        case_id
    )

    counts = [

        row["complaint_ct"]

        for row in history
    ]

    current = counts[-1] if counts else 0

    if len(history) < 7:

        return {

            "spike": False,

            "critical": False,

            "z_score": 0,

            "current": current
        }

    historical = counts[:-1]

    z_score = compute_z_score(
        historical,
        current
    )

    return {

    "spike":
    bool(z_score > 2),

    "critical":
    bool(z_score > 3),

    "z_score":
    float(round(z_score, 2)),

    "current":
    int(current)
}
def detect_spikes_bulk(cases, history_by_case):

    results = {}

    for case in cases:

        case_id = case["case_id"]

        history = history_by_case.get(
            case_id,
            []
        )

        counts = [
            row["complaint_ct"]
            for row in history
        ]

        current = (
            counts[-1]
            if counts
            else 0
        )

        if len(history) < 7:

            results[case_id] = {

                "spike": False,

                "critical": False,

                "z_score": 0,

                "current": current,
            }

            continue

        historical = counts[:-1]

        z_score = compute_z_score(
            historical,
            current
        )

        results[case_id] = {

            "spike":
            bool(z_score > 2),

            "critical":
            bool(z_score > 3),

            "z_score":
            float(round(z_score, 2)),

            "current":
            int(current),
        }

    return results