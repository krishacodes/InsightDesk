import numpy as np

from backend.database.supabase import (
    get_case_history
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


def detect_spike(
    case_id
):

    history = get_case_history(
        case_id
    )

    if len(history) < 7:

        return {
            "spike": False,
            "critical": False,
            "z_score": 0,
            "current":current
        }

    counts = [

        row["report_count"]

        for row in history
    ]

    historical = counts[:-1]

    current = counts[-1]

    z_score = compute_z_score(
        historical,
        current
    )

    return {

        "spike":
        z_score > 2,

        "critical":
        z_score > 3,

        "z_score":
        round(z_score, 2),

        "current":
        current
    }