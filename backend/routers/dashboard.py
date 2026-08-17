from collections import Counter

from fastapi import APIRouter, HTTPException

from backend.database.supabase import (
    get_cases,
    get_complaints,
    get_topics,
    get_benchmarks,
    get_rca,
    get_complaint_volume_by_day,
)

from backend.services.spike_detection import (
    detect_spike,
)


router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"],
)


# ==========================================
# OVERVIEW
# ==========================================

@router.get("/overview")
def get_dashboard_overview():

    try:

        # ----------------------------------
        # BASIC PROJECT DATA
        # ----------------------------------

        cases = get_cases()

        complaints = get_complaints()

        topics = get_topics()

        benchmarks = get_benchmarks()


        # ----------------------------------
        # COMPLAINT VOLUME BY DAY
        # ----------------------------------

        raw_volume = get_complaint_volume_by_day()

        daily_counts = Counter()

        for row in raw_volume:

            created_at = row.get("created_at")

            if not created_at:
                continue

            # Supabase timestamps may contain
            # variable fractional-second precision.
            #
            # We only need the calendar date,
            # so extracting the first 10 characters
            # is safer than datetime.fromisoformat().

            date = created_at[:10]

            daily_counts[date] += 1


        volume_by_day = [

            {
                "date": date,

                "label":
                date[5:7] + "/" + date[8:10],

                "count":
                daily_counts[date],
            }

            for date in sorted(daily_counts)
        ]


        # ----------------------------------
        # SPIKE DETECTION
        # ----------------------------------

        spike_cases = []

        for case in cases:

            result = detect_spike(
                case["case_id"]
            )

            if result["spike"]:

                spike_cases.append(

                    {
                        "case_id":
                        case["case_id"],

                        "representative_text":
                        case.get(
                            "representative_text"
                        ),

                        **result,
                    }

                )


        # ----------------------------------
        # OVERVIEW RESPONSE
        # ----------------------------------

        return {

            "complaints":
            len(complaints),

            "cases":
            len(cases),

            "topics":
            len(topics),

            "spikes":
            len(spike_cases),

            "spike_cases":
            spike_cases,

            "benchmarks":
            benchmarks,

            "topics_data":
            topics,

            "volume_by_day":
            volume_by_day,
        }


    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# CASES
# ==========================================

@router.get("/cases")
def dashboard_cases():

    try:

        return get_cases()

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# COMPLAINTS
# ==========================================

@router.get("/complaints")
def dashboard_complaints():

    try:

        return get_complaints()

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# TOPICS
# ==========================================

@router.get("/topics")
def dashboard_topics():

    try:

        return get_topics()

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# BENCHMARKS
# ==========================================

@router.get("/benchmarks")
def dashboard_benchmarks():

    try:

        return get_benchmarks()

    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# SPIKES
# ==========================================

@router.get("/spikes")
def dashboard_spikes():

    try:

        cases = get_cases()

        spikes = []

        for case in cases:

            result = detect_spike(
                case["case_id"]
            )

            if result["spike"]:

                spikes.append(

                    {
                        "case_id":
                        case["case_id"],

                        "representative_text":
                        case.get(
                            "representative_text"
                        ),

                        **result,
                    }

                )

        return spikes


    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )


# ==========================================
# RCA
# ==========================================

@router.get("/rca/{case_id}")
def dashboard_rca(
    case_id: int
):

    try:

        rca = get_rca(
            case_id
        )

        if not rca:

            raise HTTPException(

                status_code=404,

                detail=(
                    f"No RCA found "
                    f"for case {case_id}"
                ),
            )

        return rca


    except HTTPException:

        raise


    except Exception as e:

        raise HTTPException(

            status_code=500,

            detail=str(e),
        )