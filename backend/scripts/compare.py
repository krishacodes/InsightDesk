from backend.database.supabase import (
    get_all_cases,
)

from backend.scripts.evaluate_clustering import (
    load_case_snapshot,
)


def normalize(text):
    return (
        (text or "")
        .strip()
    )


def main():

    print("\n==========================================")
    print("LIVE DB VS FROZEN SNAPSHOT")
    print("==========================================\n")

    # Frozen snapshot
    snapshot_case_ids, snapshot_documents = (
        load_case_snapshot(
            refresh=False
        )
    )

    # Live DB
    live_cases = get_all_cases()

    live_case_ids = [
        int(case["case_id"])
        for case in live_cases
    ]

    live_documents = [
        case.get("representative_text") or ""
        for case in live_cases
    ]

    print(
        f"Snapshot cases: {len(snapshot_documents)}"
    )

    print(
        f"Live cases:     {len(live_documents)}"
    )

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    snapshot_case_ids = [
        int(case_id)
        for case_id in snapshot_case_ids
    ]

    print(
        "\nCase ID order identical:",
        snapshot_case_ids == live_case_ids
    )

    print(
        "Same case ID set:",
        set(snapshot_case_ids)
        == set(live_case_ids)
    )

    # --------------------------------------------------------
    # Documents
    # --------------------------------------------------------

    exact_document_match = (
        snapshot_documents
        == live_documents
    )

    print(
        "\nExact document order/text match:",
        exact_document_match
    )

    # --------------------------------------------------------
    # Compare by case ID
    # --------------------------------------------------------

    snapshot_map = {
        case_id: normalize(document)
        for case_id, document
        in zip(
            snapshot_case_ids,
            snapshot_documents
        )
    }

    live_map = {
        case_id: normalize(document)
        for case_id, document
        in zip(
            live_case_ids,
            live_documents
        )
    }

    changed_cases = []

    for case_id in sorted(
        set(snapshot_map)
        & set(live_map)
    ):

        if (
            snapshot_map[case_id]
            != live_map[case_id]
        ):

            changed_cases.append(
                case_id
            )

    missing_live = sorted(
        set(snapshot_map)
        - set(live_map)
    )

    missing_snapshot = sorted(
        set(live_map)
        - set(snapshot_map)
    )

    print(
        "\nChanged representative_text cases:",
        changed_cases
    )

    print(
        "Missing from live DB:",
        missing_live
    )

    print(
        "Missing from snapshot:",
        missing_snapshot
    )

    # --------------------------------------------------------
    # Final conclusion
    # --------------------------------------------------------

    print("\n==========================================")

    if (
        not changed_cases
        and
        not missing_live
        and
        not missing_snapshot
    ):

        print(
            "CONTENT MATCH: PASS"
        )

        if (
            snapshot_case_ids
            == live_case_ids
        ):

            print(
                "ORDER MATCH: PASS"
            )

        else:

            print(
                "ORDER MATCH: FAIL"
            )

            print(
                "Same cases/text exist, but ordering differs."
            )

    else:

        print(
            "CONTENT MATCH: FAIL"
        )

        print(
            "The frozen evaluation dataset and "
            "current live DB are not identical."
        )

    print(
        "==========================================\n"
    )


if __name__ == "__main__":
    main()