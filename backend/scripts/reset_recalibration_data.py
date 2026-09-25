"""
Safely reset the InsightDesk recalibration environment.

This script clears ONLY:
1. Data in the currently configured recalibration Supabase project.
2. Vectors inside the Pinecone namespace:
   helpdesk-recalibration

It does NOT touch Pinecone's default namespace.

Usage
-----

Dry run:
    python -m backend.scripts.reset_recalibration_data

Actual reset:
    python -m backend.scripts.reset_recalibration_data --reset
"""

import os
import sys

from dotenv import load_dotenv
from pinecone import Pinecone

from backend.database.supabase import supabase


# ==========================================================
# Environment
# ==========================================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX = os.getenv("PINECONE_INDEX")
PINECONE_NAMESPACE = os.getenv(
    "PINECONE_NAMESPACE",
    "",
)


# ==========================================================
# Safety Configuration
# ==========================================================

EXPECTED_SUPABASE_URL = (
    "https://hziqamqmdhdxycnhpmfs.supabase.co"
)

EXPECTED_NAMESPACE = (
    "helpdesk-recalibration"
)


# Delete child/dependent tables before parent tables.
TABLES_TO_CLEAR = [
    "case_rca",
    "case_history",
    "complaint_windows",
    "complaints",
    "cases",
    "topics",
    "clustering_metadata",
    "model_benchmarks",
]


# ==========================================================
# Safety Checks
# ==========================================================

def verify_environment():
    """
    Abort unless both Supabase and Pinecone point to the
    dedicated recalibration environment.
    """

    print("\nSAFETY CHECK")
    print("=" * 60)

    print(
        "Supabase URL:",
        SUPABASE_URL,
    )

    print(
        "Pinecone namespace:",
        repr(PINECONE_NAMESPACE),
    )

    if SUPABASE_URL != EXPECTED_SUPABASE_URL:

        raise RuntimeError(
            "\nABORTING.\n"
            "Unexpected Supabase project.\n"
            f"Expected: {EXPECTED_SUPABASE_URL}\n"
            f"Found:    {SUPABASE_URL}\n"
            "No further data will be deleted."
        )

    if PINECONE_NAMESPACE != EXPECTED_NAMESPACE:

        raise RuntimeError(
            "\nABORTING.\n"
            "Unexpected Pinecone namespace.\n"
            f"Expected: {EXPECTED_NAMESPACE!r}\n"
            f"Found:    {PINECONE_NAMESPACE!r}\n"
            "No further data will be deleted."
        )

    if not PINECONE_API_KEY:

        raise RuntimeError(
            "PINECONE_API_KEY is missing."
        )

    if not PINECONE_INDEX:

        raise RuntimeError(
            "PINECONE_INDEX is missing."
        )

    print("\nEnvironment verified.")


# ==========================================================
# Supabase Helpers
# ==========================================================

def get_table_count(table_name):
    """
    Return exact row count without downloading table rows.
    """

    response = (
        supabase
        .table(table_name)
        .select(
            "*",
            count="exact",
            head=True,
        )
        .execute()
    )

    return response.count or 0


def delete_all_rows(table_name):
    """
    Delete every row from a known InsightDesk table.

    We deliberately filter using known non-null columns rather
    than assuming primary-key types.

    This avoids problems such as window_id being UUID rather
    than integer.
    """

    if table_name in {
        "case_rca",
        "case_history",
        "complaint_windows",
    }:

        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "case_id",
                "null",
            )
        )

    elif table_name == "complaints":

        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "complaint_id",
                "null",
            )
        )

    elif table_name == "cases":

        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "case_id",
                "null",
            )
        )

    elif table_name == "topics":

        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "topic_id",
                "null",
            )
        )

    elif table_name == "clustering_metadata":

        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "metadata_id",
                "null",
            )
        )

    elif table_name == "model_benchmarks":

        # model_name is populated when benchmarks are created.
        query = (
            supabase
            .table(table_name)
            .delete()
            .not_
            .is_(
                "model_name",
                "null",
            )
        )

    else:

        raise RuntimeError(
            f"No safe delete rule defined "
            f"for table {table_name!r}."
        )

    return query.execute()


# ==========================================================
# Pinecone Helpers
# ==========================================================

def get_pinecone_index():

    pc = Pinecone(
        api_key=PINECONE_API_KEY
    )

    return pc.Index(
        PINECONE_INDEX
    )


def get_namespace_vector_count(index):
    """
    Return vector count for ONLY the configured namespace.
    """

    stats = (
        index.describe_index_stats()
    )

    namespaces = (
        stats.namespaces or {}
    )

    namespace_stats = (
        namespaces.get(
            PINECONE_NAMESPACE
        )
    )

    if namespace_stats is None:
        return 0

    return (
        namespace_stats.vector_count
        or 0
    )


# ==========================================================
# Current State
# ==========================================================

def print_current_state():
    """
    Display current Supabase and Pinecone counts.
    """

    print("\nSUPABASE CURRENT COUNTS")
    print("=" * 60)

    counts = {}

    for table in TABLES_TO_CLEAR:

        try:

            count = get_table_count(
                table
            )

            counts[table] = count

            print(
                f"{table:<25} {count}"
            )

        except Exception as exc:

            counts[table] = None

            print(
                f"{table:<25} "
                f"ERROR: {exc}"
            )

    print("\nPINECONE")
    print("=" * 60)

    index = get_pinecone_index()

    vector_count = (
        get_namespace_vector_count(
            index
        )
    )

    print(
        f"Namespace: {PINECONE_NAMESPACE}"
    )

    print(
        f"Vectors:   {vector_count}"
    )

    return counts, vector_count


# ==========================================================
# Dry Run
# ==========================================================

def dry_run():

    verify_environment()

    print_current_state()

    print("\nDRY RUN ONLY.")
    print("Nothing was deleted.")


# ==========================================================
# Reset
# ==========================================================

def reset():

    verify_environment()

    print_current_state()

    print("\nWARNING")
    print("=" * 60)

    print(
        "This will permanently clear the "
        "recalibration Supabase data."
    )

    print(
        "It will also delete every vector inside:"
    )

    print(
        f"    {PINECONE_NAMESPACE}"
    )

    print(
        "\nThe Pinecone default namespace "
        "will NOT be touched."
    )

    confirmation = input(
        "\nType RESET to continue: "
    )

    if confirmation != "RESET":

        print(
            "\nAborted. Nothing further "
            "was deleted."
        )

        return

    # ======================================================
    # 1. Clear Supabase
    # ======================================================

    print("\nCLEARING SUPABASE")
    print("=" * 60)

    for table in TABLES_TO_CLEAR:

        before = get_table_count(
            table
        )

        print(
            f"{table:<25} "
            f"before={before}"
        )

        if before > 0:

            delete_all_rows(
                table
            )

        after = get_table_count(
            table
        )

        print(
            f"{table:<25} "
            f"after={after}"
        )

        if after != 0:

            raise RuntimeError(
                f"\nRESET STOPPED.\n"
                f"Table {table!r} still "
                f"contains {after} rows.\n"
                "Pinecone has NOT yet been "
                "cleared by this execution."
            )

    # ======================================================
    # 2. Clear Pinecone Namespace
    # ======================================================

    print("\nCLEARING PINECONE")
    print("=" * 60)

    index = get_pinecone_index()

    before_vectors = (
        get_namespace_vector_count(
            index
        )
    )

    print(
        f"Vectors before: "
        f"{before_vectors}"
    )

    if before_vectors > 0:

        index.delete(
            delete_all=True,
            namespace=PINECONE_NAMESPACE,
        )

    print(
        "Delete request completed for "
        f"namespace {PINECONE_NAMESPACE!r}."
    )

    # ======================================================
    # 3. Final Supabase Verification
    # ======================================================

    print("\nFINAL SUPABASE VERIFICATION")
    print("=" * 60)

    for table in TABLES_TO_CLEAR:

        count = get_table_count(
            table
        )

        print(
            f"{table:<25} {count}"
        )

        if count != 0:

            raise RuntimeError(
                f"{table!r} is not empty."
            )

    # ======================================================
    # 4. Final Pinecone Verification
    # ======================================================

    print("\nFINAL PINECONE VERIFICATION")
    print("=" * 60)

    vector_count = (
        get_namespace_vector_count(
            index
        )
    )

    print(
        f"Namespace: "
        f"{PINECONE_NAMESPACE}"
    )

    print(
        f"Vectors:   {vector_count}"
    )

    if vector_count != 0:

        print(
            "\nWARNING:"
        )

        print(
            "Pinecone deletion may be "
            "eventually consistent."
        )

        print(
            "Run the script again without "
            "--reset after a few seconds "
            "to verify the namespace count."
        )

    else:

        print(
            "\nRESET COMPLETE."
        )

        print(
            "Supabase recalibration data: EMPTY"
        )

        print(
            "Pinecone recalibration namespace: EMPTY"
        )


# ==========================================================
# CLI
# ==========================================================

if __name__ == "__main__":

    if "--reset" in sys.argv:

        reset()

    else:

        dry_run()