import sys
import time
import traceback
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from backend.models.complaint import ComplaintCreate
from backend.services.duplicate_service import process_complaint


DEFAULT_CSV_PATH = "data/master_complaints_v2.csv"
FAILED_ROWS_PATH = "data/ingestion_failed_rows.csv"


def clean(value):
    """
    Convert Pandas NaN values to None.
    Strip whitespace from strings.
    """
    if pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip()

        if value == "":
            return None

    return value


def parse_historical_date(value):
    """
    Parse the historical complaint date from the CSV.

    Expected format:
        YYYY-MM-DD

    Returns:
        datetime if valid
        None if missing or invalid

    Historical dates are never fabricated.
    """
    if value is None or pd.isna(value):
        return None

    value = str(value).strip()

    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def parse_boolean(value):
    """
    Safely parse boolean-like CSV values.

    Handles:
        True / False
        1 / 0
        "true" / "false"
        "yes" / "no"

    Missing values default to False.
    """
    value = clean(value)

    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    normalized = str(value).strip().lower()

    if normalized in {"true", "1", "yes"}:
        return True

    if normalized in {"false", "0", "no"}:
        return False

    raise ValueError(
        f"Invalid boolean value for is_synthetic: {value!r}"
    )


def validate_required_columns(df):
    """
    Validate the CSV schema before ingestion starts.

    This prevents partially ingesting a file that is missing
    required columns.
    """
    required_columns = {
        "complaint_text",
        "user_id",
        "date",
        "rating",
        "source",
        "product",
        "company_size",
        "is_synthetic",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "CSV is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )


def ingest_master_csv(csv_path=DEFAULT_CSV_PATH):
    """
    Ingest historical complaints through the normal
    InsightDesk complaint-processing pipeline.

    Important:
    - Historical dates are preserved.
    - Missing/invalid historical dates are rejected.
    - No timestamps are fabricated.
    - Individual row failures are logged without stopping
      the entire ingestion run.
    """

    df = pd.read_csv(csv_path)

    validate_required_columns(df)

    print(f"\nLoaded {len(df)} complaints from {csv_path}\n")

    success = 0
    failed = 0
    failed_rows = []

    for idx, row in tqdm(
        df.iterrows(),
        total=len(df),
        desc="Ingesting complaints",
    ):

        try:
            # -------------------------------------------------
            # 1. Validate required row-level values
            # -------------------------------------------------

            complaint_text = clean(row["complaint_text"])
            user_id = clean(row["user_id"])

            if complaint_text is None:
                raise ValueError("Missing complaint_text")

            if user_id is None:
                raise ValueError("Missing user_id")

            # -------------------------------------------------
            # 2. Parse historical event date
            # -------------------------------------------------

            raw_date = row["date"]
            parsed_date = parse_historical_date(raw_date)

            if parsed_date is None:
                raise ValueError(
                    "Missing or unparseable historical date: "
                    f"{raw_date!r}"
                )

            # -------------------------------------------------
            # 3. Build complaint model
            # -------------------------------------------------

            complaint = ComplaintCreate(
                complaint_text=complaint_text,
                user_id=str(user_id),
                rating=clean(row["rating"]),
                source=clean(row["source"]),
                product=clean(row["product"]),
                company_size=clean(row["company_size"]),
                is_synthetic=parse_boolean(
                    row["is_synthetic"]
                ),
                created_at=parsed_date,
            )

            # -------------------------------------------------
            # 4. Small delay between external-service calls
            # -------------------------------------------------

            time.sleep(0.2)

            # -------------------------------------------------
            # 5. Run normal InsightDesk pipeline
            # -------------------------------------------------

            process_complaint(complaint)

            success += 1

        except Exception as e:

            failed += 1

            complaint_preview = row.get(
                "complaint_text",
                "<unreadable>",
            )

            print(
                f"\n[row {idx}] FAILED: "
                f"{complaint_preview}"
            )
            print(f"ERROR: {e}")

            traceback.print_exc()

            failed_rows.append(
                {
                    "row_index": idx,
                    "complaint_text": row.get(
                        "complaint_text",
                        "",
                    ),
                    "date": row.get("date"),
                    "product": row.get("product"),
                    "source": row.get("source"),
                    "is_synthetic": row.get(
                        "is_synthetic"
                    ),
                    "error": str(e),
                }
            )

            # One bad row should not stop the batch.
            continue

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------

    print("\n========================")
    print("INGESTION SUMMARY")
    print("========================")
    print(f"Total      : {len(df)}")
    print(f"Successful : {success}")
    print(f"Failed     : {failed}")

    if failed_rows:

        pd.DataFrame(
            failed_rows
        ).to_csv(
            FAILED_ROWS_PATH,
            index=False,
        )

        print(
            f"\nFailed rows logged to: "
            f"{FAILED_ROWS_PATH}"
        )

        print(
            "Review/fix those rows before retrying them."
        )

    else:
        print("\nAll rows ingested successfully.")


if __name__ == "__main__":

    # Run from the InsightDesk project root:
    #
    # Smoke test:
    # python -m backend.scripts.ingest_master_csv \
    #     data/master_complaints_v2_smoke.csv
    #
    # Full ingestion (ONLY after smoke-test verification):
    # python -m backend.scripts.ingest_master_csv \
    #     data/master_complaints_v2.csv

    path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else DEFAULT_CSV_PATH
    )

    ingest_master_csv(path)