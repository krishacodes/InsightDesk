"""
Merge real Play Store complaints and synthetic complaints into one
ingestion-ready dataset for InsightDesk.

Normalizes both sources to the schema expected by the current
ingestion pipeline:

    complaint_text
    rating
    date
    source
    product
    company_size
    user_id
    is_synthetic

Current experimental decision:
    All complaints use user_id = "DEFAULT_USER".

User identity is intentionally held constant because semantic
duplicate detection must depend only on complaint meaning, not
reporter identity.

Run from the InsightDesk project root:

    python scraper/merge_data.py
"""

import csv
from collections import Counter
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

REAL_PATH = Path("data/play_store_reviews.csv")
SYNTHETIC_PATH = Path("data/synthetic_complaints.csv")
OUTPUT_PATH = Path("data/master_complaints_v2.csv")

DEFAULT_USER_ID = "DEFAULT_USER"

TARGET_COLUMNS = [
    "complaint_text",
    "rating",
    "date",
    "source",
    "product",
    "company_size",
    "user_id",
    "is_synthetic",
]


# Used only as a diagnostic check.
# This script DOES NOT remove brand names from complaint text.
BRAND_CHECK_TERMS = [
    "vision helpdesk",
    "freshdesk",
    "freshworks",
    "zendesk",
    "zoho desk",
    "zoho",
    "happyfox",
    "liveagent",
    "help scout",
    "helpscout",
    "kayako",
]


# ============================================================
# HELPERS
# ============================================================

def normalize_date(raw):
    """
    Normalize either an ISO datetime or YYYY-MM-DD string
    into YYYY-MM-DD.

    Empty or invalid values are returned unchanged so that
    the merge does not invent timestamps.
    """

    if raw is None:
        return ""

    raw = str(raw).strip()

    if not raw:
        return ""

    # Handle ISO timestamps ending in Z.
    iso_value = raw.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(iso_value).strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Handle already-normalized YYYY-MM-DD dates.
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return raw


def normalize_boolean(raw, default=False):
    """
    Normalize common CSV boolean representations to
    the strings 'True' or 'False'.
    """

    if raw is None or str(raw).strip() == "":
        return "True" if default else "False"

    value = str(raw).strip().lower()

    if value in {"true", "1", "yes", "y"}:
        return "True"

    if value in {"false", "0", "no", "n"}:
        return "False"

    return "True" if default else "False"


# ============================================================
# REAL PLAY STORE DATA
# ============================================================

def load_real(path):
    rows = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(f"{path} has no CSV header.")

        if "complaint_text" not in reader.fieldnames:
            raise ValueError(
                f"{path} is missing required column: complaint_text"
            )

        for row in reader:

            complaint_text = (row.get("complaint_text") or "").strip()

            # Drop unusable rows here so composition counts later
            # represent the actual merged dataset.
            if not complaint_text:
                continue

            # Support both the newest scraper schema ("product")
            # and the previous schema ("source_product").
            product = (
                row.get("product")
                or row.get("source_product")
                or ""
            ).strip()

            # Support both created_at and older date fields.
            raw_date = (
                row.get("created_at")
                or row.get("date")
                or ""
            )

            rows.append({
                "complaint_text": complaint_text,
                "rating": (row.get("rating") or "").strip(),
                "date": normalize_date(raw_date),
                "source": (
                    row.get("source")
                    or "google_play_store"
                ).strip(),
                "product": product,
                "company_size": (
                    row.get("company_size")
                    or ""
                ).strip(),

                # Experimental design:
                # identity is intentionally held constant.
                "user_id": DEFAULT_USER_ID,

                # Real Play Store rows are always real,
                # regardless of any accidental upstream value.
                "is_synthetic": "False",
            })

    return rows


# ============================================================
# SYNTHETIC DATA
# ============================================================

def load_synthetic(path):
    rows = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(f"{path} has no CSV header.")

        if "complaint_text" not in reader.fieldnames:
            raise ValueError(
                f"{path} is missing required column: complaint_text"
            )

        for row in reader:

            complaint_text = (row.get("complaint_text") or "").strip()

            if not complaint_text:
                continue

            raw_date = (
                row.get("date")
                or row.get("created_at")
                or ""
            )

            product = (
                row.get("product")
                or row.get("source_product")
                or ""
            ).strip()

            rows.append({
                "complaint_text": complaint_text,
                "rating": (row.get("rating") or "").strip(),
                "date": normalize_date(raw_date),
                "source": (
                    row.get("source")
                    or "synthetic"
                ).strip(),
                "product": product,
                "company_size": (
                    row.get("company_size")
                    or ""
                ).strip(),

                # Ignore USER_001 / USER_002 / etc. from older
                # synthetic datasets.
                "user_id": DEFAULT_USER_ID,

                # Rows loaded from this file are synthetic by
                # definition.
                "is_synthetic": "True",
            })

    return rows


# ============================================================
# MAIN MERGE
# ============================================================

def main():

    # --------------------------------------------------------
    # Validate input files
    # --------------------------------------------------------

    if not REAL_PATH.exists():
        raise SystemExit(
            f"Missing {REAL_PATH}. "
            "Run play_store_scraper.py first."
        )

    if not SYNTHETIC_PATH.exists():
        raise SystemExit(
            f"Missing {SYNTHETIC_PATH}. "
            "Run generate_synthetic_helpdesk_data.py first."
        )

    # --------------------------------------------------------
    # Load and normalize
    # --------------------------------------------------------

    real_rows = load_real(REAL_PATH)
    synthetic_rows = load_synthetic(SYNTHETIC_PATH)

    all_rows = real_rows + synthetic_rows

    total = len(all_rows)

    if total == 0:
        raise SystemExit(
            "No valid complaint rows were found in either input dataset."
        )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    user_ids = {row["user_id"] for row in all_rows}

    if user_ids != {DEFAULT_USER_ID}:
        raise ValueError(
            "Unexpected user IDs detected. "
            f"Expected only {DEFAULT_USER_ID}, found: {user_ids}"
        )

    missing_products = sum(
        1 for row in all_rows
        if not row["product"].strip()
    )

    missing_dates = sum(
        1 for row in all_rows
        if not row["date"].strip()
    )

    # Diagnostic only — DO NOT modify complaint text here.
    brand_mentions = [
        row
        for row in all_rows
        if any(
            term in row["complaint_text"].lower()
            for term in BRAND_CHECK_TERMS
        )
    ]

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_PATH,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=TARGET_COLUMNS
        )

        writer.writeheader()
        writer.writerows(all_rows)

    # --------------------------------------------------------
    # Composition report
    # --------------------------------------------------------

    n_real = len(real_rows)
    n_synthetic = len(synthetic_rows)

    product_counts = Counter(
        row["product"] or "<MISSING>"
        for row in all_rows
    )

    rating_counts = Counter(
        str(row["rating"]) if row["rating"] != "" else "<MISSING>"
        for row in all_rows
    )

    source_counts = Counter(
        row["source"] or "<MISSING>"
        for row in all_rows
    )

    synthetic_counts = Counter(
        row["is_synthetic"]
        for row in all_rows
    )

    # --------------------------------------------------------
    # Print report
    # --------------------------------------------------------

    print()
    print("=" * 65)
    print("INSIGHTDESK MASTER DATASET MERGE")
    print("=" * 65)

    print(
        f"Real rows:       "
        f"{n_real:5} ({n_real / total:.1%})"
    )

    print(
        f"Synthetic rows:  "
        f"{n_synthetic:5} ({n_synthetic / total:.1%})"
    )

    print(f"Total rows:      {total:5}")

    print()
    print("USER IDENTIFIER")
    print("-" * 65)
    print(f"Unique user IDs: {len(user_ids)}")
    print(f"User ID:         {DEFAULT_USER_ID}")

    print()
    print("BY PRODUCT")
    print("-" * 65)

    for product, count in product_counts.most_common():
        print(f"{product:35} {count:5}")

    print()
    print("BY SOURCE")
    print("-" * 65)

    for source, count in source_counts.most_common():
        print(f"{source:35} {count:5}")

    print()
    print("BY RATING")
    print("-" * 65)

    for rating, count in sorted(rating_counts.items()):
        print(f"{rating:10} {count:5}")

    print()
    print("REAL / SYNTHETIC FLAG")
    print("-" * 65)

    for flag, count in synthetic_counts.items():
        print(f"{flag:10} {count:5}")

    print()
    print("DATA QUALITY CHECKS")
    print("-" * 65)

    print(f"Missing product values:       {missing_products}")
    print(f"Missing date values:          {missing_dates}")
    print(f"Brand mentions in raw text:   {len(brand_mentions)}")

    print()
    print(
        "NOTE: Brand mentions are reported only. "
        "This merge script does NOT modify complaint text."
    )

    print()
    print(f"Saved to: {OUTPUT_PATH}")

    print("=" * 65)


if __name__ == "__main__":
    main()