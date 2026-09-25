from google_play_scraper import reviews, Sort
import pandas as pd
import time
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

# Helpdesk / customer-support software domain
APPS = {
    "Vision Helpdesk": "com.visionhelpdesk.visionhelpdesk",
    "Freshdesk": "com.freshdesk.helpdesk",
    "Zendesk": "com.zendesk.android",
    "Zoho Desk": "com.zoho.support",
    "LiveAgent": "com.qualityunit.liveagent",
    "Help Scout": "net.helpscout.android",
    "HappyFox": "com.happyfox.helpdesk",
}

REVIEWS_PER_PRODUCT = 800

# Current controlled deduplication experiment:
# user identity is intentionally kept constant because user_id
# is NOT an input to semantic duplicate detection.
DEFAULT_USER_ID = "DEFAULT_USER"

# Run this script from the InsightDesk project root.
OUTPUT_PATH = Path("data/play_store_reviews.csv")


# ============================================================
# SCRAPE REVIEWS
# ============================================================

all_rows = []
product_stats = {}

for product_name, app_id in APPS.items():

    print(f"\nScraping {product_name}")
    print(f"Package ID: {app_id}")

    try:
        result, _ = reviews(
            app_id,
            lang="en",
            country="in",
            sort=Sort.NEWEST,
            count=REVIEWS_PER_PRODUCT,
        )

        print(f"Retrieved {len(result)} reviews.")

    except Exception as e:
        print(f"FAILED: {product_name}")
        print(f"Reason: {e}")

        product_stats[product_name] = {
            "retrieved": 0,
            "complaint_like": 0,
        }

        continue

    complaint_count = 0

    for review in result:

        rating = review.get("score")
        text = review.get("content")

        # Ignore malformed/empty reviews
        if not text or rating is None:
            continue

        # ----------------------------------------------------
        # Complaint-like review heuristic
        # ----------------------------------------------------
        # We retain reviews with ratings <= 3.
        # This is a data-collection heuristic, NOT a claim that
        # every <=3-star review is a ground-truth complaint.
        # ----------------------------------------------------
        if rating > 3:
            continue

        complaint_count += 1

        review_date = review.get("at")

        all_rows.append({
            "user_id": DEFAULT_USER_ID,
            "complaint_text": text.strip(),
            "rating": rating,
            "created_at": (
                review_date.isoformat()
                if review_date is not None
                else None
            ),
            "source": "google_play_store",
            "source_product": product_name,
            "is_synthetic": False,
        })

    product_stats[product_name] = {
        "retrieved": len(result),
        "complaint_like": complaint_count,
    }

    print(
        f"Kept {complaint_count} reviews "
        f"with rating <= 3."
    )

    # Small delay between products
    time.sleep(2)


# ============================================================
# CREATE DATASET
# ============================================================

df = pd.DataFrame(all_rows)

if df.empty:
    print("\nNo complaint-like reviews were collected.")
    raise SystemExit(0)


# Give every complaint its OWN unique identifier.
# user_id remains constant; complaint_id does not.
df.insert(
    0,
    "complaint_id",
    [f"PS_{i:05d}" for i in range(1, len(df) + 1)]
)


# ============================================================
# BASIC CLEANING
# ============================================================

# Remove exact duplicate rows based on complaint text + product.
before_dedup = len(df)

df = df.drop_duplicates(
    subset=["complaint_text", "source_product"]
).reset_index(drop=True)

removed = before_dedup - len(df)

# Regenerate sequential IDs after removing exact duplicates.
df["complaint_id"] = [
    f"PS_{i:05d}" for i in range(1, len(df) + 1)
]


# ============================================================
# SAVE
# ============================================================

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(
    OUTPUT_PATH,
    index=False,
    encoding="utf-8"
)


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("PLAY STORE DATA COLLECTION SUMMARY")
print("=" * 60)

for product_name, stats in product_stats.items():

    print(
        f"{product_name:20} "
        f"Retrieved: {stats['retrieved']:3} | "
        f"Kept: {stats['complaint_like']:3}"
    )

print("-" * 60)

print(f"Exact duplicate rows removed: {removed}")
print(f"Final dataset size:           {len(df)}")

print("\nFinal rows by product:")
print(df["source_product"].value_counts())

print("\nRating distribution:")
print(df["rating"].value_counts().sort_index())

print("\nUser IDs:")
print(df["user_id"].value_counts())

print("\nDataset columns:")
print(list(df.columns))

print(f"\nSaved dataset to: {OUTPUT_PATH}")

print("=" * 60)