from google_play_scraper import reviews, Sort
import pandas as pd

# Vision Helpdesk app ID (from Play Store URL)
APP_ID = "com.visionhelpdesk.visionhelpdesk"

print("🔄 Scraping Google Play Store reviews...")

result, _ = reviews(
    APP_ID,
    lang='en',
    country='in',
    sort=Sort.NEWEST,
    count=200
)

data = []
for r in result:
    data.append({
        "complaint_text": r["content"],
        "rating": r["score"],
        "date": str(r["at"].date()),
        "source": "play_store",
        "product": "Vision Helpdesk",
        "company_size": None,
        "user_id": r["userName"]
    })

df = pd.DataFrame(data)
df.to_csv("data/play_store_reviews.csv", index=False)
print(f"✅ Saved {len(df)} reviews to data/play_store_reviews.csv")
print(df.head())