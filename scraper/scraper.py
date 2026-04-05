import pandas as pd
import re

# ── 1. Load Play Store ───────────────────────────────────────
play = pd.read_csv("data/play_store_reviews.csv")
print(f"✅ Play Store: {len(play)} rows")

# ── 2. Load Capterra ─────────────────────────────────────────
capterra_raw = pd.read_csv("data/dataset_capterra-reviews_2026-04-02_05-51-06-491.csv")
print(f"✅ Capterra raw: {len(capterra_raw)} rows")

# ── 3. Normalize Capterra → master schema ────────────────────
def normalize_capterra(df):
    rows = []
    for _, r in df.iterrows():
        # Combine cons + comments as complaint_text (most negative signal)
        parts = []
        if pd.notna(r.get("cons")) and str(r["cons"]).strip():
            parts.append(str(r["cons"]).strip())
        if pd.notna(r.get("comments")) and str(r["comments"]).strip():
            parts.append(str(r["comments"]).strip())
        complaint_text = " | ".join(parts) if parts else None

        # Parse date
        try:
            date = pd.to_datetime(r["date"]).strftime("%Y-%m-%d")
        except:
            date = None

        # Normalize company size
        size_raw = str(r.get("companySize", "")) 
        if "1-10" in size_raw or "11-50" in size_raw:
            company_size = "Small Business"
        elif "51-200" in size_raw or "201-500" in size_raw:
            company_size = "Mid-Market"
        elif "501" in size_raw or "1000" in size_raw or "1,000" in size_raw:
            company_size = "Enterprise"
        else:
            company_size = None

        rows.append({
            "complaint_text": complaint_text,
            "rating":         r["rating"],
            "date":           date,
            "source":         "capterra",
            "product":        "Vision Helpdesk",
            "company_size":   company_size,
            "user_id":        None,
            # extra metadata
            "job_title":      r.get("jobTitle"),
            "industry":       r.get("industry"),
            "time_used":      r.get("timeUsed"),
            "pros":           r.get("pros"),
            "cons":           r.get("cons"),
        })
    return pd.DataFrame(rows)

capterra = normalize_capterra(capterra_raw)
print(f"✅ Capterra normalized: {len(capterra)} rows")

# ── 4. Add metadata columns to Play Store ────────────────────
play["job_title"] = None
play["industry"]  = None
play["time_used"] = None
play["pros"]      = None
play["cons"]      = None

# ── 5. Merge all sources ─────────────────────────────────────
master = pd.concat([play, capterra], ignore_index=True)

# ── 6. Drop rows with no complaint_text ──────────────────────
before = len(master)
master = master[master["complaint_text"].notna()]
master = master[master["complaint_text"].str.strip() != ""]
print(f"✅ Dropped {before - len(master)} empty rows")

# ── 7. Save ──────────────────────────────────────────────────
master.to_csv("data/master_complaints.csv", index=False)
print(f"\n✅ master_complaints.csv saved — {len(master)} total rows")
print(f"\n📊 Breakdown:")
print(master.groupby(["source", "product"])["rating"].count())
print(f"\n⭐ Rating distribution:")
print(master["rating"].value_counts().sort_index())