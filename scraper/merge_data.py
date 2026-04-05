import pandas as pd

# ── 1. Load real scraped data ────────────────────────────────
real = pd.read_csv("data/master_complaints.csv")
print(f"✅ Real data: {len(real)} rows")

# ── 2. Load synthetic data ───────────────────────────────────
synthetic = pd.read_csv("data/master_data_600.csv")
print(f"✅ Synthetic data: {len(synthetic)} rows")

# ── 3. Fix product names in synthetic data ───────────────────
synthetic["product"] = synthetic["product"].str.replace(
    r"Vision Helpdesk - .*", "Vision Helpdesk", regex=True
)

# ── 4. Add missing metadata columns to synthetic ─────────────
for col in ["job_title", "industry", "time_used", "pros", "cons"]:
    if col not in synthetic.columns:
        synthetic[col] = None

# ── 5. Add missing metadata columns to real ──────────────────
for col in ["job_title", "industry", "time_used", "pros", "cons"]:
    if col not in real.columns:
        real[col] = None

# ── 6. Tag synthetic rows ────────────────────────────────────
real["is_synthetic"] = False
synthetic["is_synthetic"] = True

# ── 7. Merge ─────────────────────────────────────────────────
master = pd.concat([real, synthetic], ignore_index=True)

# ── 8. Clean ─────────────────────────────────────────────────
master = master[master["complaint_text"].notna()]
master = master[master["complaint_text"].str.strip() != ""]

# ── 9. Save ──────────────────────────────────────────────────
master.to_csv("data/master_complaints.csv", index=False)
print(f"\n✅ master_complaints.csv saved — {len(master)} total rows")
print(f"\n📊 Source breakdown:")
print(master.groupby("source")["rating"].count())
print(f"\n⭐ Rating distribution:")
print(master["rating"].value_counts().sort_index())
print(f"\n🏢 Company size:")
print(master["company_size"].value_counts())
print(f"\n🔬 Synthetic vs Real:")
print(master["is_synthetic"].value_counts())