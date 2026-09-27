"""
One-off diagnostic: for each of the known contaminated cases
(synthetic-only view), check whether the SAME case_id also contains
REAL (non-synthetic) complaints in production.

This determines whether the upcoming centroid-contamination
experiment can safely use synthetic-only membership, or whether it
needs to pull full case membership (synthetic + real) to be
representative of what a real deployed centroid would average over.

Run from the project root:
    python -m scraper.check_contamination_composition
"""

from collections import defaultdict

from scraper.compare_contamination_threshold import (
    load_ground_truth,
    fetch_synthetic_complaints,
)
from backend.database.supabase import supabase


def main():
    gt = load_ground_truth()
    cs = fetch_synthetic_complaints()

    x = defaultdict(list)
    for c in cs:
        if c["complaint_text"] in gt:
            x[c["case_id"]].append(c)

    contaminated = {
        k: v for k, v in x.items()
        if len(set(gt[m["complaint_text"]] for m in v)) > 1
    }

    print(f"{len(contaminated)} contaminated cases (synthetic-only view)\n")

    for case_id in contaminated:
        all_members = (
            supabase.table("complaints")
            .select("complaint_id,is_synthetic")
            .eq("case_id", case_id)
            .execute()
            .data
        )
        n_total = len(all_members)
        n_synthetic = sum(1 for m in all_members if m["is_synthetic"])
        n_real = n_total - n_synthetic
        print(f"case_id={case_id}: total={n_total}  synthetic={n_synthetic}  real={n_real}")


if __name__ == "__main__":
    main()