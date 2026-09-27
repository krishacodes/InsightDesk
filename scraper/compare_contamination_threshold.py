"""
Measures the REAL production-scale contamination difference between
two candidate STS-B thresholds, using the same 12-contaminated-case
methodology already run once at 0.5858.

For every case containing complaints from more than one known
synthetic issue_id, scores every cross-issue-id complaint pair with
STS-B, then reports how many of those pairs would incorrectly score
above threshold (i.e. would have been merged as "duplicates" despite
describing different issues) at EACH candidate threshold, side by
side.

This answers "how much more contamination risk does 0.5858 actually
carry vs 0.6469" with real data, not inference from one calibration
pair.

Run from the project root:
    python scraper/compare_thresholds_on_contamination.py
"""

import csv
from collections import defaultdict
from itertools import combinations
from sentence_transformers import CrossEncoder

from backend.database.supabase import supabase

MODEL_NAME = "cross-encoder/stsb-distilroberta-base"
GROUND_TRUTH_PATH = "data/synthetic_ground_truth.csv"
CANDIDATE_THRESHOLDS = [0.5858, 0.6469]


def load_ground_truth():
    with open(GROUND_TRUTH_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # text -> issue_id lookup
    return {r["complaint_text"]: r["issue_id"] for r in rows}


def fetch_synthetic_complaints():
    # paginate - synthetic set is 488 rows, under the 1000 cap, but
    # paginate anyway for safety/consistency with earlier lessons
    rows = []
    offset = 0
    while True:
        batch = (
            supabase.table("complaints")
            .select("complaint_id,complaint_text,case_id,is_synthetic")
            .eq("is_synthetic", True)
            .range(offset, offset + 999)
            .execute()
            .data
        )
        if not batch:
            break
        rows.extend(batch)
        offset += 1000
    return rows


def main():
    text_to_issue = load_ground_truth()
    complaints = fetch_synthetic_complaints()

    matched = 0
    unmatched = 0
    by_case = defaultdict(list)

    for c in complaints:
        issue_id = text_to_issue.get(c["complaint_text"])
        if issue_id is None:
            unmatched += 1
            continue
        matched += 1
        by_case[c["case_id"]].append({
            "complaint_id": c["complaint_id"],
            "text": c["complaint_text"],
            "issue_id": issue_id,
        })

    print(f"Matched {matched} synthetic complaints to ground truth "
          f"({unmatched} unmatched - text may have been altered/cleaned)")

    contaminated_cases = {
        case_id: members for case_id, members in by_case.items()
        if len(set(m["issue_id"] for m in members)) > 1
    }
    print(f"Found {len(contaminated_cases)} cases with >1 issue_id")

    cross_issue_pairs = []
    for case_id, members in contaminated_cases.items():
        for a, b in combinations(members, 2):
            if a["issue_id"] != b["issue_id"]:
                cross_issue_pairs.append((a, b, case_id))

    print(f"Found {len(cross_issue_pairs)} cross-issue complaint pairs\n")

    print(f"Loading {MODEL_NAME} ...")
    model = CrossEncoder(MODEL_NAME)

    sentence_pairs = [(a["text"], b["text"]) for a, b, _ in cross_issue_pairs]
    print(f"Scoring {len(sentence_pairs)} pairs ...")
    scores = model.predict(sentence_pairs)

    for threshold in CANDIDATE_THRESHOLDS:
        above = sum(1 for s in scores if s >= threshold)
        print(f"\nThreshold {threshold}: {above}/{len(scores)} cross-issue pairs "
              f"score >= threshold (these are the contamination events that "
              f"WOULD occur if this threshold had been used)")

    # Show the specific pairs that differ between the two thresholds -
    # these are the ones directly responsible for the recall/contamination
    # tradeoff, worth reading individually.
    if len(CANDIDATE_THRESHOLDS) == 2:
        lo, hi = sorted(CANDIDATE_THRESHOLDS)
        print(f"\n--- Pairs that would merge at {lo} but NOT at {hi} ---")
        print(f"(these are exactly what you gain by loosening the threshold,")
        print(f"and exactly what you're risking as contamination)\n")
        for (a, b, case_id), score in zip(cross_issue_pairs, scores):
            if lo <= score < hi:
                print(f"[{score:.4f}] case_id={case_id}  "
                      f"{a['issue_id']} <-> {b['issue_id']}")
                print(f"  A: {a['text']}")
                print(f"  B: {b['text']}")
                print()


if __name__ == "__main__":
    main()