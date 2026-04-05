from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()

# ── Connect to Supabase ──────────────────────────────────────
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def user_prefilter(user_id: str, cluster_id: str) -> dict:
    """
    Stage 0: Pre-filter check.
    If same user_id reported same cluster_id within 24 hours → auto merge.
    Returns: {"should_merge": True/False, "merge_into_id": int or None}
    """
    if not user_id or user_id == "null" or user_id == "None":
        return {"should_merge": False, "merge_into_id": None}

    # 24 hour window
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()

    result = supabase.table("complaints")\
        .select("id, complaint_text, report_count")\
        .eq("user_id", user_id)\
        .eq("cluster_id", cluster_id)\
        .gte("created_at", cutoff)\
        .eq("is_duplicate", 0)\
        .order("created_at", desc=True)\
        .limit(1)\
        .execute()

    if result.data:
        parent = result.data[0]
        return {
            "should_merge": True,
            "merge_into_id": parent["id"],
            "parent_text": parent["complaint_text"],
            "current_count": parent["report_count"]
        }

    return {"should_merge": False, "merge_into_id": None}


def apply_merge(parent_id: int, new_user_id: str, new_complaint_id: int):
    """
    Merge a complaint into its parent:
    - Increment report_count
    - Append user_id to user_ids array
    - Update last_reported_at
    - Mark new complaint as duplicate
    """
    # Get current parent
    parent = supabase.table("complaints")\
        .select("report_count, user_ids")\
        .eq("id", parent_id)\
        .execute().data[0]

    current_count = parent["report_count"] or 1
    current_user_ids = parent["user_ids"] or []

    # Update parent
    supabase.table("complaints").update({
        "report_count": current_count + 1,
        "user_ids": current_user_ids + [new_user_id],
        "last_reported_at": datetime.utcnow().isoformat()
    }).eq("id", parent_id).execute()

    # Mark new complaint as duplicate
    supabase.table("complaints").update({
        "is_duplicate": 1,
        "merged_into_id": parent_id
    }).eq("id", new_complaint_id).execute()

    print(f"✅ Merged complaint {new_complaint_id} into parent {parent_id}")
    print(f"   report_count: {current_count} → {current_count + 1}")


# ── Test the pre-filter ──────────────────────────────────────
if __name__ == "__main__":
    print("🔍 Testing user pre-filter...")

    # Test 1: user_id is null (scraped data) → should NOT merge
    result = user_prefilter("null", "cluster_1")
    print(f"\nTest 1 (null user): {result}")
    assert result["should_merge"] == False

    # Test 2: new user, no history → should NOT merge
    result = user_prefilter("test_user_999", "cluster_1")
    print(f"Test 2 (new user): {result}")
    assert result["should_merge"] == False

    print("\n✅ Pre-filter logic working correctly!")
    print("   Real merge testing will happen during live ingestion.")