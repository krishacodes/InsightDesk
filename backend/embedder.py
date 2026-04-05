import pandas as pd
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
from dotenv import load_dotenv
import os
import uuid

load_dotenv()

# ── Connect to Pinecone ──────────────────────────────────────
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index(os.getenv("PINECONE_INDEX"))

# ── Load embedding model ─────────────────────────────────────
print("🔄 Loading all-MiniLM-L6-v2 model...")
model = SentenceTransformer("all-MiniLM-L6-v2")
print("✅ Model loaded")

# ── Load complaints ──────────────────────────────────────────
df = pd.read_csv("data/master_complaints.csv")
df = df[df["complaint_text"].notna()]
print(f"✅ Loaded {len(df)} complaints")

# ── Embed + upsert in batches ────────────────────────────────
BATCH_SIZE = 50
total_upserted = 0

for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i+BATCH_SIZE]
    texts = batch["complaint_text"].tolist()
    
    # Generate embeddings
    embeddings = model.encode(texts, show_progress_bar=False)
    
    # Build vectors with metadata
    vectors = []
    for j, (_, row) in enumerate(batch.iterrows()):
        vector_id = str(row.name)  # use row index as ID
        metadata = {
            "complaint_text": str(row["complaint_text"])[:500],
            "rating":         int(row["rating"]) if pd.notna(row["rating"]) else 0,
            "source":         str(row["source"]) if pd.notna(row["source"]) else "",
            "product":        str(row["product"]) if pd.notna(row["product"]) else "",
            "company_size":   str(row["company_size"]) if pd.notna(row["company_size"]) else "",
            "user_id":        str(row["user_id"]) if pd.notna(row["user_id"]) else "",
            "is_synthetic":   bool(row["is_synthetic"]) if "is_synthetic" in row else False,
        }
        vectors.append((vector_id, embeddings[j].tolist(), metadata))
    
    # Upsert to Pinecone
    index.upsert(vectors=vectors)
    total_upserted += len(vectors)
    print(f"  ✅ Upserted batch {i//BATCH_SIZE + 1} — {total_upserted}/{len(df)} total")

print(f"\n✅ Done! {total_upserted} vectors upserted to Pinecone")

# ── Quick test: search for similar complaints ─────────────────
print("\n🔍 Testing similarity search...")
test_query = "app crashes every time I open it"
query_vector = model.encode([test_query])[0].tolist()

results = index.query(vector=query_vector, top_k=3, include_metadata=True)
print(f"Query: '{test_query}'")
print("Top 3 similar complaints:")
for match in results["matches"]:
    print(f"  Score: {match['score']:.3f} | {match['metadata']['complaint_text'][:80]}...")