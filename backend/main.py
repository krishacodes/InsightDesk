from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
from supabase import create_client
from dotenv import load_dotenv
import os
import uuid

# Load environment variables
load_dotenv()

app = FastAPI()

# -------------------- MODELS --------------------

model = SentenceTransformer("all-MiniLM-L6-v2")

# -------------------- PINECONE (NEW SDK) --------------------

pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))

index_name = os.getenv("PINECONE_INDEX")
index = pc.Index(index_name)

# -------------------- SUPABASE --------------------

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# -------------------- CONFIG --------------------

SIMILARITY_THRESHOLD = 0.85

# -------------------- REQUEST MODEL --------------------

class Complaint(BaseModel):
    text: str

# -------------------- ROUTES --------------------

@app.get("/")
def home():
    return {"message": "InsightDesk API is running 🚀"}

# -------------------- MAIN LOGIC --------------------

@app.post("/analyze")
def analyze(complaint: Complaint):
    try:
        text = complaint.text

        # 🔹 Step 1: Embedding
        vector = model.encode(text).tolist()

        # 🔹 Step 2: Query Pinecone
        results = index.query(
            vector=vector,
            top_k=1,
            include_metadata=True
        )

        # 🔹 Step 3: Duplicate Check
        if results.get("matches"):
            top_match = results["matches"][0]

            if top_match["score"] > SIMILARITY_THRESHOLD:
                return {
                    "status": "duplicate",
                    "score": float(top_match["score"]),
                    "similar": top_match["metadata"].get("complaint_text", "")
                }

        # 🔹 Step 4: New Complaint
        complaint_id = str(uuid.uuid4())

        # 🔹 Step 5: Store in Pinecone
        index.upsert(vectors=[{
            "id": complaint_id,
            "values": vector,
            "metadata": {"complaint_text": text}
        }])

        # 🔹 Step 6: Store in Supabase
        supabase.table("complaints").insert({
            "id": complaint_id,   # MUST be uuid/text in DB
            "text": text
        }).execute()

        return {
            "status": "new complaint",
            "id": complaint_id
        }

    except Exception as e:
        print("ERROR:", str(e))  # Debug in terminal
        raise HTTPException(status_code=500, detail=str(e))