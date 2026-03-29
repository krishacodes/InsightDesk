from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

class Complaint(BaseModel):
    text: str

@app.post("/analyze")
def analyze(complaint: Complaint):
    print("Received:", complaint.text)   # ✅ debug 1

    data = {
        "text": complaint.text
    }

    response = supabase.table("complaints").insert(data).execute()

    print("Inserted:", response)         # ✅ debug 2

    return {"status": "stored", "data": data}