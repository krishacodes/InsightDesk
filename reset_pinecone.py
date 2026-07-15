import os
import time

from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec

load_dotenv()

INDEX_NAME = os.getenv("PINECONE_INDEX")

pc = Pinecone(
    api_key=os.getenv("PINECONE_API_KEY")
)

# -------------------------------
# Delete Existing Index
# -------------------------------

print(f"Deleting index: {INDEX_NAME}")

if INDEX_NAME in pc.list_indexes().names():

    pc.delete_index(INDEX_NAME)

    while INDEX_NAME in pc.list_indexes().names():
        print("Waiting for deletion...")
        time.sleep(5)

print("Index deleted successfully!")

# -------------------------------
# Recreate Index
# -------------------------------

print("Creating new index...")

pc.create_index(
    name=INDEX_NAME,
    dimension=384,
    metric="cosine",
    spec=ServerlessSpec(
        cloud="aws",
        region="us-east-1"  # Change if your index uses another region
    )
)

print("Index created successfully!")
print("Done.")