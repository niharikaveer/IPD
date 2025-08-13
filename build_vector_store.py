# build_vector_store.py  (Chroma version)
import json
from sentence_transformers import SentenceTransformer
import chromadb

CHUNKS_FILE = "cases_chunks.jsonl"
PERSIST_DIR = "chroma_index"  # folder where Chroma stores the DB

# 1. Load chunks
docs = []
with open(CHUNKS_FILE, "r", encoding="utf-8") as f:
    for line in f:
        docs.append(json.loads(line))

print(f"Loaded {len(docs)} chunks from {CHUNKS_FILE}")

# 2. Embedding model
print("Loading embedding model...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# 3. Init Chroma
client = chromadb.PersistentClient(path=PERSIST_DIR)
collection = client.get_or_create_collection(
    name="legal_cases",
    metadata={"hnsw:space": "cosine"}  # cosine similarity
)

# 4. Insert documents
print("Adding documents to Chroma...")
texts = [d["text"] for d in docs]
metadatas = [d["metadata"] for d in docs]
ids = [d["id"] for d in docs]

# Compute embeddings
embeddings = model.encode(texts, batch_size=32, show_progress_bar=True).tolist()

collection.add(
    documents=texts,
    metadatas=metadatas,
    ids=ids,
    embeddings=embeddings
)

print(f"Stored {collection.count()} chunks in ChromaDB at {PERSIST_DIR}")
