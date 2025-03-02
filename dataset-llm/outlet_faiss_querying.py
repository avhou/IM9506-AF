import faiss
from sentence_transformers import SentenceTransformer
import sys
import sqlite3
from llama_index.core.node_parser import SentenceSplitter
import numpy as np

MAX_WORDS = 250
OVERLAP = 25
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)

DIMENSIONS = 384
# DIMENSIONS = 768


def query_index(index_file: str, metadata_file: str, query: str):
    print(f"querying faiss index {index_file} with query {query}")

    # Load embedding model
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    # model = SentenceTransformer("nomic-ai/nomic-embed-text-v2-moe", trust_remote_code=True)
    print(f"embedding model loaded")

    index = faiss.read_index(index_file)
    print(f"index loaded")

    chunk_metadata = np.load(metadata_file, allow_pickle=True).item()
    print(f"metadata loaded")

    query_embedding = model.encode([query], convert_to_numpy=True)
    D, I = index.search(query_embedding, k=5)  # Get top 5 results

    # Retrieve document IDs for the found chunks
    retrieved_doc_ids = [chunk_metadata.get(int(chunk_id), None) for chunk_id in I[0]]
    print(f"Retrieved document IDs: {retrieved_doc_ids}")
    print(f"actual ids coming from index: {I}")
    print(f"actual documents coming from index: {D}")



if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : outlet_faiss_querying.py <index_file> <metadata_file> <query>")
    query_index(sys.argv[1], sys.argv[2], sys.argv[3])