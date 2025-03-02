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


def load_model(model_name: str, **kwargs):
    # Load embedding model
    model = SentenceTransformer(model_name, **kwargs)
    print(f"embedding model loaded")
    return model

def read_index_and_metadata(index_file: str, metadata_file: str):
    index = faiss.read_index(index_file)
    print(f"index loaded")

    chunk_metadata = np.load(metadata_file, allow_pickle=True).item()
    print(f"metadata loaded")
    return index, chunk_metadata

def query_index(index_file: str, metadata_file: str, query: str, nr_of_hits: int = 5):
    print(f"querying faiss index {index_file} with query {query}")

    model = load_model("sentence-transformers/all-MiniLM-L6-v2")
    index, chunk_metadata = read_index_and_metadata(index_file, metadata_file)

    unique_retrieved_doc_ids = query_index_with_model(model, index, chunk_metadata, query, nr_of_hits)
    print(f"Retrieved document IDs: {unique_retrieved_doc_ids}")


def query_index_with_model(model: SentenceTransformer, index, chunk_metadata, query: str, nr_of_hits: int = 5):
    query_embedding = model.encode([query], convert_to_numpy=True)
    D, I = index.search(query_embedding, k=nr_of_hits)

    retrieved_doc_ids = [chunk_metadata.get(int(chunk_id), None) for chunk_id in I[0]]
    doc_ids_set = set()
    unique_retrieved_doc_ids = []
    for doc_id in retrieved_doc_ids:
        if doc_id not in doc_ids_set:
            unique_retrieved_doc_ids.append(doc_id)
        doc_ids_set.add(doc_id)

    return unique_retrieved_doc_ids



if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : outlet_faiss_querying.py <index_file> <metadata_file> <query> [nr_of_hits]")
    query_index(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 5)