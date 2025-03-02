import json

import faiss
from sentence_transformers import SentenceTransformer
import sys
import sqlite3
from llama_index.core.node_parser import SentenceSplitter
import numpy as np
from outlet_faiss_querying import *


def query_index(index_file: str, metadata_file: str, questions_file: str, output_file:str, nr_of_hits: int = 5):
    model = load_model("sentence-transformers/all-MiniLM-L6-v2")
    index, chunk_metadata = read_index_and_metadata(index_file, metadata_file)

    responses = []
    total_unique_retrieved_doc_ids = set()
    with open(questions_file, "r") as f:
        for line in f:
            query = line.strip()
            if not query:
                continue

            print(f"querying faiss index {index_file} with query {line}")
            unique_retrieved_doc_ids = query_index_with_model(model, index, chunk_metadata, query, nr_of_hits)
            responses.append({'query': query, 'retrieved_doc_ids': unique_retrieved_doc_ids})
            total_unique_retrieved_doc_ids.update(unique_retrieved_doc_ids)
    responses.append({'query': 'total', 'retrieved_doc_ids': list(total_unique_retrieved_doc_ids)})
    with open(output_file, "w") as f:
        json.dump(responses, f)


if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : batch_outlet_faiss_querying.py <index_file> <metadata_file> <questions_file> <output_file> [nr_of_hits]")
    query_index(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]) if len(sys.argv) > 5 else 5)