from sentence_transformers import SentenceTransformer
import sys
from llama_index.core.node_parser import SentenceSplitter
from faiss_utils import *

MAX_WORDS = 250
OVERLAP = 25
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)

DIMENSIONS = 384
# DIMENSIONS = 768


def query_index(index_file: str, metadata_file: str, query: str, nr_of_hits: int = 5):
    print(f"querying faiss index {index_file} with query {query}")

    model = load_model("sentence-transformers/all-MiniLM-L6-v2")
    index, chunk_metadata = read_index_and_metadata(index_file, metadata_file)

    unique_retrieved_doc_ids = query_index_with_model(model, index, chunk_metadata, query, nr_of_hits)
    print(f"Retrieved document IDs: {unique_retrieved_doc_ids}")


if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : outlet_faiss_querying.py <index_file> <metadata_file> <query> [nr_of_hits]")
    query_index(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 5)