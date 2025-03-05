import numpy as np
import os
import faiss
from sentence_transformers import SentenceTransformer

from llama_index.core.node_parser import SentenceSplitter


def normalize(vectors):
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    return vectors / (norms + 1e-10)  # Avoid division by zero

def clean_model_name(model_name: str) -> str:
    return model_name.replace("/", "_")

def normalized_db_name(db_name: str) -> str:
    return os.path.basename(os.path.normpath(db_name))

def index_name(model_name: str, column_name: str, db_name: str) -> str:
    return f"faiss_index_{clean_model_name(model_name)}_{column_name}_{normalized_db_name(db_name)[:-len('.sqlite')]}.bin"


def metadata_name(model_name: str, column_name: str, db_name: str) -> str:
    return f"faiss_metadata_{clean_model_name(model_name)}_{column_name}_{normalized_db_name(db_name)[:-len('.sqlite')]}.npy"


BASE_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
MULTILINGUAL_EMBEDDING_MODEL = "nomic-ai/nomic-embed-text-v2-moe"

def models_and_params():
    return zip([BASE_EMBEDDING_MODEL, MULTILINGUAL_EMBEDDING_MODEL], [384, 768], [{}, {"trust_remote_code": True, "device": "cpu"}])


MAX_WORDS = 250
OVERLAP = 25
def get_splitter(max_words: int = MAX_WORDS, overlap: int = OVERLAP) -> SentenceSplitter:
    return SentenceSplitter(chunk_size=max_words, chunk_overlap=overlap)


def get_index_and_metadata(faiss_folder: str, model_name: str, column_name: str, db_name: str) -> (str, str):
    index_path = os.path.join(faiss_folder, index_name(model_name, column_name, db_name))
    metadata_path = os.path.join(faiss_folder, metadata_name(model_name, column_name, db_name))
    return index_path, metadata_path

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

def query_index_with_model(model: SentenceTransformer, index, chunk_metadata, query: str, nr_of_hits: int = 5):
    query_embedding = model.encode([query], convert_to_numpy=True)
    query_embedding = normalize(query_embedding)
    D, I = index.search(query_embedding, k=nr_of_hits)

    retrieved_doc_ids = [chunk_metadata.get(int(chunk_id), None) for chunk_id in I[0]]
    doc_ids_set = set()
    unique_retrieved_doc_ids = []
    for doc_id in retrieved_doc_ids:
        if doc_id not in doc_ids_set:
            unique_retrieved_doc_ids.append(doc_id)
        doc_ids_set.add(doc_id)

    return unique_retrieved_doc_ids

