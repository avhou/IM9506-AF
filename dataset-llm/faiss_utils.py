import numpy as np
import os

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
