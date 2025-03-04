import faiss
from sentence_transformers import SentenceTransformer
import sys
import sqlite3
from llama_index.core.node_parser import SentenceSplitter
import numpy as np
from faiss_utils import normalize

MAX_WORDS = 250
OVERLAP = 25
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)

# DIMENSIONS = 384
DIMENSIONS = 768

def generate_indices(outlet_db: str, column_name: str = "translated_text"):
    print(f"generating faiss indices for input file {outlet_db}")

    for (model_name, dimension, kwargs) in zip(
        ["sentence-transformers/all-MiniLM-L6-v2", "nomic-ai/nomic-embed-text-v2-moe"],
        [384, 768],
        [{}, {"trust_remote_code": True, "device": "cpu"}]
    ):
        for column_name in ["translated_text", "content"]:
            generate_index(outlet_db, column_name, model_name, dimension, **kwargs)


def generate_index(outlet_db: str, column_name: str, model_name: str, dimension: int, **kwargs):
    model = SentenceTransformer(model_name, **kwargs)
    print(f"embedding model {model_name} loaded")

    cleaned_model_name = model_name.replace("/", "_")

    index_file = f"faiss_index_{cleaned_model_name}_{column_name}_{outlet_db[:-len('.sqlite')]}.bin"
    print(f"using index file {index_file}")
    base_index = faiss.IndexFlatL2(dimension)
    index = faiss.IndexIDMap(base_index)
    print(f"Empty index created for model {model_name} and colum {column_name}")

    # Dictionary to store metadata: maps chunk IDs to document IDs
    chunk_metadata = {}

    with sqlite3.connect(outlet_db) as conn:
        conn.execute("create index if not exists idx_hits_number on outlet_hits(number);")
        result = conn.execute(f"select number, {column_name}, link_percentage from outlet_hits order by number asc;").fetchall()
        rowids = [r[0] for r in result]
        documents = [r[1] for r in result]
        link_percentages = [r[2] for r in result]
        del result

        next_id = 0  # Unique chunk ID counter

        for (document_number, document, link_percentage) in zip(rowids, documents, link_percentages):
            chunks = splitter.split_text(f"link_percentage = {link_percentage}. {document}")
            print(f"document {document_number} split in {len(chunks)} chunks")

            embeddings = model.encode(chunks, convert_to_numpy=True)
            # normalize to use cosine similarity
            embeddings = normalize(embeddings)

            chunk_ids = np.arange(next_id, next_id + len(chunks))
            next_id += len(chunks)

            for chunk_id in chunk_ids:
                chunk_metadata[chunk_id] = document_number

            index.add_with_ids(embeddings, chunk_ids)
        del documents

        print(f"all documents added")
        faiss.write_index(index, index_file)
        print("Index saved to disk.")

        metadata_file = f"faiss_metadata_{cleaned_model_name}_{column_name}_{outlet_db[:-len('.sqlite')]}.npy"
        np.save(metadata_file, chunk_metadata)
        print(f"Metadata saved to {metadata_file}")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : faiss_indexing.py <input-file>")
    generate_indices(sys.argv[1])