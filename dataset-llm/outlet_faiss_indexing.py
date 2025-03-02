import faiss
from sentence_transformers import SentenceTransformer
import sys
import sqlite3
from llama_index.core.node_parser import SentenceSplitter
import numpy as np

MAX_WORDS = 250
OVERLAP = 25
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)

# DIMENSIONS = 384
DIMENSIONS = 768


def generate_index(outlet_db: str):
    print(f"generating faiss index for input file {outlet_db}")

    # Load embedding model
    # model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    model = SentenceTransformer("nomic-ai/nomic-embed-text-v2-moe", trust_remote_code=True, device="cpu")
    print(f"embedding model loaded")

    index_file = f"faiss_index_{outlet_db[:-len('.sqlite')]}.bin"
    print(f"using index file {index_file}")
    base_index = faiss.IndexFlatL2(DIMENSIONS)
    index = faiss.IndexIDMap(base_index)
    print(f"Empty index created")

    # Dictionary to store metadata: maps chunk IDs to document IDs
    chunk_metadata = {}

    with sqlite3.connect(outlet_db) as conn:
        conn.execute("create index if not exists idx_hits_number on outlet_hits(number);")
        result = conn.execute("select number, translated_text from outlet_hits order by number asc;").fetchall()
        rowids = [r[0] for r in result]
        documents = [r[1] for r in result]
        del result

        next_id = 0  # Unique chunk ID counter

        for (document_number, document) in zip(rowids, documents):
            chunks = splitter.split_text(document)
            print(f"document {document_number} split into {len(chunks)} chunks")

            embeddings = model.encode(chunks, convert_to_numpy=True)

            chunk_ids = np.arange(next_id, next_id + len(chunks))
            next_id += len(chunks)

            for chunk_id in chunk_ids:
                chunk_metadata[chunk_id] = document_number

            index.add_with_ids(embeddings, chunk_ids)
        del documents

        print(f"all documents added")
        faiss.write_index(index, index_file)
        print("Index saved to disk.")

        metadata_file = f"faiss_metadata_{outlet_db[:-len('.sqlite')]}.npy"
        np.save(metadata_file, chunk_metadata)
        print(f"Metadata saved to {metadata_file}")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : faiss_indexing.py <input-file>")
    generate_index(sys.argv[1])