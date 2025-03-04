import faiss
from sentence_transformers import SentenceTransformer
import sys
import sqlite3
import numpy as np
from faiss_utils import normalize, models_and_params, get_splitter, metadata_name, index_name


def generate_indices(outlet_db: str, column_name: str = "translated_text"):
    print(f"generating faiss indices for input file {outlet_db}")

    for (model_name, dimension, kwargs) in models_and_params():
        for column_name in ["translated_text", "content"]:
            generate_index(outlet_db, column_name, model_name, dimension, **kwargs)


def generate_index(outlet_db: str, column_name: str, model_name: str, dimension: int, **kwargs):
    model = SentenceTransformer(model_name, **kwargs)
    print(f"embedding model {model_name} loaded")
    splitter = get_splitter()

    index_file = index_name(model_name, column_name, outlet_db)
    print(f"using index file {index_file}")
    base_index = faiss.IndexFlatL2(dimension)
    index = faiss.IndexIDMap(base_index)
    print(f"Empty index created for model {model_name} and colum {column_name}")

    # Dictionary to store metadata: maps chunk IDs to document IDs
    chunk_metadata = {}

    with sqlite3.connect(outlet_db) as conn:
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

        metadata_file = metadata_name(model_name, column_name, outlet_db)
        np.save(metadata_file, chunk_metadata)
        print(f"Metadata saved to {metadata_file}")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : faiss_indexing.py <input-file>")
    generate_indices(sys.argv[1])