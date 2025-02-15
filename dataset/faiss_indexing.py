import faiss
from sentence_transformers import SentenceTransformer
import os
import sys
import sqlite3
from transformers import MarianMTModel, MarianTokenizer
import torch

# # Example documents
# documents = [
#     "Machine learning is a method of data analysis that automates analytical model building.",
#     "Artificial intelligence (AI) is intelligence demonstrated by machines, in contrast to natural intelligence displayed by humans and animals.",
#     "Deep learning is part of a broader family of machine learning methods based on artificial neural networks.",
#     "Natural language processing (NLP) is a subfield of linguistics, computer science, and AI concerned with interactions between computers and human language.",
# ]
#
# documents_nl = [
#     "Machine learning is een data analyse methode voor het geautomatiseerd bouwen van analytische modellen.",
#     "Kunstmatige intelligentie (AI) is intelligence die zichtbaar is bij machines, in contrast met natuurlijke intelligentie die zichtbaar is bij mensen en dieren.",
#     "Deep learning is deel van een generiekere familie van machine learning methodes die gebaseerd zijn op kunstmatige neurale netwerken.",
#     "Natural language processing (NLP) is een deel van linguistiek, computerwetenschappen, en kunstmatige intelligentie die zich bezighoudt met interacties tussen computer en menselijke taal.",
# ]
#
# # Function to chunk text with overlap
# def chunk_text(text, chunk_size=50, overlap=10):
#     words = text.split()
#     chunks = []
#     for i in range(0, len(words), chunk_size - overlap):
#         chunk = " ".join(words[i:i + chunk_size])
#         chunks.append(chunk)
#     return chunks
#
# # Apply chunking to documents
# chunked_documents = []
# for doc in documents_nl:
#     chunked_documents.extend(chunk_text(doc, chunk_size=10, overlap=3))
#
# # Generate embeddings and create FAISS index
# if os.path.exists(index_file):
#     print("Loading existing FAISS index...")
#     index = faiss.read_index(index_file)
# else:
#     print("Creating new FAISS index...")
#     embeddings = model.encode(chunked_documents, convert_to_numpy=True)
#     dimension = embeddings.shape[1]
#     index = faiss.IndexFlatL2(dimension)
#     index.add(embeddings)
#     faiss.write_index(index, index_file)
#     print("Index saved to disk.")
#
# # Example query
# query = "How do machines learn from data?"
# query_nl = "Hoe leren machines van data?"
# query_embedding = model.encode([query_nl], convert_to_numpy=True)
#
# # Search for top-2 similar chunks
# k = 2
# D, I = index.search(query_embedding, k)
#
# # Print results
# print("Query:", query_nl)
# print(f"D is {D}")
# print("Top matching chunks:")
# for idx in I[0]:
#     print("-", chunked_documents[idx])
#
# from transformers import MarianMTModel, MarianTokenizer
# import torch
#
# tokenizer = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-nl-en")
# model = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-nl-en")
# # Force CPU execution
# device = torch.device("cpu")
# model.to("cpu")
# def translate(text):
#     print(f"generating inputs")
#     inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
#     print(f"generating translation")
#     translated = model.generate(**inputs)
#     print(f"batch decode")
#     return tokenizer.batch_decode(translated, skip_special_tokens=True)[0]
#
# print(translate("Hallo, hoe gaat het?"))
#
# # French → English
# print(translate("Bonjour, comment ça va?", "Helsinki-NLP/opus-mt-fr-en"))

DIMENSIONS = 384

def generate_translation(input: str, tokenizer: MarianTokenizer, model: MarianMTModel) -> str :
    inputs = tokenizer(input, return_tensors="pt", padding=True, truncation=True)
    translated = model.generate(**inputs)
    result = tokenizer.batch_decode(translated, skip_special_tokens=True)[0]
    print(f"translated {input} to {result}")
    return result


def generate_index(hits: str, translations: bool = False, remove_index_first: bool = False):
    print(f"generating faiss index for input file {hits}, translation {translations}")

    # Load embedding model
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    print(f"embedding model loaded")

    index_file = "faiss_index_translated.bin" if translations else "faiss_index.bin"
    index = faiss.IndexFlatL2(DIMENSIONS)
    print(f"empty index created")

    if (remove_index_first):
        print(f"removing existing index")
        if os.path.exists(index_file):
            os.remove(index_file)
            print(f"existing index removed")

    tokenizer_nl = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-nl-en") if translations else None
    model_translation_nl = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-nl-en") if translations else None

    tokenizer_fr = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-fr-en") if translations else None
    model_translation_fr = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-fr-en") if translations else None
    if (translations):
        print(f"forcing translation models to cpu")
        model_translation_fr.to("cpu")
        model_translation_nl.to("cpu")

    with sqlite3.connect(hits) as conn:
        result = conn.execute("select rowid, content, languages from hits order by rowid asc;").fetchall()
        rowids = [r[0] for r in result]
        documents = [r[1] for r in result]
        languages = [r[2] for r in result]
        del result

        with open("rowids-translated.txt" if translations else "rowids.txt", "w") as f:
            f.write("\n".join([str(r) for r in rowids]))
        del rowids

        chunk_size = 100
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]

            if (translations):
                translated_chunk = []
                for j, doc in enumerate(chunk):
                    if ("fra" in languages[i*chunk_size + j]):
                        print(f"document {i*chunk_size + j} is in french")
                        translated_chunk.append(generate_translation(doc, tokenizer_fr, model_translation_fr))
                    elif ("nld" in languages[i*chunk_size + j]):
                        print(f"document {i*chunk_size + j} is in dutch")
                        translated_chunk.append(generate_translation(doc, tokenizer_nl, model_translation_nl))
                    else:
                        print(f"document {i * chunk_size + j} will not be translated")
                        translated_chunk.append(doc)
                chunk = translated_chunk

            print(f"processing documents, start index {i}")
            embeddings = model.encode(chunk, convert_to_numpy=True)
            index.add(embeddings)
        del documents

        print(f"all documents added")
        faiss.write_index(index, index_file)
        print("Index saved to disk.")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : faiss_indexing.py <input-file>")
    generate_index(sys.argv[1], False, True)
    # generate_index(sys.argv[1], True, True)
