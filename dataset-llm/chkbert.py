from typing import List, Callable
import torch

from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer

from chunkey_bert.model import ChunkeyBert
import re

sentence_model: SentenceTransformer = SentenceTransformer(model_name_or_path="all-MiniLM-L6-v2")
keybert: KeyBERT = KeyBERT(model=sentence_model)
keyphrase_vectorizer: KeyphraseCountVectorizer = KeyphraseCountVectorizer(spacy_pipeline="en_core_web_trf")

chunkey_bert: ChunkeyBert = ChunkeyBert(keybert=keybert)
chunker: Callable[[str], List[str]] = lambda text: [t for t in text.split("\n\n") if len(t) > 25]
chunker_leestekens: Callable[[str], List[str]] = lambda text: [t for t in re.split("\.|\?|!", text) if len(t) > 25]

with open("translated_text.txt", "r") as f:
    text = f.read()

results = chunker_leestekens(text)
print(f"aantal chunks: {len(results)}")
print(chunker_leestekens(text))

print(
chunkey_bert.extract_keywords(
    docs=text, num_keywords=5, chunker=chunker_leestekens, vectorizer=keyphrase_vectorizer, nr_candidates=10, top_n=3
)
)
# print(chunkey_bert.extract_keywords(docs=text, num_keywords=5, vectorizer=keyphrase_vectorizer, nr_candidates=20, top_n=10))