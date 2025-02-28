from typing import List, Callable
import torch
import sys
import sqlite3

from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer
from llama_index.core.node_parser import SentenceSplitter

from chunkey_bert.model import ChunkeyBert
import re

# Detect and use MPS if available
device = "mps" if torch.backends.mps.is_available() else "cpu"
print(f"Using device: {device}")


chunker_leestekens: Callable[[str], List[str]] = lambda text: [t for t in re.split("\.|\?|!", text) if len(t) > 25]

MAX_WORDS = 350
OVERLAP = 50
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)
def chunker_llama_index(text: str) -> List[str]:
    if len(text.split()) <= MAX_WORDS:
        return chunker_leestekens(text)
    else:
        return splitter.split_text(text)

def extract(target_db: str, limit: int = 20000000, offset: int = 0, table_name: str = "hits_keywords"):
    with sqlite3.connect(target_db) as conn:
        conn.execute(f"create table if not exists {table_name}(url text primary key, keywords text);")
        count = conn.execute(f"select count(*) from hits_translation t left outer join {table_name} k on t.url = k.url where k.url is null").fetchone()[0]
        print(f"found {count} urls to summarize, limit {limit} offset {offset}")
        count = min(count, limit)

        sentence_model: SentenceTransformer = SentenceTransformer(model_name_or_path="all-MiniLM-L6-v2").to(device)
        keybert: KeyBERT = KeyBERT(model=sentence_model)
        keyphrase_vectorizer: KeyphraseCountVectorizer = KeyphraseCountVectorizer(spacy_pipeline="en_core_web_trf")
        chunkey_bert: ChunkeyBert = ChunkeyBert(keybert=keybert)

        unwanted_keywords = {"dutchnews", "dutch news", "dutchnews.nl", "n - va", "n-va"}
        max_number_of_keywords = 5
        i = 1
        for r in conn.execute(f"select t.url, t.translated_text from hits_translation t left outer join {table_name} k on t.url = k.url where k.url is null limit {limit} offset {offset}"):
            words = r[1].split(" ")
            print(f"summarizing {i}/{count}, word count {len(words)}:  {r[0]}")
            try:
                result = chunkey_bert.extract_keywords(
                    docs=r[1], num_keywords=10, chunker=chunker_leestekens, vectorizer=keyphrase_vectorizer, nr_candidates=10, top_n=3
                )

                if (len(result) > 0):
                    first_result = result[0]
                    keywords_set = set()
                    keywords = []
                    keywords_used = 0
                    for keyword, score in first_result:
                        keyword = str(keyword).strip().lower()
                        score = float(score)
                        if (keyword not in unwanted_keywords) and (keywords_used < max_number_of_keywords) and (keyword not in keywords_set):
                            keywords_set.add(keyword)
                            keywords.append(f"{keyword} ({score:.2f})")
                            keywords_used = keywords_used + 1
                keyword_str = " - ".join(keywords)
                conn.execute(f"insert into {table_name} (url, keywords) values (?, ?)", (r[0], keyword_str))
                conn.commit()
                print(f"translation {i}/{count} done, {len(words)} words summarized to {keyword_str}")

                i = i + 1
            except Exception as e:
                print(f"error summarizing {r[0]}, error was {e}, chunked text llama_index was {chunker_llama_index(r[1])}, chunked text leestekens was {chunker_leestekens(r[1])}")



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : chkbert.py target-db [limit] [offset]")
    extract(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20000000, int(sys.argv[3]) if len(sys.argv) > 3 else 0, sys.argv[4] if len(sys.argv) > 4 else "hits_keywords")
