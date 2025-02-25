from typing import List, Callable
import torch
import sys
import sqlite3

from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer

from chunkey_bert.model import ChunkeyBert
import re

# Detect and use MPS if available
device = "mps" if torch.backends.mps.is_available() else "cpu"
device = "cpu"
print(f"Using device: {device}")
chunker_leestekens: Callable[[str], List[str]] = lambda text: [t for t in re.split("\.|\?|!", text) if len(t) > 25]

def extract(target_db: str, limit: int = 20000000, offset: int = 0):
    with sqlite3.connect(target_db) as conn:
        conn.execute(f"create table if not exists hits_keywords(url text primary key, keywords text);")
        count = conn.execute("select count(*) from hits_translation t left outer join hits_keywords k on t.url = k.url where k.url is null").fetchone()[0]
        print(f"found {count} urls to summarize, limit {limit} offset {offset}")
        count = min(count, limit)

        sentence_model: SentenceTransformer = SentenceTransformer(model_name_or_path="all-MiniLM-L6-v2").to(device)
        keybert: KeyBERT = KeyBERT(model=sentence_model)
        keyphrase_vectorizer: KeyphraseCountVectorizer = KeyphraseCountVectorizer(spacy_pipeline="en_core_web_trf")
        chunkey_bert: ChunkeyBert = ChunkeyBert(keybert=keybert)

        unwanted_keywords = {"dutchnews", "dutch news"}
        max_number_of_keywords = 5
        i = 1
        for r in conn.execute(f"select t.url, t.translated_text from hits_translation t left outer join hits_keywords k on t.url = k.url where k.url is null limit {limit} offset {offset}"):
            words = r[1].split(" ")
            print(f"summarizing {i}/{count}, word count {len(words)}:  {r[0]}")
            try:
                result = chunkey_bert.extract_keywords(
                    docs=r[1], num_keywords=10, chunker=chunker_leestekens, vectorizer=keyphrase_vectorizer, nr_candidates=10, top_n=3
                )
            except Exception as e:
                print(f"error summarizing {r[0]}")

            if (len(result) > 0):
                first_result = result[0]
                keywords = set()
                keywords_used = 0
                for keyword, _ in first_result:
                    if (str(keyword).lower() not in unwanted_keywords) and (keywords_used < max_number_of_keywords):
                        keywords.add(str(keyword))
                        keywords_used = keywords_used + 1
            keyword_set = ",".join(sorted(keywords))
            conn.execute("insert into hits_keywords (url, keywords) values (?, ?)", (r[0], keyword_set))
            conn.commit()
            print(f"translation {i}/{count} done, {len(words)} words summarized to {keyword_set}")

            i = i + 1


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : chkbert.py target-db [limit] [offset]")
    extract(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20000000, int(sys.argv[3]) if len(sys.argv) > 3 else 0)
