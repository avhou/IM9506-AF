import os.path
import sys
from typing import Callable, List
import re

import chunkey_bert
import duckdb
import fasttext
import wget
from transformers import MarianMTModel, MarianTokenizer
import torch
from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer
from llama_index.core.node_parser import SentenceSplitter

from chunkey_bert.model import ChunkeyBert

chunker_leestekens: Callable[[str], List[str]] = lambda text: [t for t in re.split("\.|\?|!", text) if len(t) > 25]

MAX_WORDS = 250
OVERLAP = 25
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)


def chunker_llama_index(text: str) -> List[str]:
    if len(text.split()) <= MAX_WORDS:
        return chunker_leestekens(text)
    else:
        return splitter.split_text(text)

# Detect and use MPS if available
device = "mps" if torch.backends.mps.is_available() else "cpu"
print(f"Using device: {device}")

sentence_model: SentenceTransformer = SentenceTransformer(model_name_or_path="all-MiniLM-L6-v2").to(device)
keybert: KeyBERT = KeyBERT(model=sentence_model)
keyphrase_vectorizer: KeyphraseCountVectorizer = KeyphraseCountVectorizer(spacy_pipeline="en_core_web_trf")
ckbert: ChunkeyBert = ChunkeyBert(keybert=keybert)


def translations_reddit(reddit_db: str):
    print(f"processing input {reddit_db}")

    # Connect to DuckDB
    with duckdb.connect() as conn:
        conn.execute(f"""ATTACH '{reddit_db}' as reddit (TYPE sqlite);""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_a add column if not exists detected_language text;""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_b add column if not exists detected_language text;""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_a add column if not exists translated_text text;""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_b add column if not exists translated_text text;""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_a add column if not exists keywords text;""")
        conn.execute(f"""alter table reddit.text_2nd_stage_optie_b add column if not exists keywords text;""")

        if not os.path.exists("lid.176.bin"):
            print("downloading fasttext model")
            wget.download("https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin", "lid.176.bin")

        model = fasttext.load_model("lid.176.bin")
        detect_languages("reddit.text_2nd_stage_optie_a", conn, model)
        detect_languages("reddit.text_2nd_stage_optie_b", conn, model)
        do_translations("reddit.text_2nd_stage_optie_a", conn)
        do_translations("reddit.text_2nd_stage_optie_b", conn)
        summarize("reddit.text_2nd_stage_optie_a", conn, ckbert)
        summarize("reddit.text_2nd_stage_optie_b", conn, ckbert)


def detect_language(text, model):
    prediction = model.predict(text, k=1)  # k=1 means top-1 prediction
    lang_code = prediction[0][0].replace('__label__', '')  # Extract language code
    confidence = prediction[1][0]  # Confidence score
    return lang_code, confidence

def detect_languages(table: str, conn, model):
    print(f"processing table {table}")
    for r in conn.execute(f"select id, text from {table} where detected_language is null and text is not null order by id asc").fetchall():
        id = r[0]
        text = r[1]
        text = re.sub(r'\s+', ' ', text)
        try:
            lang_code, confidence = detect_language(text, model)
            print(f"detected language for {id} is {lang_code} with confidence {confidence}")
            conn.execute(f"update {table} set detected_language = ? where id = ?", (lang_code, id))
            conn.commit()
        except:
            print(f"could not detect language for id {id}")

def chunk_text(text):
    sentences = splitter.split_text(text)
    return sentences

def get_max_output_length(inputs, scale_factor=1.3, max_len=512):
    # Find the longest input sequence in the batch
    max_input_length = max(len(input_ids) for input_ids in inputs['input_ids'])

    # Calculate the max output length based on a scaling factor
    max_output_length = min(max_len, int(max_input_length * scale_factor))

    return max_output_length

def translate_text_batch(text: str, tokenizer: MarianTokenizer, model: MarianMTModel,
                   batch_size: int = 8) -> str:
    text_chunks = chunk_text(text)

    translated_chunks = []

    for i in range(0, len(text_chunks), batch_size):
        batch = text_chunks[i: i + batch_size]

        inputs = tokenizer(batch, return_tensors="pt", padding=True, truncation=True, max_length=512)
        max_output_length = get_max_output_length(inputs)
        with torch.no_grad():
            translated_ids = model.generate(
                **inputs,
                max_length=max_output_length,
                do_sample=True,
                num_beams=5,
                no_repeat_ngram_size=3,
                early_stopping=True,
                repetition_penalty=2.0,
                length_penalty=1.1,
                temperature=0.1,
                top_k=25,
                pad_token_id=tokenizer.pad_token_id
            )

        translated_texts = tokenizer.batch_decode(translated_ids, skip_special_tokens=True)
        translated_chunks.extend(translated_texts)

    return " ".join(translated_chunks)


def do_translations(table: str, conn, batch_size: int = 8):
    conn.execute(f"update {table} set translated_text = text where detected_language = 'en';")
    conn.commit()
    for lang in conn.execute(f"""select detected_language from {table} where detected_language is not null and detected_language != 'en' and translated_text is null group by detected_language having count(*) > 0 order by detected_language asc;""").fetchall():
        try:
            print(f"processing language {lang[0]}")
            model_name = f"Helsinki-NLP/opus-mt-{lang[0]}-en"
            print(f"instantiating model {model_name}")
            tokenizer = MarianTokenizer.from_pretrained(model_name)
            model = MarianMTModel.from_pretrained(model_name)
            print(f"model {model_name} instantiated")
            for post in conn.execute(f"""select id, text from {table} where detected_language = ? and translated_text is null and text is not null order by id asc;""", (lang[0],)).fetchall():
                text = re.sub(r'\s+', ' ', post[1])
                translation = translate_text_batch(text, tokenizer, model, batch_size)
                print(f"translation for {post[0]} done: {translation[:100]}")
                conn.execute(f"""update {table} set translated_text = ? where id = ?""", (translation, post[0]))
                conn.commit()
        except Exception as e:
            print(f"could not translate {lang[0]}, error was {e}")



def summarize(table: str, conn, ckbert: ChunkeyBert):
    unwanted_keywords = {}
    max_number_of_keywords = 5
    i = 1
    for translation in conn.execute(f"""select id, translated_text from {table} where translated_text is not null and keywords is null order by id asc;""").fetchall():
        try:
            print(f"try to summarize post {i} with id {translation[0]} and text {translation[1]}")
            result = ckbert.extract_keywords(
                docs=translation[1], num_keywords=10, chunker=chunker_llama_index, vectorizer=keyphrase_vectorizer,
                nr_candidates=10, top_n=3
            )

            if (len(result) > 0):
                first_result = result[0]
                keywords_set = set()
                keywords = []
                keywords_used = 0
                for keyword, score in first_result:
                    keyword = str(keyword).strip().lower()
                    score = float(score)
                    if (keyword not in unwanted_keywords) and (keywords_used < max_number_of_keywords) and (
                            keyword not in keywords_set):
                        keywords_set.add(keyword)
                        keywords.append(f"{keyword} ({score:.2f})")
                        keywords_used = keywords_used + 1
            keyword_str = " - ".join(keywords)
            conn.execute(f"""update {table} set keywords = ? where id = ?""", (keyword_str, translation[0]))
            conn.commit()
        except Exception as e:
            print(f"could not summarize {translation[0]}, error was {e}")
        i = i + 1
    print(f"done summarizing {table}")



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : translations_reddit.py <reddit-db.sqlite>")
    translations_reddit(sys.argv[1])
