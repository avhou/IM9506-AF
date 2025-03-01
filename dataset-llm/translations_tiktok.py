import os.path
import sys

import duckdb
import fasttext
import wget
from transformers import MarianMTModel, MarianTokenizer
from llama_index.core.node_parser import SentenceSplitter
import torch

def translations_tiktok(tiktok_db: str):
    print(f"processing input {tiktok_db}")

    # Connect to DuckDB
    conn = duckdb.connect()
    conn.execute(f"""ATTACH '{tiktok_db}' as tiktok (TYPE sqlite);""")
    try:
        conn.execute(f"""alter table tiktok.videos_2nd_stage_optie_a add column detected_language text;""")
        conn.execute(f"""alter table tiktok.videos_2nd_stage_optie_b add column detected_language text;""")
    except:
        print(f"columns detected_language already exist")
    try:
        conn.execute(f"""alter table tiktok.videos_2nd_stage_optie_a add column translated_text text;""")
        conn.execute(f"""alter table tiktok.videos_2nd_stage_optie_b add column translated_text text;""")
    except:
        print(f"columns detected_language already exist")

    if not os.path.exists("lid.176.bin"):
        print("downloading fasttext model")
        wget.download("https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin", "lid.176.bin")

    model = fasttext.load_model("lid.176.bin")
    detect_languages("tiktok.videos_2nd_stage_optie_a", conn, model)
    detect_languages("tiktok.videos_2nd_stage_optie_b", conn, model)
    do_translations("tiktok.videos_2nd_stage_optie_a", conn)
    do_translations("tiktok.videos_2nd_stage_optie_b", conn)

def detect_language(text, model):
    prediction = model.predict(text, k=1)  # k=1 means top-1 prediction
    lang_code = prediction[0][0].replace('__label__', '')  # Extract language code
    confidence = prediction[1][0]  # Confidence score
    return lang_code, confidence

def detect_languages(table: str, conn, model):
    print(f"processing table {table}")
    for r in conn.execute(f"select id, transcription from {table} where detected_language is null and transcription is not null").fetchall():
        id = r[0]
        transcription = r[1]
        try:
            lang_code, confidence = detect_language(transcription, model)
            print(f"detected language for {id} is {lang_code} with confidence {confidence}")
            conn.execute(f"update {table} set detected_language = ? where id = ?", (lang_code, id))
        except:
            print(f"could not detect language for id {id} and text {transcription}")

def chunk_text(text, max_words=250):
    splitter = SentenceSplitter(chunk_size=max_words, chunk_overlap=25)
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
    conn.execute(f"""update table {table} set translated_text = transcription where detected_language is 'en';""")
    for lang in conn.execute(f"""select distinct detected_language from {table} where detected_language is not null and detected_language != 'en';""").fetchall():
        print(f"processing language {lang[0]}")
        model_name = f"Helsinki-NLP/opus-mt-{lang[0]}-en"
        print(f"instantiating model {model_name}")
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)
        print(f"model {model_name} instantiated")
        for video in conn.execute(f"""select id, transcription from {table} where detected_language = ? and translated_text is null and transcription is not null;""", (lang[0],)).fetchall():
            translation = translate_text_batch(video[1], tokenizer, model, batch_size)
            print(f"translation for {video[0]} done: {translation}")
            conn.execute(f"""update {table} set translated_text = ? where id = ?""", (translation, video[0]))



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : translations_tiktok.py <tiktok-db.sqlite>")
    translations_tiktok(sys.argv[1])
