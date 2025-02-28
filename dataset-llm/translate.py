import torch
from transformers import MarianTokenizer, MarianMTModel
import re
import sys
import sqlite3

# duckdb :
# bash -c "cat *translated.csv > all-translations.csv"
# visidata --header 0 all-translations.csv
# dan column hiden en opnieuw opslaan als translations.csv
# create table hits_translation as select * from 'translations.csv';
# insert into hits_translation(url, translated_text) select url, content from hits where host = 'dutchnews.nl' and relevant is null;

# select count(distinct url) from hits_keywords_llama where keywords like '%ukrai%' or keywords like '%refugee%' or keywords like '%migrant%' or keywords like '%russia%' or keywords like '%diaspora%' or keywords like '%racism%' or keywords like '%identity%';

# Detect and use MPS if available
device = "mps" if torch.backends.mps.is_available() else "cpu"
print(f"Using device: {device}")

# Choose the appropriate model based on input language
def get_translation_model(source_lang):
    if source_lang == "fr":
        model_name = "Helsinki-NLP/opus-mt-fr-en"
    elif source_lang == "nl":
        model_name = "Helsinki-NLP/opus-mt-nl-en"
    else:
        raise ValueError("Unsupported language. Use 'fr' for French or 'nl' for Dutch.")

    tokenizer = MarianTokenizer.from_pretrained(model_name)
    model = MarianMTModel.from_pretrained(model_name).to(device)
    return tokenizer, model

# Chunking function to avoid truncation
def chunk_text(text, max_length=250):
    sentences = re.split(r'\.|\?|!', text)
    chunks = []
    current_chunk = ""

    for sentence in sentences:
        sentence = re.sub(r'\s+', ' ', sentence.strip()).strip()

        # If sentence is too long, split it by words
        if len(sentence) > max_length:
            words = sentence.split()
            for i in range(0, len(words), max_length):
                split_sentence = " ".join(words[i:i + max_length]) + "."
                chunks.append(split_sentence.strip())
        else:
            # Normal chunking logic
            if len(current_chunk) + len(sentence) < max_length:
                current_chunk += sentence + " "
            else:
                chunks.append(current_chunk.strip())
                current_chunk = sentence + " "

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


# Token-based chunking function with MPS support
def chunk_text_by_tokens(text, tokenizer, max_tokens=512):
    sentences = re.split(r'\.|\?|!', text)
    chunks = []
    current_chunk = []
    current_length = 0

    for sentence in sentences:
        cleaned_sentence = re.sub(r'\s+', ' ', sentence.strip())
        print(f"found sentence '{cleaned_sentence.strip()}'")
        tokenized_sentence = tokenizer(sentence, return_tensors="pt", add_special_tokens=False).input_ids.to(device)  # Move to MPS
        sentence_length = tokenized_sentence.shape[1]  # Get number of tokens

        if current_length + sentence_length > max_tokens:
            chunks.append(" ".join(current_chunk))  # Store as text, not tensors
            current_chunk = [sentence]
            current_length = sentence_length
        else:
            current_chunk.append(sentence)
            current_length += sentence_length

    if current_chunk:
        chunks.append(" ".join(current_chunk))  # Store remaining sentences

    return chunks

# Translate text chunk by chunk
def translate_text(text, tokenizer: MarianTokenizer, model: MarianMTModel) -> str:
    text_chunks = chunk_text(text)
    # text_chunks = chunk_text_by_tokens(text, tokenizer)

    translated_chunks = []
    for chunk in text_chunks:
        # print(f"translating chunk: {chunk}")
        inputs = tokenizer(chunk, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
        with torch.no_grad():
            translated_ids = model.generate(**inputs)
        translated_text = tokenizer.batch_decode(translated_ids, skip_special_tokens=True)[0]
        # print(f"chunk was translated to : {translated_text}")
        translated_chunks.append(translated_text)

    return " ".join(translated_chunks)

# with open("input_text.txt", "r") as f:
#     input_text = f.read()
#
# translated_text = translate_text(input_text, source_lang="nl")
# print("Translated Text:", translated_text)
# with open("translated_text.txt", "w") as f:
#     f.write(translated_text)
#

def translate_nl(target_db: str, limit: int, offset: int = 0):
    print(f"NL translations for {target_db}")
    MAX_WORDS = 1500
    with sqlite3.connect(target_db) as conn:
        count = conn.execute("select count(*) from hits h left outer join hits_translation t on h.url = t.url where t.url is null and h.languages = 'nld' and h.relevant is null").fetchone()[0]
        print(f"found {count} urls to translate, limit {limit} offset {offset}")
        count = min(count, limit)
        tokenizer_nl, model_nl = get_translation_model("nl")
        i = 1
        for r in conn.execute(f"select h.url, h.content from hits h left outer join hits_translation t on h.url = t.url where t.url is null and h.languages = 'nld' and h.relevant is null limit {limit} offset {offset}"):
            words = r[1].split(" ")
            nr_of_words_to_use = min(MAX_WORDS, len(words))
            print(f"translating {i}/{count}, word count {len(words)}, will use {nr_of_words_to_use}:  {r[0]}")
            translation = translate_text(" ".join(words[:nr_of_words_to_use]), tokenizer_nl, model_nl)
            conn.execute("insert into hits_translation (url, translated_text) values (?, ?)", (r[0], translation))
            conn.commit()
            print(f"translation {i}/{count} done, {nr_of_words_to_use} words in Dutch translated to {len(translation.split(' '))} words in English")
            i = i + 1

def translate_fr(target_db: str, limit: int, offset: int = 0):
    print(f"FR translations for {target_db}")
    MAX_WORDS = 1500
    with sqlite3.connect(target_db) as conn:
        count = conn.execute("select count(*) from hits h left outer join hits_translation t on h.url = t.url where t.url is null and h.languages = 'fra' and h.relevant is null").fetchone()[0]
        print(f"found {count} urls to translate, limit {limit} offset {offset}")
        count = min(count, limit)
        tokenizer_nl, model_nl = get_translation_model("fr")
        i = 1
        for r in conn.execute(f"select h.url, h.content from hits h left outer join hits_translation t on h.url = t.url where t.url is null and h.languages = 'fra' and h.relevant is null limit {limit} offset {offset}"):
            words = r[1].split(" ")
            nr_of_words_to_use = min(MAX_WORDS, len(words))
            print(f"translating {i}/{count}, word count {len(words)}, will use {nr_of_words_to_use}:  {r[0]}")
            translation = translate_text(" ".join(words[:nr_of_words_to_use]), tokenizer_nl, model_nl)
            conn.execute("insert into hits_translation (url, translated_text) values (?, ?)", (r[0], translation))
            conn.commit()
            print(f"translation {i}/{count} done, {nr_of_words_to_use} words in French translated to {len(translation.split(' '))} words in English")
            i = i + 1


def translate(target_db: str, lang: str, limit: int = 20000000, offset: int = 0):
    print(f"translating {target_db} for lang {lang}, limit {limit}, offset {offset}")
    if lang == "nl":
        translate_nl(target_db, limit, offset)
    elif lang == "fr":
        translate_fr(target_db, limit, offset)
    else:
        raise ValueError("Unsupported language. Use 'fr' for French or 'nl' for Dutch.")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : translate.py target-db [nl|fr] [limit] [offset]")
    translate(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "nl", int(sys.argv[3]) if len(sys.argv) > 3 else 20000000, int(sys.argv[4]) if len(sys.argv) > 4 else 0)
