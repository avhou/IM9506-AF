import sqlite3
import json
import sys
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import re
import os
from googletrans import Translator
from llama_index.core.node_parser import SentenceSplitter

import wget
import fasttext
import asyncio


class RedditEntry(BaseModel):
    url: str
    id: str
    text: str
    comments: List['RedditEntry']

    def to_str(self, level: int = 0) -> str:
        indent = ' ' * (level * 2)
        cleaned_text = clean_text(self.text)
        if "[removed]" not in cleaned_text and "[deleted]" not in cleaned_text:
            result = f"{indent}{cleaned_text}{os.linesep}"
        else:
            result = ""
        for comment in self.comments:
            result += comment.to_str(level + 1)
        return result

    def __str__(self):
        return self.to_str(level=0)

    def number_of_comments(self) -> int:
        count = 0
        for comment in self.comments:
            count += 1 + comment.number_of_comments()
        return count

    def id_set(self) -> set:
        ids = {self.id}
        for comment in self.comments:
            ids.update(comment.id_set())
        return ids

class FlatCommentEntry(BaseModel):
    id: str
    name: Optional[str] = Field(default=None)
    body: str
    parent_id: Optional[str | int] = Field(default=None)

class FlatEntry(BaseModel):
    url: str
    id: str
    name: str
    title: str
    selftext: Optional[str] = Field(default=None)
    comments: Optional[List[FlatCommentEntry]] = Field(default=None)

    # omzetten van de commentaren naar een map van parent_id naar text
    def comments_to_map(self) -> Dict[str, List[FlatCommentEntry]]:
        data = {}
        for comment in self.comments:
            if comment.parent_id is not None and isinstance(comment.parent_id, str) :
                if comment.parent_id not in data:
                    data[comment.parent_id] = []
                data[comment.parent_id].append(comment)
        return data

    def id_set(self) -> set:
        ids = {self.name}
        for comment in self.comments:
            ids.add(comment.id)
        return ids


def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text)

def find_children_of(parent_id: str, comment_map: Dict[str, List[FlatCommentEntry]]) -> List[RedditEntry]:
    children = []
    for child_flat_comment in comment_map.get(parent_id, []):
        children.append(flat_comment_entry_to_reddit_entry(child_flat_comment, comment_map))
    return children


def flat_comment_entry_to_reddit_entry(flat_comment_entry: FlatCommentEntry, comment_map: Dict[str, List[FlatCommentEntry]]) -> RedditEntry:
    id = flat_comment_entry.id
    name = flat_comment_entry.name
    text = clean_text(flat_comment_entry.body)
    comments = []
    if name is not None:
        comments = find_children_of(name, comment_map)
    return RedditEntry(id=id, text=text, comments=comments, url="")


def flat_entry_to_reddit_entry(flat_entry: FlatEntry) -> RedditEntry:
    id = flat_entry.id
    text = flat_entry.title if flat_entry.selftext is None else f"{clean_text(flat_entry.title)}. {clean_text(flat_entry.selftext)}"
    comment_map = flat_entry.comments_to_map()
    comments = find_children_of(flat_entry.name, comment_map)
    reddit_id_set = RedditEntry(id=id, text=text, comments=comments, url=flat_entry.url).id_set()
    names_in_flat_comments = set(str(c.name) for c in flat_entry.comments if c.name is not None and isinstance(c.name, str))

    # voor diegenen die nog niet zijn omgezet, maar die wel in de flat_id_set zitten en die niet de flat_entry.id zijn
    # gaan we proberen toch nog kettingen te vinden.   dit doen we door naar onbestaande parent_ids te kijken (het zijn t1-ids, want comments)
    extra_comments = []
    for flat_entry_comment in flat_entry.comments:
        if flat_entry_comment.id not in reddit_id_set:
            if flat_entry_comment.parent_id not in names_in_flat_comments:
                extra_comments.append(flat_comment_entry_to_reddit_entry(flat_entry_comment, comment_map))

    comments.extend(extra_comments)
    return RedditEntry(id=id, text=text, comments=comments, url=flat_entry.url)

def process_reddit(input_folder: str, input_db: str, output_db: str):
    data_per_subreddit: Dict[str, List[FlatEntry]] = {}
    pattern = r"^reddit-(?P<subreddit>[^-]+)-(?P<keyword>[^-]+)\.json$"

    # de ids die we willen bekijken
    ids = set()
    id_to_url_map = {}
    subreddits = set()

    print(f"getting unique ids and subreddits")
    with sqlite3.connect(input_db) as conn:
        for row in conn.execute(f"select distinct json_extract(metadata, '$.id'), json_extract(metadata, '$.subreddit'), url from articles where source = 'reddit'"):
            id = row[0]
            ids.add(id)
            subreddit = row[1]
            url = row[2]
            id_to_url_map[id] = url

            subreddits.add(subreddit)
            if subreddit not in data_per_subreddit:
                data_per_subreddit[subreddit] = []

    print(f"found {len(ids)} unique ids and {len(subreddits)} unique subreddits")


    seen = set()
    for reddit_file in Path(input_folder).rglob("reddit-*.json"):
        match = re.match(pattern, reddit_file.name)
        if match:
            subreddit = match.group("subreddit")
            keyword = match.group("keyword")
            if subreddit in subreddits:
                print(f"processing file {reddit_file} for subreddit {subreddit} and keyword {keyword}")
                with open(reddit_file, "r") as f:
                    data = json.load(f)
                    for id, value in data.items():
                        if id in ids and id not in seen:
                            seen.add(id)
                            # print(f"adding entry {id}")
                            print(f"adding url {id_to_url_map[id]} for id {id}")
                            value["url"] = id_to_url_map[id]
                            entry = FlatEntry(**value)
                            data_per_subreddit[subreddit].append(entry)

    if not os.path.exists("lid.176.bin"):
        print("downloading fasttext model")
        wget.download("https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin", "lid.176.bin")

    model = fasttext.load_model("lid.176.bin")
    translator = Translator()
    with sqlite3.connect(input_db) as conn_input:
        with sqlite3.connect(output_db) as conn_output:
            conn_output.execute(f"create table if not exists articles_reddit(source text, url text, timestamp text, metadata text, detected_language text, text text, translated_text text, keywords text, relevant text, disinformation text)")
            for subreddit, entries in data_per_subreddit.items():
                for entry in entries:
                    print(f"processing subreddit {subreddit}, entry {entry.url}")
                    reddit_entry = flat_entry_to_reddit_entry(entry)
                    print(f"====== nr of flat comments {0 if entry.comments is None else len(entry.comments)} nr of comments {reddit_entry.number_of_comments()} ======")
                    process_reddit_entry(reddit_entry, conn_input, conn_output, model, translator)


def detect_language(text, model):
    prediction = model.predict(text, k=1)  # k=1 means top-1 prediction
    lang_code = prediction[0][0].replace('__label__', '')  # Extract language code
    confidence = prediction[1][0]  # Confidence score
    return lang_code, confidence


# Configurable limits
MAX_WORDS = 500  # Chunk size in words
OVERLAP = 0  # Overlapping words to maintain context
BATCH_SIZE = 10  # Rows to process in one batch
MAX_CONCURRENT_REQUESTS = 1  # Limit simultaneous translations

# Initialize the LlamaIndex Sentence Splitter
splitter = SentenceSplitter(chunk_size=MAX_WORDS, chunk_overlap=OVERLAP)

async def translate_text(translator: Translator, text: str, lang: str):
    """Translates text using LlamaIndex chunking for large inputs."""
    try:
        # Split text into chunks
        chunks = splitter.split_text(text)

        # Translate each chunk separately
        translated_chunks = []
        for chunk in chunks:
            print(f"translating chunk: {chunk[:100]}")
            translated = await translator.translate(chunk, src=lang, dest='en')
            translated_chunks.append(translated.text)

        return " ".join(translated_chunks)  # Rejoin translated parts
    except Exception as e:
        return f"ERROR: {e}"

def run_async_task(coroutine):
    """Runs an async task safely, avoiding 'event loop is closed' errors."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:  # No event loop exists
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coroutine)

def process_reddit_entry(reddit_entry: RedditEntry, conn_input: sqlite3.Connection, conn_output: sqlite3.Connection, model, translator: Translator):
    input_row = conn_input.execute(f"select source, url, timestamp, metadata, detected_language, text, translated_text, keywords, relevant, disinformation from articles where url = ?", (reddit_entry.url, )).fetchone()
    input_source = input_row[0]
    input_url = input_row[1]
    input_timestamp = input_row[2]
    input_metadata = input_row[3]
    input_detected_language = input_row[4]
    input_text = input_row[5]
    input_translated_text = input_row[6]
    input_keywords = input_row[7]
    input_relevant = input_row[8]
    input_disinformation = input_row[9]
    thread_number = 0
    print(f"processing entry {reddit_entry.url}, nr of threads {len(reddit_entry.comments)}")
    for thread_text in [reddit_entry.text] + [str(c) for c in reddit_entry.comments]:
        lang_code, _ = detect_language(re.sub(r'\n', ' ', thread_text), model)
        if (lang_code != input_detected_language):
            print(f"detected language {lang_code} != {input_detected_language}")
        if (lang_code != "en"):
            print(f"will translate to english")
            translated_text = run_async_task(translate_text(translator, thread_text, lang_code))
        else:
            translated_text = thread_text

        if (len(re.sub(r'\s+', '', thread_text)) == 0):
            print(f"skipping empty thread")
            continue

        thread_number = thread_number + 1
        # print(f"must process thread {thread}")
        conn_output.execute(f"insert into articles_reddit(source, url, timestamp, metadata, detected_language, text, translated_text, keywords, relevant, disinformation) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (input_source, f"{input_url}-thread-{thread_number}", input_timestamp, input_metadata, lang_code, thread_text, translated_text, input_keywords, input_relevant, input_disinformation if input_disinformation == "n" else ""))
        conn_output.commit()


if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : reddit-comment-extractor.py <reddit_folder> <input_db> <output_db>")
    process_reddit(sys.argv[1], sys.argv[2], sys.argv[3])
