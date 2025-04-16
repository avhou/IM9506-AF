import sqlite3
import json
import sys
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import re
import os


class RedditEntry(BaseModel):
    url: str
    id: str
    text: str
    comments: List['RedditEntry']

    def to_str(self, level: int = 0) -> str:
        indent = ' ' * (level * 2)
        result = f"{indent}{clean_text(self.text)}{os.linesep}"
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
        for row in conn.execute(f"select distinct json_extract(metadata, '$.id'), json_extract(metadata, '$.subreddit'), url from articles where source = 'reddit' and disinformation = 'y'"):
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

    with sqlite3.connect(input_db) as conn_input:
        with sqlite3.connect(output_db) as conn_output:
            conn_output.execute(f"create table if not exists articles_reddit(source text, url text, timestamp text, metadata text, detected_language text, text text, translated_text text, keywords text, relevant text, disinformation text)")
            for subreddit, entries in data_per_subreddit.items():
                for entry in entries:
                    print(f"processing subreddit {subreddit}, entry {entry.url}")
                    reddit_entry = flat_entry_to_reddit_entry(entry)
                    # print(RedditEntry.model_dump_json(reddit_entry, indent=2))
                    print(f"====== nr of flat comments {0 if entry.comments is None else len(entry.comments)} nr of comments {reddit_entry.number_of_comments()} ======")
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
                    print(f"processing subreddit {subreddit}, entry {entry.url}, nr of threads {len(reddit_entry.comments)}")
                    for thread in reddit_entry.comments:
                        thread_text = str(thread)
                        if "[removed]" not in thread_text and "[deleted]" not in thread_text:
                            thread_number = thread_number + 1
                            # print(f"must process thread {thread}")
                            conn_output.execute(f"insert into articles_reddit(source, url, timestamp, metadata, detected_language, text, translated_text, keywords, relevant, disinformation) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (input_source, f"{input_url}-thread-{thread_number}", input_timestamp, input_metadata, input_detected_language, thread_text, thread_text, input_keywords, input_relevant, ""))
                            conn_output.commit()




if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : reddit-comment-extractor.py <reddit_folder> <input_db> <output_db>")
    process_reddit(sys.argv[1], sys.argv[2], sys.argv[3])
