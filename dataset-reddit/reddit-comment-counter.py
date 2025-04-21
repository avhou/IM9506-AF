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

def process_reddit(input_folder: str):
    entries: List[FlatEntry] = []
    pattern = r"^reddit-(?P<subreddit>[^-]+)-(?P<keyword>[^-]+)\.json$"

    seen = set()
    for reddit_file in Path(input_folder).rglob("reddit-*.json"):
        match = re.match(pattern, reddit_file.name)
        if match:
            subreddit = match.group("subreddit")
            keyword = match.group("keyword")
            print(f"processing file {reddit_file} for subreddit {subreddit} and keyword {keyword}")
            with open(reddit_file, "r") as f:
                data = json.load(f)
                for id, value in data.items():
                    if id not in seen:
                        seen.add(id)
                        # print(f"adding entry {id}")
                        value["url"] = ''
                        if "name" not in value:
                            value["name"] = value["id"]
                        entry = FlatEntry(**value)
                        entries.append(entry)

    total = 0
    for entry in entries:
        reddit_entry = flat_entry_to_reddit_entry(entry)
        total += 1 + len(reddit_entry.comments)
    print(f"{total} entries processed")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : reddit-comment-extractor.py <reddit_folder>")
    process_reddit(sys.argv[1])
