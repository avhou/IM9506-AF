import sys

import duckdb

def create_tables(conn):
    print(f"create table optie a")
    conn.execute(f"""create table if not exists reddit.text_2nd_stage_optie_a as select * from reddit.reddit_hits where regexp_matches(text, '(?i)(russia|ukraine|russie|rusland|oekraine|oekraïne)');""")
    conn.execute(f"""alter table reddit.text_2nd_stage_optie_a add column if not exists transcription text;""")
    print(f"create table optie b")
    conn.execute(f"""create table if not exists reddit.text_2nd_stage_optie_b as select * from reddit.reddit_hits where regexp_matches(text, '(?i)(russia|ukraine|russie|rusland|oekraine|oekraïne|syria|syrie|syrië|israel|israël|palestine|palestina)');""")
    conn.execute(f"""alter table reddit.text_2nd_stage_optie_b add column if not exists transcription text;""")

def stage_2_reddit(reddit_db: str):
    print(f"processing input {reddit_db}")

    # Connect to DuckDB
    conn = duckdb.connect()
    conn.execute(f"""ATTACH '{reddit_db}' as reddit (TYPE sqlite);""")

    create_tables(conn)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : 2nd_stage_reddit.py <reddit-db.sqlite>")
    stage_2_reddit(sys.argv[1])
