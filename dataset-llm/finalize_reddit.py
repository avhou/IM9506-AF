import sys
import duckdb

# create table existing_judgments as (select * from read_xlsx('../database/reddit_russia_ukraine_max_1_min.xlsx'));
# update video_hits h set disinformation = (select e.disinformation from existing_judgments e where e.id = h.id);
# update video_hits h set relevant = (select e.relevant from existing_judgments e where e.id = h.id);
# drop table existing_judgments;

def table_exists(conn, table_name) -> bool:
    result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()
    return result is not None

def finalize_reddit(reddit_db: str):
    print(f"processing input {reddit_db}")

    # Connect to DuckDB
    with duckdb.connect() as conn:
        conn.execute("install excel;")
        conn.execute("load excel;")
        conn.execute(f"""ATTACH '{reddit_db}' as reddit (TYPE sqlite);""")
        for table, filename in zip(["text_2nd_stage_optie_a", "text_2nd_stage_optie_b"],
                         ["reddit_russia_ukraine", "reddit_all_conflicts"]):
            print(f"processing table {table} and file {filename}")
            if table_exists(conn, table):
                print(f"table {table} exists, will export now")
                conn.execute(f"""copy({generate_query(table)}) to '{filename}.xlsx' with (format xlsx, header true);""")
                conn.execute(f"""attach '{filename}.sqlite' as {filename} (TYPE sqlite);""")
                conn.execute(f"""create table {filename}.reddit_hits as ({generate_query(table)});""")
            else:
                print(f"table {table} does not exist, skipping")


def generate_query(table: str) -> str:
    return f"""
                select row_number() over() as number, 
                       id::text as id, 
                       subreddit,
                       url,
                       created,
                       text,
                       translated_text, 
                       keywords,
                       '' as relevant,
                       '' as disinformation
            from reddit.{table}
            """



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : finalize_reddit.py <reddit-db.sqlite>")
    finalize_reddit(sys.argv[1])
