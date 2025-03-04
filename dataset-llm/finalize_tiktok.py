import sys
import duckdb

# create table existing_judgments as (select * from read_xlsx('../database/tiktok_russia_ukraine_max_1_min.xlsx'));
# update video_hits h set disinformation = (select e.disinformation from existing_judgments e where e.id = h.id);
# update video_hits h set relevant = (select e.relevant from existing_judgments e where e.id = h.id);
# drop table existing_judgments;

def table_exists(conn, table_name) -> bool:
    result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()
    return result is not None

def finalize_tiktok(tiktok_db: str):
    print(f"processing input {tiktok_db}")

    # Connect to DuckDB
    with duckdb.connect() as conn:
        conn.execute("install excel;")
        conn.execute("load excel;")
        conn.execute(f"""ATTACH '{tiktok_db}' as tiktok (TYPE sqlite);""")
        for table, filename in zip(["videos_2nd_stage_optie_a", "videos_2nd_stage_optie_b", "long_videos_2nd_stage_optie_a", "long_videos_2nd_stage_optie_b"],
                         ["tiktok_russia_ukraine_max_1_min", "tiktok_more_wars_max_1_min", "tiktok_russia_ukraine_max_2_min", "tiktok_more_wars_max_2_min"]):
            print(f"processing table {table} and file {filename}")
            if table_exists(conn, table):
                print(f"table {table} exists, will export now")
                conn.execute(f"""copy({generate_query(table)}) to '{filename}.xlsx' with (format xlsx, header true);""")
                conn.execute(f"""attach '{filename}.sqlite' as {filename} (TYPE sqlite);""")
                conn.execute(f"""create table {filename}.video_hits as ({generate_query(table)});""")
            else:
                print(f"table {table} does not exist, skipping")


def generate_query(table: str) -> str:
    return f"""
                select row_number() over() as number, 
                       id::text as id, 
                       video_description as description, 
                       'https://www.tiktok.com/@' || user_name || '/video/' || id as url, 
                       create_time, 
                       video_duration, 
                       comment_count, 
                       transcription, 
                       translated_text, 
                       keywords,
                       '' as relevant,
                       '' as disinformation
            from tiktok.{table}
            """



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : finalize_tiktok.py <tiktok-db.sqlite>")
    finalize_tiktok(sys.argv[1])
