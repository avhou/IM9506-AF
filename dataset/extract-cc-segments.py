import sys

import duckdb

def extract_cc_segments(input_dir: str):
    print(f"processing input dir {input_dir}")

    # Connect to DuckDB
    conn = duckdb.connect()

    conn.execute(f"""create table index_matches as select * from read_json_auto('{input_dir}/*', format='newline_delimited');""")
    print(conn.execute(f"""select count(distinct filename) from index_matches where url !~ '.*robots\\.txt.*'""").fetchone()[0])
    conn.execute(f"""copy (select distinct filename from index_matches where url !~ '.*robots\\.txt.*') TO 'output.csv';""")



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : extract-cc-segments.sh <input-dir>")
    extract_cc_segments(sys.argv[1])
