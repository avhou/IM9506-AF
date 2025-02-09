import sys

import duckdb
import sqlite3

def delete_superfluous_urls(target_db: str):
    print(f"processing {target_db}")

    conn = duckdb.connect()
    conn.execute(f"""ATTACH '{target_db}' as target_db (TYPE sqlite);""")
    print(f"creating the table")
    conn.execute(f"""create table urls as select url, urlp1, urlp2, urlp3 from target_db.download_progress;""")
    count_before = conn.execute(f"""select count(*) from urls""").fetchone()[0]
    with open("exclude-uri-parts1.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""processing urlp1 = '{line}';""")
            conn.execute(f"""delete from urls where urlp1 = '{line}';""")
    with open("exclude-uri-parts2.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""processing urlp2 = '{line}';""")
            conn.execute(f"""delete from urls where urlp2 = '{line}';""")
    with open("exclude-uri-parts3.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""processing urlp3 = '{line}';""")
            conn.execute(f"""delete from urls where urlp3 = '{line}';""")
    with open("exclude-uri-regex.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""processing regex = '{line}';""")
            conn.execute(f"""delete from urls where url ~ '{line}';""")
    conn.execute(f"""copy (select url, urlp1, urlp2, urlp3 from urls order by url asc) TO 'na_deletes.csv';""")
    count_after = conn.execute(f"""select count(*) from urls""").fetchone()[0]
    print(f"count before : {count_before}, count after : {count_after}, delta : {count_before - count_after}")
    print(f"deleting superfluous rows")
    conn.execute(f"""delete from target_db.download_progress where url not in (select url from urls);""")
    print(f"vacuum")
    print(f"vacuum")
    sqlite_conn = sqlite3.connect(target_db)
    sqlite_conn.execute("vacuum;")
    sqlite_conn.commit()
    sqlite_conn.close()




if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : delete_superfluous_urls.sh target-db")
    delete_superfluous_urls(sys.argv[1])
