import sys

import duckdb
import os

def remove_snapshot_hits(snapshot_db: str, target_db: str):
    print(f"processing snapshot db {snapshot_db} in {target_db}")

    conn = duckdb.connect()
    conn.execute(f"""ATTACH '{snapshot_db}' as snapshot_db (TYPE sqlite);""")
    conn.execute(f"""ATTACH '{target_db}' as target_db (TYPE sqlite);""")
    conn.execute(f"""create table target_db.delta_urls as select distinct url from snapshot_db.download_progress;""")
    alle_hits = conn.execute(f"""select count(*) from target_db.download_progress t;""")
    print(f"totaal aantal hits zonder delta berekening: {alle_hits.fetchone()[0]}")
    result = conn.execute(f"""select count(*) from target_db.download_progress t where t.url not in (select url from target_db.delta_urls);""")
    print(f"totaal aantal hits enkel delta: {result.fetchone()[0]}")
    conn.execute(f"""delete from target_db.download_progress t where t.url in (select url from target_db.delta_urls);""")
    result_na_delta = conn.execute(f"""select count(*) from target_db.download_progress t where t.url not in (select url from target_db.delta_urls);""")
    print(f"totaal aantal hits na delta: {result_na_delta.fetchone()[0]}")



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : remove-snapshot-hits.sh snapshot-db target-db")
    remove_snapshot_hits(sys.argv[1], sys.argv[2])
