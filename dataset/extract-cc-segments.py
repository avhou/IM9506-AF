import sys

import duckdb
import sqlite3
import os

def extract_cc_segments(input: str):
    print(f"processing input {input}")

    # Connect to DuckDB
    conn = duckdb.connect()
    sqlite_file = f"output-{input}.sqlite" if os.path.isdir(input) else f"output-{os.path.splitext(input)[0]}.sqlite"

    print(f"read json file(s)")
    if os.path.isdir(input):
        conn.execute(f"""create table index_matches as select * from read_json_auto('{input}/*', format='newline_delimited');""")
    else:
        conn.execute(f"""create table index_matches as select * from read_json_auto('{input}', format='newline_delimited');""")
    print(f"remove diagnostics hits")
    conn.execute(f"""delete from index_matches where filename ilike '%crawldiagnostics%';""")
    print(f"keep only top level domains in url")
    conn.execute(f"""delete from index_matches \
                            where url !~ '.*npo\\.nl.*' \
                              and url !~ '.*nos\\.nl.*' \
                              and url !~ '.*rtl\\.nl.*' \
                              and url !~ '.*hartvannederland\\.nl.*' \
                              and url !~ '.*volkskrant\\.nl.*' \
                              and url !~ '.*telegraaf\\.nl.*' \
                              and url !~ '.*nrc\\.nl.*' \
                              and url !~ '.*trouw\\.nl.*' \
                              and url !~ '.*ad\\.nl.*' \
                              and url !~ '.*dutchnews\\.nl.*' \
                              and url !~ '.*nltimes\\.nl.*' \
                              and url !~ '.*pvv\\.nl.*' \
                              and url !~ '.*fvd\\.nl.*' \
                              and url !~ '.*groenlinks\\.nl.*' \
                              and url !~ '.*pvda\\.nl.*' \
                              and url !~ '.*vvd\\.nl.*' \
                              and url !~ '.*partijnieuwsociaalcontract\\.nl.*' \
                              and url !~ '.*partijvoordedieren\\.nl.*' \
                              and url !~ '.*d66\\.nl.*' \
                              and url !~ '.*cda\\.nl.*' \
                              and url !~ '.*boerburgerbeweging\\.nl.*' \
                              and url !~ '.*sp\\.nl.*' \
                              and url !~ '.*voltnederland\\.org.*' \
                              and url !~ '.*ja21\\.nl.*' \
                              and url !~ '.*bewegingdenk\\.nl.*' \
                              and url !~ '.*hbvl\\.be.*' \
                              and url !~ '.*gva\\.be.*' \
                              and url !~ '.*nieuwsblad\\.be.*' \
                              and url !~ '.*hln\\.be.*' \
                              and url !~ '.*demorgen\\.be.*' \
                              and url !~ '.*standaard\\.be.*' \
                              and url !~ '.*tijd\\.be.*' \
                              and url !~ '.*lavenir\\.net.*' \
                              and url !~ '.*sudinfo\\.be.*' \
                              and url !~ '.*dhnet\\.be.*' \
                              and url !~ '.*echo\\.be.*' \
                              and url !~ '.*lalibre\\.be.*' \
                              and url !~ '.*lesoir\\.be.*' \
                              and url !~ '.*pvda\\.be.*' \
                              and url !~ '.*ps\\.be.*' \
                              and url !~ '.*vooruit\\.org.*' \
                              and url !~ '.*ecolo\\.be.*' \
                              and url !~ '.*groen\\.be.*' \
                              and url !~ '.*fouadahidar\\.com.*' \
                              and url !~ '.*defi\\.be.*' \
                              and url !~ '.*lesengages\\.be.*' \
                              and url !~ '.*cdenv\\.be.*' \
                              and url !~ '.*openvld\\.be.*' \
                              and url !~ '.*mr\\.be.*' \
                              and url !~ '.*n-va\\.be.*' \
                              and url !~ '.*vlaamsbelang\\.org.*' 
                            ;""")
    conn.execute(f"""delete from index_matches where url ~ 'tv-gids';""")
    conn.execute(f"""delete from index_matches where url ~ 'offre-start';""")
    conn.execute(f"""delete from index_matches where url ~ 'offre-decouverte';""")
    conn.execute(f"""delete from index_matches where url ~ 'vacature';""")
    print(f"generate uri parts")
    conn.execute(f"""alter table index_matches add column urlp1 text;""")
    conn.execute(f"""alter table index_matches add column urlp2 text;""")
    conn.execute(f"""alter table index_matches add column urlp3 text;""")
    conn.execute(f"""update index_matches set urlp1 = regexp_extract(url, '^https?://[^/]+/');""")
    conn.execute(f"""update index_matches set urlp2 = regexp_extract(url, '^https?://[^/]+/[^/]+/?');""")
    conn.execute(f"""update index_matches set urlp3 = regexp_extract(url, '^https?://[^/]+/[^/]+/[^/]+/?');""")
    with open("exclude-uri-parts1.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""delete from index_matches where urlp1 = '{line}';""")
            conn.execute(f"""delete from index_matches where urlp1 = '{line}';""")
    with open("exclude-uri-parts2.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""delete from index_matches where urlp2 = '{line}';""")
            conn.execute(f"""delete from index_matches where urlp2 = '{line}';""")
    with open("exclude-uri-parts3.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""delete from index_matches where urlp3 = '{line}';""")
            conn.execute(f"""delete from index_matches where urlp3 = '{line}';""")
    with open("exclude-uri-regex.txt", "r") as f:
        lines = [line.strip() for line in f]
        for line in lines:
            print(f"""delete from index_matches where url ~ '{line}';""")
            conn.execute(f"""delete from index_matches where url ~ '{line}';""")
    conn.execute(f"""create table urlp1_counts as select urlp1, count(*) from index_matches group by urlp1 order by 2 desc;""")
    conn.execute(f"""create table urlp2_counts as select urlp2, count(*) from index_matches group by urlp2 order by 2 desc;""")
    conn.execute(f"""create table urlp3_counts as select urlp3, count(*) from index_matches group by urlp3 order by 2 desc;""")
    print(f"add range and data_url")
    conn.execute(f"""alter table index_matches add column "range" text;""")
    conn.execute(f"""update index_matches set "range" = "offset" || '-' || ("offset"::int + "length"::int - 1)::text;""")
    conn.execute(f"""alter table index_matches add column data_url text;""")
    conn.execute(f"""update index_matches set data_url = 'https://data.commoncrawl.org/' || filename;""")
    print(f"keep max timestamp")
    conn.execute(f"""create table max_timestamp_per_url as select url, max(timestamp) as timestamp from index_matches group by url;""")
    print(f"add download indicators")
    conn.execute(f"""create table download_progress as select m.* from index_matches m, max_timestamp_per_url mm where m.url = mm.url and m.timestamp = mm.timestamp;""")
    conn.execute(f"""alter table download_progress add column downloaded boolean;""")
    conn.execute(f"""update download_progress set downloaded = false;""")
    print(f"export to sqlite")
    conn.execute(f"""ATTACH '{sqlite_file}' as sqlite_output (TYPE sqlite);""")
    conn.execute(f"""drop table if exists sqlite_output.download_progress;""")
    conn.execute(f"""drop table if exists sqlite_output.urlp1_counts;""")
    conn.execute(f"""drop table if exists sqlite_output.urlp2_counts;""")
    conn.execute(f"""drop table if exists sqlite_output.urlp3_counts;""")
    conn.execute(f"""create table sqlite_output.download_progress as select * from download_progress;""")
    conn.execute(f"""create table sqlite_output.urlp1_counts as select * from urlp1_counts;""")
    conn.execute(f"""create table sqlite_output.urlp2_counts as select * from urlp2_counts;""")
    conn.execute(f"""create table sqlite_output.urlp3_counts as select * from urlp3_counts;""")
    conn.execute(f"""alter table sqlite_output.download_progress add column downloaded_content BYTE;""")
    conn.execute(f"""alter table sqlite_output.download_progress add column time_downloaded TEXT;""")
    # print(conn.execute(f"""select count(distinct filename) from index_matches where url !~ '.*robots\\.txt.*'""").fetchone()[0])
    # conn.execute(f"""copy (select distinct filename from index_matches where url !~ '.*robots\\.txt.*') TO 'output.csv';""")

    print(f"create index")
    # Open the SQLite database
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_conn.execute("CREATE INDEX url_and_range ON download_progress(data_url, range)")
    sqlite_conn.commit()
    sqlite_conn.close()



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : extract-cc-segments.sh <input-dir>")
    extract_cc_segments(sys.argv[1])
