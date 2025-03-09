import sys
import duckdb

# create table existing_judgments as (select * from read_xlsx('../database/outlets_russia_ukraine.xlsx'));
# update outlet_hits h set disinformation = (select case when trim(e.disinformation) = 'y' then 1 when trim(e.disinformation) = 'n' then 0 else null end as disinformation from existing_judgments e where e.url = h.url);
# update outlet_hits h set relevant = (select case when trim(e.relevant) = 'y' then 1 when trim(e.relevant) = 'n' then 0 else null end as relevant from existing_judgments e where e.url = h.url);
# drop table existing_judgments;
def table_exists(conn, table_name) -> bool:
    result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()
    return result is not None

def finalize_outlets(outlet_db: str, keep_first: int = 4000):
    print(f"processing input {outlet_db}")

    # Connect to DuckDB
    with duckdb.connect() as conn:
        conn.execute("install excel;")
        conn.execute("load excel;")
        conn.execute(f"""ATTACH '{outlet_db}' as outlet (TYPE sqlite);""")
        for pattern, filename_prefix in zip(["(russia|ukraine)", "(russia|ukraine|syria|israel|palestine)"], ["outlets_russia_ukraine", "outlets_all_conflicts"]):
            print(f"processing pattern {pattern} and filename prefix {filename_prefix}")
            conn.execute(f""" copy ({generate_query(pattern)}) to '{filename_prefix}.xlsx' with (format xlsx, header true);""")
            conn.execute(f"""detach database if exists output;""")
            conn.execute(f"""ATTACH '{filename_prefix}.sqlite' as output (TYPE sqlite);""")
            conn.execute(f""" drop table if exists output.outlet_hits;""")
            conn.execute(f""" create table output.outlet_hits as ({generate_query(pattern)});""")

def generate_query(pattern: str, keep_first: int = 4000) -> str:
    return f"""
            select row_number() over(order by h.url asc) as number, 
                   h.url, 
                   h.host, 
                   h.timestamp, 
                   h.political_party,
                   (round(h.link_percentage, 2) * 100)::integer as link_percentage, 
                   h.total_nr_hits, 
                   h.distinct_nr_hits, 
                   {generate_truncate_to("h.content", keep_first)} as content, 
                   {generate_truncate_to("t.translated_text", keep_first)} as translated_text, 
                   ll.keywords as keywords, 
                   null as relevant, 
                   null as disinformation 
            from outlet.hits h, outlet.hits_translation t, outlet.hits_keywords_llama ll 
            where h.url = t.url 
              and t.url = ll.url 
              and regexp_matches(ll.keywords, '{pattern}', 'i') 
              and h.relevant is null
    """

def generate_truncate_to(column_name: str, keep_first: int) -> str:
    return f"""
    array_to_string(
        array_slice(regexp_split_to_array(regexp_replace(regexp_replace({column_name}, '[\n\r]', ' ', 'g'), '\s+', ' ', 'g'), '\s+'), 1, {keep_first}),
        ' ' ) 
    """


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : finalize_outlets.py <hits-db.sqlite>")
    finalize_outlets(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 4000)
