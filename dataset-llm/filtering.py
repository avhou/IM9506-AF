import sys

import sqlite3

def prepare_filtering(target_db: str):
    print(f"processing {target_db}")

    sqlite_conn = sqlite3.connect(target_db)

    sqlite_conn.execute("create table if not exists hits_translation(url text, translated_text text);")
    sqlite_conn.commit()
    print(f"marking hits as irrelevant")
    sqlite_conn.execute(f"""update hits set relevant = 0 where total_nr_hits < 3 or link_percentage >= 0.7;""")
    sqlite_conn.commit()

    print(f"aanmaken van index op host indien deze nog niet bestaat")
    sqlite_conn.execute(f"""create index if not exists idx_host on hits(host);""")
    sqlite_conn.commit()
    print(f"verwijderen van host ongekend")
    sqlite_conn.execute(f"""delete from hits where host = 'ongekend';""")
    sqlite_conn.commit()
    print(f"copieren van dutchnews.nl hits naar hits_translation")
    sqlite_conn.execute(f"""insert into hits_translation(url, translated_text) select url, content from hits where host = 'dutchnews.nl' and relevant is null and not exists (select 1 from hits_translation tt where tt.url = url);""")
    sqlite_conn.commit()

    nl_hosts = [
        "ad.nl",
        "bewegingdenk.nl",
        "boerburgerbeweging.nl",
        "cda.nl",
        "cdenv.be",
        "d66.nl",
        "demorgen.be",
        "fvd.nl",
        "groen.be",
        "groenlinks.nl",
        "gva.be",
        "hartvannederland.nl",
        "hbvl.be",
        "hln.be",
        "ja21.nl",
        "n-va.be",
        "nieuwsblad.be",
        "nos.nl",
        "npo.nl",
        "nrc.nl",
        "openvld.be",
        "partijnieuwsociaalcontract.nl",
        "partijvoordedieren.nl",
        "pvda.be",
        "pvda.nl",
        "pvv.nl",
        "rtl.nl",
        "sp.nl",
        "standaard.be",
        "telegraaf.nl",
        "tijd.be",
        "trouw.nl",
        "vlaamsbelang.org",
        "volkskrant.nl",
        "voltnederland.org",
        "vooruit.org",
        "vvd.nl",
    ]

    fr_hosts = [
        "defi.be",
        "dhnet.be",
        "ecolo.be",
        "fouadahidar.com",
        "lalibre.be",
        "lavenir.net",
        "lesengages.be",
        "lesoir.be",
        "mr.be",
        "ps.be",
        "sudinfo.be",
    ]

    for nl_host in nl_hosts:
        print(f"zetten van NL voor host {nl_host}")
        sqlite_conn.execute(f"""update hits set languages = 'nld' where host = '{nl_host}';""")
    sqlite_conn.commit()
    for fr_host in fr_hosts:
        print(f"zetten van FR voor host {fr_host}")
        sqlite_conn.execute(f"""update hits set languages = 'fra' where host = '{fr_host}';""")
    sqlite_conn.commit()

    sqlite_conn.close()



if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : filtering.py target-db")
    prepare_filtering(sys.argv[1])
