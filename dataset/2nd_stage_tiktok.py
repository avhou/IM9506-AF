import sys

import duckdb
import os
import yt_dlp

def create_tables(conn, prefix: str, min_duration: int = 30, max_duration: int = 60):
    # print(f"drop table optie a")
    # conn.execute(f"""drop table if exists tiktok.{prefix}videos_2nd_stage_optie_a;""")
    print(f"create table optie a")
    conn.execute(f"""create table if not exists tiktok.{prefix}videos_2nd_stage_optie_a as select * from tiktok.videos where regexp_matches(video_description, '(?i)(russia|ukraine|russie|rusland|oekraine|oekraïne)') and video_duration >= {min_duration} and video_duration <= {max_duration};""")
    conn.execute(f"""alter table tiktok.{prefix}videos_2nd_stage_optie_a add column transcription text;""")
    # print(f"drop table optie b")
    # conn.execute(f"""drop table if exists tiktok.videos_2nd_stage_optie_b;""")
    print(f"create table optie b")
    conn.execute(f"""create table if not exists tiktok.{prefix}videos_2nd_stage_optie_b as select * from tiktok.videos where regexp_matches(video_description, '(?i)(russia|ukraine|russie|rusland|oekraine|oekraïne|syria|syrie|syrië|israel|israël|palestine|palestina)') and video_duration >= {min_duration} and video_duration <= {max_duration};""")
    conn.execute(f"""alter table tiktok.{prefix}videos_2nd_stage_optie_b add column transcription text;""")

def stage_2_tiktok(tiktok_db: str, min_duration: int = 30, max_duration: int = 60, include_long: bool = False):
    print(f"processing input {tiktok_db}")

    # Connect to DuckDB
    conn = duckdb.connect()
    conn.execute(f"""ATTACH '{tiktok_db}' as tiktok (TYPE sqlite);""")

    create_tables(conn, "", min_duration, max_duration)
    if include_long:
        create_tables(conn, "long_", min_duration, max_duration * 2)

    download_videos("tiktok.videos_2nd_stage_optie_a", "videos_2nd_stage_optie_a", conn)
    download_videos("tiktok.videos_2nd_stage_optie_b", "videos_2nd_stage_optie_b", conn)
    if include_long:
        download_videos("tiktok.long_videos_2nd_stage_optie_a", "long_videos_2nd_stage_optie_a", conn)
        download_videos("tiktok.long_videos_2nd_stage_optie_b", "long_videos_2nd_stage_optie_b", conn)


def download_tiktok_video(url: str, output_path: str):
    ydl_opts = {
        'outtmpl': output_path,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

def download_videos(table: str, folder: str, conn):
    print(f"downloading videos from {table} to {folder}")
    if not os.path.exists(folder):
        os.makedirs(folder)
    for r in conn.execute(f"select id, user_name from {table}").fetchall():
        url = f"https://www.tiktok.com/@{r[1]}/video/{r[0]}"
        print(f"downloading {url}")
        if not os.path.exists(f"{folder}/{r[0]}.mp4"):
            try:
                download_tiktok_video(url, f"{folder}/{r[0]}.mp4")
            except Exception as e:
                print(f"error downloading {url}, error was {e}")
        else:
            print(f"skipping {url}, already downloaded")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : 2nd_stage_tiktok.py <tiktok-db.sqlite> [min_duration] [max_duration]")
    stage_2_tiktok(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 30, int(sys.argv[3]) if len(sys.argv) > 3 else 60, True if len(sys.argv) > 4 else False)
