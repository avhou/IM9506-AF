import sys

import duckdb
import requests
import logging

def download_cc_segments(input_file: str):
    print(f"processing input file {input_file}")

    # Connect to DuckDB
    conn = duckdb.connect(input_file)

    for data_url, range in conn.execute(f"""select data_url, range from download_urls where downloaded = 0;""").fetchmany(10):
        byte_range = f'bytes={range}'
        print(f"xh {data_url} Range:{byte_range}")
        response = requests.get(
            data_url,
            headers={'user-agent': 'cc-ou-nl', 'Range': byte_range},
            stream=True
        )

        if response.status_code == 206:
            print(f"received data")
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : download-cc-segments.sh <input-file>")

    logging.basicConfig(level=logging.INFO)  # You can set it to INFO, DEBUG, WARNING, etc.
    logging.getLogger("requests").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    download_cc_segments(sys.argv[1])
