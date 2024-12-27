import os
import sys
from cc_indices import indices

def search_indices(uri_file: str, output_dir: str):
    print(f"reading uri file {uri_file}")
    lines = []
    with open(uri_file, "r") as uris:
        lines = [line.strip() for line in uris if line.strip()]

    with open("search-indices.sh", "w") as script:
        for index in indices:
            for line in lines:
                script.write(f"""python3 cdx-index-client.py -c {index} "{line}" -d {output_dir} -j{os.linesep}""")
                script.write(f"""sleep 10{os.linesep}""")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise RuntimeError("usage : search-indices.sh <uri-file> <output-dir>")
    search_indices(sys.argv[1], sys.argv[2])
