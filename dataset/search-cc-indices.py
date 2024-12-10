import os
import sys

# we want to check from Feb 2022 - Nov 2024
indices = [
            "CC-MAIN-2024-46",
            "CC-MAIN-2024-42",
            "CC-MAIN-2024-38",
            "CC-MAIN-2024-33",
            "CC-MAIN-2024-30",
            "CC-MAIN-2024-26",
            "CC-MAIN-2024-22",
            "CC-MAIN-2024-18",
            "CC-MAIN-2024-10",
            "CC-MAIN-2023-50",
            "CC-MAIN-2023-40",
            "CC-MAIN-2023-23",
            "CC-MAIN-2023-14",
            "CC-MAIN-2023-06",
            "CC-MAIN-2022-49",
            "CC-MAIN-2022-40",
            "CC-MAIN-2022-33",
            "CC-MAIN-2022-27",
            "CC-MAIN-2022-21",
]

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
