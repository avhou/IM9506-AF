import os
from cc_indices import indices


def download_indices():
    with open("download-cc-indices.sh", "w") as f:
        for index in indices:
            f.write(
                f"xh https://data.commoncrawl.org/crawl-data/{index}/cc-index.paths.gz > {index}-index.paths.gz{os.linesep}")


if __name__ == "__main__":
    download_indices()
