import os
from typing import List
import cc_indices


def download_indices(filename: str, indices: List[str]):
    with open(filename, "w") as f:
        for index in indices:
            f.write(
                f"xh https://data.commoncrawl.org/crawl-data/{index}/cc-index.paths.gz > {index}-index.paths.gz{os.linesep}")


if __name__ == "__main__":
    download_indices("download-cc-indices.sh", cc_indices.indices)
    download_indices("download-cc-snapshot-before-indices.sh", cc_indices.snapshot_before_timeperiod_indices)
