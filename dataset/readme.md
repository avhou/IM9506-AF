# dataset generation CC

## CC

### determine relevant CC index paths

there is a list of cc index paths, see all-collections.json.   pick the relevant indices for the correct time period.

the relevant index paths are listed in `cc_indices.py` and are used in several python scripts.

### downloading the relevant CC index paths

see script `download-cc-index-paths.py`.  This script generates `download-cc-indices.sh` that will download all relevant index paths.

every index path is a gz file that contains the list of indices.

### downloading the actual CC indices

there is a script `generate-index_segment-downloads.sh`.

This is basically a simple script to generate more download links.  it will generate `xh` commands for each CC index separately.

Some `xh` commands will fail.  execute the script(s) repeatedly until no files are left to download.

### searching the downloaded CC indices

this is done using ripgrep, which allows for concurrent searching.

see `do-searches.sh` for an example statement. 

```bash
rg -IN --search-zip -i "\b(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|\b(hbvl|gva|nieuwsblad|hln|demorgen|standaard|tijd|sudinfo|dhnet|echo|lalibre|lesoir|pvda|ps|ecolo|groen|defi|lesengages|cdenv|openvld|mr|n-va)\.be/|\b(vooruit|voltnederland|vlaamsbelang)\.org/|\bfouadahidar\.com/|\blavenir\.net/" /Volumes/data-1/cc/indices/CC-MAIN-2024-46*_indexes_cdx-*.gz | rg -IN -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk | zstd -19 --ultra > ~/ou/IM9506-AF/dataset/hits-2024-46.zst
```

the result of these ripgrep search are a number of hits*.zst files containing hits for the relevant URLs.

Unfortunately, only the date crawled is available in this result.

### group and process all hits into downloadable sqlite

given all hits*.zst, the python script `extract-cc-segments.py` will prepare a sqlite file that contains all metadata and adds additional columns (like a flag indicating whether the content of the hit was already downloaded).

pass the `extract-cc-segments.py` script a folder to include all files in that folder in the analysis, or pass it a single file. 

### download the downloadable sqlite

See separate project `dataset-downloader`.  The downloader program repeatedly tries to download the bytes from CC and stores it in the sqlite DB. 

### filter the downloaded sqlite

applies additional filtering to the fully downloaded sqlite DB and outputs a new sqlite DB.
here, word boundaries and keyword matching is done.


## Reddit

### scraping the reddits

there is a python script `reddit_scraper.py` that is hardcoded with reddits and period to fetch all relevant entries.  this will produce a number of json files. 

### processing the reddits

see scripts `reddit-concat.sh`.