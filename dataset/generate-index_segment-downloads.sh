#!/bin/bash

gzcat *index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads.sh

gzcat CC-MAIN-2022-05-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-05.sh
gzcat CC-MAIN-2022-05-index-table.paths.gz | rg -i parquet | awk -F'/' '{print "xh -dco " $NF " https://data.commoncrawl.org/" $0}' > do-downloads-2022-05.sh
gzcat CC-MAIN-*-index-table.paths.gz | rg -i parquet | awk -F'/' '{print "xh -dco " $NF " https://data.commoncrawl.org/" $0}' > do-downloads.sh

-- ATTACH 'output-hits-snapshot-before.sqlite' as sqlite_output (TYPE sqlite);
-- create table sqlite_output.download_progress as (select distinct url from read_parquet('/Volumes/data-1/cc/indices-table-snapshot-before/**/part-*.c000.gz.parquet') where regexp_matches(url, '\b(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|\b(hbvl|gva|nieuwsblad|hln|demorgen|standaard|tijd|sudinfo|dhnet|echo|lalibre|lesoir|pvda|ps|ecolo|groen|defi|lesengages|cdenv|openvld|mr|n-va)\.be/|\b(vooruit|voltnederland|vlaamsbelang)\.org/|\bfouadahidar\.com/|\blavenir\.net/'));



gzcat CC-MAIN-2022-21-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-21.sh
gzcat CC-MAIN-2022-27-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-27.sh
gzcat CC-MAIN-2022-33-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-33.sh
gzcat CC-MAIN-2022-40-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-40.sh
gzcat CC-MAIN-2022-49-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2022-49.sh

gzcat CC-MAIN-2023-06-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2023-06.sh
gzcat CC-MAIN-2023-14-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2023-14.sh
gzcat CC-MAIN-2023-23-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2023-23.sh
gzcat CC-MAIN-2023-40-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2023-40.sh
gzcat CC-MAIN-2023-50-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2023-50.sh

gzcat CC-MAIN-2024-10-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-10.sh
gzcat CC-MAIN-2024-18-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-18.sh
gzcat CC-MAIN-2024-22-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-22.sh
gzcat CC-MAIN-2024-26-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-26.sh
gzcat CC-MAIN-2024-30-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-30.sh
gzcat CC-MAIN-2024-33-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-33.sh
gzcat CC-MAIN-2024-38-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-38.sh
gzcat CC-MAIN-2024-42-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-42.sh
gzcat CC-MAIN-2024-46-index.paths.gz | rg -i cdx | awk -F/ '{print "xh -dco " $(NF-2) "_" $(NF-1) "_" $NF " https://data.commoncrawl.org/" $0 }' > do-downloads-2024-46.sh
