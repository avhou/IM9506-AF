#!/bin/bash

gzcat /Volumes/data-1/cc/indices/CC-MAIN-2022-21_indexes_cdx-00000.gz | head -n 1 | jq -Rc 'split(" ") | {"urlkey": .[0], "timestamp": .[1]} + (.[2:] | join(" ") | fromjson)'


gzcat /Volumes/data-1/cc/indices/CC-MAIN-2022-*_indexes_cdx-*.gz | rg -i "\.nl/|\.be/|\.org/|fouadahidar\.com/" | rg -i "npo\.nl/|nos\.nl/|rtl\.nl\/|hartvannederland\.nl/|volkskrant\.nl/|telegraaf\.nl/|nrc\.nl/|trouw\.nl/|ad\.nl/|dutchnews\.nl/|nltimes\.nl/|pvv\.nl/|fvd\.nl/|groenlinks\.nl/|pvda\.nl/|vvd\.nl/|partijnieuwsociaalcontract\.nl/|partijvoordedieren\.nl/|d66\.nl/|cda\.nl/|boerburgerbeweging\.nl/|sp\.nl/|voltnederland\.org/|ja21\.nl/|bewegingdenk\.nl/|hbvl\.be/|gva\.be/|nieuwsblad\.be/|hln\.be/|demorgen\.be/|standaard\.be/|tijd\.be/|lavenir\.net/|sudinfo\.be/|dhnet\.be/|echo\.be/|lalibre\.be/|lesoir\.be/|pvda\.be/|ps\.be/|vooruit\.org/|ecolo\.be/|groen\.be/|fouadahidar\.com/|defi\.be/|lesengages\.be/|cdenv\.be/|openvld\.be/|mr\.be/|n-va\.be/|vlaamsbelang\.org/" | rg -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk



gzcat /Volumes/data-1/cc/indices/CC-MAIN-2022-*_indexes_cdx-*.gz | rg -i "\b(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|\b()\.be/|\b()\.org/|\bfouadahidar\.com" | rg -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk




time gzcat /Volumes/data-1/cc/indices/CC-MAIN-2022-*_indexes_cdx-*.gz | rg -i "\.nl/|\.be/|\.org/|fouadahidar\.com/|lavenir\.net/" | rg -i "(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|(hbvl|gva|nieuwsblad|hln|demorgen|standaard|tijd|sudinfo|dhnet|echo|lalibre|lesoir|pvda|ps|ecolo|groen|defi|lesengages|cdenv|openvld|mr|n-va)\.be/|(vooruit|voltnederland|vlaamsbelang)\.org/|fouadahidar\.com/|lavenir\.net/" | rg -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk

time rg --search-zip -i "\.nl/|\.be/|\.org/|fouadahidar\.com/|lavenir\.net/" /Volumes/data-1/cc/indices/CC-MAIN-2022-*_indexes_cdx-*.gz | rg -i "\b(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|\b(hbvl|gva|nieuwsblad|hln|demorgen|standaard|tijd|sudinfo|dhnet|echo|lalibre|lesoir|pvda|ps|ecolo|groen|defi|lesengages|cdenv|openvld|mr|n-va)\.be/|\b(vooruit|voltnederland|vlaamsbelang)\.org/|\bfouadahidar\.com/|\blavenir\.net/" | rg -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk

time rg -NI --search-zip -i "\b(npo|nos|rtl|hartvannederland|volkskrant|telegraaf|nrc|trouw|ad|dutchnews|nltimes|pvv|fvd|groenlinks|pvda|vvd|partijnieuwsociaalcontract|partijvoordedieren|d66|cda|boerburgerbeweging|sp|ja21|bewegingdenk)\.nl/|\b(hbvl|gva|nieuwsblad|hln|demorgen|standaard|tijd|sudinfo|dhnet|echo|lalibre|lesoir|pvda|ps|ecolo|groen|defi|lesengages|cdenv|openvld|mr|n-va)\.be/|\b(vooruit|voltnederland|vlaamsbelang)\.org/|\bfouadahidar\.com/|\blavenir\.net/" /Volumes/data-1/cc/indices/CC-MAIN-2022-*_indexes_cdx-*.gz | rg -NI -v "robots\.txt" | awk -f ~/ou/IM9506-AF/dataset/cc-index-to-json.awk
