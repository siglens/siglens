#!/bin/bash

###### Set up SigLens


TODO


###### Data loading (JSON dump via ES Bulk API insert)

# Download and unzip dataset
wget https://datasets.clickhouse.com/hits_compatible/hits.json.gz
gzip -d hits.json.gz

## add the _index line and fix the UserID from string to num
python3 fix_hits.py

## split into 10 files to increase parallelism, since the client in single threaded
split -l 20000000 sighits.json  splithits_


# command to load data into SigLens - process can take hours, so better run in background and disown
time for file in splithits_*; do python3 send_data.py ${file}&; done


Add a cmd to make sure the recs ingested are more than 100M

######  Run the queries
./run.sh
