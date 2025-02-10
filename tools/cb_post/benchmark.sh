#!/bin/bash


CWD=`/usr/bin/pwd`
GO_INSTALL_URL="https://go.dev/dl/go1.23.5.linux-amd64.tar.gz"
SRC="~/src"
SIGLENS_SRC="$SRC/siglens"



###### Set up Siglens

# Get git
sudo apt install git-all


# Follow 'Go Install' instructions: https://go.dev/doc/install
wget GO_INSTALL_URL
sudo rm -rf /usr/local/go && tar -C /usr/local -xzf go1.23.5.linux-amd64.tar.gz


# Get SigLens 
mkdir $SRC
cd $SRC
git clone https://github.com/siglens/siglens.git


# Build and launch SigLens
cd $SIGLENS_SRC
/usr/local/go/bin/go mod tidy
/usr/local/go/bin/go  build -o siglens cmd/siglens/main.go
./siglens &> siglens.out &



###### Data loading 

# Download and unzip dataset
wget https://datasets.clickhouse.com/hits_compatible/hits.json.gz
gzip -d hits.json.gz


# Install python3
sudo apt install python3


## Add the _index line and fix the UserID from string to num
python3 fix_hits.py

## split into 10 files to increase parallelism, since the client in single threaded
split -l 20000000 sighits.json  splithits_


# command to load data into SigLens - process can take hours
time for file in splithits_*; do python3 send_data.py ${file}&; done



######  Run the queries
./run.sh
