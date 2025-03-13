#!/bin/bash

#The script assumes 
#1. yum is the package manager for target machine
#2. current user has sudo privileges
#3. python 3 is already installed and is on path


#Current directory should be on a disk with around 600GB of free space
CWD=`/usr/bin/pwd`

#Set SRC to a directory on a disk that has atleast 50GB free space
SRC="/data1/src"
SIGLENS_SRC="$SRC/siglens"



###### Set up Siglens

# Get git and go
sudo yum install git -y
sudo yum install golang -y

# Get SigLens 
mkdir -p "$SRC"
cd "$SRC"
git clone https://github.com/siglens/siglens.git --branch 1.0.25


# Build and launch SigLens
cd "$SIGLENS_SRC"
go mod tidy
go  build -o siglens cmd/siglens/main.go
./siglens &> siglens.out &



###### Data loading 

# Download and unzip dataset
cd "$CWD"
wget https://datasets.clickhouse.com/hits_compatible/hits.json.gz
gzip -d hits.json.gz

## Add the _index line and fix the UserID from string to num
python3 fix_hits.py

## split into 10 files to increase parallelism
rm hits.json
split -l 20000000 sighits.json  splithits_
rm sighits.json

# command to load data into SigLens - process can take a few hours
time python3 send_data.py 

######  Run the queries
./run.sh
