#!/bin/bash

mkdir -p ~/scripts

# These are the HotRod endpoints available on port 8080
# HotRod is installed on the playground using install.sh
apis=(
"http://playground.sigscalr.io:8080/dispatch?customer=123"
"http://playground.sigscalr.io:8080/config?customer=123"
"http://playground.sigscalr.io:8080/dispatch?customer=392"
"http://playground.sigscalr.io:8080/config?customer=392"
"http://playground.sigscalr.io:8080/dispatch?customer=731"
"http://playground.sigscalr.io:8080/config?customer=731"
"http://playground.sigscalr.io:8080/dispatch?customer=567"
"http://playground.sigscalr.io:8080/config?customer=567"
)

# Select 10 APIs randomly
selected_apis=()
for i in {1..10}
do
    selected_apis+=("${apis[$RANDOM % ${#apis[@]}]}")
done

# Send GET request to each selected API
for api in "${selected_apis[@]}"
do
    echo "$(date) - Sending GET request to $api" >> ~/scripts/send_traces.log
    curl -X GET $api >> ~/scripts/send_traces.log
    if [ $? -ne 0 ]; then
        echo "$(date) - Error when sending GET request to $api" >> ~/scripts/send_traces.log
    fi
done