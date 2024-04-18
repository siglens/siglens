#!/bin/bash
# Copyright (c) 2021-2024 SigScalr, Inc.
#
# This file is part of SigLens Observability Solution
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

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