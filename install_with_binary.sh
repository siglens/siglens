#!/bin/bash

# Function to display step messages
display_step() {
    echo "Step $1: $2"
}

# Step 1: Detect the current OS and Architecture type
display_step 1 "Detecting current OS and Arch "
os_name=$(uname -sr)
case "$(uname -sr)" in
   Darwin*)
     os="darwin" ;;
   Ubuntu*|Pop!_OS)
     os="linux" ;;
   Linux*amzn2*)
     os="amazon linux" ;;
   Linux*)
     os="linux" ;;
   Debian*)
     os="linux" ;;
   Linux\ Mint*)
     os="linux" ;;
   Red\ Hat*)
     os="linux" ;;
   CentOS*)
     os="linux" ;;
   *)
     echo "OS ${os_name} Not Supported "
     exit 1
     ;;
esac

arch=$(uname -m)
if [[ $arch == x86_64* ]]; then
    arch="amd64"
elif  [[ $arch == arm* || $arch == "aarch64" ]]; then
    arch="arm64"
else
    echo "Architecture ${arch} Not Supported "
    exit 1
fi

# Step 2: Detect the latest version from siglens release
display_step 2 "Getting lastest version of siglens release"

latest_version=`\
    curl  --silent "https://api.github.com/repos/siglens/siglens/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"([^"]+)".*/\1/'`

# Step 3: Fetching latest binary based on OS and Arch
display_step 3 "Fetching latest binary for $os-$arch and version $latest_version"

url="https://github.com/siglens/siglens/releases/download/$latest_version/siglens-$latest_version-$os-$arch.tar.gz"
wget $url
if [[ $? -ne 0 ]]; then
    echo "wget failed to get latest binary from $url"
    exit 1; 
fi

# Step 4: Install statistics
display_step 4 "Install statistics"
# Extract the first occurrence of a valid MAC address
computer_specific_identifier=$(ifconfig 2>/dev/null | grep -o -E '([0-9a-fA-F]{2}:){5}([0-9a-fA-F]{2})' | head -n 1)

# If it can not get the mac address, use hostname as computer-specific identifier 
if [ -z "$computer_specific_identifier" ]; then
  computer_specific_identifier=$(hostname)
fi

# Get OS information
runtime_os=$(uname)
runtime_arch=$(uname -m)

curl -X POST \
https://api.segment.io/v1/track \
-H 'Content-Type: application/json' \
-H 'Authorization: Basic QlBEam5lZlBWMEpjMkJSR2RHaDdDUVRueWtZS2JEOGM6' \
-d '{
  "userId": "'"$computer_specific_identifier"'",
  "event": "install (not running)",
  "properties": {
    "runtime_arch": "'"$runtime_os"'",
    "runtime_os": "'"$runtime_arch"'"
  }
}'

tar -xvf "siglens-$latest_version-$os-$arch.tar.gz"
PORT=80 
display_step 5 "Running the Server on http://localhost:$PORT"
if [ $PORT == 80 ]; then 
   echo "If you are not able to access localhost:80, try running '"export PORT=8090"' and rerunning this script"
fi

PORT=$PORT "siglens-$latest_version-$os-$arch/siglens" --config "siglens-$latest_version-$os-$arch/server.yaml"
