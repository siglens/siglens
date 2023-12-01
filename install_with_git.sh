#!/bin/bash

# Function to display step messages
display_step() {
    echo "Step $1: $2"
}

# Step 1: Check if Git is installed
display_step 1 "Checking if Git is installed"
if ! command -v git &> /dev/null; then
    echo "Git is not installed. Please install Git locally and create a GitHub account before proceeding."
    echo "You can get started with your GitHub account here: https://docs.github.com/en/get-started/onboarding/getting-started-with-your-github-account"
    exit 1
fi

# Step 2: Create directories and clone the repository
display_step 2 "Creating local directories and cloning Git repository"
mkdir -p siglens-contrib
cd siglens-contrib
git clone git@github.com:siglens/siglens.git

# Step 3: Change directory and download Go dependencies
display_step 3 "Changing directory and downloading Go dependencies"
cd siglens
go mod tidy

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

PORT=80 
display_step 5 "Running the Server on http://localhost:$PORT"
if [ $PORT == 80 ]; then 
    echo "If you are not able to access localhost:80, try running '"export PORT=8090"' and rerunning this script"
fi
PORT=$PORT go run cmd/siglens/main.go -config server.yaml




