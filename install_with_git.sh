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

PORT=80 
display_step 4 "Running the Server on http://localhost:$PORT"
if [ $PORT == 80 ]; then 
	echo "If you are not able to access localhost:80 to changing the PORT=8090"
fi
PORT=$PORT go run cmd/siglens/main.go -config server.yaml




