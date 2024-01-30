#! /bin/bash

# Extract the latest version number from the SigLens GitHub releases
SIGLENS_VERSION=$(curl --silent "https://api.github.com/repos/siglens/siglens/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"([^"]+)".*/\1/')

sudo_cmd=""

# Text color
RED_TEXT='\e[31m'
GREEN_TEXT='\e[32m'
RESET_COLOR='\e[0m'

# Check sudo permissions
if (( $EUID != 0 )); then
    echo "===> Running installer with non-sudo permissions."
    echo "     In case of any failure or prompt, run the script with sudo privileges."
    echo ""
else
    sudo_cmd="sudo"
fi

os=""
case "$(uname -sr)" in
   Ubuntu*|Pop!_OS|Debian*|Linux*|Mint*)
     os="linux"
     package_manager="apt-get" ;;
   *)
     os="Not Found: $os_name"
     echo 'Not Supported OS'
     exit 1
     ;;
esac

arch=$(uname -m)
if [[ $arch == x86_64* ]]; then
    arch="amd64"
elif  [[ $arch == arm* || $arch == "aarch64" ]]; then
    arch="arm64"
else
    echo 'Not Supported Architecture'
fi

install_podman() {
    echo "----------Setting up Podman----------"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update
        $apt_cmd install software-properties-common
        . /etc/os-release
        $sudo_cmd sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_$VERSION_ID/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list"
        wget -nv https://download.opensuse.org/repositories/devel:kubic:libcontainers:stable/xUbuntu_$VERSION_ID/Release.key -O Release.key
        $sudo_cmd apt-key add - < Release.key
        rm -f Release.key
        $apt_cmd update
        echo "Installing Podman"
        $apt_cmd install podman || {
            print_error_and_exit "Podman installation failed during apt-get install on $os"
        }
    fi
    podman_version=$(podman --version) || {
        print_error_and_exit "Podman is not working correctly. Please install Podman manually and re-run the command."
    }
    print_success_message "Podman installed successfully. $podman_version"
}

print_error_and_exit() {
    printf "${RED_TEXT}$1${RESET_COLOR}\n"
    exit 1
}

print_success_message() {
    printf "${GREEN_TEXT}$1${RESET_COLOR}\n"
}

if ! type podman >/dev/null 2>&1; then
    if [[ $package_manager == "apt-get" ]]; then
        install_podman
    else
        print_error_and_exit "\nPodman must be installed manually on your machine to proceed. Podman can only be installed automatically on Ubuntu / Debian based distributions."
    fi
fi

# Check if podman-compose is installed
is_podman_compose_installed() {
    if ! type podman-compose >/dev/null 2>&1; then
        return 1
    else
        return 0
    fi
}

# Install Python and pip if they are not present
install_python_and_pip() {
    echo "Checking and installing Python and pip if necessary..."

    # Check for Python3 and install if not present
    if ! type python3 >/dev/null 2>&1; then
        echo "Python 3 not found. Installing Python 3..."
        $sudo_cmd apt-get install -y python3 || {
            print_error_and_exit "Failed to install Python 3. Please check your system's repositories."
        }
    else
        echo "Python 3 is already installed."
    fi

    # Check for pip3 and install if not present
    if ! type pip3 >/dev/null 2>&1; then
        echo "pip for Python 3 not found. Installing pip3..."
        $sudo_cmd apt-get install -y python3-pip || {
            print_error_and_exit "Failed to install pip for Python 3. Please check your system's repositories."
        }
    else
        echo "pip3 is already installed."
    fi
}


# Install podman-compose
install_podman_compose() {
    echo "Installing podman-compose..."

    # Ensure Python and pip are installed
    install_python_and_pip

    # Use pip to install podman-compose
    if ! type pip3 >/dev/null 2>&1; then
        $sudo_cmd apt-get install -y python3-pip
    fi

    $sudo_cmd pip3 install podman-compose || {
        print_error_and_exit "Failed to install podman-compose. Please check your Python/pip configuration."
    }
    print_success_message "podman-compose installed successfully."
}

# Check if podman-compose is installed, install if not
if ! is_podman_compose_installed; then
    install_podman_compose
else
    print_success_message "podman-compose is already installed."
fi

# Define the Podman image name and compose file variables
PODMAN_IMAGE_NAME="${PODMAN_IMAGE_NAME:-docker.io/siglens/siglens:${SIGLENS_VERSION}}"
PODMAN_COMPOSE_FILE="${PODMAN_COMPOSE_FILE:-docker-compose.yml}"

echo -e "\n----------Pulling the latest Podman image for SigLens----------"

# Check if the user wants to use a local compose file
if [ "$USE_LOCAL_PODMAN_COMPOSE" != true ]; then
    curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
    #curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/docker-compose.yml"
    curl -O -L https://raw.githubusercontent.com/Macbeth98/siglens/install-with-podman/podman-compose.yml"
    echo "Pulling the latest Podman image for SigLens from upstream"
    $sudo_cmd podman pull $PODMAN_IMAGE_NAME || {
        print_error_and_exit "Failed to pull $PODMAN_IMAGE_NAME. Please check your internet connection and Podman installation."
    }
fi

# Create and set permissions for data and logs directories
mkdir -p data || {
    print_error_and_exit "Failed to create directory 'data'. Please check your permissions."
}
chmod a+rwx data || {
    print_error_and_exit "Failed to change permissions for directory 'data'. Please check your file permissions."
}

mkdir -p logs || {
    print_error_and_exit "Failed to create directory 'logs'. Please check your permissions."
}
chmod a+rwx logs || {
    print_error_and_exit "Failed to change permissions for directory 'logs'. Please check your file permissions."
}
print_success_message "\n===> SigLens installation complete with version: ${SIGLENS_VERSION}"

# Fetch the system's unique identifier
csi=$(ifconfig 2>/dev/null | grep -o -E '([0-9a-fA-F]{2}:){5}([0-9a-fA-F]{2})' | head -n 1)
if [ -z "$csi" ]; then
  csi=$(hostname)
fi

runtime_os=$(uname)
runtime_arch=$(uname -m)

# Function to check if a port is available
check_ports() {
    PORT=$1
    if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || podman ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
        CONTAINER_ID=$(podman ps --format "{{.ID}}:{{.Image}}:{{.Ports}}" | grep "siglens/siglens.*0.0.0.0:${PORT}" | cut -d ":" -f 1 2>/dev/null)
        if [ -n "$CONTAINER_ID" ]; then
            podman stop $CONTAINER_ID
            if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || podman ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
                print_error_and_exit "\nError: Port ${PORT} is already in use."
            fi
        else
            print_error_and_exit "\nError: Port ${PORT} is already in use."
        fi
    fi
    print_success_message "Port ${PORT} is available"
}

# Check if required ports are available
check_ports 5122
check_ports 8081

send_events() {
    curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz
    if [ $? -ne 0 ]; then
        post_event "install_failed" "send_events: Failed to download sample log dataset from https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz"
        print_error_and_exit "Failed to download sample log dataset"
    fi
    tar -xvf 2kevents.json.tar.gz || {
        post_event "install_failed" "send_events: Failed to extract 2kevents.json.tar.gz"
        print_error_and_exit "Failed to extract 2kevents.json.tar.gz"
    }
    for i in $(seq 1 20)
    do
        curl -s http://localhost:8081/elastic/_bulk --data-binary "@2kevents.json" -o res.txt
        if [ $? -ne 0 ]; then
            post_event "install_failed" "send_events: Failed to send sample log dataset to http://localhost:8081/elastic/_bulk"
            print_error_and_exit "Failed to send sample log dataset"
        fi
    done
    print_success_message "\n Sample log dataset sent successfully"
}

# Define UI port and configuration file
UI_PORT=5122
CFILE="server.yaml"
if [ -n "${CONFIG_FILE}" ]; then
    CFILE=${CONFIG_FILE}
fi

print_success_message "\n Starting Siglens with image: ${PODMAN_IMAGE_NAME}"
CSI=${csi} UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${PODMAN_IMAGE_NAME} podman-compose -f $PODMAN_COMPOSE_FILE up -d || {
    print_error_and_exit "Failed to start Podman Compose"
}
UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${PODMAN_IMAGE_NAME} podman-compose logs -t --tail 20 >> pclogs.txt
sample_log_dataset_status=$(curl -s -o /dev/null -I -X HEAD -w "%{http_code}" http://localhost:5122/elastic/sample-log-dataset)

if [ "$sample_log_dataset_status" -eq 200 ]; then
    FIRST_RUN=false
elif [ "$sample_log_dataset_status" -eq 404 ]; then
    FIRST_RUN=true
else
    echo "Failed to check sample log dataset status"
    FIRST_RUN=true
fi

if $FIRST_RUN; then
    send_events
    post_event "fresh_install_success" "Fresh installation was successful using docker on $os"
else
    post_event "repeat_install_success" "Repeat installation of Docker was successful using docker on $os"
    echo "Skipping sendevents as this is not the first run"
fi

tput bold
print_success_message "\n===> Frontend can be accessed on http://localhost:${UI_PORT}"
echo ""
tput sgr0

if [ $? -ne 0 ]; then
    tput bold
    printf "\n${RED_TEXT}Error: Podman failed to start. This could be due to a permission issue.${RESET_COLOR}"
    tput sgr0
    exit 1
fi

echo -e "\n*** Thank you! ***\n"