#! /bin/bash

# USAGE:
# export CONTAINER_TOOL=podman/docker
# ./install.sh

# Default container tool
CONTAINER_TOOL=${CONTAINER_TOOL:-docker}

# Check if an argument is provided
if [[ "$#" -eq 1 ]]; then
    if [[ "$1" == "docker" || "$1" == "podman" ]]; then
        CONTAINER_TOOL="$1"
    else
        echo "Invalid argument: $1. Please use 'docker' or 'podman'."
        exit 1
    fi
fi

echo "===> Selected container tool: $CONTAINER_TOOL"


# Extract the version number from pkg/config/version.go by getting everything
# inside the quotes. Use -n to supress printing each line, and p to print the
# modified line.
SIGLENS_VERSION=`\
    curl  --silent "https://api.github.com/repos/siglens/siglens/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"([^"]+)".*/\1/'`

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
package_manager=""
case "$(uname -sr)" in
   Darwin*)
     os="darwin"
     package_manager="brew" ;;
   Linux*amzn2*)
     os="amazon linux"
     package_manager="yum" ;;
   Red\ Hat*|CentOS*|Fedora*)
     os="linux"
     package_manager="yum" ;;
   Ubuntu*|Pop!_OS|Debian*|Linux*|Mint*)
     os="linux"
     package_manager="apt-get" ;;
   *)
     os="Not Found: $os_name"
     echo 'Not Supported OS'
     exit 1
     ;;
esac

if [[ -z $package_manager ]]; then
    echo "Unsupported Linux distribution for Docker installation."
    exit 1
fi

arch=$(uname -m)
if [[ $arch == x86_64* ]]; then
    arch="amd64"
elif  [[ $arch == arm* || $arch == "aarch64" ]]; then
    arch="arm64"
else
    echo 'Not Supported Architecture'
fi

# Fetch IP information
fetch_ip_info() {
    response=$(curl -s https://ipinfo.io)

    ip=$(echo "$response" | awk -F'"' '/ip/{print $4}' | head -n 1)
    city=$(echo "$response" | awk -F'"' '/city/{print $4}')
    region=$(echo "$response" | awk -F'"' '/region/{print $4}')
    country=$(echo "$response" | awk -F'"' '/country/{print $4}')
    loc=$(echo "$response" | awk -F'"' '/loc/{print $4}')
    timezone=$(echo "$response" | awk -F'"' '/timezone/{print $4}')
    latitude=$(echo "$loc" | cut -d',' -f1)
    longitude=$(echo "$loc" | cut -d',' -f2)
}

fetch_ip_info

post_event() {
  local event_code=$1
  local message=$2
    curl -X POST \
    https://api.segment.io/v1/track \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Basic QlBEam5lZlBWMEpjMkJSR2RHaDdDUVRueWtZS2JEOGM6' \
    -d '{
    "userId": "'"$csi"'",
    "event":  "'"$event_code"'",
    "properties": {
        "os": "'"$os"'",
        "arch": "'"$arch"'",
        "package_manager": "'"$package_manager"'",
        "message": "'"$message"'",
        "ip": "'"$ip"'",
        "city": "'"$city"'",
        "region": "'"$region"'",
        "country": "'"$country"'"
    }
    }'
}

print_error_and_exit() {
    printf "${RED_TEXT}$1${RESET_COLOR}\n"
    exit 1
}

print_success_message() {
    printf "${GREEN_TEXT}$1${RESET_COLOR}\n"
}

is_command_present() {
    type "$1" >/dev/null 2>&1
}

request_sudo() {
    if hash sudo 2>/dev/null; then
        echo -e "\n Need sudo access to complete the installation."
        if (( $EUID != 0 )); then
            sudo_cmd="sudo"
            echo -e "Please enter your sudo password, if prompted."
            if ! $sudo_cmd -l | grep -e "NOPASSWD: ALL" > /dev/null && ! $sudo_cmd -v; then
                post_event "install_failed" "request_sudo: Sudo access required but not available"
                print_error_and_exit "Need sudo privileges to proceed with the installation."
            fi

            echo -e "Got Sudo access.\n"
        fi
    fi
}


IMAGE_NAME=""
COMPOSE_FILE=""


###### START: DOCKER ######

install_docker() {
    echo "----------Setting up docker----------"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update
        $apt_cmd install software-properties-common gnupg-agent
        curl -fsSL "https://download.docker.com/linux/$os/gpg" | $sudo_cmd apt-key add -
        $sudo_cmd add-apt-repository \
            "deb [arch=$arch] https://download.docker.com/linux/$os $(lsb_release -cs) stable"
        $apt_cmd update
        echo "Installing docker"
        $apt_cmd install docker-ce docker-ce-cli containerd.io || {
            post_event "install_failed" "install_docker: Docker installation failed during apt-get install on $os"
            print_error_and_exit "install_docker: Docker installation failed during apt-get install on $os"
        }
    elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
        $sudo_cmd yum install -y amazon-linux-extras
        $sudo_cmd amazon-linux-extras enable docker
        $sudo_cmd yum install -y docker || {
            post_event "install_failed" "install_docker: Docker installation failed during yum install on Amazon Linux"
            print_error_and_exit "install_docker: Docker installation failed during yum install on Amazon Linux"
        }
    else
        yum_cmd="$sudo_cmd yum --assumeyes --quiet"
        $yum_cmd install yum-utils
        $sudo_cmd yum-config-manager --add-repo https://download.docker.com/linux/$os/docker-ce.repo
        echo "Installing docker"
        $yum_cmd install docker-ce docker-ce-cli containerd.io || {
            post_event "install_failed" "install_docker: Docker installation failed during yum install on $os"
            print_error_and_exit "install_docker: Docker installation failed during yum install on $os"
        }
    fi
    docker_version=$(docker --version) || {
        post_event "install_failed" "install_docker: Failed to check docker version post-installation on $os"
        print_error_and_exit "Docker is not working correctly. Please install docker manually and re-run the command."
    }
    print_success_message "Docker installed successfully. $docker_version"
}

install_docker_compose() {
    echo "----------Setting up docker compose----------"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update || {
            post_event "install_failed" "install_docker_compose: apt-get update failed during Docker Compose setup"
            print_error_and_exit "apt-get update failed."
        }
        $apt_cmd install docker-compose || {
            post_event "install_failed" "install_docker_compose: apt-get install docker-compose failed during Docker Compose setup"
            print_error_and_exit "Docker Compose installation failed."
        }
    elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
        curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose || {
            post_event "install_failed" "install_docker_compose: Downloading Docker Compose binary failed during Docker Compose setup"
            print_error_and_exit "Downloading Docker Compose binary failed."
        }
        chmod +x /usr/local/bin/docker-compose || {
            post_event "install_failed" "install_docker_compose: Making Docker Compose executable failed during Docker Compose setup"
            print_error_and_exit "Making Docker Compose executable failed."
        }
    elif [[ $package_manager == brew ]]; then
        brew install docker-compose || {
            post_event "install_failed" "install_docker_compose: Docker Compose installation via brew failed during Docker Compose setup"
            print_error_and_exit "Docker Compose installation failed."
        }
    else
        post_event "install_failed" "install_docker_compose: Docker Compose not installed, manual installation required during Docker Compose setup"
        echo "---------Docker Compose must be installed manually to proceed---------"
        print_error_and_exit "Docker Compose Not installed"
    fi
    docker_compose_version=$(docker-compose --version) || {
        post_event "install_failed" "install_docker_compose: Docker Compose post-installation check failed during Docker Compose setup"
        print_error_and_exit "Docker Compose is not working correctly."
    }
    print_success_message "Docker Compose installed successfully. $docker_compose_version"
}

start_docker() {
    echo -e "\n===> Starting Docker ...\n"
    if [[ $os == "darwin" ]]; then
        open --background -a Docker && while ! docker system info > /dev/null 2>&1; do sleep 1; done || {
            post_event "install_failed" "start_docker: Failed to start Docker on macOS"
            print_error_and_exit "Failed to start Docker"
        }
    else
        if ! $sudo_cmd systemctl is-active docker.service > /dev/null; then
            echo "Starting docker service"
            $sudo_cmd systemctl start docker.service || {
                post_event "install_failed" "start_docker: Failed to start systemctl docker service"
                print_error_and_exit "Failed to start Docker service"
            }
        fi
        if [[ -z $sudo_cmd ]]; then
            if ! docker ps > /dev/null && true; then
                request_sudo
            fi
        fi
    fi
    docker info > /dev/null 2>&1 || {
        post_event "install_failed" "start_docker: Failed to retrieve Docker info, Docker may not have started correctly"
        print_error_and_exit "Docker did not start correctly."
    }
    print_success_message "Docker started successfully."
}

pull_siglens_docker_image() {
    echo -e "\n----------Pulling the latest docker image for SigLens----------"

    if [ "$USE_LOCAL_DOCKER_COMPOSE" != true ]; then
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/docker-compose.yml"
        echo "Pulling the latest docker image for SigLens from upstream"
        $sudo_cmd docker pull $IMAGE_NAME || {
        post_event "install_failed" "Failed to pull Docker image $IMAGE_NAME"
        print_error_and_exit "Failed to pull $IMAGE_NAME. Please check your internet connection and Docker installation."
    }
    fi

    echo -e "\n-----------------Docker image pulled successfully-----------------"
}

###### END: DOCKER ######


###### START: PODMAN ######

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

# Fetch and set up the custom network configuration file
get_podman_custom_network_configuration() {
    echo "Setting up custom Podman network configuration..."
    curl -O -L "https://raw.githubusercontent.com/Macbeth98/siglens/install-with-podman/podman-network_siglens.conflist" || {
        print_error_and_exit "Failed to download custom network configuration file."
    }
    $sudo_cmd mv podman-network_siglens.conflist ~/.config/cni/net.d/ || {
        print_error_and_exit "Failed to move custom network configuration file to ~/.config/cni/net.d"
    }
    echo "Custom network configuration set up successfully."
}

create_podman_network() {
    echo "Creating custom Podman network: podman-network_siglens"

    # Check if the network already exists
    if ! podman network inspect podman-network_siglens >/dev/null 2>&1; then
        get_podman_custom_network_configuration
    else
        echo "Custom Podman network already exists."
    fi
}

check_and_create_ssmetrics_otel_config(){
    # Define the full path of the file using PWD
    FILE_PATH="${PWD}/ssmetrics-otel-collector-config.yaml"

    # Check if the file exists
    if [ -f "$FILE_PATH" ]; then
        echo "The file already exists: $FILE_PATH"
    else
        # Create the file if it does not exist
        touch "$FILE_PATH"
        if [ $? -eq 0 ]; then
            echo "File created successfully: $FILE_PATH"
        else
            echo "Failed to create the file: $FILE_PATH"
            exit 1
        fi
    fi
}

pull_siglens_podman_image() {
    echo -e "\n----------Pulling the latest Podman image for SigLens----------"

    # Check if the user wants to use a local compose file
    if [ "$USE_LOCAL_PODMAN_COMPOSE" != true ]; then
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
        curl -O -L "https://raw.githubusercontent.com/Macbeth98/siglens/install-with-podman/podman-compose.yml"
        echo "Pulling the latest Podman image for SigLens from upstream"
        $sudo_cmd podman pull $IMAGE_NAME || {
            print_error_and_exit "Failed to pull $IMAGE_NAME. Please check your internet connection and Podman installation."
        }
    fi

    check_and_create_ssmetrics_otel_config

    echo -e "\n-----------------Podman image pulled successfully-----------------"
}

###### END: PODMAN ######


# Install with Docker or Podman based on the user's choice
if [[ $CONTAINER_TOOL == "docker" ]]; then
    # Check if docker is installed
    if ! is_command_present docker; then
        if [[ $package_manager == "apt-get" || $package_manager == "yum" ]]; then
            request_sudo
            install_docker
            install_docker_compose
        elif [[ $os == "darwin" ]]; then
            post_event "install_failed" "Docker Desktop not installed on Mac OS. Automatic installation is not supported."
            print_error_and_exit "\nDocker Desktop must be installed manually on Mac OS to proceed. \n You can install Docker from here - https://docs.docker.com/docker-for-mac/install/"
        else
            post_event "install_failed" "Docker not installed. Automatic installation is only supported on Ubuntu / Redhat."
            print_error_and_exit "\nDocker must be installed manually on your machine to proceed. Docker can only be installed automatically on Ubuntu / Redhat. \n You can install Docker from here - https://docs.docker.com/get-docker/"
        fi
    fi

    IMAGE_NAME="${DOCKER_IMAGE_NAME:-siglens/siglens:${SIGLENS_VERSION}}"
    COMPOSE_FILE="${DOCKER_COMPOSE_FILE:-docker-compose.yml}"

    start_docker

    pull_siglens_docker_image
else
    # Request sudo permissions if not already present
    request_sudo

    # Check if podman is installed
    if ! is_command_present podman; then
        install_podman
    fi

    # Check if podman-compose is installed
    if ! is_command_present podman-compose; then
        install_podman_compose
    fi

    # Create the custom network configuration
    create_podman_network

    IMAGE_NAME="${PODMAN_IMAGE_NAME:-docker.io/siglens/siglens:${SIGLENS_VERSION}}"
    COMPOSE_FILE="${PODMAN_COMPOSE_FILE:-podman-compose.yml}"

    pull_siglens_podman_image
fi

mkdir -p data || {
    post_event "install_failed" "Failed to create directory 'data'."
    print_error_and_exit "Failed to create directory 'data'. Please check your permissions."
}
chmod a+rwx data || {
    post_event "install_failed" "Failed to change permissions for directory 'data'."
    print_error_and_exit "Failed to change permissions for directory 'data'. Please check your file permissions."
}

mkdir -p logs || {
    post_event "install_failed" "Failed to create directory 'logs'"
    print_error_and_exit "Failed to create directory 'logs'. Please check your permissions."
}
chmod a+rwx logs || {
    post_event "install_failed" "Failed to change permissions for directory 'logs'."
    print_error_and_exit "Failed to change permissions for directory 'logs'. Please check your file permissions."
}
print_success_message "\n===> SigLens installation complete with version: ${SIGLENS_VERSION}"

csi=$(ifconfig 2>/dev/null | grep -o -E '([0-9a-fA-F]{2}:){5}([0-9a-fA-F]{2})' | head -n 1)
if [ -z "$csi" ]; then
  csi=$(hostname)
fi

runtime_os=$(uname)
runtime_arch=$(uname -m)

check_ports() {
    PORT=$1
    if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || $CONTAINER_TOOL ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
        CONTAINER_ID=$($CONTAINER_TOOL ps --format "{{.ID}}:{{.Image}}:{{.Ports}}" | grep "siglens/siglens.*0.0.0.0:${PORT}" | cut -d ":" -f 1 2>/dev/null)
        if [ -n "$CONTAINER_ID" ]; then
            $CONTAINER_TOOL stop $CONTAINER_ID
            if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || $CONTAINER_TOOL ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
                post_event "install_failed" "Port ${PORT} is already in use after attempting to stop our ${CONTAINER_TOOL} container"
                print_error_and_exit "\nError: Port ${PORT} is already in use."
            fi
        else
            post_event "install_failed" "Port ${PORT} is already in use and no ${CONTAINER_TOOL} container could be stopped"
            print_error_and_exit "\nError: Port ${PORT} is already in use."
        fi
    fi
    print_success_message "Port ${PORT} is available"
}

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

UI_PORT=5122

CFILE="server.yaml"
if [ -n "${CONFIG_FILE}" ]; then
    CFILE=${CONFIG_FILE}
fi

print_success_message "\n Starting Siglens with image: ${IMAGE_NAME}"
CSI=${csi} UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${IMAGE_NAME} ${CONTAINER_TOOL}-compose -f $COMPOSE_FILE up -d || {
    post_event "install_failed" "Failed to start $CONTAINER_TOOL Compose on $os with $COMPOSE_FILE"
    print_error_and_exit "Failed to start $CONTAINER_TOOL Compose"
}
CSI=${csi} UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${IMAGE_NAME} $CONTAINER_TOOL-compose logs -t --tail 20 >> ${CONTAINER_TOOL}_logs.txt

# Check if the sample log dataset is available
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

# Check the exit status
if [ $? -ne 0 ]; then
    tput bold
    if [[ $CONTAINER_TOOL == "docker" ]]; then
        printf "\n${RED_TEXT}Error: Docker failed to start. This could be due to a permission issue or Docker not being installed.${RESET_COLOR}\n"
        printf "\nPlease try these steps:\n"
        printf "1. Ensure Docker is installed on your system.\n"
        printf "2. Run: sudo groupadd docker (if not already created)\n"
        printf "3. Run: sudo usermod -aG docker \${USER}\n"
        printf "4. You should log out and log back in so that your group membership is re-evaluated.\n"
        printf "\nIf the issue persists, consider installing SigLens with Podman as an alternative:\n"
        printf "   ./install_script.sh podman\n"
    elif [[ $CONTAINER_TOOL == "podman" ]]; then
        printf "\n${RED_TEXT}Error: Podman failed to start. This could be due to a missing Podman or Podman-compose installation.${RESET_COLOR}\n"
        printf "\nPlease ensure Podman is installed and try again. For Podman, usually, no group adjustments are needed due to its rootless feature.\n"
        printf "\nIf the issue persists, consider installing SigLens with Docker as an alternative:\n"
        printf "   ./install_script.sh docker\n"
    fi
    tput sgr0
    exit 1
fi

echo -e "\n*** Thank you! ***\n"

