#! /bin/bash

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
case "$(uname -sr)" in
   Darwin*)
     os="darwin"
     package_manager="brew" ;;
   Ubuntu*|Pop!_OS)
     os="linux"
     package_manager="apt-get" ;;
   Linux*amzn2*)
     os="amazon linux"
     package_manager="yum" ;;
   Linux*)
     os="linux"
     package_manager="apt-get" ;;
   Debian*)
     os="linux"
     package_manager="apt-get" ;;
   Linux\ Mint*)
     os="linux"
     package_manager="apt-get" ;;
   Red\ Hat*)
     os="linux"
     package_manager="yum" ;;
   CentOS*)
     os="linux"
     package_manager="yum" ;;
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
                print_error_and_exit "Need sudo privileges to proceed with the installation."
            fi

            echo -e "Got Sudo access.\n"
        fi
    fi
}

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
        $apt_cmd install docker-ce docker-ce-cli containerd.io || print_error_and_exit "Docker installation failed. Please install docker manually and re-run the command."
    elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
        $sudo_cmd yum install -y amazon-linux-extras
        $sudo_cmd amazon-linux-extras enable docker
        $sudo_cmd yum install -y docker || print_error_and_exit "Docker installation failed. Please install docker manually and re-run the command."
    else
        yum_cmd="$sudo_cmd yum --assumeyes --quiet"
        $yum_cmd install yum-utils
        $sudo_cmd yum-config-manager --add-repo https://download.docker.com/linux/$os/docker-ce.repo
        echo "Installing docker"
        $yum_cmd install docker-ce docker-ce-cli containerd.io || print_error_and_exit "Docker installation failed. Please install docker manually and re-run the command."
    fi
    docker_version=$(docker --version) || print_error_and_exit "Docker is not working correctly. Please install docker manually and re-run the command."
    print_success_message "Docker installed successfully. $docker_version"
}

install_docker_compose() {
    echo "----------Setting up docker compose----------"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update || print_error_and_exit "apt-get update failed."
        $apt_cmd install docker-compose || print_error_and_exit "Docker Compose installation failed."
    elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
        curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose || print_error_and_exit "Downloading Docker Compose binary failed."
        chmod +x /usr/local/bin/docker-compose || print_error_and_exit "Making Docker Compose executable failed."
    elif [[ $package_manager == brew ]]; then
        brew install docker-compose || print_error_and_exit "Docker Compose installation failed."
    else
        echo "---------Docker Compose must be installed manually to proceed---------"
        print_error_and_exit "Docker Compose Not installed"
    fi
    docker_compose_version=$(docker-compose --version) || print_error_and_exit "Docker Compose is not working correctly."
    print_success_message "Docker Compose installed successfully. $docker_compose_version"
}

start_docker() {
    echo -e "\n===> Starting Docker ...\n"
    if [[ $os == "darwin" ]]; then
        open --background -a Docker && while ! docker system info > /dev/null 2>&1; do sleep 1; done || print_error_and_exit "Failed to start Docker"
    else
        if ! $sudo_cmd systemctl is-active docker.service > /dev/null; then
            echo "Starting docker service"
            $sudo_cmd systemctl start docker.service || print_error_and_exit "Failed to start Docker service"
        fi
        if [[ -z $sudo_cmd ]]; then
            if ! docker ps > /dev/null && true; then
                request_sudo
            fi
        fi
    fi
    docker info > /dev/null 2>&1 || print_error_and_exit "Docker did not start correctly."
    print_success_message "Docker started successfully."
}

if ! is_command_present docker; then
    if [[ $package_manager == "apt-get" || $package_manager == "yum" ]]; then
        request_sudo
        install_docker
        install_docker_compose
    elif [[ $os == "darwin" ]]; then
        print_error_and_exit "\nDocker Desktop must be installed manually on Mac OS to proceed. \n You can install Docker from here - https://docs.docker.com/docker-for-mac/install/"
    else
        print_error_and_exit "\nDocker must be installed manually on your machine to proceed. Docker can only be installed automatically on Ubuntu / Redhat. \n You can install Docker from here - https://docs.docker.com/get-docker/"
    fi
fi


start_docker

echo -e "\n----------Pulling the latest docker image for SigLens----------"

curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/docker-compose.yml"

$sudo_cmd docker pull siglens/siglens:${SIGLENS_VERSION} || print_error_and_exit "Failed to pull siglens/siglens:${SIGLENS_VERSION}. Please check your internet connection and Docker installation."
mkdir -p data || print_error_and_exit "Failed to create directory 'data'. Please check your permissions."
chmod a+rwx data || print_error_and_exit "Failed to change permissions for directory 'data'. Please check your file permissions."

mkdir -p logs || print_error_and_exit "Failed to create directory 'logs'. Please check your permissions."
chmod a+rwx logs || print_error_and_exit "Failed to change permissions for directory 'logs'. Please check your file permissions."

print_success_message "\n===> SigLens installation complete"

csi=$(ifconfig 2>/dev/null | grep -o -E '([0-9a-fA-F]{2}:){5}([0-9a-fA-F]{2})' | head -n 1)
if [ -z "$csi" ]; then
  csi=$(hostname)
fi

runtime_os=$(uname)
runtime_arch=$(uname -m)

curl -X POST \
https://api.segment.io/v1/track \
-H 'Content-Type: application/json' \
-H 'Authorization: Basic QlBEam5lZlBWMEpjMkJSR2RHaDdDUVRueWtZS2JEOGM6' \
-d '{
  "userId": "'"$csi"'",
  "event": "install (not running)",
  "properties": {
    "runtime_arch": "'"$runtime_os"'",
    "runtime_os": "'"$runtime_arch"'"
  }
}'

PORT=5122

check_ports() {
    if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || docker ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
        CONTAINER_ID=$(docker ps --format "{{.ID}}:{{.Image}}:{{.Ports}}" | grep "siglens/siglens.*0.0.0.0:${PORT}" | cut -d ":" -f 1 2>/dev/null)
        if [ -n "$CONTAINER_ID" ]; then
            docker stop $CONTAINER_ID
            if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || docker ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
                print_error_and_exit "\nError: Port ${PORT} is already in use."
            fi
        else
            print_error_and_exit "\nError: Port ${PORT} is already in use."
        fi
    fi
    print_success_message "\nStarting Siglens on Port ${PORT}"
}

check_ports

send_events() {
    if $FIRST_RUN; then
        curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz
        if [ $? -ne 0 ]; then
            print_error_and_exit "Failed to download sample log dataset"
        fi
        tar -xvf 2kevents.json.tar.gz || print_error_and_exit "Failed to extract 2kevents.json.tar.gz"
        for i in $(seq 1 20)
        do
            curl -s http://localhost:8081/elastic/_bulk --data-binary "@2kevents.json" -o res.txt
            if [ $? -ne 0 ]; then
                print_error_and_exit "Failed to send sample log dataset"
            fi
        done
        print_success_message "\n Sample log dataset sent successfully"
    else
        echo "Skipping sendevents as this is not the first run"
    fi
}

# Run Docker compose files
UI_PORT=${PORT} WORK_DIR="$(pwd)" SIGLENS_VERSION=${SIGLENS_VERSION} docker-compose -f ./docker-compose.yml up -d || print_error_and_exit "Failed to start Docker Compose"
UI_PORT=${PORT} WORK_DIR="$(pwd)" SIGLENS_VERSION=${SIGLENS_VERSION} docker-compose logs -t --tail 20 >> dclogs.txt
sample_log_dataset_status=$(curl -s -o /dev/null -I -X HEAD -w "%{http_code}" http://localhost:5122/elastic/sample-log-dataset)

if [ "$sample_log_dataset_status" -eq 200 ]; then
    FIRST_RUN=false
elif [ "$sample_log_dataset_status" -eq 404 ]; then
    FIRST_RUN=true
else
    echo "Failed to check sample log dataset status"
    FIRST_RUN=true
fi

send_events

tput bold
print_success_message "\n===> Frontend can be accessed on http://localhost:${PORT}"
echo ""
tput sgr0

if [ $? -ne 0 ]; then
    tput bold
    printf "\n${RED_TEXT}Error: Docker failed to start. This could be due to a permission issue.${RESET_COLOR}"
    printf "\nPlease try these steps:"
    printf "\n1. Run: sudo groupadd docker"
    echo ""
    printf '2. Run: sudo usermod -aG docker ${USER}'
    printf "\n3. You should log out and log back in so that your group membership is re-evaluated\n"
    tput sgr0
    exit 1
fi

echo -e "\n*** Thank you! ***\n"
