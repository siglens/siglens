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
                echo "Need sudo privileges to proceed with the installation."
                exit 1;
            fi

            echo -e "Got Sudo access.\n"
        fi
	fi
}


install_docker() {
    echo "Setting up docker"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update
        $apt_cmd install software-properties-common gnupg-agent
        curl -fsSL "https://download.docker.com/linux/$os/gpg" | $sudo_cmd apt-key add -
        $sudo_cmd add-apt-repository \
            "deb [arch=$arch] https://download.docker.com/linux/$os $(lsb_release -cs) stable"
        $apt_cmd update
        echo "Installing docker"
        $apt_cmd install docker-ce docker-ce-cli containerd.io
    elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
        $sudo_cmd yum install -y amazon-linux-extras
        $sudo_cmd amazon-linux-extras enable docker
        $sudo_cmd yum install -y docker
    else
        yum_cmd="$sudo_cmd yum --assumeyes --quiet"
        $yum_cmd install yum-utils
        $sudo_cmd yum-config-manager --add-repo https://download.docker.com/linux/$os/docker-ce.repo
        echo "Installing docker"
        $yum_cmd install docker-ce docker-ce-cli containerd.io

    fi

}

install_docker_compose() {
  echo "Setting up docker compose"
  if [[ $package_manager == apt-get ]]; then
    apt_cmd="$sudo_cmd apt-get --yes --quiet"
    $apt_cmd update
    echo "Installing docker compose"
    $apt_cmd install docker-compose
  elif [[ $package_manager == yum && $os == 'amazon linux' ]]; then
    echo "Installing docker compose"
    sudo yum install -y epel-release
    sudo yum install -y docker-compose
  elif [[ $package_manager == brew ]]; then
    echo "Installing docker compose"
    brew install docker-compose
  else
    echo "Docker Compose must be installed manually to proceed. "
    echo "docker_compose_not_installed"
    exit 1

  fi
}

start_docker() {
    echo -e "\n===> Starting Docker ...\n"
    if [[ $os == "darwin" ]]; then
        open --background -a Docker && while ! docker system info > /dev/null 2>&1; do sleep 1; done
    else 
        if ! $sudo_cmd systemctl is-active docker.service > /dev/null; then
            echo "Starting docker service"
            $sudo_cmd systemctl start docker.service
        fi
        if [[ -z $sudo_cmd ]]; then
            if ! docker ps > /dev/null && true; then
                request_sudo
            fi
        fi
    fi
}


if ! is_command_present docker; then
    if [[ $package_manager == "apt-get" || $package_manager == "yum" ]]; then
        request_sudo
        install_docker
        install_docker_compose
    elif [[ $os == "darwin" ]]; then
        echo "Docker Desktop must be installed manually on Mac OS to proceed. "
        echo "https://docs.docker.com/docker-for-mac/install/"
        echo "docker_not_installed"
        exit 1
    else
        echo ""
        echo "Docker must be installed manually on your machine to proceed. Docker can only be installed automatically on Ubuntu / Redhat "
        echo "https://docs.docker.com/get-docker/"
        echo "docker_not_installed"
        exit 1
    fi
fi


start_docker

echo -e "\n===> Pulling the latest docker image for SigLens"

curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/docker-compose.yml"
curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/otel-collector-config.yaml"

$sudo_cmd docker pull siglens/siglens:${SIGLENS_VERSION}
mkdir -p data
echo ""
echo -e "\n===> SigLens installation complete"

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

INITIAL_PORT=5122
START_PORT=5122
END_PORT=5122

check_ports() {
    
    if lsof -Pi :$INITIAL_PORT -sTCP:LISTEN -t > /dev/null || docker ps --format "{{.Ports}}" | grep -q "0.0.0.0:${INITIAL_PORT}->"; then
        for port in $(seq $START_PORT $END_PORT); do
            if lsof -Pi :$port -sTCP:LISTEN -t > /dev/null || docker ps --format "{{.Ports}}" | grep -q "0.0.0.0:$port->"; then
                continue
            else
                echo $port
                return 0
            fi
        done
        echo "-1"
        return 1
    else
        echo "${INITIAL_PORT}"
        return 0
    fi
}

UI_PORT=$(check_ports)

if [ ${UI_PORT} == "-1" ]; then
    echo ""
    tput bold
    printf "${RED_TEXT}Error: Port ${INITIAL_PORT} is already in use.\n"
    tput sgr0
    exit 1
fi

echo -e "\n===> In case ports 80 and 8081 are in use change the settings as follows"
echo -e "ingestPort(8081) and queryPort(80) can be changed using in server.yaml."
echo -e "queryPort(80) can also be changed by setting the environment variable $PORT."

tput bold
printf "\n===> ${GREEN_TEXT}Frontend can be accessed on http://localhost:${UI_PORT}${RESET_COLOR}"
echo ""
tput sgr0

# Run Docker compose files
UI_PORT=${UI_PORT} WORK_DIR="$(pwd)" SIGLENS_VERSION=${SIGLENS_VERSION} docker-compose -f ./docker-compose.yml up -d

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
