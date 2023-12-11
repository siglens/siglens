#! /bin/bash

# Extract the version number from pkg/config/version.go by getting everything
# inside the quotes. Use -n to supress printing each line, and p to print the
# modified line.
SIGLENS_VERSION=`\
    curl  --silent "https://api.github.com/repos/siglens/siglens/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"([^"]+)".*/\1/'`

sudo_cmd=""

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

wget "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
$sudo_cmd docker pull siglens/siglens:${SIGLENS_VERSION}
$sudo_cmd mkdir data
echo ""
echo -e "\n===> SigLens installation complete"

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

echo ""
tput bold
echo -e "\n===> ${bold}Run the following command to start the siglens server"
tput sgr0
echo "====================**************************************************============================"
echo ""
echo -e "docker run -it --mount type=bind,source="$(pwd)"/data,target=/siglens/data \
    --mount type=bind,source="$(pwd)"/server.yaml,target=/siglens/server.yaml \
    -p 8081:8081 -p 80:80 siglens/siglens:0.1.0"
echo ""
echo "====================**************************************************============================"

echo -e "\n===> In case ports 80 and 8081 are in use change the settings as follows"
echo -e "ingestPort(8081) and queryPort(80) can be changed using in server.yaml."
echo -e "queryPort(80) can also be changed by setting the environment variable $PORT."
echo ""
echo -e "To be able to query data across restarts, set ssInstanceName in server.yaml."
echo ""
echo -e "The target for the data directory mounting should be the same as the data directory (dataPath configuration) in server.yaml"
tput bold
echo -e "\n===> ${bold}Frontend can be accessed on http://localhost:80"
echo ""
tput sgr0
echo -e "\n*** Thank you! ***\n"