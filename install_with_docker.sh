#!/usr/bin/env bash

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
   Linux*)
     os="linux" 
     package_manager="apt-get" ;;
   Ubuntu*|Pop!_OS)
     os="linux"
     package_manager="apt-get" ;;
   Amazon\ Linux*)
     os="amazon linux"
     package_manager="yum" ;;
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
elif  [[ $arch == arm* ]]; then
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

echo -e "\n===>     Pulling the latest docker image for SigLens"

wget https://sigscalr-configs.s3.amazonaws.com/1.1.31/server.yaml
$sudo_cmd docker pull siglens/siglens:0.1.0 
$sudo_cmd mkdir data
echo ""
echo -e "\n===> SigLens installation complete"
echo ""
echo -e "\n===> Run the following command to start the siglens server"
echo -e "\n docker run -it --mount type=bind,source="$(pwd)"/data,target=/siglens/data \
    --mount type=bind,source="$(pwd)"/server.yaml,target=/siglens/server.yaml \
    -p 8081:8081 -p 80:80 siglens/siglens:0.1.0"
echo ""
echo -e "\n===> Frontend can be accessed on http://localhost:80"
echo ""
echo -e "\n*** Thank you! ***\n"