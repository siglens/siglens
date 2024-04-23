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


# Extract the latest version number from the SigLens GitHub repository
SIGLENS_VERSION=`\
    curl  --silent "https://api.github.com/repos/siglens/siglens/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"([^"]+)".*/\1/'`

sudo_cmd=""

# Text color
RED_TEXT='\e[31m'
GREEN_TEXT='\e[32m'
BLUE_TEXT="\033[0;34m"
RESET_COLOR='\e[0m'

# Check sudo permissions
if (( $EUID != 0 )); then
    echo "===> Running installer with non-sudo permissions."
    echo "     In case of any failure or prompt, run the script with sudo privileges."
    echo ""
else
    sudo_cmd="sudo"
fi

is_command_present() {
    type "$1" >/dev/null 2>&1
}

dist_id=""
dist_version=""

# Check if the command is available
if is_command_present lsb_release; then
    dist_id=$(lsb_release -is)
    dist_version=$(lsb_release -rs)
elif  [ -f /etc/os-release ]; then
    . /etc/os-release
    dist_id=$NAME
    dist_version=$VERSION_ID
elif is_command_present distro; then
    dist_id=$(distro | awk '{print $1}')
    dist_version=$(distro | awk '{print $2}')
else 
    echo "Couldn't determine the OS distribution"
fi

# convert dist_id to lowercase
if [[ -n $dist_id ]]; then
    dist_id=$(echo "$dist_id" | tr '[:upper:]' '[:lower:]')
fi

# Check if dist_id contains 'red hat' and change it to 'rhel'
if [[ $dist_id == *"red hat"* ]]; then
    dist_id="rhel"
fi

echo "===> Detected OS: $dist_id $dist_version"

os=""
package_manager=""
case "$(uname -sr)" in
    Darwin*)
        os="darwin"
        package_manager="brew" ;;
    Linux*amzn2*)
        os="amazon linux"
        package_manager="yum" ;;
    *Fedora*|*CentOS*|*Red\ Hat*)
        os="linux"
        package_manager="yum" ;;
    Ubuntu*|Pop!_OS|Debian*|Linux\ Mint*)
        os="linux"
        package_manager="apt-get" ;;
    Linux*)
        os="linux"
        if is_command_present apt-get; then
            package_manager="apt-get"
        elif is_command_present yum; then
            package_manager="yum"
        else 
            os="Package Manager Not Found"
            echo 'Unsupported package manager'
            exit 1
        fi ;;
    *)
        os="Not Found"
        echo 'Unsupported OS'
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
        "runtime_os": "'"$os"'",
        "runtime_arch": "'"$arch"'",
        "package_manager": "'"$package_manager"'",
        "message": "'"$message"'",
        "ip": "'"$ip"'",
        "city": "'"$city"'",
        "region": "'"$region"'",
        "country": "'"$country"'"
    }
    }' > /dev/null 2>&1
}

print_error_and_exit() {
    printf "${RED_TEXT}$1${RESET_COLOR}\n"
    exit 1
}

print_success_message() {
    printf "${GREEN_TEXT}$1${RESET_COLOR}\n"
}

print_info_message() {
    printf "${BLUE_TEXT}$1${RESET_COLOR}\n"
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
    dist=$os
    if ! [[ -z $dist_id ]]; then
        dist=$dist_id
    fi
    echo "===> Installing Docker on $dist using $package_manager"
    if [[ $package_manager == apt-get ]]; then
        apt_cmd="$sudo_cmd apt-get --yes --quiet"
        $apt_cmd update
        $apt_cmd install software-properties-common gnupg-agent
        curl -fsSL "https://download.docker.com/linux/$dist/gpg" | $sudo_cmd apt-key add -
        $sudo_cmd add-apt-repository \
            "deb [arch=$arch] https://download.docker.com/linux/$dist $(lsb_release -cs) stable"
        $apt_cmd update
        echo "Installing docker"
        $apt_cmd install docker-ce docker-ce-cli containerd.io || {
            post_event "install_failed" "install_docker: Docker installation failed during apt-get install on $dist"
            print_error_and_exit "install_docker: Docker installation failed during apt-get install on $dist"
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
        $sudo_cmd yum-config-manager --add-repo https://download.docker.com/linux/$dist/docker-ce.repo
        echo "Installing docker"
        $yum_cmd install docker-ce docker-ce-cli containerd.io || {
            post_event "install_failed" "install_docker: Docker installation failed during yum install on $dist"
            print_error_and_exit "install_docker: Docker installation failed during yum install on $dist"
        }
    fi
    docker_version=$(docker --version) || {
        post_event "install_failed" "install_docker: Failed to check docker version post-installation on $dist"
        print_error_and_exit "Docker is not working correctly. Please install docker manually and re-run the command."
    }
    print_success_message "Docker installed successfully. $docker_version"
}

install_docker_compose() {
    echo "----------Setting up docker compose----------"
    request_sudo
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
        sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose || {
            post_event "install_failed" "install_docker_compose: Downloading Docker Compose binary failed during Docker Compose setup"
            print_error_and_exit "Downloading Docker Compose binary failed."
        }
        sudo chmod +x /usr/local/bin/docker-compose || {
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
    request_sudo
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
        if [ "$SERVERNAME" = "playground" ]; then
            curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/playground.yaml"
        fi
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/docker-compose.yml"
        curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/ssmetrics-otel-collector-config.yaml"
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
    case $package_manager in
        apt-get)
            apt_cmd="$sudo_cmd apt-get --yes --quiet"
            $apt_cmd update || { 
                print_error_and_exit "apt-get update failed." 
            }
            $apt_cmd install software-properties-common || { 
                print_error_and_exit "Failed to install software-properties-common." 
            }
            
            . /etc/os-release
            if [[ "$dist_id" == "ubuntu" ]]; then
                repo_url="http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_$VERSION_ID/"
            else
                # For other Debian-based systems
                repo_url="http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/Debian_$VERSION_ID/"
            fi
            
            $sudo_cmd sh -c "echo 'deb $repo_url /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list" || {
                print_error_and_exit "Failed to add Podman repository." 
            }
            wget -nv "$repo_url/Release.key" -O Release.key || { 
                print_error_and_exit "Failed to download Podman repository key." 
            }
            $sudo_cmd apt-key add - < Release.key || { 
                print_error_and_exit "Failed to add Podman repository key."
            }
            rm -f Release.key
            $apt_cmd update || { 
                print_error_and_exit "Failed to update package lists after adding Podman repository." 
            }
            $apt_cmd install podman || { 
                print_error_and_exit "Podman installation failed." 
            }
            ;;
        yum)
            # Update package lists
            $sudo_cmd yum update -y || { 
                print_error_and_exit "Failed to update package lists." 
            }

            # Amazon Linux specific handling
            if grep -q 'amzn' /etc/os-release; then
                echo "Detected Amazon Linux. Installing Podman from Amazon Linux Extras..."
                $sudo_cmd yum install -y amazon-linux-extras
                $sudo_cmd amazon-linux-extras enable podman
                $sudo_cmd yum install -y podman || {
                    print_error_and_exit "Failed to install Podman from Amazon Linux Extras."
                }
            else
                # For other yum-based distros, attempt to add and use EPEL if Podman isn't available
                if ! $sudo_cmd yum list podman >/dev/null 2>&1; then
                    echo "Adding EPEL repository for Podman..."
                    $sudo_cmd yum install -y epel-release || { 
                        print_error_and_exit "Failed to install EPEL repository." 
                    }
                    $sudo_cmd yum update -y || { 
                        print_error_and_exit "Failed to update package lists after adding EPEL repository." 
                    }
                fi

                # Install Podman
                $sudo_cmd yum install -y podman || { 
                    print_error_and_exit "Podman installation failed." 
                }
            fi
            ;;
        brew)
            # On macOS, Podman should be available via Homebrew
            brew install podman || { 
                print_error_and_exit "Podman installation failed." 
            }
            ;;
        *)
            print_error_and_exit "Unsupported package manager: $package_manager."
            ;;
    esac
    podman_version=$(podman --version) || { 
        print_error_and_exit "Podman is not working correctly." 
    }
    print_success_message "Podman installed successfully. $podman_version"
}


# Install Python and pip if they are not present
install_python_and_pip() {
    echo "Checking and installing Python and pip if necessary..."

    # Check for Python3 and install if not present
    if ! type python3 >/dev/null 2>&1; then
        echo "Python 3 not found. Installing Python 3..."
        case $package_manager in
            apt-get)
                $sudo_cmd apt-get install -y python3 || {
                    print_error_and_exit "Failed to install Python 3. Please check your system's repositories."
                }
                ;;
            yum)
                $sudo_cmd yum install -y python3 || {
                    print_error_and_exit "Failed to install Python 3. Please check your system's repositories."
                }
                ;;
            brew)
                brew install python3 || {
                    print_error_and_exit "Failed to install Python 3. Please check your system's repositories."
                }
                ;;
            *)
                print_error_and_exit "Unsupported package manager for installing Python 3."
                ;;
        esac
    else
        echo "Python 3 is already installed."
    fi

    # Check for pip3 and install if not present
    if ! type pip3 >/dev/null 2>&1; then
        echo "pip for Python 3 not found. Installing pip3..."
        case $package_manager in
            apt-get)
                $sudo_cmd apt-get install -y python3-pip || {
                    print_error_and_exit "Failed to install pip for Python 3. Please check your system's repositories."
                }
                ;;
            yum)
                $sudo_cmd yum install -y python3-pip || {
                    print_error_and_exit "Failed to install pip for Python 3. Please check your system's repositories."
                }
                ;;
            brew)
                brew install python3-pip || {
                    print_error_and_exit "Failed to install pip for Python 3. Please check your system's repositories."
                }
                ;;
            *)
                print_error_and_exit "Unsupported package manager for installing pip3."
                ;;
        esac
    else
        echo "pip3 is already installed."
    fi
}


# Install podman-compose
install_podman_compose() {
    echo "Installing podman-compose..."

    # Ensure Python and pip are installed
    install_python_and_pip

    # Install podman-compose with pip
    $sudo_cmd pip3 install podman-compose || {
        # trying to install podman-compose with pipx
        echo "Failed to install podman-compose using pip. Attempting to install using pipx..."

        # Attempt to install pipx if not already installed
        if ! type pipx >/dev/null 2>&1; then
            echo "pipx not found. Attempting to install pipx..."
            $sudo_cmd apt install -y pipx && pipx ensurepath || {
                print_error_and_exit "Failed to install pipx. Please check your Python/pip configuration."
            }
        else
            echo "pipx is already installed."
        fi

        # Use pipx to install podman-compose
        pipx install podman-compose || {
            print_error_and_exit "Failed to install podman-compose using pipx."
        }

        print_info_message "podman-compose installed successfully using pipx."

        print_info_message "Please open a new terminal or re-login to apply changes."
        print_info_message "After that, Please run the install script again to continue the installation by running the below commands."
        print_info_message "export CONTAINER_TOOL=podman"
        print_info_message "./install.sh"

        exit 0
    }
    print_success_message "podman-compose installed successfully."
}

# Fetch and set up the custom network configuration file
get_podman_custom_network_configuration() {
    echo "Setting up custom Podman network configuration..."
    curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/podman-network_siglens.conflist" || {
        print_error_and_exit "Failed to download custom network configuration file."
    }

    # Detect macOS
    if [[ $os == "darwin" ]]; then
        echo "Running on macOS. Attempting to Start Podman VM."
        # Initialize and start Podman VM if not already done
        podman machine init  # This command is idempotent
        podman machine start
    fi

    # For running as root
    $sudo_cmd cp podman-network_siglens.conflist /etc/cni/net.d/ || {
        echo "Failed to move custom network configuration file to /etc/cni/net.d"
    }

    # For running as non-root
    $sudo_cmd mv podman-network_siglens.conflist ~/.config/cni/net.d/ || {
        echo "Failed to move custom network configuration file to ~/.config/cni/net.d"
    }

    if ! podman network inspect podman-network_siglens >/dev/null 2>&1; then
        podman network create podman-network_siglens || {
            print_error_and_exit "Failed to create custom Podman network."
        }
    fi
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

pull_siglens_podman_image() {
    echo -e "\n----------Pulling the latest Podman image for SigLens----------"

    # Check if the user wants to use a local compose file
    if [ "$USE_LOCAL_PODMAN_COMPOSE" != true ]; then
        # Download playground.yaml
        if [ "$SERVERNAME" = "playground" ]; then
            curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/playground.yaml"
        fi

        # Download server.yaml
        if ! curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/server.yaml"; then
            print_error_and_exit "Failed to download server.yaml."
        fi
        
        # Download podman-compose.yml
        if ! curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/podman-compose.yml"; then
            print_error_and_exit "Failed to download podman-compose.yml."
        fi
        
        # Download ssmetrics-otel-collector-config.yaml
        if ! curl -O -L "https://github.com/siglens/siglens/releases/download/${SIGLENS_VERSION}/ssmetrics-otel-collector-config.yaml"; then
            print_info_message "Failed to download ssmetrics-otel-collector-config.yaml."
        fi

        # Attempt to read files and change permissions if necessary
        for file in server.yaml ssmetrics-otel-collector-config.yaml; do   
            echo "Attempting to change permissions........ $file"
            if ! $sudo_cmd chmod 644 "$file"; then
                print_error_and_exit "Failed to change permissions for $file."
            fi
            echo "Permissions changed for $file."
        done

        echo "Pulling the latest Podman image for SigLens from upstream"
        if ! $sudo_cmd podman pull $IMAGE_NAME; then
            print_error_and_exit "Failed to pull $IMAGE_NAME. Please check your internet connection and Podman installation."
        fi
    fi

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
        elif [[ $os == "darwin" ]]; then
            post_event "install_failed" "Docker Desktop not installed on Mac OS. Automatic installation is not supported."
            print_error_and_exit "\nDocker Desktop must be installed manually on Mac OS to proceed. \n You can install Docker from here - https://docs.docker.com/docker-for-mac/install/"
        else
            post_event "install_failed" "Docker not installed. Automatic installation is only supported on Ubuntu / Redhat."
            print_error_and_exit "\nDocker must be installed manually on your machine to proceed. Docker can only be installed automatically on Ubuntu / Redhat. \n You can install Docker from here - https://docs.docker.com/get-docker/"
        fi
    fi

    if ! is_command_present docker-compose; then
        if [[ $package_manager == "apt-get" || $package_manager == "yum" ]]; then
            request_sudo
            install_docker_compose
        else
            post_event "install_failed" "Docker Compose not installed. Automatic installation is only supported on Ubuntu / Redhat."
            print_error_and_exit "\nDocker Compose must be installed manually on your machine to proceed. Docker Compose can only be installed automatically on Ubuntu / Redhat. \n You can install Docker Compose from here - https://docs.docker.com/compose/install/"
        fi
    fi

    IMAGE_NAME="${DOCKER_IMAGE_NAME:-siglens/siglens:${SIGLENS_VERSION}}"
    COMPOSE_FILE="${DOCKER_COMPOSE_FILE:-docker-compose.yml}"

start_docker_with_timeout() {
    start_docker &
    start_docker_pid=$!

    # Wait for up to 180 seconds for start_docker to finish
    for i in {1..180}; do
        if ! ps -p $start_docker_pid > /dev/null; then
            wait $start_docker_pid
            exit_code=$?
            if [ $exit_code -ne 0 ]; then
                print_error_and_exit "Docker failed to start"
            fi
            break
        fi
        if (( i % 30 == 0 )); then
            echo "Attempting to start docker... ($((i / 60)) minute(s))"
        fi
        sleep 1
    done

    # If docker is not up after 180 seconds, print an error message and exit
    docker info > /dev/null 2>&1 || {
        post_event "install_failed" "start_docker_with_timeout: Failed to retrieve Docker info after 180 seconds"
        print_error_and_exit "Docker failed to start. Pleas start docker and try again"
    }
}

    start_docker_with_timeout

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

csi=$(ifconfig 2>/dev/null | grep -o -E --color='never' '([0-9a-fA-F]{2}:){5}([0-9a-fA-F]{2})' | head -n 1)
if [ -z "$csi" ]; then
  csi=$(hostname)
fi

runtime_os=$(uname)
runtime_arch=$(uname -m)

check_ports() {
    PORT=$1
    IMAGE=$2
    if lsof -Pi :$PORT -sTCP:LISTEN -t > /dev/null || $CONTAINER_TOOL ps --format "{{.Ports}}" | grep -q "0.0.0.0:${PORT}->"; then
        CONTAINER_ID=$($CONTAINER_TOOL ps --format "{{.ID}}:{{.Image}}:{{.Ports}}" | grep "${IMAGE}.*0.0.0.0:${PORT}" | cut -d ":" -f 1 2>/dev/null)
        if [ -n "$CONTAINER_ID" ]; then
            $CONTAINER_TOOL stop $CONTAINER_ID > /dev/null 2>&1
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

check_ports 5122 "siglens/siglens"
check_ports 8081 "siglens/siglens"
check_ports 8080 "jaegertracing/example-hotrod"
check_ports 2222 "otel/opentelemetry-collector"
check_ports 4318 "otel/opentelemetry-collector"

send_events() {
    curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz
    if [ $? -ne 0 ]; then
        post_event "install_failed" "send_events: Failed to download sample log dataset from https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz"
        print_error_and_exit "Failed to download sample log dataset"
    fi
    tar -xvf 2kevents.json.tar.gz --warning=no-unknown-keyword || {
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

# Check if CONFIG_FILE is set and not empty
if [ -n "${CONFIG_FILE}" ]; then
    CFILE=${CONFIG_FILE}
# Check if the script is running in playground environment
elif [ "$SERVERNAME" = "playground" ]; then
    echo "Running in playground environment. Using playground.yaml."
    CFILE="playground.yaml"
fi

print_success_message "\n Starting Siglens with image: ${IMAGE_NAME}"
CSI=${csi} UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${IMAGE_NAME} ${CONTAINER_TOOL}-compose -f $COMPOSE_FILE up -d || {
    post_event "install_failed" "Failed to start $CONTAINER_TOOL Compose on $os with $COMPOSE_FILE"
    print_error_and_exit "Failed to start $CONTAINER_TOOL Compose"
}
CSI=${csi} UI_PORT=${UI_PORT} CONFIG_FILE=${CFILE} WORK_DIR="$(pwd)" IMAGE_NAME=${IMAGE_NAME} $CONTAINER_TOOL-compose logs -t --tail 20 >> ${CONTAINER_TOOL}_logs.txt

# Create .env file for docker-compose down
if [[ $CONTAINER_TOOL == "docker" ]]; then
request_sudo
sudo cat << EOF > .env
    IMAGE_NAME=${IMAGE_NAME}
    UI_PORT=${UI_PORT}
    CONFIG_FILE=${CFILE}
    WORK_DIR="$(pwd)"
    CSI=${csi}
EOF
fi


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

