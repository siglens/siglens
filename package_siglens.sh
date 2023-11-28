#! /bin/bash

# Extract the version number from pkg/config/version.go by getting everything
# inside the quotes. Use -n to supress printing each line, and p to print the
# modified line.
SIGLENS_VERSION=$(sed -n 's/const SigLensVersion = "\(.*\)"/\1/p' pkg/config/version.go)

platforms=("$(go env GOOS)/$(go env GOARCH)")
add_playground=""
while getopts p:b:g: flag
do
    case "${flag}" in
        p) IFS=',' read -r -a platforms <<< "${OPTARG}";;
        g) add_playground="${OPTARG}";;
    esac
done

echo "Compiling SigLens for ${platforms[@]}. Using version number ${SIGLENS_VERSION}, which was read from pkg/config/version.go. Uploading to S3 bucket=${upload_bucket}"
for platform in "${platforms[@]}"; do
    platform_split=(${platform//\// })
    export GOOS=${platform_split[0]}
    export GOARCH=${platform_split[1]}
    if [ ${GOOS} = "linux" ]; then
        if [ ${GOARCH} = "amd64" ]; then
            CC="zig cc -target x86_64-linux-gnu"
            CXX="zig cc -target x86_64-linux-gnu"
            export CC=$CC
            export CXX=$CXX
            export CGO_ENABLED=1
            echo "Compiling SigLens for CGO_ENABLED=1, GOOS=${GOOS} and GOARCH=${GOARCH}."
            go build -o siglens cmd/siglens/main.go
        fi
        if [ ${GOARCH} = "arm64" ]; then
            CC="zig cc -target aarch64-linux-musl"
            CXX="zig cc -target aarch64-linux-musl"
            CGO_CFLAGS="-D_LARGEFILE64_SOURCE"
            export CC=$CC
            export CXX=$CXX
            export CGO_ENABLED=1
            export CGO_CFLAGS=$CGO_CFLAGS
            echo "Compiling SigLens for CGO_ENABLED=1, GOOS=${GOOS} and GOARCH=${GOARCH}."
            go build -o siglens cmd/siglens/main.go
        fi
    fi
    if [ ${GOOS} = "darwin" ]; then
        if [ ${GOARCH} = "arm64" ]; then
            export CC="clang -arch ${GOARCH}"
            export CGO_ENABLED=1
            echo "Compiling SigLens for GOOS=${GOOS} and GOARCH=${GOARCH}"
            go build -o siglens cmd/siglens/main.go
        fi
        if [ ${GOARCH} = "amd64" ]; then
            export CC="clang -arch x86-64"
            export CGO_ENABLED=1
            echo "Compiling SigLens for GOOS=${GOOS} and GOARCH=${GOARCH}"
            go build -o siglens cmd/siglens/main.go
        fi
        if [ ${GOARCH} = "x86_64" ]; then
            export CC="clang -arch x86-64"
            export CGO_ENABLED=1
            echo "Compiling SigLens for GOOS=${GOOS} and GOARCH=${GOARCH}"
            go build -o siglens cmd/siglens/main.go
        fi
    fi

    if [ $? -eq 0 ]
    then
        echo "Successfully built siglens binary for GOOS=${GOOS} and GOARCH=${GOARCH}"
    else
        echo "Could not create siglens binary for GOOS=${GOOS} and GOARCH=${GOARCH}"
        exit 1
    fi

    dirname="siglens-${SIGLENS_VERSION}-${GOOS}-${GOARCH}/"
    mkdir ${dirname}
    mv siglens ${dirname}

    cp -r static ${dirname}/

    if [ "$add_playground" == "true" ]; then
        echo "-----------------------------------------------------"
        echo "using playground.yaml as config file"
        cp playground.yaml ${dirname}
    else
        echo "-----------------------------------------------------"
        echo "using server.yaml as config file"
        cp server.yaml ${dirname}
    fi

    cp README.md ${dirname}

    outputname="siglens-${SIGLENS_VERSION}-${GOOS}-${GOARCH}.tar.gz"
    echo "Building tar archive at ${outputname}..."
    tar -czf ${outputname} ${dirname}
    rm -rf ${dirname}
done
