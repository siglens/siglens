#! /bin/bash

SIGCLIENT_VERSION=$(cat SIGCLIENT_VERSION)
platforms=("$(go env GOOS)/$(go env GOARCH)")
upload_bucket=""

while getopts p:b: flag
do
    case "${flag}" in
        p) IFS=',' read -r -a platforms <<< "${OPTARG}";;
        b) upload_bucket="${OPTARG}";;
    esac
done

echo "Compiling Sigclient for ${platforms[@]}. Using version number ${SIGCLIENT_VERSION}, which was read from SIGCLIENT_VERSION. Uploading to S3 bucket=${upload_bucket}"
for platform in "${platforms[@]}"; do
    platform_split=(${platform//\// })
    export GOOS=${platform_split[0]}
    export GOARCH=${platform_split[1]}
    echo "Compiling Sigclient for GOOS=${GOOS} and GOARCH=${GOARCH}."
    go build -o load-test main.go
    if [ $? -eq 0 ]
    then
        echo "Successfully built load-test binary for GOOS=${GOOS} and GOARCH=${GOARCH}"
    else
        echo "Could not create load-test binary for GOOS=${GOOS} and GOARCH=${GOARCH}"
        exit 1
    fi

    dirname="load-test-${SIGCLIENT_VERSION}-${GOOS}-${GOARCH}/"
    mkdir ${dirname}
    mv load-test ${dirname}

    outputname="load-test-${SIGCLIENT_VERSION}-${GOOS}-${GOARCH}.tar.gz"
    echo "Building tar archive at ${outputname}..."
    tar -czf ${outputname} ${dirname}
    rm -rf ${dirname}
    if [ "$upload_bucket" == "" ]; then
        echo "-----------------------------------------------------"
        echo "To upload tar file to S3, run package_sigclient.sh -b s3://bucket_name"
        echo "Packaged Sigclient to ${outputname}"
        echo "-----------------------------------------------------"
    else
        echo "Uploading file to S3 bucket ${upload_bucket}"
        aws s3 cp ${outputname} ${upload_bucket}
        echo "-----------------------------------------------------"
        echo "Uploaded ${outputname} to ${upload_bucket}"
        echo "-----------------------------------------------------"
    fi
done
