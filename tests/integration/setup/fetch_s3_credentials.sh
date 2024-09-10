#!/bin/bash

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

# Use static credentials from MinIO
ACCESS_KEY=minio
SECRET_KEY=minio123

get_s3_endpoint(){
    # Get S3 endpoint from MinIO
    kubectl get service minio -n minio-operator -o jsonpath='{.spec.clusterIP}' 
}

wait_and_retry(){
    # Retry a command for a number of times by waiting a few seconds.

    command="$@"
    retries=0
    max_retries=50
    until [ "$retries" -ge $max_retries ]
    do
        $command &> /dev/null && break
        retries=$((retries+1)) 
        echo "Trying to execute command ${command}..."
        sleep 5
    done

    # If the command was not successful even on maximum retries
    if [ "$retries" -ge $max_retries ]; then
        echo "Maximum number of retries ($max_retries) reached. ${command} returned with non zero status."
        exit 1
    fi
}

# Wait for `minio` service to be ready and S3 endpoint to be available
wait_and_retry get_s3_endpoint
S3_ENDPOINT="http://$(get_s3_endpoint)"

echo $S3_ENDPOINT
echo $ACCESS_KEY
echo $SECRET_KEY