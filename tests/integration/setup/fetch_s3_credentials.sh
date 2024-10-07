#!/bin/bash

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

get_s3_endpoint(){
    # Get S3 endpoint from MinIO
    kubectl get service minio -n minio-operator -o jsonpath='{.spec.clusterIP}' 
}

get_s3_access_key(){
  # Print the S3 Access Key by reading it from K8s secret or by outputting the default value
    kubectl get secret -n minio-operator microk8s-user-1 &> /dev/null
    if [ $? -eq 0 ]; then
        # echo "Use access-key from secret"
        access_key=$(kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_ACCESS_KEY}' | base64 -d)
    else
        # echo "use default access-key"
        access_key="minio"
    fi
    echo "$access_key"
}


get_s3_secret_key(){
  # Print the S3 Secret Key by reading it from K8s secret or by outputting the default value
    kubectl get secret -n minio-operator microk8s-user-1 &> /dev/null
    if [ $? -eq 0 ]; then
      # echo "Use access-key from secret"
      secret_key=$(kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_SECRET_KEY}' | base64 -d)
    else
      # echo "use default access-key"
      secret_key="minio123"
    fi
    echo "$secret_key"
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
echo "$(get_s3_access_key)"
echo "$(get_s3_secret_key)"