#!/bin/bash

echo "Installing spark-client snap..."
sudo snap install spark-client --channel edge

mkdir -p ~/.kube
sudo microk8s config | tee ~/.kube/config >> /dev/null

NAMESPACE=$1
SERVICE_ACCOUNT=$2

echo "Creating namespace $NAMESPACE..."
kubectl create namespace $NAMESPACE

echo "Creating service account $SERVICE_ACCOUNT..."
spark-client.service-account-registry create --username $SERVICE_ACCOUNT --namespace $NAMESPACE
