#!/bin/bash

set -x

pipx install tox
pipx ensurepath
sudo microk8s status --wait-ready
mkdir ~/.kube
mkdir ~/workdir
sudo microk8s config > ~/.kube/config
sudo microk8s enable hostpath-storage dns rbac nvidia
sudo microk8s status --wait-ready

while ! sudo microk8s.kubectl logs -n gpu-operator-resources -l app=nvidia-operator-validator | grep "all validations are successful"
do
  echo "------------------------------------------------------------------------------------------"
  echo "waiting for validations"
  sudo microk8s.kubectl get pods -A
  sudo microk8s.kubectl logs -n kube-system -l k8s-app=hostpath-provisioner
  sudo microk8s.kubectl describe pod -n gpu-operator-resources nvidia-operator-validator
  sudo microk8s.status
  sleep 60
  echo "------------------------------------------------------------------------------------------"
done


mkdir -p ~/.local/share/juju
juju add-k8s mk8s --client
juju bootstrap mk8s mk8s --agent-version 3.6.9

set +x
