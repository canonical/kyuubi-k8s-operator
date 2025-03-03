#!/bin/bash

# Ensure the script is running as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root"
    exit 1
fi

# Python setup
export DEBIAN_FRONTEND=noninteractive
apt-get update -yqq
apt-get install -yqq python3-pip
apt-get install -yqq python3-dev
apt-get install -yqq libpq-dev
# pip3 install tox
export HOME=/root
# Restart the systemd service
systemctl restart snapd.service
sleep 30
# Microk8s setup
echo "Installing microk8s: $MICROK8S_CHANNEL"
snap install microk8s --channel=$MICROK8S_CHANNEL --classic
usermod -a -G microk8s ubuntu
newgrp microk8s