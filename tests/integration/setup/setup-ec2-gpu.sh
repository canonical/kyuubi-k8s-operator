#!/bin/bash

set -x

pipx install tox
pipx ensurepath

mkdir -p ~/.local/share/juju
juju add-k8s mk8s --client
juju bootstrap mk8s mk8s --agent-version 3.6.9

set +x
