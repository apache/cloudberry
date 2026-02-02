#!/bin/bash
set -ex

# Install latest Go compiler
sudo add-apt-repository ppa:longsleep/golang-backports 
sudo apt update
sudo apt install -y golang-go

# Install lib dependencies
sudo apt install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake

# Fetch project and build
git clone https://github.com/open-gpdb/yproxy.git
cd yproxy
make build

mv devbin/yproxy /usr/bin/yproxy

#Check the installation
yproxy --version
