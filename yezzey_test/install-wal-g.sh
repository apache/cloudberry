#!/bin/bash
set -ex

# Install latest Go compiler
sudo add-apt-repository ppa:longsleep/golang-backports 
sudo apt update
sudo apt install -y golang-go

# Install lib dependencies
sudo apt install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake

# Fetch project and build
git clone https://github.com/wal-g/wal-g.git
cd wal-g
make deps
make gp_build
mv main/gp/wal-g /usr/bin/wal-g

#Check the installation
wal-g --version
