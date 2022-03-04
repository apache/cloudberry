#!/bin/bash

# Script to install build & test dependencies
# Ideally all dependencies should exist in the docker image. Use this script to install them only
# if it is more difficult to change it in the image side.
# Download the dependencies with concourse resources as much as possible, then we could benifit from
# concourse's resource cache system.

set -eox

_install_cmake() {
    # cmake_new to avoid name collision with the docker image.
    local cmake_home="/opt/cmake_new"
    if [ -e "${cmake_home}" ]; then
        echo "cmake might have been installed in ${cmake_home}"
        return
    fi
    echo "Installing cmake to ${cmake_home}..."
    pushd bin_cmake
    mkdir -p "${cmake_home}"
    sh cmake-*-linux-x86_64.sh --skip-license --prefix="${cmake_home}"
    popd
    export PATH="${cmake_home}/bin":"$PATH"
}

_install_cmake
