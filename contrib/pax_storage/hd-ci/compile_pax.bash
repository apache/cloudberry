#!/bin/bash

#!/usr/bin/env bash
set -exo pipefail
CBDB_INSTALL_PATH="/usr/local/cloudberry-db-devel"
CBDB_PAX_SRC_PATH="/code/gpdb_pax_src"
GPDB_PAX_UNIT_TEST_BIN="$CBDB_PAX_SRC_PATH/build/src/cpp/test_main"

compile_pax() {
	source $CBDB_INSTALL_PATH/greenplum_path.sh
	mkdir -p $CBDB_PAX_SRC_PATH/build
	pushd $CBDB_PAX_SRC_PATH/build
	cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1 ..
	make
	popd
}

function compile_cmake() {
	wget -O /root/cmake-3.25.1-linux-x86_64.tar.gz https://artifactory.hashdata.xyz/artifactory/utility/cmake-3.25.1-linux-x86_64.tar.gz
	mkdir -p /root/cmake-3.25.1-linux-x86_64
	tar -xvf /root/cmake-3.25.1-linux-x86_64.tar.gz -C /root/cmake-3.25.1-linux-x86_64
	rm -rf /usr/bin/cmake
	rm -rf /opt/rh/llvm-toolset-13.0/root/usr/bin/cmake
	ln -s /root/cmake-3.25.1-linux-x86_64/cmake-3.25.1-linux-x86_64/bin/cmake /usr/bin/cmake
	ln -s /root/cmake-3.25.1-linux-x86_64/cmake-3.25.1-linux-x86_64/bin/cmake /opt/rh/llvm-toolset-13.0/root/usr/bin/cmake
}

function run_unit() {
	$GPDB_PAX_UNIT_TEST_BIN
}

main() {
	compile_cmake
	compile_pax
	run_unit
}

main
