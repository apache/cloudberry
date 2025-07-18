#!/bin/bash
set -exo pipefail

GPDB_SRC_PATH=${GPDB_SRC_PATH:=gpdb_src}
CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${CWDIR}/common.bash"

function test_orca
{
    if [ -n "${SKIP_UNITTESTS}" ]; then
        return
    fi
    OUTPUT_DIR="../../../../gpAux/ext/${BLD_ARCH}"
    pushd ${GPDB_SRC_PATH}/src/backend/gporca
    concourse/build_and_test.py --build_type=RelWithDebInfo --output_dir=${OUTPUT_DIR}
    concourse/build_and_test.py --build_type=Debug --output_dir=${OUTPUT_DIR}
    popd
}

function _main
{
  export BLD_ARCH=$(build_arch)
  if [[ ${BLD_ARCH} == "rhel9"* ]]; then
    export PYTHONHOME=$(find /opt -maxdepth 1 -type d -name "python-2*")
    export PATH="${PYTHONHOME}/bin:${PATH}"
    export LD_LIBRARY_PATH="${PYTHONHOME}/lib/:${LD_LIBRARY_PATH}"
    ln -s "${PYTHONHOME}"/bin/python2 /usr/bin/python
  fi
  mkdir -p gpdb_src/gpAux/ext
  test_orca
}

_main "$@"
