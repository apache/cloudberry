#!/usr/bin/env bash

set -ex

# shellcheck source=/dev/null
source "$CI_REPO_DIR/common/entry_common.sh"

install_cmake

start_gpdb

create_fake_gpdb_src
