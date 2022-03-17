#!/bin/bash
# Due to the limitation of concourse git/github-pr resource, it is difficult to
# only check the format of the git diff. So all the source code are being
# checked.

set -eox pipefail

src_dir=$(dirname "${BASH_SOURCE[0]}")/../..
pushd "${src_dir}"
git ls-files '*.c' '*.h' | \
    xargs clang-format --style=file --verbose --Werror -dry-run
popd
