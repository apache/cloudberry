#!/usr/bin/env bash
set -exo pipefail
CBDB_PAX_SRC_PATH="/code/gpdb_pax_src"
CBDB_PAX_DEV_BRANCH="origin/dev"
CBDB_PAX_EXT=("*\.cc" "*\.h")
CBDB_PAXC_GREP="paxc_"
CLANG_TIDY_OUT_FILE_NAME="clang-tidy.result"

modified_exts=()

function do_git_diff() {
    current_commit=$(git rev-parse HEAD)
    dev_branch_commit=$(git rev-parse $CBDB_PAX_DEV_BRANCH)
    if [ "$current_commit" = "$dev_branch_commit" ]; then
        echo "Current commit is the '$CBDB_PAX_DEV_BRANCH' branch."
        exit 0
    fi

    modified_files=$(git diff --name-only $CBDB_PAX_DEV_BRANCH -- ':!icw_test')
    for extension in "${CBDB_PAX_EXT[@]}"; do
        if echo "$modified_files" | grep -E -e "$extension" | grep -q -v "$CBDB_PAXC_GREP"; then
            files=$(echo "$modified_files" | grep -E -e "$extension" | grep -v "$CBDB_PAXC_GREP")
            if [ -z "$files" ]; then
                continue
            fi
            for file in $files; do
                if [ -e "$file" ]; then
                    modified_exts+=("$file")
                fi
            done
        fi
    done
}

function run_clang_tidy() {
    if [ -z "$modified_exts" ]; then
        return 0
    fi

    echo "clang-tidy result will generate in $(pwd)/$CLANG_TIDY_OUT_FILE_NAME"
    clang-tidy -p build/ ${modified_exts[@]} > $CLANG_TIDY_OUT_FILE_NAME 2>&1
}

function _main() {
    pushd $CBDB_PAX_SRC_PATH
    do_git_diff
    run_clang_tidy
    popd
}

_main "$@"
