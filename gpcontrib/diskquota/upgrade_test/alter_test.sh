#!/bin/bash
# Test if all the previous diskquota minor versions can be directly upgraded
# to the current version.

set -ex

SCRIPT_PATH="${BASH_SOURCE[0]}"
SRC_DIR="$(cd "$(dirname "${SCRIPT_PATH}")"/.. >/dev/null 2>&1 && pwd)"

# Versions like major.minor
CUR_VERSION=$(cut --delimiter="." --fields=1-2 "${SRC_DIR}/VERSION")
ALL_VERSIONS=$(cd "${SRC_DIR}" && git tag | cut --delimiter="." --fields=1-2 | sort -V -u)
VERSIONS_TO_TEST=()

test_alter_from() {
    local from_ver=$1
    local to_ver=$CUR_VERSION

    gpconfig -c shared_preload_libraries -v ""
    gpstop -rai
    dropdb diskquota --if-exists
    dropdb diskquota_alter_test --if-exists
    createdb diskquota

    local from_so_name="diskquota"
    if [ "${from_ver}" != "1.0" ];then
        from_so_name="diskquota-${from_ver}"
    fi
    local to_so_name="diskquota-${to_ver}"

    # Preload the old diskquota so
    gpconfig -c shared_preload_libraries -v "${from_so_name}"
    gpstop -rai

    createdb diskquota_alter_test

    # Test if the extension and be upgraded directly
    psql -d diskquota_alter_test -c "CREATE EXTENSION diskquota version '${from_ver}'"

    # Preload the new diskquota so
    gpconfig -c shared_preload_libraries -v "${to_so_name}"
    gpstop -rai

    psql -d diskquota_alter_test -c "ALTER EXTENSION diskquota update to '${to_ver}'"
    # Sleep wait for bgworker starting, otherwise, we will get a warning
    # 'cannot remove the database from db list, dbid not found'.
    sleep 5
    psql -d diskquota_alter_test -c "DROP EXTENSION diskquota"
}

_determine_gp_major_version() {
    local includedir="$(pg_config --includedir)"
    GP_MAJORVERSION=$(grep -oP '.*GP_MAJORVERSION.*"\K[^"]+' "${includedir}/pg_config.h")
}
_determine_gp_major_version

compare_versions() {
     # implementing string manipulation
     local a=${1%%.*} b=${2%%.*}
     [[ "10#${a:-0}" -gt "10#${b:-0}" ]] && return 1
     [[ "10#${a:-0}" -lt "10#${b:-0}" ]] && return 2
     # re-assigning a and b with greatest of 1 and 2 after manipulation
     a=${1:${#a} + 1}
     b=${2:${#b} + 1}
     # terminal condition for recursion
     [[ -z $a && -z $b ]] || compare_versions "$a" "$b"
}


# Find all minor versions before current one
while IFS= read -r ver; do
    if [ "${ver}" = "${CUR_VERSION}" ]; then
        break
    fi
    if [ "${ver}" = "0.8" ]; then
        continue
    fi
    # The first version of diskquota for GP7 is 2.2
    if [ "$GP_MAJORVERSION" -eq "7" ]; then
        set +e
        compare_versions $ver "2.2"
        cmp_res=$?
        set -e
        if [ $cmp_res -eq "2" ]; then
            continue
        fi
    fi
    VERSIONS_TO_TEST+=("${ver}")
done <<< "$ALL_VERSIONS"

for from_ver in "${VERSIONS_TO_TEST[@]}"; do
    test_alter_from "${from_ver}"
done
