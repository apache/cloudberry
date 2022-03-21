#!/bin/bash

if [ -z "${SHOW_REGRESS_DIFF}" ]; then
    exit 1
fi

diff_files=$(find "$1" -name regression.diffs)
for diff_file in ${diff_files}; do
    if [ -f "${diff_file}" ]; then
        cat <<-FEOF
======================================================================
DIFF FILE: ${diff_file}
======================================================================

$(grep -v GP_IGNORE "${diff_file}")
FEOF
    fi
done
exit 1
