#!/usr/bin/env bash

set -e

echo "set PYTHONHOME if first argument is 'yes'"
./generate-greengage-path.sh yes | grep -q 'PYTHONHOME="${GPHOME}/ext/python"'

echo "do not set PYTHONHOME if first argument is not 'yes'"
[ $(./generate-greengage-path.sh no | grep -c PYTHONHOME) -eq 0 ]

echo "do not set PYTHONHOME if first argument is missing"
[ $(./generate-greengage-path.sh | grep -c PYTHONHOME) -eq 0 ]

echo "ALL TEST PASSED"
