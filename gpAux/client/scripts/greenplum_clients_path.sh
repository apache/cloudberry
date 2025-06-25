# Notice from the Apache Cloudberry PPMC team as follows:
# These files use the term 'greenplum' to maintain compatibility with
# original versions of Apache Cloudberry, which was originally called
# Greenplum. This usage does not relate to the VMware Tanzu Greenplum
# product, neither do we imply that Apache Cloudberry (Incubating) is
# affiliated with, endorsed by, or sponsored by Broadcom Inc.
# See discussion at https://lists.apache.org/thread/b8o974mnnqk6zpy86dgll2pgqcvqgnwm

if test -n "${ZSH_VERSION:-}"; then
    # zsh
    SCRIPT_PATH="${(%):-%x}"
elif test -n "${BASH_VERSION:-}"; then
    # bash
    SCRIPT_PATH="${BASH_SOURCE[0]}"
else
    # Unknown shell, hope below works.
    # Tested with dash
    result=$(lsof -p $$ -Fn | tail --lines=1 | xargs --max-args=2 | cut --delimiter=' ' --fields=2)
    SCRIPT_PATH=${result#n}
fi

if test -z "$SCRIPT_PATH"; then
    echo "The shell cannot be identified. \$GPHOME_CLIENTS may not be set correctly." >&2
fi
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"

if [ ! -L "${SCRIPT_DIR}" ]; then
    GPHOME_CLIENTS=${SCRIPT_DIR}
else
    GPHOME_CLIENTS=$(readlink "${SCRIPT_DIR}")
fi

PATH=${GPHOME_CLIENTS}/bin:${PATH}
PYTHONPATH=${GPHOME_CLIENTS}/bin/ext:${PYTHONPATH}

# Export GPHOME_LOADERS for GPDB5 compatible
GPHOME_LOADERS=${GPHOME_CLIENTS}
export GPHOME_CLIENTS
export GPHOME_LOADERS
export PATH
export PYTHONPATH

# Mac OSX uses a different library path variable
if [ xDarwin = x`uname -s` ]; then
  DYLD_LIBRARY_PATH=${GPHOME_CLIENTS}/lib:${DYLD_LIBRARY_PATH}
  export DYLD_LIBRARY_PATH
else
  LD_LIBRARY_PATH=${GPHOME_CLIENTS}/lib:${LD_LIBRARY_PATH}
  export LD_LIBRARY_PATH
fi

if [ "$1" != "-q" ]; then
  command -v python3 >/dev/null || echo "Warning: Python 3 not found, which is required to run gpload."
fi
