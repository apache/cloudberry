#!/usr/bin/env bash
# Notice from the Apache Cloudberry PPMC team as follows:
# These files use the term 'greenplum' to maintain compatibility with
# original versions of Apache Cloudberry, which was originally called
# Greenplum. This usage does not relate to the VMware Tanzu Greenplum
# product, neither do we imply that Apache Cloudberry (Incubating) is
# affiliated with, endorsed by, or sponsored by Broadcom Inc.
# See discussion at https://lists.apache.org/thread/b8o974mnnqk6zpy86dgll2pgqcvqgnwm

cat <<"EOF"
if [ -n "${PS1-}" ]; then
    echo "Notice from the Apache Cloudberry PPMC team
-----------------------------------------------------------------------
This file uses the term 'greenplum' to maintain compatibility with
original versions of Apache Cloudberry, which was originally called
Greenplum. This usage does not relate to the VMware Tanzu Greenplum
product, neither do we imply that Apache Cloudberry (Incubating) is
affiliated with, endorsed by, or sponsored by Broadcom Inc.

This file will be renamed in the next Apache Cloudberry release to
ensure better compliance with Apache guidelines. We will announce
this change on both the mailing list and the project website.

See discussion at https://lists.apache.org/thread/b8o974mnnqk6zpy86dgll2pgqcvqgnwm
-----------------------------------------------------------------------
"
fi
EOF

cat <<"EOF"
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
    echo "The shell cannot be identified. \$GPHOME may not be set correctly." >&2
fi
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"

if [ ! -L "${SCRIPT_DIR}" ]; then
    GPHOME=${SCRIPT_DIR}
else
    GPHOME=$(readlink "${SCRIPT_DIR}")
fi
EOF

cat <<"EOF"
PYTHONPATH="${GPHOME}/lib/python"
PATH="${GPHOME}/bin:${PATH}"
LD_LIBRARY_PATH="${GPHOME}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

if [ -e "${GPHOME}/etc/openssl.cnf" ]; then
	OPENSSL_CONF="${GPHOME}/etc/openssl.cnf"
fi

#setup JAVA_HOME
if [ -x "${GPHOME}/ext/jdk/bin/java" ]; then
    JAVA_HOME="${GPHOME}/ext/jdk"
    PATH="${JAVA_HOME}/bin:${PATH}"
    CLASSPATH=${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tool.jar
fi

export GPHOME
export PATH
export PYTHONPATH
export LD_LIBRARY_PATH
export OPENSSL_CONF
export JAVA_HOME
export CLASSPATH

# Load the external environment variable files
if [ -d "${GPHOME}/etc/environment.d" ]; then
	LOGGER=$(which logger 2> /dev/null || which true)
	set -o allexport
	for env in $(find "${GPHOME}/etc/environment.d" -regextype sed -regex '.*\/[0-9][0-9]-.*\.conf$' -type f | sort -n); do
		$LOGGER -t "greenplum-path.sh" "loading environment from ${env}"
		source "${env}"
	done
	set +o allexport
fi
EOF
