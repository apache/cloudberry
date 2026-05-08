# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------

import testinfra

def test_installed_packages(host):
    """
    Test if the essential packages are installed.
    """
    packages = [
        "bat",
        "bison",
        "cmake",
        "flex",
        "g++-11",
        "gcc-11",
        "git",
        "htop",
        "iproute2",
        "iputils-ping",
        "libapr1-dev",
        "libbz2-dev",
        "libcurl4-gnutls-dev",
        "libevent-dev",
        "libipc-run-perl",
        "libkrb5-dev",
        "libldap-dev",
        "liblz4-dev",
        "libpam0g-dev",
        "libperl-dev",
        "libprotobuf-dev",
        "libreadline-dev",
        "libssl-dev",
        "libuv1-dev",
        "libxerces-c-dev",
        "libxml2-dev",
        "libyaml-dev",
        "libzstd-dev",
        "locales",
        "lsof",
        "make",
        "openssh-server",
        "pkg-config",
        "protobuf-compiler",
        "python3-distutils",
        "python3-pip",
        "python3-setuptools",
        "python3.10",
        "python3.10-dev",
        "rsync",
        "silversearcher-ag",
        "sudo",
        "tzdata",
        "vim",
        "wget",
        "zlib1g-dev"
    ]
    for package in packages:
        pkg = host.package(package)
        assert pkg.is_installed


def test_user_gpadmin_exists(host):
    """
    Test if the gpadmin user exists and is configured properly.
    """
    user = host.user("gpadmin")
    assert user.exists
    assert "gpadmin" in user.groups


def test_ssh_service(host):
    """
    Test if SSH service is configured correctly.
    """
    sshd_config = host.file("/etc/ssh/sshd_config")
    assert sshd_config.exists


def test_timezone(host):
    """
    Test if the timezone is configured correctly.
    """
    localtime = host.file("/etc/localtime")
    assert localtime.exists


def test_system_limits_configured(host):
    """
    Test if the custom system limits are applied.
    """
    limits_file = host.file("/etc/security/limits.d/90-cbdb-limits")
    assert limits_file.exists


def test_init_system_script(host):
    """
    Test if the init_system.sh script is present and executable.
    """
    script = host.file("/tmp/init_system.sh")
    assert script.exists
    assert script.mode == 0o755


def test_locale_generated(host):
    """
    Test if the en_US.UTF-8 locale is correctly generated.
    """
    locale = host.run("locale -a | grep en_US.utf8")
    assert locale.exit_status == 0
    assert "en_US.utf8" in locale.stdout
