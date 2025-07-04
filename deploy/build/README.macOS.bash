#!/bin/bash

echo "Caching password..."
sudo -K
sudo true;

if [ ! -d /tmp/cbdb-devenv ] ; then
  mkdir /tmp/cbdb-devenv && cd /tmp/cbdb-devenv || exit
fi

if hash brew 2>/dev/null; then
  echo "Homebrew is already installed!"
else
  echo "Installing Homebrew..."
  echo | /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  if [ $? -eq 1 ]; then
        echo "ERROR : Homebrew Installation Failed, fix the failure and re-run the script."
        exit 1
  fi
fi

brew install bash-completion
brew install conan
brew install cmake # gporca
#brew install xerces-c #gporca
brew install libyaml # enables `--enable-mapreduce`
brew install libevent # gpfdist
brew install apr # gpfdist
brew install apr-util # gpfdist
brew install zstd
brew install pkg-config
brew install perl
brew link --force apr
brew link --force apr-util
brew install gcc@13
brew install gnu-sed

#Install xerces-c  library
if [ ! -d /tmp/cbdb-devenv/gp-xerces ] ; then
  echo "INFO: xerces is not installed, Installing...."
  git clone https://github.com/greenplum-db/gp-xerces.git -v /tmp/cbdb-devenv/gp-xerces
  mkdir /tmp/cbdb-devenv/gp-xerces/build
  cd /tmp/cbdb-devenv/gp-xerces/build || exit
  /tmp/cbdb-devenv/gp-xerces/configure --prefix="$(brew --prefix)"
  make -j"$(nproc)"
  make install
  cd - || exit
fi

# Needed for pygresql, or you can source cloudberry-env.sh after compiling database and installing python-dependencies then
brew install postgresql@14

brew install python3

# Due to recent update on OS net-tools package. Mac doesn't have support for ss and ip by default.
# Hence as a workaround installing iproute2mac for ip support and creating soft link for ss support
brew install iproute2mac
sudo ln -s /usr/sbin/netstat "$(brew --prefix)"/bin/ss

echo 127.0.0.1$'\t'$HOSTNAME | sudo tee -a /etc/hosts

# OS settings
sudo sysctl -w kern.sysv.shmmax=2147483648
sudo sysctl -w kern.sysv.shmmin=1
sudo sysctl -w kern.sysv.shmmni=64
sudo sysctl -w kern.sysv.shmseg=16
sudo sysctl -w kern.sysv.shmall=524288
sudo sysctl -w net.inet.tcp.msl=60

sudo sysctl -w net.local.dgram.recvspace=262144
sudo sysctl -w net.local.dgram.maxdgram=16384
sudo sysctl -w kern.maxfiles=131072
sudo sysctl -w kern.maxfilesperproc=131072
sudo sysctl -w net.inet.tcp.sendspace=262144
sudo sysctl -w net.inet.tcp.recvspace=262144
sudo sysctl -w kern.ipc.maxsockbuf=8388608

sudo tee -a /etc/sysctl.conf << EOF
kern.sysv.shmmax=2147483648
kern.sysv.shmmin=1
kern.sysv.shmmni=64
kern.sysv.shmseg=16
kern.sysv.shmall=524288
net.inet.tcp.msl=60

net.local.dgram.recvspace=262144
net.local.dgram.maxdgram=16384
kern.maxfiles=131072
kern.maxfilesperproc=131072
net.inet.tcp.sendspace=262144
net.inet.tcp.recvspace=262144
kern.ipc.maxsockbuf=8388608
EOF

# Configure
cat >> ~/.bashrc << EOF
ulimit -n 131072 131072  # Increases the number of open files
export PGHOST="$(hostname)"
export LC_CTYPE="en_US.UTF-8"
EOF

cat >> ~/.zshrc << EOF
source ~/.bashrc
EOF

cat << EOF

================

Please source cloudberry-env.sh after compiling database, then

pip3 install --user -r ../../python-dependencies.txt

EOF
