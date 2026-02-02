#!/bin/bash
set -ex

eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
sudo service ssh start
ssh -o StrictHostKeyChecking=no gpadmin@$(hostname) "echo 'Hello world'"

sudo bash -c 'cat >> /etc/ld.so.conf <<-EOF
/usr/local/lib

EOF'
sudo ldconfig

sudo bash -c 'cat >> /etc/sysctl.conf <<-EOF
kernel.shmmax = 500000000
kernel.shmmni = 4096
kernel.shmall = 4000000000
kernel.sem = 500 1024000 200 4096
kernel.sysrq = 1
kernel.core_uses_pid = 1
kernel.msgmnb = 65536
kernel.msgmax = 65536
kernel.msgmni = 2048
net.ipv4.tcp_syncookies = 1
net.ipv4.ip_forward = 0
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.tcp_tw_recycle = 1
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.conf.all.arp_filter = 1
net.ipv4.ip_local_port_range = 1025 65535
net.core.netdev_max_backlog = 10000
net.core.rmem_max = 2097152
net.core.wmem_max = 2097152
vm.overcommit_memory = 2

EOF'

sudo bash -c 'cat >> /etc/security/limits.conf <<-EOF
* soft nofile 65536
* hard nofile 65536
* soft nproc 131072
* hard nproc 131072

EOF'

export GPHOME=/usr/local/gpdb
source $GPHOME/cloudberry-env.sh
ulimit -n 65536
make destroy-demo-cluster && make create-demo-cluster
export USER=gpadmin
source gpAux/gpdemo/gpdemo-env.sh

gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a

createdb $USER


gpconfig -c yezzey.yproxy_socket -v "'/tmp/yproxy.sock'"
psql -c "ALTER SYSTEM SET yezzey.use_gpg_crypto TO false"
gpconfig -c yezzey.use_otm_feature -v "true"
gpconfig -c yezzey.use_gpg_crypto -v "false"

gpstop -a -i && gpstart -a

#run yproxy in daemon mode
/usr/bin/yproxy -c /tmp/yproxy.yaml -ldebug > yproxy.log 2>&1 &

i=0
while (! [ -S /tmp/yproxy.sock ]) && [ $i -lt 20 ]; do sleep 1; i=$(($i+1)) ; done

cd gpcontrib/yezzey
make installcheck || (echo Yproxy logs; cat ../../yproxy.log; cat /home/gpadmin/gpcontrib/yezzey/regression.diffs && exit 1) 
