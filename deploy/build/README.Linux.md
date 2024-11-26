# Compile and Install Apache Cloudberry on Linux

This document shares how to compile and install Apache Cloudberry on Linux systems (CentOS 7, RHEL, and Ubuntu). Note that this document is for developers to try out Apache Cloudberry in a single-node environments. DO NOT use this document for production environments.

Take the following steps to set up the development environments:

1. [Clone the GitHub repo](#step-1-clone-the-github-repo).
2. [Install dependencies](#step-2-install-dependencies).
3. [Perform prerequisite platform tasks](#step-3-perform-prerequisite-platform-tasks).
4. [Build Apache Cloudberry](#step-4-build-apache-cloudberry).
5. [Verify the cluster](#step-5-verify-the-cluster).

## Step 1. Clone the GitHub repo

Clone the GitHub repository `apache/cloudberry` to the target machine:

```shell
git clone https://github.com/apache/cloudberry.git
```

## Step 2. Install dependencies

Enter the repository and install dependencies according to your operating systems:

- [For CentOS 7](#for-centos-7)
- [For RHEL 8 or Rocky Linux 8](#for-rhel-8-or-rocky-linux-8)
- [For Ubuntu 18.04 or later](#for-ubuntu-1804-or-later)

### For CentOS 7

> [!WARNING]
> CentOS Linux 7 reached the end of life (EOL) on June 30, 2024. You can change the mirror to vault.centos.org to install software.

The following steps work on CentOS 7. For other CentOS versions, these steps might work but are not guaranteed to work.

1. Run the bash script `README.CentOS.bash` in the `deploy/build` directory of the `apache/cloudberry` repository. To run this script, password is required. Then, some required dependencies will be automatically downloaded.

    ```bash
    cd cloudberry/deploy/build
    ./README.CentOS.bash
    ```

2. Install additional packages required for configurations.

    ```bash
    yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb iproute krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd make openldap openssh openssh-clients openssh-server openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zlib && \
    yum -y install epel-release
    ```

3. Update the GNU Compiler Collection (GCC) to version `devtoolset-10` to support C++ 14.

    ```bash
    yum install centos-release-scl 
    yum -y install devtoolset-10-gcc devtoolset-10-gcc-c++ devtoolset-10-binutils 
    scl enable devtoolset-10 bash 
    source /opt/rh/devtoolset-10/enable 
    echo "source /opt/rh/devtoolset-10/enable" >> /etc/bashrc
    source /etc/bashrc
    gcc -v
    ```

4. Link cmake3 to cmake:

    ```bash
    sudo ln -sf /usr/bin/cmake3 /usr/local/bin/cmake
    ```

### For RHEL 8 or Rocky Linux 8

1. Install Development Tools.

    ```bash
    sudo yum group install -y "Development Tools"
    ```

2. Install dependencies:

    ```bash
    sudo yum install -y epel-release

    sudo yum install -y apr-devel bison bzip2-devel cmake3 flex gcc gcc-c++ krb5-devel libcurl-devel libevent-devel libkadm5  libxml2-devel libzstd-devel openssl-devel perl-ExtUtils-Embed python3-devel python3-pip readline-devel xerces-c-devel zlib-devel
    ```

3. Install more dependencies by running the `README.Rhel-Rocky.bash` script.

    ```bash
    ~/cloudberry/deploy/build/README.Rhel-Rocky.bash
    ```

### For Ubuntu 18.04 or later

1. Install dependencies by running the `README.Ubuntu.bash` script in the `deploy/build` directory.

    ```shell
    # You need to enter your password to run.
    sudo ~/cloudberry/deploy/build/README.Ubuntu.bash
    ```

    > [!Note]
    > - When you run the `README.Ubuntu.bash` script for dependencies, you will be asked to configure `realm` for Kerberos. You can enter any realm, because this is just for testing, and during testing, it will reconfigure a local server/client. If you want to skip this manual configuration, run `export DEBIAN_FRONTEND=noninteractive`.
    > - If the script fails to download packages, we recommend that you can try another one software source for Ubuntu.

2. Install GCC 10. Ubuntu 18.04 and later versions should use GCC 10 or later:

    ```bash
    # Install gcc-10.
    sudo apt install software-properties-common
    sudo add-apt-repository ppa:ubuntu-toolchain-r/test
    sudo apt install gcc-10 g++-10
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100
    ```

## Step 3. Perform prerequisite platform tasks

After you have installed all the dependencies for your operating system, it is time to do some prerequisite platform tasks before you go on building Apache Cloudberry. These operations include manually running `ldconfig`, creating the `gpadmin` user, and setting up a password to start the Apache Cloudberry and test.

1. Make sure that you add `/usr/local/lib` and `/usr/local/lib64` to the `/etc/ld.so.conf` file.

    ```bash
    echo -e "/usr/local/lib \n/usr/local/lib64" >> /etc/ld.so.conf
    ldconfig
    ```

2. Create the `gpadmin` user and set up the SSH key. Manually create SSH keys based on different operating systems, so that you can run `ssh localhost` without a password.

    - For CentOS, RHEL, and Rocky Linux:

        ```bash
        useradd gpadmin  # Creates gpadmin user
        su - gpadmin  # Uses the gpadmin user
        ssh-keygen  # Creates SSH key
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        chmod 600 ~/.ssh/authorized_keys
        exit
        ```

    - For Ubuntu:

        ```bash
        useradd -r -m -s /bin/bash gpadmin  # Creates gpadmin user
        su - gpadmin  # Uses the gpadmin user
        ssh-keygen  # Creates SSH key
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        chmod 600 ~/.ssh/authorized_keys 
        exit
        ```

## Step 4. Build Apache Cloudberry

After you have installed all the dependencies and performed the prerequisite platform tasks, you can start to build Apache Cloudberry. Run the following commands in sequence.

1. Configure the build environment. Enter the `cloudberry` directory and run the `configure` script.

    ```bash
    cd cloudberry
    ./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/cloudberry
    ```

    > [!Note]
    > Cloudberry is built with GPORCA by default. If you want to build Cloudberry without GPORCA, add the `--disable-orca` flag in the `./configure` command.
    >
    > ```bash
    > ./configure --disable-orca --with-perl --with-python --with-libxml --prefix=/usr/local/cloudberry
    > ```

2. Compile the code and install the database.

    ```bash
    make -j8
    make -j8 install
    ```

3. Bring in the Greenplum environment for your running shell.

    ```bash
    cd ..
    cp -r cloudberry/ /home/gpadmin/
    cd /home/gpadmin/
    chown -R gpadmin:gpadmin cloudberry/
    su - gpadmin
    cd cloudberry/
    source /usr/local/cloudberry/greenplum_path.sh
    ```

4. Start the demo cluster.

    - For CentOS:

        ```bash
        scl enable devtoolset-10 bash 
        source /opt/rh/devtoolset-10/enable 
        make create-demo-cluster
        ```

    - For Ubuntu, Rocky, and RHEL:

        ```bash
        make create-demo-cluster
        ```

5. Prepare the test by running the following command. This command will configure the port and environment variables for the test.

    Environment variables such as `PGPORT` and `MASTER_DATA_DIRECTORY` will be configured, which are the default port and the data directory of the master node.

    ```bash
    source gpAux/gpdemo/gpdemo-env.sh
    ```

## Step 5. Verify the cluster

1. You can verify whether the cluster has started successfully by running the following command. If successful, you can see multiple active `postgres` processes with ports ranging from `7000` to `7007`.

    ```bash
    ps -ef | grep postgres
    ```
    
2. Connect to the Apache Cloudberry and see the active segment information by querying the system table `gp_segement_configuration`. For detailed description of this table, see the Greenplum document [here](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-gp_segment_configuration.html).

    ```sql
    $ psql -p 7000 postgres
    psql (14.4, server 14.4)
    Type "help" for help.
    
    postgres=# select version();
                                                                                            version                                                                                         
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PostgreSQL 14.4 (Apache Cloudberry 1.0.0+1c0d6e2224 build dev) on x86_64( GCC 13.2.0) 13.2.0, 64-bit compiled on Sep 22 2023 10:56:01
    (1 row)
    
    postgres=# select * from gp_segment_configuration;
     dbid | content | role | preferred_role | mode | status | port |  hostname  |  address   |                                   datadir                                    | warehouseid 
    ------+---------+------+----------------+------+--------+------+------------+------------+------------------------------------------------------------------------------+-------------
        1 |      -1 | p    | p              | n    | u      | 7000 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/qddir/demoDataDir-1         |           0
        8 |      -1 | m    | m              | s    | u      | 7001 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/standby                     |           0
        3 |       1 | p    | p              | s    | u      | 7003 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1        |           0
        6 |       1 | m    | m              | s    | u      | 7006 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1 |           0
        2 |       0 | p    | p              | s    | u      | 7002 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0        |           0
        5 |       0 | m    | m              | s    | u      | 7005 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast_mirror1/demoDataDir0 |           0
        4 |       2 | p    | p              | s    | u      | 7004 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2        |           0
        7 |       2 | m    | m              | s    | u      | 7007 | i-6wvpa9wt | i-6wvpa9wt | /home/gpadmin/cloudberry/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2 |           0
    (8 rows)
    ```
