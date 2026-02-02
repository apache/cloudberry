FROM ubuntu:focal

ARG accessKeyId
ARG secretAccessKey
ARG bucketName
ARG s3endpoint
ARG yezzeyRef

ENV YEZZEY_REF=${yezzeyRef:-v1.8_opengpdb}

ENV AWS_ACCESS_KEY_ID=${accessKeyId}
ENV AWS_SECRET_ACCESS_KEY=${secretAccessKey}
ENV S3_BUCKET=${bucketName}
ENV WALG_S3_PREFIX=s3://${bucketName}/yezzey-test-files
ENV S3_ENDPOINT=${s3endpoint}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive

RUN useradd -rm -d /home/gpadmin -s /bin/bash -g root -G sudo -u 1001 gpadmin

RUN ln -snf /usr/share/zoneinfo/Europe/London /etc/localtime && echo Europe/London > /etc/timezone \
&& apt-get update -o Acquire::AllowInsecureRepositories=true && apt-get install -y --no-install-recommends --allow-unauthenticated \
  build-essential libssl-dev gnupg devscripts \
  openssl libssl-dev debhelper debootstrap \
  make equivs bison ca-certificates-java ca-certificates \
  cmake curl cgroup-tools flex gcc-8 g++-8 g++-8-multilib \
  git krb5-multidev libapr1-dev libbz2-dev libcurl4-gnutls-dev \
  libevent-dev libkrb5-dev libldap2-dev libperl-dev libreadline6-dev \
  libssl-dev libxml2-dev libyaml-dev libzstd-dev libaprutil1-dev \
  libpam0g-dev libpam0g libcgroup1 libyaml-0-2 libldap-2.4-2 libssl1.1 \
  ninja-build python-dev python-setuptools quilt unzip wget zlib1g-dev libuv1-dev \
  libgpgme-dev libgpgme11 sudo iproute2 less software-properties-common \
  openssh-client openssh-server

COPY yezzey_test/install_yproxy.sh /home/gpadmin

RUN ["/home/gpadmin/install_yproxy.sh"]

RUN apt-get install -y locales \
&& locale-gen "en_US.UTF-8" \
&& update-locale LC_ALL="en_US.UTF-8"

RUN echo 'gpadmin ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers

USER gpadmin
WORKDIR /home/gpadmin

COPY yezzey_test/import_gpg_keys.sh /home/gpadmin/
COPY yezzey_test/priv.gpg /home/gpadmin/yezzey_test/priv.gpg
COPY yezzey_test/pub.gpg /home/gpadmin/yezzey_test/pub.gpg

RUN ["/home/gpadmin/import_gpg_keys.sh"]

COPY yezzey_test/generate_ssh_key.sh /home/gpadmin/

RUN ["/home/gpadmin/generate_ssh_key.sh"]


RUN cd /tmp/ \
&& git clone https://github.com/boundary/sigar.git \
&& cd ./sigar/ \
&& mkdir build && cd build && cmake .. && make \
&& sudo make install

COPY . /home/gpadmin

RUN sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  bison \
  ccache \
  cmake \
  curl \
  flex \
  git-core \
  gcc \
  g++ \
  inetutils-ping \
  krb5-kdc \
  krb5-admin-server \
  libapr1-dev \
  libbz2-dev \
  libcurl4-gnutls-dev \
  libevent-dev \
  libkrb5-dev \
  libpam-dev \
  libperl-dev \
  libreadline-dev \
  libssl-dev \
  libxml2-dev \
  libyaml-dev \
  libzstd-dev \
  locales \
  net-tools \
  ninja-build \
  openssh-client \
  openssh-server \
  openssl \
  python3-dev \
  python3-pip \
  python3-psutil \
  python3-pygresql \
  python-yaml \
  zlib1g-dev \
  rsync \
&& sudo apt install -y libhyperic-sigar-java libaprutil1-dev libuv1-dev

RUN sudo mkdir /usr/local/gpdb \
&& sudo chown gpadmin:root /usr/local/gpdb

RUN sudo chown -R gpadmin:root /home/gpadmin \
&& git status

RUN  git submodule update --init 
RUN rm -fr gpcontrib/yezzey 

# Fetch latest yezzey version
RUN git clone https://github.com/open-gpdb/yezzey.git gpcontrib/yezzey && cd gpcontrib/yezzey && git fetch origin $YEZZEY_REF:test_branch && git checkout test_branch && cd /home/gpadmin 
RUN sed -i '/^trusted/d' gpcontrib/yezzey/yezzey.control 
RUN ./configure --with-perl --with-python --with-libxml --disable-orca --prefix=/usr/local/gpdb \
--enable-depend --enable-cassert --enable-debug --without-mdblocales --without-zstd CFLAGS='-fno-omit-frame-pointer -Wno-implicit-fallthrough -O3 -pthread' 
RUN make -j8 && make -j8 install && make -C gpcontrib/yezzey -j8 install


RUN echo ${s3endpoint}

RUN sed -i "s/\$AWS_ACCESS_KEY_ID/${accessKeyId}/g" yezzey_test/yproxy.conf \
&& sed -i "s/\$AWS_SECRET_ACCESS_KEY/${secretAccessKey}/g" yezzey_test/yproxy.conf \
&& sed -i "s/\$AWS_ENDPOINT/${s3endpoint}/g" yezzey_test/yproxy.conf \
&& sed -i "s/\$WALG_S3_PREFIX/${bucketName}\/yezzey-test-files/g" yezzey_test/yproxy.conf && cp yezzey_test/yproxy.conf /tmp/yproxy.yaml

ENTRYPOINT ["./yezzey_test/run_tests.sh"]
