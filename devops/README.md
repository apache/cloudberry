<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Auto-Build Apache Cloudberry from Source Code

You can build Apache Cloudberry from source code in two ways: manually or automatically.

For the manual build, you need to manually set up many system configurations and download third-party dependencies, which is quite cumbersome and error-prone.

To make the job easier, it is recommended that you use the automated deployment method and scripts provided here. The automation method simplifies the deployment process, reduces time costs, and allows developers to focus more on business code development.

## Local test container with a mounted checkout

You can run tests against a local checkout without installing build
dependencies on the host by using the published Cloudberry build image and a
bind mount. The source tree remains on the host, so edits made on the host or
inside the container affect the same files.

From the repository root, start a long-running container:

```bash
docker run -dit \
    --privileged \
    --user root \
    --hostname cdw \
    --name cloudberry-devtest \
    --shm-size=2gb \
    --ulimit core=-1 \
    --cgroupns=host \
    -e SRC_DIR=/workspace/cloudberry \
    -e BUILD_DESTINATION=/usr/local/cloudberry-db \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    -v "$PWD":/workspace/cloudberry \
    -w /workspace/cloudberry \
    apache/incubator-cloudberry:cbdb-build-rocky9-latest \
    bash -lc "sleep infinity"
```

Enter the container as `gpadmin`:

```bash
docker exec -it --user gpadmin -w /workspace/cloudberry cloudberry-devtest bash
```

Configure new interactive shells to source the installed and demo cluster
environments automatically after those files exist:

```bash
grep -q "Cloudberry devtest environment" ~/.bashrc || cat >> ~/.bashrc <<'EOF'
# Cloudberry devtest environment
if [ -r "$BUILD_DESTINATION/cloudberry-env.sh" ]; then
    source "$BUILD_DESTINATION/cloudberry-env.sh"
fi
if [ -r "$SRC_DIR/gpAux/gpdemo/gpdemo-env.sh" ]; then
    source "$SRC_DIR/gpAux/gpdemo/gpdemo-env.sh"
fi
EOF
```

After the build and demo cluster are created, run `exec bash` in the current
shell to reload `~/.bashrc` and pick up those environments immediately.

Prepare the build and demo cluster inside the container:

```bash
/tmp/init_system.sh
cd "$SRC_DIR"

./devops/build/automation/cloudberry/scripts/configure-cloudberry.sh
./devops/build/automation/cloudberry/scripts/build-cloudberry.sh
./devops/build/automation/cloudberry/scripts/create-cloudberry-demo-cluster.sh

exec bash
```

After changing code, rebuild the relevant parts or rerun the build script, then
run the tests you need. For example:

```bash
PGOPTIONS="-c optimizer=off" make -C src/test/regress installcheck-small
PGOPTIONS="-c optimizer=off" make -C src/test/regress installcheck-tests TESTS="insert"
make -C src/test/isolation2 installcheck-isolation2
```

If `psql`, `pg_config`, or the Python `pg` module is missing, run `exec bash`
to reload `~/.bashrc`. You can also source the environments explicitly:

```bash
source "$BUILD_DESTINATION/cloudberry-env.sh"
source "$SRC_DIR/gpAux/gpdemo/gpdemo-env.sh"
```

Stop and remove the container when it is no longer needed:

```bash
docker stop cloudberry-devtest
docker rm cloudberry-devtest
```

## 1. Setup Docker environment

Nothing special, just follow the [official documentation](https://docs.docker.com/engine/install/) to install Docker on your machine based on your OS.

## 2. Create Docker build image

Go to the supported OS directory, for example Rocky Linux 8:

```bash
cd devops/deploy/docker/build/rocky8/
```

And build image:

```bash
docker build -t apache-cloudberry-env .
```

The whole process usually takes about 5 minutes. You can use the created base image as many times as you want, just launch a new container for your specific task.

## 3. Launch container

Launch the container in detached mode with a long-running process:

```bash
docker run -h cdw -d --name cloudberry-build apache-cloudberry-env bash -c "/tmp/init_system.sh && tail -f /dev/null"
```

> [!NOTE]
> The container will be named `cloudberry-build` and run in the background for easy reference in subsequent commands.
> If you need to:
>  - access the container interactively, use `docker exec -it cloudberry-build bash`
>  - check if the container is running, use `docker ps`

## 4. Checkout git repo inside container

The same way you did it on your laptop

```bash
docker exec cloudberry-build bash -c "cd /home/gpadmin && git clone --recurse-submodules --branch main --depth 1 https://github.com/apache/cloudberry.git"
```

## 5. Set environment and configure build container

Create direcory for store logs:

```bash
SRC_DIR=/home/gpadmin/cloudberry && docker exec cloudberry-build  bash -c "mkdir ${SRC_DIR}/build-logs"
```

Execute configure and check if system is ready for build:

```bash
SRC_DIR=/home/gpadmin/cloudberry && docker exec cloudberry-build bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/configure-cloudberry.sh"
```

## 6. Build and install binary

The building consumes all available CPU resources and can take minutes to complete:

```bash
SRC_DIR=/home/gpadmin/cloudberry && docker exec cloudberry-build bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/build-cloudberry.sh"
```

## 7. Install binary and create demo cluster

The build script above has already installed the binaries to `/usr/local/cloudberry-db` inside the container. Now create the demo cluster just launch `create-cloudberry-demo-cluster.sh`

```bash
SRC_DIR=/home/gpadmin/cloudberry && docker exec cloudberry-build bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/create-cloudberry-demo-cluster.sh"
```

## 8. Execute test query

Now you could set environment and execute queries:

```bash
docker exec cloudberry-build bash -c "source /usr/local/cloudberry-db/cloudberry-env.sh && source /home/gpadmin/cloudberry/gpAux/gpdemo/gpdemo-env.sh && psql -U gpadmin -d postgres -c 'SELECT 42'"
```

All done!
