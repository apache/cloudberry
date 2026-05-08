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
