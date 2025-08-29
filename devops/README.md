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

# Auto-Build Cloudberry Database from Source Code

You can build Cloudberry Database from source code in two ways: manually or automatically.

For the manual build, you need to manually set up many system configurations and download third-party dependencies, which is quite cumbersome and error-prone.

To make the job easier, it is recommended that you use the automated deployment method and scripts provided here. The automation method simplifies the deployment process, reduces time costs, and allows developers to focus more on business code development.

## 1. Setup docker environment

Nothing special, just follow the [official documentation](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)

## 2. Create docker build image

Go to the supported OS directory, for example Rocky Linux 9

`cd devops/deploy/docker/build/rocky8/`

And build image

`docker build -t cloudberry-db-env . `

The whole process usually takes about 5 minutes. You can use the created base image as many times as you want, just launch a new container for your specific task.

## 3. Launch container

Just run

`docker run -h cdw -it cloudberry-db-env`

## 4. Checkout git repo inside container

The same way you did it on your laptop

`docker exec <container ID> bash -c "cd /home/gpadmin && git clone --recurse-submodules https://github.com/apache/cloudberry.git"`

## 5. Set envoronment and configure build container

Create direcory for store logs

`SRC_DIR=/home/gpadmin/cloudberry && docker exec <container ID>  bash -c "mkdir ${SRC_DIR}/build-logs"`

Execute configure and check if system is ready for build

`SRC_DIR=/home/gpadmin/cloudberry && docker exec <container ID> bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/configure-cloudberry.sh"`

## 6. Build binary

The building consumes all available CPU resources and can take minutes to complete

`SRC_DIR=/home/gpadmin/cloudberry && docker exec <container ID> bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/build-cloudberry.sh"`

## 7. Install binary and create demo cluster

By default `make install` copy compiled binary to  `/usr/local/cloudberry-db`

`SRC_DIR=/home/gpadmin/cloudberry && docker exec <container ID> bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} make install"`

To create demo cluster just launch `create-cloudberry-demo-cluster.sh`

`SRC_DIR=/home/gpadmin/cloudberry && docker exec <container ID> bash -c "cd ${SRC_DIR} && SRC_DIR=${SRC_DIR} ./devops/build/automation/cloudberry/scripts/create-cloudberry-demo-cluster.sh"`

## 8. Execute test query

Now you could set environment and execute queries

`docker exec 7197206b0645 bash -c "source /usr/local/cloudberry-db/cloudberry-env.sh && source /home/gpadmin/cloudberry/gpAux/gpdemo/gpdemo-env.sh && psql -U gpadmin -d postgres -c 'SELECT 42'"`

All done!
