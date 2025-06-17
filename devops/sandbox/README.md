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
---
title: Sandbox of Apache Cloudberry
---

# Install Apache Cloudberry With Docker

This document guides you on how to quickly set up and connect to Apache Cloudberry in a Docker environment. You can try out Apache Cloudberry by performing some basic operations and running SQL commands.

> [!WARNING]
> This guide is intended for testing or development. DO NOT use it for production.

> [!WARNING]
> **Security Notice: Embedded SSH Keys**
>
> For ease of use, this sandbox environment includes **pre-generated SSH keys embedded in the Docker image**. All containers built from the same image share the same SSH keypair, allowing passwordless SSH communication between nodes.
>
> **This is ONLY acceptable for local testing and development environments.** These embedded keys are **NOT secure** and must **NEVER** be used in production or any environment where security is a concern. Anyone with access to the Docker image can extract these keys.


## Prerequisites

Make sure that your environment meets the following requirements:

- Platform requirement: Any platform with Docker runtime. For details, refer to [Get Started with Docker](https://www.docker.com/get-started/).
- Other dependencies: Git, SSH, and internet connection

## Build the Sandbox

When building and deploying Apache Cloudberry in Docker, you will have 2 different deployment options as well as different build options.

**Deployment Options**
1. **Single Container** (Default) - With the single container option, you will have the coordinator as well as the Apache Cloudberry segments all running on a single container. This is the default behavior when deploying using the `run.sh` script provided.
2. **Multi-Container** - Deploying with the multi-container option will give you a more realistic deployment of what actual production Apache Cloudberry clusters look like. With multi-node, you will have the coordinator, the standby coordinator, and 2 segment hosts all on their own respective containers. This is to both highlight the distributed nature of Apache Cloudberry as well as highlight how high availability (HA) features work in the event of a server (or in this case a container) failing. This is enabled by passing the -m flag to the `run.sh` script which will be highlighted below.

![cloudberry Sandbox Deployments](./sandbox-deployment.jpg)

**Build Options**

1. Compile with the source code of the latest Apache Cloudberry (released in [Apache Cloudberry Release Page](https://github.com/apache/cloudberry/releases)). The base OS will be Rocky Linux 9 Docker image.
2. Method 2 - Compile with the latest Apache Cloudberry [main](https://github.com/apache/cloudberry/tree/main) branch. The base OS will be Rocky Linux 9 Docker image.

Build and deploy steps:

1. Start Docker Desktop and make sure it is running properly on your host platform.

2. Clone the Apache Cloudberry repository to the target machine.

    ```shell
    git clone https://github.com/apache/cloudberry.git
    ```

3. Enter the repository and run the `run.sh` script to start the Docker container. This will start the automatic installation process. Depending on your environment, you may need to run this with 'sudo' command.

    - For latest Apache Cloudberry release running on a single container

    ```shell
    cd cloudberry/devops/sandbox
    ./run.sh -c 2.0.0
    ```
    - For latest Apache Cloudberry release running across multiple containers

    ```shell
    cd cloudberry/devops/sandbox
    ./run.sh -c 2.0.0 -m
    ```
    - For latest main branch running on a single container

    ```shell
    cd cloudberry/devops/sandbox
    ./run.sh -c main
    ```

    - For latest main branch running across multiple containers

    ```shell
    cd cloudberry/devops/sandbox
    ./run.sh -c main -m
    ```

    Once the script finishes without error, the sandbox is built and running successfully. The `docker run` and `docker compose` commands use the --detach option allowing you to ssh or access the running Apache Cloudberry instance remotely.

    Please review run.sh script for additional options (e.g. setting Timezone in running container, only building container). You can also execute `./run.sh -h` to see the usage.

## Connect to the database

> [!NOTE]
> When deploying the multi-container Apache Cloudberry environment it may take extra time for the database to initialize, so you may need to wait a few minutes before you can execute the psql prompt successfully. You can run `docker logs cbdb-cdw -f` to see the current state of the database initialization process, you'll know the process is finished when you see the "Deployment Successful" output.

You can now connect to the database and try some basic operations.

1. Connect to the Docker container from the host machine:

    ```shell
    docker exec -it cbdb-cdw /bin/bash
    ```

    If it is successful, you will see the following prompt:

    ```shell
    [gpadmin@cdw /]$
    ```

2. Log into Apache Cloudberry in Docker. See the following commands and example outputs:

    ```shell
    [gpadmin@cdw ~]$ psql  # Connects to the database with the default database name "gpadmin".

    # psql (14.4, server 14.4)
    # Type "help" for help.
    # Note: No password is required for the gpadmin user in this sandbox environment.
    ```

    ```sql
    gpadmin=# SELECT VERSION();  -- Checks the database version.

    PostgreSQL 14.4 (Apache Cloudberry 1.0.0 build dev) on aarch64-unknown-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Oct 24 2023 10:24:28
    (1 row)
    ```

3. Alternatively, you can connect to the database directly from your host machine without entering the container:

    The Apache Cloudberry coordinator port (default 5432) is mapped to port **15432** on your host machine. You can use the `psql` client on your host to connect directly:

    ```shell
    # Connect from host machine
    psql -h localhost -p 15432 -d postgres -U gpadmin
    ```

    > [!NOTE]
    > - No password is required for the `gpadmin` user in this sandbox environment.
    > - Make sure you have PostgreSQL client (`psql`) installed on your host machine.
    > - The port mapping is: Container port `5432` → Host port `15432`

Now you have an Apache Cloudberry and can continue with [Apache Cloudberry Tutorials](https://cloudberry.apache.org/docs/)! Enjoy!

## Working with your Apache Cloudberry Docker environment

When working with the Apache Cloudberry Docker environment there are a few commands that will be useful to you.

**Stopping Your Single Container Deployment With Docker**

To stop the **single container** deployment while _keeping the data and state_ within the container, you can run the command below. This means that you can later start the container again and any changes you made to the containers will be persisted between runs.

```shell
docker stop cbdb-cdw
```

To stop the **single container** deployment and also remove the volume that belongs to the container, you can run the following command. Keep in mind this will remove the volume as well as the container associated which means any changes you've made inside of the container or any database state will be wiped and unrecoverable.

```shell
docker rm -f cbdb-cdw
```

**Stopping Your Multi-Container Deployment With Docker**

To stop the **multi-container** deployment while _keeping the data and state_ within the container, you can run the command below. This means that you can later start the container again and any changes you made to the containers will be persisted between runs.

```shell
docker compose -f docker-compose-rockylinux9.yml stop
```

To stop the **multi-container** deployment and also remove the network and volumes that belong to the containers, you can run the command below. Running this command means it will delete the containers as well as remove the volumes that the containers are associated with. This means any changes you've made inside of the containers or any database state will be wiped and unrecoverable.

```shell
docker compose -f docker-compose-rockylinux9.yml down -v
```

**Starting A Stopped Single Container Apache Cloudberry Docker Deployment**

If you've run any of the commands above that keep the Docker volumes persisted between shutting the containers down, you can use the following commands to bring that same deployment back up with it's previous state.

To start a **single container** deployment after it was shut down, you can simply run the following

```shell
docker start cbdb-cdw
```

**Starting A Stopped Multi-Container Apache Cloudberry Docker Deployment**

To start a **multi-container** deployment after it was shut down, you can run the following command.

```shell
docker compose -f docker-compose-rockylinux9.yml start
```

> [!NOTE]
> When starting a previously stopped Apache Cloudberry Docker environment, you'll need to manually start the database back up. To do this, just run the following commands once the container(s) are back up and running. The `gpstart` command is used for starting the database, and -a is a flag saying to start the database without prompting (non-interactive).

```shell
docker exec -it cbdb-cdw /bin/bash

[gpadmin@cdw /] gpstart -a
```