# Auto-Build Cloudberry Database from Source Code

You can build Cloudberry Database from source code in two ways: manually or automatically.

For the manual build, you need to manually set up many system configurations and download third-party dependencies, which is quite cumbersome and error-prone.

To make the job easier, you are recommended to use the automated deployment method blessed by virtualization technology. The automation method simplifies the deployment process, reduces time costs, and allows developers to focus more on business code development.

To go automated, you can choose either of the following ways based on your needs and project requirements:

- [Vagrant deployment](#deploy-with-vagrant): Vagrant allows you to easily create and manage vbox virtual machine environments, streamlining the deployment process.
- [Docker deployment](#deploy-with-docker): Docker provides a containerized environment, offering higher levels of isolation and ease of management for building and testing.

This document introduces how to use Vagrant and Docker to auto-build Cloudberry Database from source code, respectively. In addition, it also introduces the file sharing mechanism that ensures that the Cloudberry Database is mapped to a specific path in the virtualized environment.

## Ready for Work

Before you begin your automation build, make sure the following preparation works are done:

1. Download the Cloudberry Database source code into your local machine:

    ```bash
    git clone https://github.com/apache/cloudberry.git
    ```

2. Install the required [Vagrant](https://developer.hashicorp.com/vagrant/docs/installation) or [Docker](https://docs.docker.com/desktop/install/mac-install/) in your local environment, depending on your deployment preference.


## Deploy with Vagrant

1. Start your Vagrant virtual machine.
2. Start the automatic compilation and deployment process.

    ```
    $ cd /cloudberry/deploy/vagrant
    $ vagrant up
    ```

3. Log into your Vagrant virtual machine via `ssh`:

    ```
    $ vagrant ssh
    ```

4. Connect to Cloudberry Database, and check whether the psql client can be started and connect to the database.

    ```
    $ su - gpadmin 
    $ psql postgres
    postgres=# select * from gp_segment_configuration;
    ```

5. Start the regression test. Note that this test is expected to be performed with the `gpadmin` user.

    ```
    $ cd ~/workspace/cbdb_dev
    $ make installcheck-world
    ```

## Deploy with Docker

1. Configure the Docker image repository.  

   Configure the Docker image repository URL by adding the following JSON format configuration in the **Preferences** -\> **Docker Engine** tab in your Docker Desktop setting.

    ```json
    "insecure-registries": [
        "docker.artifactory.hashdata.xyz"
    ]
    ```

2. Start the Docker container.

   The `docker_start.sh` scripts will automatically launch the Docker container, and compile and deploy Cloudberry Database within the container.

    ```bash
    $ cd ~/workspace/cbdb/deploy/docker
    $ bash docker_start.sh
    ```

3. Log into the Docker container.

    ```bash
    $ docker exec -it (cotainer ID) /bin/bash
    ```

4. Connect to Cloudberry Database.

    ```bash
    $ su gpadmin
    $ psql postgres
    postgres=# select * from gp_segment_configuration;
    ```    

5. Start the regression test.

    ```bash
    $ cd ~/workspace/cbdb_dev
    $ make installcheck-world
    ```

## About the automation script

The whole script is subdivided into three main parts:

```
|-- cbdb_deploy.sh
|-- cbdb_env.sh
|-- docker
|   |-- Dockerfile
|   |-- docker_deploy.sh
|   `-- docker_start.sh
|-- script
|   `-- install_etcd.sh
`-- vagrant
    |-- Vagrantfile
    `-- vagrant_release.sh
```

1. The automation deployment script running in the virtualization environment.

   Through the mechanism provided by Vagrant vagrantfile provision or Docker dockerfile ENTRYPOINTS:

    * Provides subcommands that operates the build & test environment
    * Provides the necessary configuration variables to the internal script of the virtual machine and trigger specific operations

   Passing the following compilation options through `/code/depoly/cbdb_ deploy.sh` script:

    * Compilation options
    * Deployment options
    * Test options

   Passing the environment variable parameters required for automation build & test through `/code/depoly/cbdb_env.sh` script.

2. Vagrant scripts

   2.1 `Vagrantfile`

    * Downloads the Vagrant virtual machine box and local virtual box creation.
    * The host code repository path is mounted on virtual machine with read-only permission, for example: source code path to `/home/gpadmin/workspace/cbdb` directory
    * Copy the source file directory to `/home/gpadmin/workspace/cbdb_dev`, which is the actual working directory for automation build & test.
    * Ensure that the login user is `gpadmin`.



3. Docker scripts

   3.1 `Dockerfile`

    * Docker image file packaging
    * Docker container service startup configuration, such as SSH service

   3.2 `docker_deploy.sh`

    * Generates SSH public keys and automatic SSH login configuration
    * Start Docker automation deployment for build & test

   3.3 `docker_start.sh`

    * Downloads Docker image version remotely
    * The host code repository path is mounted on virtual machine with read-only permission, for example: source code path to `/home/gpadmin/workspace/cbdb` directory
    * Copy the source file directory to `/home/gpadmin/workspace/cbdb_dev`, which is the actual working directory for automation build & test.
    * Ensure that the login user is `gpadmin`

