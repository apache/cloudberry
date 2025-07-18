# How to run tests

## Build docker gpdb image with developer options

Change directory to gpdb sources destination. Make sure that directry doesn't contain binary objects from previous builds. Then run:

```bash 
docker build -t gpdb6_regress:latest -f ci/Dockerfile.ubuntu .
```


## Full regression tests suite run

We need to execute [../concourse/scripts/ic_gpdb.bash](../concourse/scripts/ic_gpdb.bash) in container to create demo cluster and run different test suites against it:

```bash
docker run --name gpdb6_opt_on --rm -it -e TEST_OS=centos \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on' installcheck-world" \
  --sysctl 'kernel.sem=500 1024000 200 4096' gpdb6_regress:latest \
  bash -c "ssh-keygen -A && /usr/sbin/sshd && bash /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash"
```

* we need to modify `MAKE_TEST_COMMAND` environment variable to run different suite. e.g. we may run test againt Postgres optimizer or ORCA with altering `PGOPTIONS` environment variable;
* we need to run container as `--privileged` to run debugger inside it
* we need to increase semaphore amount to be able to run demo cluster
* we need running ssh server to be able to run demo cluster

## ORCA linter

```bash
docker build -t orca-linter:test -f ci/Dockerfile.linter .
docker run --rm -it orca-linter:test
```

The work directory must be clean to pass this test. Please, stage or even commit your changes.

## ORCA unit test run

```bash
docker run --rm -it gpdb6_regress:latest bash -c "gpdb_src/concourse/scripts/unit_tests_gporca.bash"
```

## How to run demo cluster inside docker container manually

1. Build or pull from internal registry (see above) needed image
1. Start container with
   ```bash
   docker run --name gpdb6_demo --rm -it --sysctl 'kernel.sem=500 1024000 200 4096' gpdb6_regress:latest \
     bash -c "ssh-keygen -A && /usr/sbin/sshd && bash"
   ```
1. Run the next commands in container
   ```bash
   source gpdb_src/concourse/scripts/common.bash
   # this command unpack binaries to `/usr/local/greengage-db-devel/`
   install_and_configure_gpdb
   gpdb_src/concourse/scripts/setup_gpadmin_user.bash
   make_cluster
   su - gpadmin -c '
   source /usr/local/greengage-db-devel/greengage_path.sh;
   source gpdb_src/gpAux/gpdemo/gpdemo-env.sh; 
   psql postgres'
   ```

## Behave test run

Behave tests now can run locally with docker-compose.

Feature files are located in `gpMgmt/test/behave/mgmt_utils`
Before run tests you need to build a docker-image
```bash
docker build -t "greengage6_regress:${BRANCH_NAME}" -f ci/Dockerfile.ubuntu .
```

Command to run features:
```bash
# Run all tests
export IMAGE="greengage6_regress:${BRANCH_NAME}"
bash ci/scripts/run_behave_tests.bash

# Run specific features
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```


Tests use `allure-behave` package and store allure output files in `allure-results` folder
**NOTE** that `allure-behave` has too old a version because it is compatible with `python2`.
Also, the allure report for each failed test has gpdb logs attached files. See `gpMgmt/test/behave_utils/ci/formatter.py`
It required to add `gpMgmt/tests` directory to `PYTHONPATH`. 

Greengage cluster in Docker containers has its own peculiarities in preparing a cluster for tests.
All tests are run in one way or another on the demo cluster, wherever possible. 
For example, cross_subnet tests or tests with tag `concourse_cluster` currently not worked because of too complex cluster preconditions.

Tests in a `docker compose` cluster use the same ssh keys for `gpadmin` user and pre-add the cluster hosts to `.ssh/know_hosts` and `/etc/hosts`.

Docker containers have installed `sigar` libraries. It is required only for `gpperfmon` tests.

## Resource group test

```bash
bash ci/scripts/run_resgroup_test.bash
```
