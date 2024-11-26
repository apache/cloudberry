<!-- For a better file structure, we moved this guide from original
Greenplum Database READE.md here. Thanks all the original writers.-->

# Build Apache Cloudberry from Source Code

This guides describes how to build Apache Cloudberry from source code.

- For building on Linux systems, see [Compile and Install Apache Cloudberry on Linux](./README.Linux.md).
- For building on macOS system, see [Compile and Install Apache Cloudberry on macOS](./README.macOS.md).

## Build the database

```
# Configure build environment to install at /usr/local/cloudberry
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/cloudberry

# Compile and install
make -j8
make -j8 install

# Bring in greenplum environment for CBDB into your running shell
source /usr/local/cloudberry/greenplum_path.sh

# Start demo cluster
make create-demo-cluster
# (gpdemo-env.sh contains __PGPORT__, __COORDINATOR_DATA_DIRECTORY__ and __MASTER_DATA_DIRECTORY__ values)
source gpAux/gpdemo/gpdemo-env.sh
```

- The directory and the TCP ports for the demo cluster can be changed on the fly.
Instead of `make cluster`, consider:

```
DATADIRS=/tmp/cbdb-cluster PORT_BASE=5555 make cluster
```

- The TCP port for the regression test can be changed on the fly:

```
PGPORT=5555 make installcheck-world
```

- To turn GPORCA off and use Postgres planner for query optimization:
```
set optimizer=off;
```

- If you want to clean all generated files
```
make distclean
```

## Running tests

* The default regression tests

```
make installcheck-world
```

* The top-level target __installcheck-world__ will run all regression
  tests in CBDB against the running cluster. For testing individual
  parts, the respective targets can be run separately.

* The PostgreSQL __check__ target does not work. Setting up a
  Apache Cloudberry cluster is more complicated than a single-node
  PostgreSQL installation, and no-one's done the work to have __make
  check__ create a cluster. Create a cluster manually or use
  gpAux/gpdemo/ (example below) and run the toplevel __make
  installcheck-world__ against that. Patches are welcome!

* The PostgreSQL __installcheck__ target does not work either, because
  some tests are known to fail with Apache Cloudberry. The
  __installcheck-good__ schedule in __src/test/regress__ excludes
  those tests.

* When adding a new test, please add it to one of the CBDB-specific tests,
  in greenplum_schedule, rather than the PostgreSQL tests inherited from the
  upstream. We try to keep the upstream tests identical to the upstream
  versions, to make merging with newer PostgreSQL releases easier.

# Alternative Configurations

## Building Apache Cloudberry without GPORCA

Currently, CBDB is built with GPORCA by default. If you want to build CBDB
without GPORCA, configure requires `--disable-orca` flag to be set.

```
# Clean environment
make distclean

# Configure build environment to install at /usr/local/cloudberry
./configure --disable-orca --with-perl --with-python --with-libxml --prefix=/usr/local/cloudberry
```

## Building Apache Cloudberry with PXF

PXF is an extension framework for Greenplum Database/Apache Cloudberry
to enable fast access to external Hadoop datasets. Refer to
[PXF extension](../../gpcontrib/pxf_fdw/README.md) for more information.

Currently, Cloudberry is built with PXF by default (--enable-pxf is on).
In order to build Cloudberry without pxf, simply invoke `./configure` with additional option `--disable-pxf`.
PXF requires curl, so `--enable-pxf` is not compatible with the `--without-libcurl` option.

## Building Apache Cloudberry with Python3 enabled

Apache Cloudberry supports Python3 with plpython3u UDF

See [how to enable Python3](../../src/pl/plpython/README.md) for details.


# Development with Vagrant

There is a Vagrant-based [quickstart guide for developers](../vagrant/README.md).
