![Greengage](ggdb_100x100.png)

Greengage Database (GPDB) is an advanced, fully featured, open
source data warehouse, based on PostgreSQL. It provides powerful and rapid analytics on
petabyte scale data volumes. Uniquely geared toward big data
analytics, Greengage Database is powered by the world’s most advanced
cost-based query optimizer delivering high analytical query
performance on large data volumes.

The Greengage project is released under the [Apache 2
license](http://www.apache.org/licenses/LICENSE-2.0). We want to thank
all our past and present community contributors and are really interested in
all new potential contributions. For the Greengage Database community
no contribution is too small, we encourage all types of contributions.

## Overview

A Greengage cluster consists of a __master__ server, and multiple
__segment__ servers. All user data resides in the segments, the master
contains only metadata. The master server, and all the segments, share
the same schema.

Users always connect to the master server, which divides up the query
into fragments that are executed in the segments, and collects the results.

More information can be found on the [project website](https://greengagedb.org/).

## Building Greengage Database with GPORCA
GPORCA is a cost-based optimizer which is used by Greengage Database in
conjunction with the PostgreSQL planner.  It is also known as just ORCA, and
Pivotal Optimizer. The code for GPORCA resides src/backend/gporca. It is built
automatically by default.

### Installing dependencies (for macOS developers)
Follow [these macOS steps](README.macOS.md) for getting your system ready for GPDB

### Installing dependencies (for Linux developers)
Follow [appropriate linux steps](README.linux.md) for getting your system ready for GPDB

### Build the database

```
# Configure build environment to install at /usr/local/gpdb
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb

# Compile and install
make -j8
make -j8 install

# Bring in greengage environment into your running shell
source /usr/local/gpdb/greengage_path.sh

# Start demo cluster
make create-demo-cluster
# (gpdemo-env.sh contains __PGPORT__ and __MASTER_DATA_DIRECTORY__ values)
source gpAux/gpdemo/gpdemo-env.sh
```

The directory, the TCP ports, the number of segments, and the existence of
standbys for segments and coordinator for the demo cluster can be changed
on the fly.
Instead of `make create-demo-cluster`, consider:

```
DATADIRS=/tmp/gpdb-cluster PORT_BASE=5555 NUM_PRIMARY_MIRROR_PAIRS=1 WITH_MIRRORS=false make create-demo-cluster
```

The TCP port for the regression test can be changed on the fly:

```
PGPORT=5555 make installcheck-world
```

To turn GPORCA off and use Postgres planner for query optimization:
```
set optimizer=off;
```

If you want to clean all generated files
```
make distclean
```

## Running tests

* The default regression tests

```
make installcheck-world
```

* The top-level target __installcheck-world__ will run all regression
  tests in GPDB against the running cluster. For testing individual
  parts, the respective targets can be run separately.

* The PostgreSQL __check__ target does not work. Setting up a
  Greengage cluster is more complicated than a single-node PostgreSQL
  installation, and no-one's done the work to have __make check__
  create a cluster. Create a cluster manually or use gpAux/gpdemo/
  (example below) and run the toplevel __make installcheck-world__
  against that. Patches are welcome!

* The PostgreSQL __installcheck__ target does not work either, because
  some tests are known to fail with Greengage. The
  __installcheck-good__ schedule in __src/test/regress__ excludes those
  tests.

* When adding a new test, please add it to one of the GPDB-specific tests,
  in greengage_schedule, rather than the PostgreSQL tests inherited from the
  upstream. We try to keep the upstream tests identical to the upstream
  versions, to make merging with newer PostgreSQL releases easier.

## Alternative Configurations

### Building GPDB without GPORCA

Currently, GPDB is built with GPORCA by default. If you want to build GPDB
without GPORCA, configure requires `--disable-orca` flag to be set.
```
# Clean environment
make distclean

# Configure build environment to install at /usr/local/gpdb
./configure --disable-orca --with-perl --with-python --with-libxml --prefix=/usr/local/gpdb
```

### Building GPDB with gpperfmon enabled

gpperfmon tracks a variety of queries, statistics, system properties, and metrics.
To build with it enabled, change your `configure` to have an additional option
`--enable-gpperfmon`

See [more information about gpperfmon here](gpAux/gpperfmon/README.md)

gpperfmon is dependent on several libraries like apr, apu, and libsigar

### Building GPDB with Python3 enabled

GPDB supports Python3 with plpython3u UDF

See [how to enable Python3](src/pl/plpython/README.md) for details.


### Building GPDB client tools on Windows

See [Building GPDB client tools on Windows](README.windows.md) for details.

## Development with Docker

See [README.docker.md](README.docker.md).

We provide a docker image with all dependencies required to compile and test
GPDB [(See Usage)](src/tools/docker/README.md).

## Development with Vagrant

There is a Vagrant-based [quickstart guide for developers](src/tools/vagrant/README.md).

## Code layout

The directory layout of the repository follows the same general layout
as upstream PostgreSQL. There are changes compared to PostgreSQL
throughout the codebase, but a few larger additions worth noting:

* __gpMgmt/__

  Contains Greengage-specific command-line tools for managing the
  cluster. Scripts like gpinit, gpstart, gpstop live here. They are
  mostly written in Python.

* __gpAux/__

  Contains Greengage-specific release management scripts, and vendored
  dependencies. Some additional directories are submodules and will be
  made available over time.

* __gpcontrib/__

  Much like the PostgreSQL contrib/ directory, this directory contains
  extensions such as gpfdist and gpmapreduce which are Greengage-specific.

* __doc/__

  In PostgreSQL, the user manual lives here. In Greengage, the user
  manual is maintained separately and only the reference pages used
  to build man pages are here.

* __ci/__

  Contains configuration files for the GPDB continuous integration system.

* __src/backend/cdb/__

  Contains larger Greengage-specific backend modules. For example,
  communication between segments, turning plans into parallelizable
  plans, mirroring, distributed transaction and snapshot management,
  etc. __cdb__ stands for __Cluster Database__ - it was a workname used in
  the early days. That name is no longer used, but the __cdb__ prefix
  remains.

* __src/backend/gpopt/__

  Contains the so-called __translator__ library, for using the GPORCA
  optimizer with Greengage. The translator library is written in C++
  code, and contains glue code for translating plans and queries
  between the DXL format used by GPORCA, and the PostgreSQL internal
  representation.

* __src/backend/gporca/__

  Contains the GPORCA optimizer code and tests. This is written in C++. See
  [README.md](src/backend/gporca/README.md) for more information and how to
  unit-test GPORCA.

* __src/backend/fts/__

  FTS is a process that runs in the master node, and periodically
  polls the segments to maintain the status of each segment.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
