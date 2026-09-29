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

# Apache Cloudberry (Incubating) License Audit Notes

This file documents licensing clarifications and exceptions as part of ASF release readiness for Apache Cloudberry (Incubating).

## GPL-licensed File Removed

- gpMgmt/bin/pythonSrc/ext/pylint-0.21.0.tar.gz

This file is licensed under the GPL (Category X) and has been removed from the source tree to comply with ASF release policy.

## Historical Attribution Under Apache License 2.0

The following entities have contributed to the Greenplum-based source code under the Apache License 2.0:

- Greenplum, Inc.
- EMC Corporation
- VMware, Inc.
- Pivotal Software
- Broadcom Inc.

RAT matchers are used to classify their license headers accordingly.

## Compressed Files in Source

The following compressed files are included in the source tree. These files are archives of text files used for testing purposes and do not contain binary executables. They are not used during the build process.

- contrib/formatter_fixedwidth/data/fixedwidth_small_correct.tbl.gz
- gpMgmt/demo/gppkg/sample-sources.tar.gz
- src/bin/gpfdist/regress/data/exttab1/nation.tbl.gz
- src/bin/gpfdist/regress/data/gpfdist2/gz_multi_chunk.tbl.gz
- src/bin/gpfdist/regress/data/gpfdist2/gz_multi_chunk_2.tbl.gz
- src/bin/gpfdist/regress/data/gpfdist2/lineitem.tbl.bz2
- src/bin/gpfdist/regress/data/gpfdist2/lineitem.tbl.gz

## Binary Files in Source

A source release should not carry compiled artifacts, so the licence audit
workflow fails on binary file extensions unless the file is allowlisted. The
following are allowed, with the reason for each.

- gpMgmt/test/behave/mgmt_utils/steps/data/sample.gppkg

  A small sample package that the `gppkg` behave suite installs and removes.
  The tests exercise package handling itself, so the fixture has to be a real
  package.

- src/bin/pgevent/MSG00001.bin

  Inherited from PostgreSQL, where it is also shipped. It is the output of the
  Microsoft Message Compiler and is referenced from `pgmsgevent.rc` when
  building the Windows event log DLL; `src/bin/pgevent/README` describes how it
  is produced. Cloudberry does not build it: `src/bin/Makefile` only puts
  `pgevent` in `SUBDIRS` when `PORTNAME` is `win32`.

The PAX Python API test data under
`contrib/pax_storage/src/api/python3/test/` (`test.file1` through `test.file9`,
plus `test.file3.vm1`, `test.file3.vm2` and `test.file7.toast`) is also binary:
each file is a PAX-format data file covering a particular set of column types,
read by `paxpy_test.py`. These names carry no recognised extension, so the
workflow's extension-based check does not see them; they are recorded here so
the set is documented rather than invisible.
