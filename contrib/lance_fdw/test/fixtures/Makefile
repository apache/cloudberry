# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Lance fixtures for the lance_fdw regression suites.  See README.md.
#
#   make gen      regenerate data/, expected/*.jsonl and manifest.json
#   make upload   sync data/ into the MinIO bucket from test/run/env.sh
#   make test     run the fixture tests (generates data/ if it is missing)
#
# The host virtualenv holds pylance/pyarrow/pytest/minio; override PYTHON to
# point somewhere else.

PYTHON ?= python3
GEN_FLAGS ?=
UPLOAD_FLAGS ?=
PYTEST_FLAGS ?= -q

.PHONY: all gen upload test clean

all: gen

gen:
	$(PYTHON) gen_fixtures.py $(GEN_FLAGS)

upload:
	$(PYTHON) upload_minio.py $(UPLOAD_FLAGS)

test:
	$(PYTHON) -m pytest $(CURDIR) $(PYTEST_FLAGS)

# Only removes generated datasets; manifest.json and expected/ are committed.
clean:
	rm -rf $(CURDIR)/data
