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
# contrib/datalake_fdw/test/automation/Makefile

.PHONY: all test smoke-test check-services list-categories help clean

all: test

help:
	@echo 'datalake_fdw automation tests'
	@echo
	@echo '  make test             run the smoke categories (default)'
	@echo '  make smoke-test       same'
	@echo '  make check-services   report which external services answer'
	@echo '  make list-categories  list categories and what each one needs'
	@echo '  make clean            remove run output'
	@echo
	@echo '  CATEGORIES="..."      run only these categories'
	@echo
	@echo 'Service addresses come from config/test_config.env, which reads them'
	@echo 'from the environment first.  A category whose services are absent is'
	@echo 'skipped and reported as skipped.'

test: smoke-test

smoke-test:
	@bash scripts/test/run_smoke_tests.sh

check-services:
	@bash scripts/setup/check_services.sh

list-categories:
	@printf '%-16s %s\n' 'CATEGORY' 'REQUIRES'
	@printf '%-16s %s\n' 'iceberg_am' '(nothing external)'

clean:
	rm -rf sqlrepo/smoke/*/results
