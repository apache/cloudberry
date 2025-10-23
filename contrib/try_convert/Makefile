# contrib/try_convert/Makefile

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

MODULE_big	= try_convert
OBJS		= try_convert.o $(WIN32RES)

EXTENSION = try_convert
DATA = try_convert--1.0.sql
PGFILEDESC = "try_convert - function for type cast"
REGRESS = try_convert

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/try_convert
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: generate-tests
generate-tests:
	mkdir -p input
	mkdir -p output
	python3 scripts/generate_test.py

all: generate-tests

.PHONY: verify
verify:
	python3 scripts/verify.py
