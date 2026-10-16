#-------------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Makefile for anser_test, the harness the anser regression tests use to
# reach Anser's C API from SQL.
#
# A library of its own on purpose: linked into anser.so it would ride along
# into every backend, since anser is preloaded.  Nothing here belongs in a
# running cluster, so the parent's install does not build it -- see the
# install-anser-test target there.
#
# It links against nothing: anser.so is preloaded and dfmgr.c loads modules
# RTLD_GLOBAL, so the AnserXxx entry points resolve at load time.  That is
# what the PGDLLEXPORT markers on the public headers are for.
#
# IDENTIFICATION
#    gpcontrib/anser/anser_test/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = anser_test
OBJS = \
	$(WIN32RES) \
	anser_test.o

PGFILEDESC = "anser_test - test helpers for the anser extension"

EXTENSION = anser_test
DATA = anser_test--1.0.sql

PG_CPPFLAGS = -I$(srcdir)/../include

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/anser/anser_test
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
