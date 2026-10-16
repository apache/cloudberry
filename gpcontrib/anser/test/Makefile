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
# Unit tests for anser: plain programs, no cluster.
#
# The backend's own unit tests live in src/backend/*/test and go through
# mock.mk, which links a mock of every backend file; an extension is not
# backend code and does not fit that.  What is tested here does not need it
# either -- it works on caller-supplied memory -- so the few backend symbols
# anserfilter.c refers to from its other functions are stubbed in the test
# itself, where a wrong signature is a compile error.
#
# Built and run by the parent's installcheck; never installed.
#
# IDENTIFICATION
#    gpcontrib/anser/test/Makefile
#
#-------------------------------------------------------------------------

PROGRAM = anserfold_test
OBJS = anserfold_test.o anserfilter.o

PGFILEDESC = "anserfold_test - unit test for the Anser bloom part fold"
NO_INSTALL = 1

PG_CPPFLAGS = -I$(srcdir)/../include
PG_LIBS_INTERNAL += $(libpgport)

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/anser/test
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Its own copy rather than the one the extension builds: that one is built for
# a shared library, and depending on it would tie this to the parent's build
# order for no gain.
anserfilter.o: $(srcdir)/../src/anserfilter.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

# Not called "check": pgxs.mk already defines that target, and overriding it
# here would only earn a warning from make.
.PHONY: unitcheck
unitcheck: $(PROGRAM)
	./$(PROGRAM)
