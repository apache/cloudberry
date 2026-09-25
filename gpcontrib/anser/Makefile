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
# Makefile for the anser extension.
#
# IDENTIFICATION
#    gpcontrib/anser/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = anser
OBJS = \
	$(WIN32RES) \
	src/anserbloomconsume.o \
	src/anserbloomproduce.o \
	src/anserdispatch.o \
	src/anserfilter.o \
	src/anserinit.o \
	src/anserpayload.o \
	src/anserplan.o \
	src/anserplanexec.o \
	src/ansersideband.o

PGFILEDESC = "anser - adaptive information sharing runtime filters"

# The subsystem needs no catalog objects: it is configured entirely by GUCs and
# travels over the dispatch connection, so there is nothing to CREATE EXTENSION.

# src/anserdispatch.c writes sideband messages onto the dispatcher's own libpq
# connections, so it needs libpq's internal headers (as the dispatcher itself
# does).  Deliberately NOT linked against libpq: the pq* writers we use stay
# global in the postgres binary, while the public PQ* API is made local there
# on purpose (src/backend/Makefile), so linking libpq.so would put a second
# copy of libpq on connections the backend's copy created.
PG_CPPFLAGS = -I$(srcdir)/include -I$(libpq_srcdir)
SHLIB_PREREQS = submake-libpq

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/anser
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
