# contrib/diskquota/Makefile

MODULE_big = diskquota

EXTENSION = diskquota
DATA = diskquota--1.0.sql
SRCDIR = ./
FILES = diskquota.c enforcement.c quotamodel.c gp_activetable.c
OBJS = diskquota.o enforcement.o quotamodel.o gp_activetable.o 
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
SHLIB_PREREQS = submake-libpq

REGRESS = dummy
REGRESS_OPTS = --schedule=diskquota_schedule

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp_diskquota
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
