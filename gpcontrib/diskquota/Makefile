# contrib/diskquota/Makefile

MODULE_big = diskquota

EXTENSION = diskquota
DATA = diskquota--1.0.sql
SRCDIR = ./
FILES = diskquota.c enforcement.c quotamodel.c gp_activetable.c
OBJS = diskquota.o enforcement.o quotamodel.o gp_activetable.o 
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

REGRESS = dummy
REGRESS_OPTS = --schedule=diskquota_schedule --init-file=init_file

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
