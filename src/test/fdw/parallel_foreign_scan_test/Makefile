EXTENSION = parallel_foreign_scan_test_fdw
DATA = parallel_foreign_scan_test_fdw--1.0.sql
MODULE_big = parallel_foreign_scan_test_fdw
OBJS = parallel_foreign_scan_test_fdw.o
REGRESS = parallel_foreign_scan_test

subdir = src/test/fdw/parallel_foreign_scan_test
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
NO_PGXS = 1
include $(top_srcdir)/src/makefiles/pgxs.mk

installcheck: install
