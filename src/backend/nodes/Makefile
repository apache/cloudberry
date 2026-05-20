#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes
#
# IDENTIFICATION
#    src/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/nodes
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	bitmapset.o \
	copyfuncs.o \
	equalfuncs.o \
	extensible.o \
	list.o \
	makefuncs.o \
	multibitmapset.o \
	nodeFuncs.o \
	nodes.o \
	outfast.o \
	outfuncs.o \
	params.o \
	print.o \
	read.o \
	readfast.o \
	readfuncs.o \
	tidbitmap.o \
	value.o

# readfast.c #includes readfuncs.c. Same with outfast.c and outfast.c
readfast.o: readfuncs.c readfast.c readfuncs_common.c
outfast.o: outfuncs.c outfast.c outfuncs_common.c

include $(top_srcdir)/src/backend/common.mk
