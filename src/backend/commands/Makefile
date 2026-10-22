#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/commands
#
# IDENTIFICATION
#    src/backend/commands/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/commands
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

explain.o: explain_gp.c

OBJS = \
	aggregatecmds.o \
	alter.o \
	amcmds.o \
	analyze.o \
	async.o \
	cluster.o \
	collationcmds.o \
	comment.o \
	constraint.o \
	conversioncmds.o \
	copy.o \
	copyfrom.o \
	copyfromparse.o \
	copyto.o \
	createas.o \
	dbcommands.o \
	define.o \
	dirtablecmds.o \
	discard.o \
	dropcmds.o \
	event_trigger.o \
	explain.o \
	extension.o \
	foreigncmds.o \
	functioncmds.o \
	indexcmds.o \
	lockcmds.o \
	matview.o \
	opclasscmds.o \
	operatorcmds.o \
	policy.o \
	portalcmds.o \
	prepare.o \
	proclang.o \
	publicationcmds.o \
	schemacmds.o \
	seclabel.o \
	sequence.o \
	statscmds.o \
	subscriptioncmds.o \
	tablecmds.o \
	tablespace.o \
	trigger.o \
	tsearchcmds.o \
	typecmds.o \
	user.o \
	vacuum.o \
	vacuumparallel.o \
	variable.o \
	view.o \
	pg_profile.o \
	storagecmds.o \
	tag.o

OBJS += analyzefuncs.o analyzeutils.o extprotocolcmds.o exttablecmds.o queue.o
OBJS += resgroupcmds.o tablecmds_gp.o vacuum_ao.o taskcmds.o

include $(top_srcdir)/src/backend/common.mk
