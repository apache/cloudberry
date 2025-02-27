# ----------------------------------------------------------------------
# Makefile for Apache Cloudberry Demo
# ----------------------------------------------------------------------

#
# INCLUDES
#

top_builddir = ../..
-include $(top_builddir)/src/Makefile.global

SHELL := /usr/bin/bash

all:
	$(MAKE) clean
	$(MAKE) check
	$(MAKE) create-demo-cluster
	$(MAKE) probe

cluster create-demo-cluster:
	@source gpdemo-defaults.sh && ./demo_cluster.sh
	@echo ""

probe:
	@source gpdemo-defaults.sh && ./probe_config.sh
	@echo ""

check:
	@source gpdemo-defaults.sh && ./demo_cluster.sh -c
	@echo ""

clean destroy-demo-cluster:
	@echo "======================================================================"
	@echo "Deleting cluster.... "
	@echo "======================================================================"
	@source gpdemo-defaults.sh && ./demo_cluster.sh -d
	@echo ""

.PHONY: all cluster create-demo-cluster create-segwalrep-cluster probe clean check clean destroy-demo-cluster
