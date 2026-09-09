# Copyright (c) 2022-2023, PostgreSQL Global Development Group

backend_sources += files(
  'analyzejoins.c',
  'createplan.c',
  'initsplan.c',
  'planagg.c',
  'planmain.c',
  'planner.c',
  'setrefs.c',
  'subselect.c',
  'aqumv.c',
  'joinpartprune.c',
  'planshare.c',
  'transform.c',
)

# orca.c is the entry point into the ORCA optimizer; built only with ORCA.
# Mirrors: ifeq ($(enable_orca),yes) / OBJS += orca.o
if get_option('orca').enabled()
  backend_sources += files('orca.c')
endif
