# Copyright (c) 2022-2023, PostgreSQL Global Development Group

backend_sources += files(
  'bitmapset.c',
  'extensible.c',
  'list.c',
  'makefuncs.c',
  'multibitmapset.c',
  'nodeFuncs.c',
  'nodes.c',
  'params.c',
  'print.c',
  'read.c',
  'tidbitmap.c',
  'value.c',
)

# these include .c files generated in ../../include/nodes, seems nicer to not
# add that as an include path for the whole backend
#
# outfast.c and readfast.c belong here too: Cloudberry's plan serialisation for
# dispatch includes outfast.funcs.c / readfast.funcs.c the same way.
nodefunc_sources = files(
  'copyfuncs.c',
  'equalfuncs.c',
  'outfast.c',
  'outfuncs.c',
  'readfast.c',
  'readfuncs.c',
)
nodefuncs = static_library('nodefuncs',
  nodefunc_sources,
  dependencies: [backend_code],
  include_directories: include_directories('../../include/nodes'),
  kwargs: internal_lib_args,
)
backend_link_with += nodefuncs
