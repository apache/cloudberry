# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# The node-defining headers gen_node_support.pl reads, in the order
# src/backend/nodes/Makefile lists them as node_headers. The order is not
# cosmetic: it decides the numbering of the NodeTag enum, so it has to match
# the Makefile exactly or a meson build and an autoconf build of the same
# source assign different tags -- an ABI difference between two builds of the
# same release. The Makefile's own warning: "re-ordering this list risks ABI
# breakage".
#
# Upstream's list, plus the Cloudberry headers that define nodes.
node_support_input_i = [
  'nodes/nodes.h',
  'nodes/primnodes.h',
  'nodes/parsenodes.h',
  'nodes/pathnodes.h',
  'nodes/plannodes.h',
  'nodes/execnodes.h',
  'access/amapi.h',
  'access/extprotocol.h',
  'access/formatter.h',
  'access/sdir.h',
  'access/tableam.h',
  'access/tsmapi.h',
  'access/tupdesc.h',
  'catalog/heap.h',
  'cdb/cdbgang.h',
  'commands/event_trigger.h',
  'commands/explain_gp.h',
  'commands/trigger.h',
  'executor/execdesc.h',
  'executor/tuptable.h',
  'foreign/fdwapi.h',
  'nodes/altertablenodes.h',
  'nodes/bitmapset.h',
  'nodes/extensible.h',
  'nodes/lockoptions.h',
  'nodes/miscnodes.h',
  'nodes/replnodes.h',
  'nodes/supportnodes.h',
  'nodes/tidbitmap.h',
  'nodes/value.h',
  'utils/queryenvironment.h',
  'utils/rel.h',
  'catalog/gp_distribution_policy.h',
  'cdb/cdbpathlocus.h',
]

node_support_input = []
foreach i : node_support_input_i
  node_support_input += meson.source_root() / 'src' / 'include' / i
endforeach

# Upstream's outputs, plus the outfast/readfast pair Cloudberry uses to
# serialise plans for dispatch to the segments.
node_support_output = [
  'nodetags.h',
  'outfuncs.funcs.c', 'outfuncs.switch.c',
  'readfuncs.funcs.c', 'readfuncs.switch.c',
  'outfast.funcs.c', 'outfast.switch.c',
  'readfast.funcs.c', 'readfast.switch.c',
  'copyfuncs.funcs.c', 'copyfuncs.switch.c',
  'equalfuncs.funcs.c', 'equalfuncs.switch.c',
  'queryjumblefuncs.funcs.c', 'queryjumblefuncs.switch.c',
]
# Only nodetags.h is installed, as src/include/Makefile does.
node_support_install = [dir_include_server / 'nodes']
foreach o : node_support_output
  if o != 'nodetags.h'
    node_support_install += false
  endif
endforeach

generated_nodes = custom_target('nodetags.h',
  input: node_support_input,
  output: node_support_output,
  depend_files: catalog_pm,
  command: [
    perl, files('../../backend/nodes/gen_node_support.pl'),
    '-o', '@OUTDIR@',
    '@INPUT@'],
  install: true,
  install_dir: node_support_install,
)
generated_headers += generated_nodes[0]

# autoconf generates the file there, ensure we get a conflict
generated_sources_ac += {'src/backend/nodes': node_support_output + ['node-support-stamp']}
generated_sources_ac += {'src/include/nodes': ['header-stamp']}
