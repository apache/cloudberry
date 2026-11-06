# Copyright (c) 2022-2023, PostgreSQL Global Development Group

backend_build_deps = [backend_code]
backend_sources = []
backend_link_with = [pgport_srv, common_srv]

generated_backend_sources = []
post_export_backend_sources = []

subdir('access')
subdir('archive')
subdir('backup')
subdir('bootstrap')
subdir('catalog')
subdir('commands')
subdir('executor')
subdir('foreign')
subdir('jit')
subdir('lib')
subdir('libpq')
subdir('main')
subdir('nodes')
subdir('optimizer')
subdir('parser')
subdir('partitioning')
subdir('port')
subdir('postmaster')
subdir('regex')
subdir('replication')
subdir('rewrite')
subdir('statistics')
subdir('storage')
subdir('tcop')
subdir('tsearch')
subdir('utils')
subdir('task')
subdir('crypto')
subdir('fts')
subdir('cdb')

# ifeq ($(enable_orca),yes) / SUBDIRS += gporca gpopt
if xerces.found()
  subdir('gporca')
  subdir('gpopt')
endif

subdir('po', if_found: libintl)


backend_link_args = []
backend_link_depends = []

# Cloudberry compiles frontend symbols into the postgres binary (the backend
# connects to other segments), while extensions dynamically link libpq.so.
# Some symbols are unsafe to export from both -- e.g. functions that use
# palloc/pfree in the backend and malloc/free in the frontend. The linker is
# told to keep them private in postgres, matching SYMBOL_MAPPING_FLAGS in
# src/backend/Makefile.
gen_symbol_map = files('gen_symbol_map.py')
libpq_exports_txt = meson.source_root() / 'src/interfaces/libpq/exports.txt'

if host_system == 'darwin'
  symbol_map = custom_target('hide_symbols.list',
    output: 'hide_symbols.list',
    command: [python, gen_symbol_map, 'darwin', libpq_exports_txt, '@OUTPUT@'],
  )
  backend_link_args += '-Wl,-unexported_symbols_list,@0@'.format(symbol_map.full_path())
  backend_link_depends += symbol_map
elif host_system == 'linux'
  symbol_map = custom_target('postgres_symbols.map',
    output: 'postgres_symbols.map',
    command: [python, gen_symbol_map, 'linux', libpq_exports_txt, '@OUTPUT@'],
  )
  backend_link_args += '-Wl,--version-script=@0@'.format(symbol_map.full_path())
  backend_link_depends += symbol_map
else
  symbol_map = []
endif


# On windows when compiling with msvc we need to make postgres export all its
# symbols so that extension libraries can use them. For that we need to scan
# the constituting objects and generate a file specifying all the functions as
# exported (variables need an "import" declaration in the header, hence
# PGDLLEXPORT, but functions work without that, due to import libraries
# basically being trampolines).
#
# For dtrace probes we need to invoke dtrace on all input files, before
# linking the final executable (see more below).
#
#
# On meson there's currently no easy way to do this that I found. So we build
# a static library with all the input objects, run our script to generate
# exports, and build the final executable using that static library
#
# We could do that only if either dtrace or msvc is in use, but it seems
# easier to just always do so.
#
# Can't name the static library 'postgres', because msbuild ends up with a
# conflict for the .pdb file otherwise.

postgres_lib = static_library('postgres_lib',
  backend_sources + timezone_sources + generated_backend_sources,
  link_whole: backend_link_with,
  dependencies: backend_build_deps,
  c_pch: pch_postgres_h,
  kwargs: internal_lib_args,
)

if cc.get_id() == 'msvc'
  postgres_def = custom_target('postgres.def',
    command: [perl, files('../tools/msvc/gendef.pl'),
              '--arch', host_cpu,
              '--tempdir', '@PRIVATE_DIR@',
              '--deffile', '@OUTPUT@',
              '@INPUT@'],
    input: [postgres_lib, common_srv, pgport_srv],
    output: 'postgres.def',
    depends: [postgres_lib, common_srv, pgport_srv],
    install: false,
    build_by_default: false,
  )

  backend_link_args += '/DEF:@0@'.format(postgres_def.full_path())
  backend_link_depends += postgres_def
  # Due to the way msvc and meson's precompiled headers implementation
  # interact, we need to have symbols from the full library available. Could
  # be restricted to b_pch=true.
  backend_link_with += postgres_lib

elif host_system == 'aix'
  # The '.' argument leads mkldexport.sh to emit "#! .", which refers to the
  # main executable, allowing extension libraries to resolve their undefined
  # symbols to symbols in the postgres binary.
  postgres_imp = custom_target('postgres.imp',
    command: [files('port/aix/mkldexport.sh'), '@INPUT@', '.'],
    input: postgres_lib,
    output: 'postgres.imp',
    capture: true,
    install: true,
    install_dir: dir_lib,
    build_by_default: false,
  )
  backend_link_args += '-Wl,-bE:@0@'.format(postgres_imp.full_path())
  backend_link_depends += postgres_imp
endif

backend_input = []
backend_objs = [postgres_lib.extract_all_objects(recursive: false)]

# As of 1/2010:
# The probes.o file is necessary for dtrace support on Solaris, and on recent
# versions of systemtap.  (Older systemtap releases just produce an empty
# file, but that's okay.)  However, macOS's dtrace doesn't use it and doesn't
# even recognize the -G option.  So, build probes.o except on macOS.
# This might need adjustment as other platforms add dtrace support.
#
# On at least linux we don't actually need to pass in all the objects, but
# at least on FreeBSD and Solaris we have to.
#
# XXX: The reason we don't use the objects for generated sources is that doing
# so is not supported in older meson versions. Luckily we don't have probes in
# generated sources...
if dtrace.found() and host_system != 'darwin'
  backend_input += custom_target(
    'probes.o',
    input: ['utils/probes.d', postgres_lib.extract_objects(backend_sources, timezone_sources)],
    output: 'probes.o',
    command: [dtrace, '-C', '-G', '-o', '@OUTPUT@', '-s', '@INPUT@'],
    install: false,
  )
endif

if host_system == 'windows'
  post_export_backend_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'postgres',
    '--FILEDESC', 'PostgreSQL Server',])
endif

# src/backend/Makefile links postgres with $(CXX) because ORCA is C++.
# meson picks the linker from the target's own sources, which are all C, so
# the C++ linker has to be requested explicitly when ORCA is linked in.
postgres_link_language = xerces.found() ? 'cpp' : 'c'

postgres = executable('postgres',
  backend_input,
  # main.c is not part of backend_sources: src/backend/Makefile filters
  # main/main.o out of libpostgres.so and links it into this binary instead.
  sources: post_export_backend_sources + main_file,
  objects: backend_objs,
  link_args: backend_link_args,
  link_with: backend_link_with,
  link_language: postgres_link_language,
  link_depends: backend_link_depends,
  export_dynamic: true,
  implib: true,
  dependencies: backend_build_deps,
  kwargs: default_bin_args,
)

backend_targets += postgres

# --enable-shared-postgres-backend (default yes): in addition to the fully
# linked postgres binary, expose the backend as a shared library. PAX links
# against it. main.c is excluded (see src/backend/main/meson.build).
#
# --enable-link-postgres-with-shared (default no) would make postgres link
# against this library instead of the objects directly; that variant is not
# implemented yet.
if get_option('shared_postgres_backend')
  # main.c provides progname and other globals but is excluded from this
  # library (it is linked into the postgres executable), so the library has
  # undefined symbols that are resolved at load time.
  #
  # The Makefile only needs an explicit flag on darwin, because it never asks
  # the linker to reject undefined symbols in the first place. meson does:
  # b_lundef defaults to true, which passes -Wl,--no-undefined on Linux and
  # -Wl,-undefined,error on darwin. Turning it off for this target covers both
  # platforms -- on darwin meson then emits -Wl,-undefined,dynamic_lookup,
  # which is exactly what LIBPOSTGRES_SO_LDFLAGS sets by hand.
  libpostgres_so = shared_library('postgres',
    objects: backend_objs,
    link_whole: backend_link_with,
    link_args: backend_link_args,
    link_language: postgres_link_language,
    link_depends: backend_link_depends,
    dependencies: backend_build_deps,
    name_prefix: 'lib',
    override_options: ['b_lundef=false'],
    install: true,
    install_dir: dir_lib,
  )
  backend_targets += libpostgres_so
endif

pg_mod_c_args = cflags_mod
pg_mod_cpp_args = cxxflags_mod
pg_mod_link_args = ldflags_sl + ldflags_mod
pg_mod_link_depend = []

# A few platforms like MacOS and Windows link shared modules against postgres,
# or a [import] library derived from it. Set up the link flags for that.
if mod_link_args_fmt.length() > 0
  # To avoid unnecessary build-time dependencies on other operating systems,
  # only add the dependency when necessary.
  pg_mod_link_depend += postgres

  name = mod_link_with_name.format('postgres')
  link_with_uninst = meson.current_build_dir() / name
  link_with_inst = '${@0@}/@1@'.format(mod_link_with_dir, name)

  foreach el : mod_link_args_fmt
    pg_mod_link_args += el.format(link_with_uninst)
  endforeach
endif


# Note there's intentionally no dependency on pgport/common here - we want the
# symbols from the main binary for extension modules, rather than the
# extension linking separately to pgport/common.
backend_mod_code = declare_dependency(
  compile_args: pg_mod_c_args,
  include_directories: postgres_inc,
  link_args: pg_mod_link_args,
  sources: generated_headers + generated_backend_headers,
  dependencies: backend_mod_deps,
)

# normal extension modules
pg_mod_args = default_mod_args + {
  'dependencies': [backend_mod_code],
  'cpp_args': pg_mod_cpp_args,
  'link_depends': pg_mod_link_depend,
}

# extension modules that shouldn't be installed by default, as they're only
# for testing
pg_test_mod_args = pg_mod_args + {
  'install': false
}



# Shared modules that, on some system, link against the server binary. Only
# enter these after we defined the server build.

subdir('jit/llvm')
subdir('replication/pgoutput')
subdir('snowball')
subdir('utils/mb/conversion_procs')
