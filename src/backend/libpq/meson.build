# Copyright (c) 2022-2023, PostgreSQL Global Development Group

backend_sources += files(
  'auth-sasl.c',
  'auth-scram.c',
  'auth.c',
  'be-fsstubs.c',
  'be-secure-common.c',
  'be-secure.c',
  'crypt.c',
  'hba.c',
  'ifaddr.c',
  'pqcomm.c',
  'pqformat.c',
  'pqmq.c',
  'pqsignal.c',
)

if ssl.found()
  backend_sources += files('be-secure-openssl.c')
endif

if gssapi.found()
  backend_sources += files(
    'be-secure-gssapi.c',
    'be-gssapi-common.c'
  )
endif

# Cloudberry compiles libpq's frontend sources into the backend as well, to get
# an internal libpq for QD<->QE communication. The Makefile symlinks them into
# this directory ("# Cloudberry objects follow"); meson references them in place.
# They must NOT be built with -DFRONTEND -- these files switch on that macro,
# and the backend copy needs the backend side (see fe-protocol3.c's header
# comment and src/interfaces/libpq/meson.build).
backend_sources += files(
  '../../interfaces/libpq/fe-auth-scram.c',
  '../../interfaces/libpq/fe-auth.c',
  '../../interfaces/libpq/fe-connect.c',
  '../../interfaces/libpq/fe-exec.c',
  '../../interfaces/libpq/fe-misc.c',
  '../../interfaces/libpq/fe-protocol3.c',
  '../../interfaces/libpq/fe-secure.c',
  '../../interfaces/libpq/fe-trace.c',
  '../../interfaces/libpq/libpq-events.c',
  '../../interfaces/libpq/pqexpbuffer.c',
)

if ssl.found()
  backend_sources += files(
    '../../interfaces/libpq/fe-secure-common.c',
    '../../interfaces/libpq/fe-secure-openssl.c',
  )
endif

if gssapi.found()
  backend_sources += files(
    '../../interfaces/libpq/fe-gssapi-common.c',
    '../../interfaces/libpq/fe-secure-gssapi.c',
  )
endif

install_data(
  'pg_hba.conf.sample',
  'pg_ident.conf.sample',
  install_dir: dir_data,
)
