# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

interconnect_sources = files(
  'ic_common.c',
  'ic_modules.c',
  'tcp/ic_tcp.c',
  'udp/ic_udpifc.c',
)

# Proxy mode is gated on --enable-ic-proxy and requires libuv.
# Mirrors: ifeq ($(enable_ic_proxy), yes) in this directory's Makefile.
interconnect_mod_args = contrib_mod_args
if get_option('ic_proxy').enabled()
  interconnect_sources += files(
    'proxy/ic_proxy_addr.c',
    'proxy/ic_proxy_backend.c',
    'proxy/ic_proxy_client.c',
    'proxy/ic_proxy_iobuf.c',
    'proxy/ic_proxy_key.c',
    'proxy/ic_proxy_main.c',
    'proxy/ic_proxy_packet.c',
    'proxy/ic_proxy_peer.c',
    'proxy/ic_proxy_pkt_cache.c',
    'proxy/ic_proxy_router.c',
  )
  interconnect_mod_args += {
    'dependencies': contrib_mod_args['dependencies'] + [libuv],
  }
endif

if host_system == 'windows'
  interconnect_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'interconnect',
    '--FILEDESC', 'interconnect - inter connection module',])
endif

interconnect = shared_module('interconnect',
  interconnect_sources,
  include_directories: include_directories('.'),
  kwargs: interconnect_mod_args,
)
contrib_targets += interconnect

install_data(
  'interconnect--1.0.sql',
  'interconnect.control',
  kwargs: contrib_data_args,
)
