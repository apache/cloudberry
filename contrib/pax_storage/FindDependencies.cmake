# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
## protobuf
include(FindProtobuf)
find_package(Protobuf 3.5.0 REQUIRED)

# ztsd
# in our image snapshot, zstd is managed using pkg-config, so so the pkg-config method is used first here
find_package(PkgConfig QUIET)
if(PKGCONFIG_FOUND)
    pkg_check_modules(ZSTD libzstd)
endif()
if(NOT ZSTD_FOUND)
    find_package(ZSTD QUIET)
    if(NOT ZSTD_FOUND)
        message(FATAL_ERROR "zstd not found")
    endif()
endif()

## for vectorazition
if (VEC_BUILD)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GLIB REQUIRED glib-2.0)
endif(VEC_BUILD)
