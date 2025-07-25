#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from setuptools import find_packages
from setuptools import setup, Extension

import os

# python3 setup.py install 
# How To Import ?
# ```python
# import paxpy
# ```
work_dir = os.path.dirname(__file__)

def abs_path(file_name):
       return "{}/{}".format(work_dir,file_name)

gp_home = os.environ['GPHOME'] # Setup the CBDB before build the paxpy. 

paxpy_src = [abs_path('paxpy_modules.cc'), abs_path('paxfile_type.cc'), abs_path('paxfilereader_type.cc'), abs_path('paxtype_cast.cc')]
include_dirs = ["{}/include/postgresql/server/".format(gp_home), "{}/include/pax/".format(gp_home)]
library_dirs = ["{}/lib".format(gp_home)]
libraries = ['paxformat', 'postgres']

paxpy_module = Extension('paxpy',
                    define_macros = [('MAJOR_VERSION', '1'),
                                     ('MINOR_VERSION', '0')],
                    sources = paxpy_src,
                    include_dirs=include_dirs,
                    library_dirs=library_dirs,
                    libraries=libraries)

setup (name = 'paxpy',
       version = '1.0',
       description = 'PAXPY is the PYTHON3 API of PAX',
       author = 'jiaqizho',
       author_email = 'jiaqizho@hashdata.cn',
       url = '-',
       ext_modules = [paxpy_module]
)