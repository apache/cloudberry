from setuptools import find_packages
from setuptools import setup, Extension

import os

work_dir = os.path.dirname(__file__)
top_dir = work_dir + "/../../../../../"
pax_dir = work_dir + "/../../../"
thirdparty_dir = top_dir + "/thirdparty"

def abs_path(file_name):
       return "{}/{}".format(work_dir,file_name)

paxpy_src = [abs_path('paxpy_modules.cc'), abs_path('paxfile_type.cc'), abs_path('paxfilereader_type.cc'), abs_path('paxtype_cast.cc')]
include_dirs = ["{}/src/include/".format(top_dir), "{}/src/cpp/".format(pax_dir), "{}/include".format(thirdparty_dir)];
library_dirs = ["{}/src/backend/".format(top_dir),  # for postgres
    "{}/build/src/cpp/".format(pax_dir)] # for paxformat
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