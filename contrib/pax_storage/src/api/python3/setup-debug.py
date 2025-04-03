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