# Module to find Xerces-C.

find_path(XERCES_INCLUDE_DIR xercesc/sax2/DefaultHandler.hpp)

find_library(XERCES_LIBRARY NAMES xerces-c libxerces-c)

set(XERCES_LIBRARIES ${XERCES_LIBRARY})
set(XERCES_INCLUDE_DIRS ${XERCES_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Xerces DEFAULT_MSG
                                  XERCES_LIBRARY XERCES_INCLUDE_DIR)

mark_as_advanced(XERCES_INCLUDE_DIR XERCES_LIBRARY)
