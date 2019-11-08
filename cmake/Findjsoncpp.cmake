# Try to find libjsoncpp
# Once done, this will define
#
# JSONCPP_FOUND         - system has jsoncpp
# JSONCPP_INCLUDE_DIRS  - the jsoncpp include directories
# JSONCPP_LIBRARIES     - libaries needed to use jsoncpp
#
# and the following imported target
#
# JSONCPP::JSONCPP

find_package(PkgConfig)
pkg_check_modules(PC_JSONCPP QUIET jsoncpp)
set(JSONCPP_VERSION ${PC_JSONCPP_VERSION})

find_path(JSONCPP_INCLUDE_DIR
  NAMES json.h
  HINTS ${JSONCPP_ROOT}
  PATH_SUFFIXES jsoncpp/json )

find_library(JSONCPP_LIBRARY
  NAMES jsoncpp
  HINTS ${JSONCPP_ROOT}
  PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(jsoncpp
  REQUIRED_VARS JSONCPP_INCLUDE_DIR JSONCPP_LIBRARY
  VERSION_VAR JSON_VERSION)
mark_as_advanced(JSONCPP_INCLUDE_DIR JSONCPP_LIBRARY)

if (JSONCPP_FOUND AND NOT TARGET JSONCPP::JSONCPP)
  add_library(JSONCPP::JSONCPP UNKNOWN IMPORTED)
  set_target_properties(JSONCPP::JSONCPP PROPERTIES
    IMPORTED_LOCATION "${JSONCPP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${JSONCPP_INCLUDE_DIR}")
endif()

set(JSONCPP_INCLUDE_DIRS ${JSONCPP_INCLUDE_DIR})
set(JSONCPP_LIBRARIES ${JSONCPP_LIBRARY})
unset(JSONCPP_INCLUDE_DIR)
unset(JSONCP_LIBRARY)
