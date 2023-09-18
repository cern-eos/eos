# Try to find abseil library
# Once done, this will define
#
# GFLAGS_FOUND          - system has gflags library
# GFLAGS_INCLUDE_DIR    - gflags include directories
# GFLAGS_LIBRARY        - libraries needed to use gflags
#
#

find_path(GFLAGS_INCLUDE_DIR
    NAMES gflags/gflags.h
    HINTS /usr ${GFLAGS_ROOT}
    PATH_SUFFIXES include)

find_library(GFLAGS_LIBRARY
    NAMES gflags
    HINTS ${GFLAGS_ROOT}
    PATHS /usr
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gflags
    REQUIRED_VARS GFLAGS_LIBRARY GFLAGS_INCLUDE_DIR)

if (GFLAGS_FOUND)
    message(STATUS "GFLAGS_LIBRARY=${GFLAGS_LIBRARY}")
    message(STATUS "GFLAGS_INCLUDE_DIR=${GFLAGS_INCLUDE_DIR}")
endif()
