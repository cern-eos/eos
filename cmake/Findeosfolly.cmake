# Try to find eos folly.
# Once done, this will define
#
# EOS_FOLLY_FOUND            - system has eos-folly
# EOS_FOLLY_INCLUDE_DIRS     - eos-folly include directories
# EOS_FOLLY_LIBRARIES        - eos-folly library library
#
# EOS_FOLLY_ROOT may be defined as a hint for where to look
#
# and the following imported targets
#
# FOLLY::FOLLY

find_path(EOSFOLLY_INCLUDE_DIR
  NAMES folly/folly-config.h
  HINTS /opt/eos-folly/ ${EOSFOLLY_ROOT}
  PATH_SUFFIXES include)

find_library(EOSFOLLY_LIBRARY
  NAMES libfolly.so
  HINTS /opt/eos-folly/ ${EOSFOLLY_ROOT}
  PATH_SUFFIXES lib lib64)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(eosfolly
  REQUIRED_VARS EOSFOLLY_LIBRARY EOSFOLLY_INCLUDE_DIR)

mark_as_advanced(EOSFOLLY_FOUND EOSFOLLY_LIBRARY EOSFOLLY_INCLUDE_DIR)

if(EOSFOLLY_FOUND AND NOT TARGET FOLLY::FOLLY)
  # Note: this target is not used for the moment as the folly lib is only
  # liked directly into qclient.
  add_library(FOLLY::FOLLY STATIC IMPORTED)
  set_target_properties(FOLLY::FOLLY PROPERTIES
    IMPORTED_LOCATION "${EOSFOLLY_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${EOSFOLLY_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS HAVE_FOLLY)
endif()


# This is done to preserve compatibility with qclient
find_package(glog REQUIRED)
set(FOLLY_INCLUDE_DIRS ${EOSFOLLY_INCLUDE_DIR})
set(FOLLY_LIBRARIES    ${EOSFOLLY_LIBRARY} glog::glog gflags)
set(FOLLY_FOUND TRUE)
unset(EOSFOLLY_LIBRARY)
unset(EOSFOLLY_INCLUDE_DIR)
