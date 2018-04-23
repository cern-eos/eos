# Try to find eos folly.
# Once done, this will define
#
# EOS_FOLLY_FOUND            - system has eos-folly
# EOS_FOLLY_INCLUDE_DIRS     - eos-folly include directories
# EOS_FOLLY_LIBRARIES        - eos-folly library library
#
# EOS_FOLLY_ROOT_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(EOS_FOLLY_INCLUDE_DIRS AND EOS_FOLLY_LIBRARIES)
  set(EOS_FOLLY_FIND_QUIETLY TRUE)
else()
  find_path(
    EOS_FOLLY_INCLUDE_DIR
    NAMES folly/folly-config.h
    HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
    PATH_SUFFIXES include
  )

  find_library(
    EOS_FOLLY_LIBRARY
    NAMES libfolly.so
    HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
    PATH_SUFFIXES lib lib64
  )

  set(EOS_FOLLY_LIBRARIES ${EOS_FOLLY_LIBRARY})
  set(EOS_FOLLY_INCLUDE_DIRS ${EOS_FOLLY_INCLUDE_DIR})

  find_package_handle_standard_args(
    eosfolly
    DEFAULT_MSG
    EOS_FOLLY_LIBRARY
    EOS_FOLLY_INCLUDE_DIR)

  if(EOSFOLLY_FOUND)

    set(FOLLY_FOUND TRUE)
    set(FOLLY_INCLUDE_DIRS ${EOS_FOLLY_INCLUDE_DIRS})
    set(FOLLY_LIBRARIES    ${EOS_FOLLY_LIBRARIES})

    add_library(eosfolly STATIC IMPORTED)
    set_property(TARGET eosfolly PROPERTY IMPORTED_LOCATION ${EOS_FOLLY_LIBRARIES})
    add_definitions(-DHAVE_FOLLY=1)
  endif()
endif()
