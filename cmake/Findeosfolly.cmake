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

  if (AARCH64)
    find_library(
      EOS_FOLLY_LIBRARY
      NAMES libfolly.a
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_CONTEXT_LIBRARY
      NAMES libboost_context.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_CHRONO_LIBRARY
      NAMES libboost_chrono.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_DATETIME_LIBRARY
      NAMES libboost_date_time.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_FILESYSTEM_LIBRARY
      NAMES libboost_filesystem.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_PROGRAM_LIBRARY
      NAMES libboost_program_options.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_REGEX_LIBRARY
      NAMES libboost_regex.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_SYSTEM_LIBRARY
      NAMES libboost_system.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_THREAD_LIBRARY
      NAMES libboost_thread.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_GLOG_LIBRARY
      NAMES libglog.a
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    find_library(
      EOS_FOLLY_GFLAGS_LIBRARY
      NAMES libgflags.a
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )

    find_library(
      EOS_FOLLY_DOUBLE_LIBRARY
      NAMES libdouble-conversion.a
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )

    find_library(
      EOS_FOLLY_IBERTY_LIBRARY
      NAMES iberty
      HINTS /usr
      PATH_SUFFIXES lib64 lib
    )

    find_library(
      EOS_FOLLY_EVENT_LIBRARY
      NAMES event
      HINTS /usr
      PATH_SUFFIXES lib64 lib
    )
    find_library(
      EOS_FOLLY_EVENT_CORE_LIBRARY
      NAMES event_core
      HINTS /usr
      PATH_SUFFIXES lib64 lib
    )

    find_library(
      EOS_FOLLY_DL_LIBRARY
      NAMES dl
      HINTS /usr
      PATH_SUFFIXES lib64 lib
    )
    set(EOS_FOLLY_LIBRARIES ${EOS_FOLLY_LIBRARY} ${EOS_FOLLY_CHRONO_LIBRARY}  ${EOS_FOLLY_CONTEXT_LIBRARY}  ${EOS_FOLLY_DATETIME_LIBRARY} ${EOS_FOLLY_FILESYSTEM_LIBRARY} ${EOS_FOLLY_PROGRAM_LIBRARY} ${EOS_FOLLY_REGEX_LIBRARY} ${EOS_FOLLY_SYSTEM_LIBRARY} ${EOS_FOLLY_THREAD_LIBRARY} ${EOS_FOLLY_GLOG_LIBRARY} ${EOS_FOLLY_GFLAGS_LIBRARY} ${EOS_FOLLY_DOUBLE_LIBRARY} ${EOS_FOLLY_IBERTY_LIBRARY} ${EOS_FOLLY_EVENT_LIBRARY} ${EOS_FOLLY_EVENT_CORE_LIBRARY} ${EOS_FOLLY_DL_LIBRARY})
  else ()
    find_library(
      EOS_FOLLY_LIBRARY
      NAMES libfolly.so
      HINTS /opt/eos-folly ${EOS_FOLLY_ROOT_DIR}
      PATH_SUFFIXES lib lib64
    )
    set(EOS_FOLLY_LIBRARIES ${EOS_FOLLY_LIBRARY})
  endif()

  set(EOS_FOLLY_INCLUDE_DIRS ${EOS_FOLLY_INCLUDE_DIR})

  if (AARCH64)
  find_package_handle_standard_args(
    eosfolly
    DEFAULT_MSG
    EOS_FOLLY_LIBRARY
    EOS_FOLLY_CHRONO_LIBRARY
    EOS_FOLLY_CONTEXT_LIBRARY
    EOS_FOLLY_DATETIME_LIBRARY
    EOS_FOLLY_FILESYSTEM_LIBRARY
    EOS_FOLLY_PROGRAM_LIBRARY
    EOS_FOLLY_REGEX_LIBRARY
    EOS_FOLLY_SYSTEM_LIBRARY
    EOS_FOLLY_THREAD_LIBRARY
    EOS_FOLLY_GLOG_LIBRARY
    EOS_FOLLY_GFLAGS_LIBRARY
    EOS_FOLLY_DOUBLE_LIBRARY
    EOS_FOLLY_IBERTY_LIBRARY
    EOS_FOLLY_EVENT_LIBRARY
    EOS_FOLLY_EVENT_CORE_LIBRARY
    EOS_FOLLY_INCLUDE_DIR)
  else()
  find_package_handle_standard_args(
    eosfolly
    DEFAULT_MSG
    EOS_FOLLY_LIBRARY
    EOS_FOLLY_INCLUDE_DIR)
  endif()

  if(EOSFOLLY_FOUND)

    set(FOLLY_FOUND TRUE)
    set(FOLLY_INCLUDE_DIRS ${EOS_FOLLY_INCLUDE_DIRS})
    set(FOLLY_LIBRARIES    ${EOS_FOLLY_LIBRARIES})

    add_library(eosfolly STATIC IMPORTED)
    set_property(TARGET eosfolly PROPERTY IMPORTED_LOCATION ${EOS_FOLLY_LIBRARIES})
    add_definitions(-DHAVE_FOLLY=1)
  endif()
endif()
