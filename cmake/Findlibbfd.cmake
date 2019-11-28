# Try to find libbfd
# Once done, this will define
#
# LIBBFD_FOUND               - system has libbfd
# LIBBFD_INCLUDE_DIRS        - libbfd include directories
# LIBBFD_LIBRARIES           - libbfd library
#
# LIBBFD_ROOT_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(LIBBFD_INCLUDE_DIRS AND LIBBFD_LIBRARIES)
  set(LIBBFD_FIND_QUIETLY TRUE)
else()
  find_path(
    LIBBFD_INCLUDE_DIR
    NAMES bfd.h
    HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT_DIR}
    PATH_SUFFIXES include
  )

  find_library(
    LIBBFD_LIBRARY
    NAMES libbfd.a
    HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT_DIR}
    PATH_SUFFIXES lib lib64
  )

  find_library(
    LIBIBERTY_LIBRARY
    NAMES libiberty.a
    HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT_DIR}
    PATH_SUFFIXES lib lib64
  )

  set(LIBBFD_LIBRARIES ${LIBBFD_LIBRARY} ${LIBIBERTY_LIBRARY})
  set(LIBBFD_INCLUDE_DIRS ${LIBBFD_INCLUDE_DIR})

  find_package_handle_standard_args(
    libbfd
    DEFAULT_MSG
    LIBBFD_LIBRARY
    LIBBFD_INCLUDE_DIR)

  if(LIBBFD_FOUND)
    add_library(libbfd STATIC IMPORTED)
    set_property(TARGET libbfd PROPERTY IMPORTED_LOCATION ${LIBBFD_LIBRARY})
  endif()
endif()
