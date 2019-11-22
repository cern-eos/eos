# Try to find attr
# Once done, this will define
#
# LDAP_FOUND        - system has libldap
# LDAP_INCLUDE_DIRS - ldap include directories
# LDAP_LIBRARIES    - ldap libraries directories
#
# and the following imported target
#
# LDAP::LDAP

find_path(LDAP_INCLUDE_DIR
  NAMES ldap.h
  HINTS ${LDAP_ROOT}
  PATH_SUFFIXES include)

find_library(LDAP_LIBRARY
  NAMES ldap
  HINTS ${LDAP_ROOT}
  PATH_SUFFIEX ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ldap
  REQUIRED_VARS LDAP_LIBRARY LDAP_INCLUDE_DIR)
mark_as_advanced(LDAP_LIBRARY LDAP_INCLUDE_DIR)

if (LDAP_FOUND AND NOT TARGET LDAP::LDAP)
  add_library(LDAP::LDAP UNKNOWN IMPORTED)
  set_target_properties(LDAP::LDAP PROPERTIES
    IMPORTED_LOCATION "${LDAP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LDAP_INCLUDE_DIR}")
endif()

set(LDAP_INCLUDE_DIRS ${LDAP_INCLUDE_DIR})
set(LDAP_LIBRARIES ${LDAP_LIBRARY})
