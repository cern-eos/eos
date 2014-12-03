# Try to find attr
# Once done, this will define
#
# LDAP_FOUND        - system has libldap
# LDAP_INCLUDE_DIRS - ldap include directories
# LDAP_LIBRARIES    - ldap libraries directories

include(FindPackageHandleStandardArgs)

if(LDAP_INCLUDE_DIRS AND LDAP_LIBRARIES)
  set(LDAP_FIND_QUIETLY TRUE)
else()
  find_path(
    LDAP_INCLUDE_DIR
    NAMES ldap.h
    HINTS ${LDAP_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    LDAP_LIBRARY
    NAMES ldap
    HINTS ${LDAP_ROOT_DIR}
    PATH_SUFFIEX ${LIBRARY_PATH_PREFIX})

  set(LDAP_INCLUDE_DIRS ${LDAP_INCLUDE_DIR})
  set(LDAP_LIBRARIES ${LDAP_LIBRARY})

  find_package_handle_standard_args(
    ldap DEFAULT_MSG LDAP_LIBRARY LDAP_INCLUDE_DIR)

  mark_as_advanced(LDAP_LIBRARY LDAP_INCLUDE_DIR)
endif()