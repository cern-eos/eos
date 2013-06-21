# Try to find attr
# Once done, this will define
#
# LDAP_FOUND - system has libldap
# LDAP_INCLUDE_DIRS - the ldap include directories
# LDAP_LIBRARIES - ldap libraries directories

if(LDAP_INCLUDE_DIRS AND LDAP_LIBRARIES)
set(LDAP_FIND_QUIETLY TRUE)
endif(LDAP_INCLUDE_DIRS AND LDAP_LIBRARIES)

find_path(LDAP_INCLUDE_DIR ldap.h)
find_library(LDAP_LIBRARY ldap)

set(LDAP_INCLUDE_DIRS ${LDAP_INCLUDE_DIR})
set(LDAP_LIBRARIES ${LDAP_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set LDAP_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(attr DEFAULT_MSG LDAP_INCLUDE_DIR LDAP_LIBRARY)

mark_as_advanced(LDAP_INCLUDE_DIR LDAP_LIBRARY)
