#  Try to find Kerberos5
#  Check for libkrb5.a
#
#  KRB5_FOUND       - True if Kerberos 5 libraries found.
#  KRB5_INCLUDE_DIR - where to find krb5.h, etc.
#  KRB5_LIBRARIES   - List of libraries needed to use krb5
#
# and the following imported targets
#
# KRB5::KRB5

find_package(PkgConfig)
pkg_check_modules(PC_krb5 QUIET krb5)

find_path(KRB5_INCLUDE_DIR
  NAMES krb5/krb5.h
  HINTS ${KRB5_ROOT} ${PC_krb5_INCLUDEDIR} ${PC_kbr5_INCLUDE_DIRS})

find_library(KRB5_MIT_LIBRARY
  NAMES k5crypto
  HINTS ${KRB5_ROOT} ${PC_krb5_LIBDIR} ${PC_krb5_LIBRARY_DIRS})

find_library(KRB5_LIBRARY
  NAMES krb5
  HINTS ${KRB5_ROOT} ${PC_krb5_LIBDIR} ${PC_krb5_LIBRARY_DIRS})

find_program(KRB5_INIT NAMES kinit
  HINTS ${KRB5_ROOT} /usr/bin/ /usr/local/bin)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(krb5
  REQUIRED_VARS KRB5_LIBRARY KRB5_INCLUDE_DIR KRB5_MIT_LIBRARY)
mark_as_advanced(KRB5_INCLUDE_DIR KRB5_MIT_LIBRARY KRB4_MIT_LIBRARY)

if (KRB5_FOUND AND NOT TARGET KRB5::KRB5)
  add_library(KRB5::KRB5 UNKNOWN IMPORTED)
  set_target_properties(KRB5::KRB5 PROPERTIES
    IMPORTED_LOCATION "${KRB5_LIBRARY}"
    INTERFACE_LINK_LIBRARIES "${KRB5_MIT_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${KRB5_INCLUDE_DIR}")
endif()

set(KRB5_INCLUDE DIRS ${KRB5_INCLUDE_DIR})
set(KRB5_LIBRARIES ${KRB5_LIBRARY} ${KRB5_MIT_LIBRARY})
unset(KRB5_INCLUDE_DIR)
unset(KRB5_LIBRARY)
unset(KRB5_MIT_LIBRARY)
