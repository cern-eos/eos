# Try to find openssl
# Once done, this will define
#
# OPENSSL_FOUND          - system has openssl
# OPENSSL_INCLUDE_DIRS   - openssl include directories
# OPENSSL_LIBRARY_STATIC - openssl static libraries directory
# OPENSSL_LIBRARIES      - libraries needed to use openssl

include(FindPackageHandleStandardArgs)

if(OPENSSL_INCLUDE_DIRS AND OPENSSL_LIBRARIES)
  set(OPENSSL_FIND_QUIETLY TRUE)
else()
  find_path(
    OPENSSL_INCLUDE_DIR
    NAMES openssl/ssl.h
    HINTS
    /usr /usr/local /opt/local
    PATH_SUFFIXES
    include)

  find_library(
    OPENSSL_LIBRARY
    NAMES crypto
    HINTS
    /usr /usr/local /opt/local
    PATH_SUFFIXES
    ${LIBRARY_PATH_PREFIX})

  find_library(
    OPENSSL_LIBRARY_STATIC
    NAMES libcrypto.a
    HINTS
    /usr /usr/local /opt/local
    PATH_SUFFIXES
    ${LIBRARY_PATH_PREFIX})

  set(OPENSSL_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR})
  set(OPENSSL_LIBRARIES ${OPENSSL_LIBRARY} ${OPENSSL_LIBRARY_STATIC})

  find_package_handle_standard_args(
    openssl
    DEFAULT_MSG
    OPENSSL_LIBRARY OPENSSL_LIBRARY_STATIC OPENSSL_INCLUDE_DIR)

  mark_as_advanced(OPENSSL_LIBRARY OPENSSL_LIBRARY_STATIC OPENSSL_INCLUDE_DIR)
endif()
