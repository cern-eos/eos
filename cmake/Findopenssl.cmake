# Try to find openssl
# Once done, this will define
#
# OPENSSL_FOUND          - system has openssl
# OPENSSL_INCLUDE_DIRS   - openssl include directories
# OPENSSL_CRYPTO_LIBRARY - openssl crypto library directory

include(FindPackageHandleStandardArgs)

if(OPENSSL_INCLUDE_DIRS AND OPENSSL_LIBRARIES)
  set(OPENSSL_FIND_QUIETLY TRUE)
else()
  find_path(
    OPENSSL_INCLUDE_DIR
    NAMES openssl/ssl.h
    HINTS ${OPENSSL_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    OPENSSL_CRYPTO_LIBRARY
    NAMES crypto
    HINTS ${OPENSSL_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(OPENSSL_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR})

  find_package_handle_standard_args(
    openssl
    DEFAULT_MSG
    OPENSSL_CRYPTO_LIBRARY
    OPENSSL_INCLUDE_DIR)

  mark_as_advanced(
    OPENSSL_CRYPTO_LIBRARY
    OPENSSL_INCLUDE_DIR)
endif()
