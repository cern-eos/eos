# Try to find libisa-l_crypto (devel)
# Once done, this will define
#
# ISALCRYPTO_FOUND          - system has isa-l_crypto
# ISALCRYPTO_INCLUDE_DIRS   - the isa-l_crypto include directories
# ISALCRYPTO_LIBRARIES      - isa-l_crypto libraries directories
# ISALCRYPTO_LIBRARY_STATIC - isa-l_crypto static library
#
# and the following imported targets
#
# ISAL::ISAL_CRYPTO
# ISAL::ISAL_CRYPTO_STATIC

find_path(ISAL_CRYPTO_INCLUDE_DIR
  NAMES isa-l_crypto.h
  HINTS ${ISAL_CRYPTO_ROOT}
  PATH_SUFFIXES include)

find_library(ISAL_CRYPTO_LIBRARY
  NAME isal_crypto
  HINTS ${ISAL_CRYPTO_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(isal_crypto
  REQUIRED_VARS ISAL_CRYPTO_LIBRARY ISAL_CRYPTO_INCLUDE_DIR)
mark_as_advanced(ISAL_CRYPTO_LIBRARY ISAL_CRYPTO_INCLUDE_DIR)

if (ISAL_CRYPTO_FOUND AND NOT TARGET ISAL::ISAL_CRYPTO)
  add_library(ISAL::ISAL_CRYPTO UNKNOWN IMPORTED)
  set_target_properties(ISAL::ISAL_CRYPTO PROPERTIES
    IMPORTED_LOCATION "${ISAL_CRYPTO_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ISAL_CRYPTO_INCLUDE_DIR}")
  target_compile_definitions(ISAL::ISAL_CRYPTO INTERFACE ISALCRYPTO_FOUND)
else()
  message(WARNING "Notice: ISAL_CRYPTO not found, no ISAL_CRYPTO support")
  add_library(ISAL::ISAL_CRYPTO INTERFACE IMPORTED)
endif()

unset(ISAL_CRYPTO_INCLUDE_DIR)
unset(ISAL_CRYPTO_LIBRARY)
