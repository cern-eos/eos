# Try to find libisa-l_crypto (devel)
# Once done, this will define
#
# ISALCRYPTO_FOUND - system has isa-l_crypto
# ISALCRYPTO_INCLUDE_DIRS - the isa-l_crypto include directories
# ISALCRYPTO_LIBRARIES - isa-l_crypto libraries directories
# ISALCRYPTO_LIBRARY_STATIC - isa-l_crypto static library 

if(ISALCRYPTO_INCLUDE_DIRS AND ISALCRYPTO_LIBRARIES)
set(ISALCRYPTO_FIND_QUIETLY TRUE)
endif(ISALCRYPTO_INCLUDE_DIRS AND ISALCRYPTO_LIBRARIES)

find_path( ISALCRYPTO_INCLUDE_DIR isa-l_crypto.h
  HINTS
  /usr
  ${ISALCRYPTO_DIR}
  PATH_SUFFIXES include )

find_library(
    ISALCRYPTO_LIBRARY_STATIC
    NAMES libisal_crypto.a
    HINTS ${ISALCRYPTO_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX}
)

find_library( ISALCRYPTO_LIBRARY isal_crypto
  HINTS
  /usr
  ${ISALCRYPTO_DIR}
  PATH_SUFFIXES lib )

set(ISALCRYPTO_INCLUDE_DIRS ${ISALCRYPTO_INCLUDE_DIR})
set(ISALCRYPTO_LIBRARIES ${ISALCRYPTO_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set ISALCRYPTO_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(isalcrypto DEFAULT_MSG ISALCRYPTO_INCLUDE_DIR ISALCRYPTO_LIBRARY)

mark_as_advanced(ISALCRYPTO_INCLUDE_DIR ISALCRYPTO_LIBRARY)
