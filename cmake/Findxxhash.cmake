# Try to find libxxhash (devel)
# Once done, this will define
#
# XXHASH_FOUND          - system nhas xxhash
# XXHASH_INCLUDE_DIRS   - xxhash include directories
# XXHASH_LIBRARIES      - xxhash libraries directories
# XXHASH_LIBRARY_STATIC - xxhash static library
#
# and the following imported target
#
# XXHASH::XXHASH

find_path(XXHASH_INCLUDE_DIR
  NAMES xxhash.h
  HINTS ${XXHASH_ROOT}
  PATH_SUFFIXES include)

find_library(XXHASH_LIBRARY
  NAME xxhash
  HINTS ${XXHASH_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xxhash
  REQUIRED_VARS XXHASH_LIBRARY XXHASH_INCLUDE_DIR)
mark_as_advanced(XXHASH_LIBRARY XXHASH_INCLUDE_DIR)

if (XXHASH_FOUND AND NOT TARGET XXHASH::XXHASH)
  add_library(XXHASH::XXHASH STATIC IMPORTED)
  set_target_properties(XXHASH::XXHASH PROPERTIES
    IMPORTED_LOCATION "${XXHASH_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIR}")
  target_compile_definitions(XXHASH::XXHASH INTERFACE XXHASH_FOUND)
endif()

set(XXHASH_INCLUDE_DIRS ${XXHASH_INCLUDE_DIR})
set(XXHASH_LIBRARIES ${XXHASH_LIBRARY})
