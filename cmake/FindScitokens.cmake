# Try to find scitokens
# Once done, this will define
#
# SCITOKENS_FOUND          - system has zlib
# SCITOKENS_INCLUDE_DIRS   - zlib include directories
#
# and the following imported targets
#
# SCITOKENS::SCITOKENS

find_path(SCITOKENS_INCLUDE_DIR
  NAME scitokens/scitokens.h
  HINTS ${SCITOKENS_ROOT})

find_library(SCITOKENS_LIBRARY
  NAME SciTokens
  HINTS ${SCITOKENS_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Scitokens
  REQUIRED_VARS SCITOKENS_LIBRARY SCITOKENS_INCLUDE_DIR)
mark_as_advanced(SCITOKENS_FOUND SCITOKENS_LIBRARY SCITOKENS_INCLUDE_DIR)

if (SCITOKENS_FOUND AND NOT TARGET SCITOKENS::SCITOKENS)
  add_library(SCITOKENS::SCITOKENS UNKNOWN IMPORTED)
  set_target_properties(SCITOKENS::SCITOKENS PROPERTIES
    IMPORTED_LOCATION "${SCITOKENS_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${SCITOKENS_INCLUDE_DIR}")
endif()

unset(SCITOKENS_LIBRARY)
unset(SCITOKENS_INCLUDE_DIR)
