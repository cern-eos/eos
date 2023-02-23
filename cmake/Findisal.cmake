# Try to find libisa-l (devel)
# Once done, this will define
#
# ISAL_FOUND          - system has isa-l
# ISAL_INCLUDE_DIRS   - isa-l include directories
# ISAL_LIBRARIES      - isa-l library
# ISAL_LIBRARY_STATIC - isa-l static library
#
# and the following imported targets
#
# ISAL::ISAL
# ISAL::ISAL_STATIC

find_path(ISAL_INCLUDE_DIR
  NAMES isa-l.h
  HINTS ${ISAL_ROOT}
  PATH_SUFFIXES include)

find_library(ISAL_LIBRARY
  NAME isal
  HINTS ${ISAL_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(isal
  REQUIRED_VARS ISAL_LIBRARY ISAL_INCLUDE_DIR)
mark_as_advanced(ISAL_LIBRARY ISAL_INCLUDE_DIR)

if (ISAL_FOUND AND NOT TARGET ISAL::ISAL)
  add_library(ISAL::ISAL UNKNOWN IMPORTED)
  set_target_properties(ISAL::ISAL PROPERTIES
    IMPORTED_LOCATION "${ISAL_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ISAL_INCLUDE_DIR}")
  target_compile_definitions(ISAL::ISAL INTERFACE ISAL_FOUND)
else()
  message(WARNING "Notice: ISAL not found, no ISAL support")
  add_library(ISAL::ISAL INTERFACE IMPORTED)
endif()

unset(ISAL_INCLUDE_DIR)
unset(ISAL_LIBRARY)
