# Try to find libreadline
# Once done, this will define
#
# READLINE_FOUND        - system has readline
# READLINE_INCLUDE_DIRS - readline include directories
# READLINE_LIBRARIES    - libraries need to use readline
#
# and the following imported targets
#
# READLINE::READLINE

find_path(READLINE_INCLUDE_DIR
  NAMES readline/readline.h
  HINTS ${READLINE_ROOT})

find_library(READLINE_LIBRARY
  NAMES readline
  HINTS ${READLINE_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(readline
  REQUIRED_VARS READLINE_LIBRARY READLINE_INCLUDE_DIR)

mark_as_advanced(READLINE_FOUND READLINE_LIBRARY READLINE_INCLUDE_DIR)

if (READLINE_FOUND AND NOT TARGET READLINE::READLINE)
  add_library(READLINE::READLINE UNKNOWN IMPORTED)
  set_target_properties(READLINE::READLINE PROPERTIES
    IMPORTED_LOCATION "${READLINE_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${READLINE_INCLUDE_DIR}")
endif()

set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIR})
set(READLINE_LIBRARIES ${READLINE_LIBRARY})
