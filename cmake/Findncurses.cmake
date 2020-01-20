# Try to find libncurses
# Once done, this will define
#
# NCURSES_FOUND           - system has libncurses
# NCURSES_INCLUDE_DIRS    - libncurses include directories
# NCURSES_LIBRARY         - ncurses library

include(FindPackageHandleStandardArgs)

if(NCURSES_INCLUDE_DIRS AND NCURSES_LIBRARY AND NCURSES_LIBRARY_STATIC)
  set(NCURSES_FIND_QUIETLY TRUE)
else()
  find_path(
    NCURSES_INCLUDE_DIR
    NAMES curses.h
    HINTS ${NCURSES_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    NCURSES_LIBRARY
    NAMES ncurses
    HINTS ${NCURSES_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(NCURSES_INCLUDE_DIRS ${NCURSES_INCLUDE_DIR})
  set(NCURSES_LIBRARIES ${NCURSES_LIBRARY})

  find_package_handle_standard_args(
    ncurses
    DEFAULT_MSG
    NCURSES_LIBRARY
    NCURSES_INCLUDE_DIR)

  mark_as_advanced(
    NCURSES_LIBRARY
    NCURSES_INCLUDE_DIR)
endif()
