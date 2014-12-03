# Try to find libncurses
# Once done, this will define
#
# NCURSES_FOUND           - system has libncurses
# NCURSES_INCLUDE_DIRS    - libncurses include directories
# NCURSES_LIBRARY         - ncurses library
# NCURSES_LIBRARY_STATIC  - ncurses static library

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

  find_library(
    NCURSES_LIBRARY_STATIC
    NAMES libcurses.a
    HINTS ${NCURSES_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PRFIX})

  set(NCURSES_INCLUDE_DIRS ${NCURSES_INCLUDE_DIR})

  find_package_handle_standard_args(
    ncurses
    DEFAULT_MSG
    NCURSES_LIBRARY NCURSES_LIBRARY_STATIC NCURSES_INCLUDE_DIR)

  mark_as_advanced(NCURSES_LIBRARY NCURSES_LIBRARY_STATIC NCURSES_INCLUDE_DIR)
endif()