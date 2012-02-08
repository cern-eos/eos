# Try to find libncurses
# Once done, this will define
#
# NCURSES_FOUND - system has libncurses
# NCURSES_INCLUDE_DIRS - the libncurses include directories
# NCURSES_LIBRARIES - libncurses libraries directories

if(NCURSES_INCLUDE_DIRS AND NCURSES_LIBRARIES)
set(NCURSES_FIND_QUIETLY TRUE)
endif(NCURSES_INCLUDE_DIRS AND NCURSES_LIBRARIES)

find_path(NCURSES_INCLUDE_DIR curses.h)
find_library(NCURSES_LIBRARY ncurses)

set(NCURSES_INCLUDE_DIRS ${NCURSES_INCLUDE_DIR})
set(NCURSES_LIBRARIES ${NCURSES_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set NCURSES_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ncurses DEFAULT_MSG NCURSES_INCLUDE_DIR NCURSES_LIBRARY)

mark_as_advanced(NCURSES_INCLUDE_DIR NCURSES_LIBRARY)
