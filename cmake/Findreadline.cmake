# Try to find libreadline
# Once done, this will define
#
# READLINE_FOUND - system has readline
# READLINE_INCLUDE_DIRS - the readline include directories
# READLINE_LIBRARIES - readline libraries directories

if(READLINE_INCLUDE_DIRS AND READLINE_LIBRARIES)
set(READLINE_FIND_QUIETLY TRUE)
endif(READLINE_INCLUDE_DIRS AND READLINE_LIBRARIES)

find_path(READLINE_INCLUDE_DIR readline/readline.h)
find_library(READLINE_LIBRARY readline)

set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIR})
set(READLINE_LIBRARIES ${READLINE_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set READLINE_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(readline DEFAULT_MSG READLINE_INCLUDE_DIR READLINE_LIBRARY)

mark_as_advanced(READLINE_INCLUDE_DIR READLINE_LIBRARY)
