# Try to find libreadline
# Once done, this will define
#
# READLINE_FOUND        - system has readline
# READLINE_INCLUDE_DIRS - readline include directories
# READLINE_LIBRARIES    - libraries need to use readline

include(FindPackageHandleStandardArgs)

if(READLINE_INCLUDE_DIRS AND READLINE_LIBRARIES)
  set(READLINE_FIND_QUIETLY TRUE)
else()
  find_path(
    READLINE_INCLUDE_DIR
    NAMES readline/readline.h
    HINTS ${READLINE_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    READLINE_LIBRARY
    NAMES readline
    HINTS ${READLINE_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIR})
  set(READLINE_LIBRARIES ${READLINE_LIBRARY})

  find_package_handle_standard_args(
    readline
    DEFAULT_MSG READLINE_LIBRARY READLINE_INCLUDE_DIR)

  mark_as_advanced(READLINE_LIBRARY READLINE_INCLUDE_DIR)
endif()