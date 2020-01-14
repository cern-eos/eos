# Try to find libisa-l (devel)
# Once done, this will define
#
# ISAL_FOUND - system has isa-l
# ISAL_INCLUDE_DIRS - the isa-l include directories
# ISAL_LIBRARIES - isa-l libraries directories
# ISAL_LIBRARY_STATIC - isa-l static library 

if(ISAL_INCLUDE_DIRS AND ISAL_LIBRARIES)
set(ISAL_FIND_QUIETLY TRUE)
endif(ISAL_INCLUDE_DIRS AND ISAL_LIBRARIES)

find_path( ISAL_INCLUDE_DIR isa-l.h
  HINTS
  /usr
  ${ISAL_DIR}
  PATH_SUFFIXES include )

find_library(
    ISAL_LIBRARY_STATIC
    NAMES libisal.a
    HINTS ${ISAL_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX}
)

find_library( ISAL_LIBRARY isal
  HINTS
  /usr
  ${ISAL_DIR}
  PATH_SUFFIXES lib )

set(ISAL_INCLUDE_DIRS ${ISAL_INCLUDE_DIR})
set(ISAL_LIBRARIES ${ISAL_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set ISAL_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(isal DEFAULT_MSG ISAL_INCLUDE_DIR ISAL_LIBRARY)

mark_as_advanced(ISAL_INCLUDE_DIR ISAL_LIBRARY)
