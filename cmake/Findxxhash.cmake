# Try to find libxxhash (devel)
# Once done, this will define
#
# XXHASH_FOUND - system has xxhash
# XXHASH_INCLUDE_DIRS - the xxhash include directories
# XXHASH_LIBRARIES - xxhash libraries directories
# XXHASH_LIBRARY_STATIC - xxhash static library 

if(XXHASH_INCLUDE_DIRS AND XXHASH_LIBRARIES)
set(XXHASH_FIND_QUIETLY TRUE)
endif(XXHASH_INCLUDE_DIRS AND XXHASH_LIBRARIES)

find_path( XXHASH_INCLUDE_DIR xxhash.h
  HINTS
  /usr
  ${XXHASH_DIR}
  PATH_SUFFIXES include )

find_library( XXHASH_LIBRARY xxhash
  HINTS
  /usr
  ${XXHASH_DIR}
  PATH_SUFFIXES lib )

set(XXHASH_INCLUDE_DIRS ${XXHASH_INCLUDE_DIR})
set(XXHASH_LIBRARIES ${XXHASH_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set XXHASH_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xxhash DEFAULT_MSG XXHASH_INCLUDE_DIR XXHASH_LIBRARY)

mark_as_advanced(XXHASH_INCLUDE_DIR XXHASH_LIBRARY)
