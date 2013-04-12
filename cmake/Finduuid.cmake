# Try to find uuid
# Once done, this will define
#
# UUID_FOUND        - system has uuid
# UUID_INCLUDE_DIRS - the uuid include directories
# UUID_LIBRARIES    - uuid libraries directories

if(UUID_INCLUDE_DIRS AND UUID_LIBRARIES)
set(UUID_FIND_QUIETLY TRUE)
endif(UUID_INCLUDE_DIRS AND UUID_LIBRARIES)

find_path(UUID_INCLUDE_DIR uuid.h
                           HINTS
                           /usr/include/
			   /usr/include/uuid/
			   )
find_library(UUID_LIBRARY uuid
			  HINTS
			  /opt/local/
			  /usr/lib/
			  PATH_SUFFIXES lib
			  )

set(UUID_INCLUDE_DIRS ${UUID_INCLUDE_DIR})
set(UUID_LIBRARIES ${UUID_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set UUID_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uuid DEFAULT_MSG UUID_INCLUDE_DIRS UUID_LIBRARIES)

mark_as_advanced(UUID_INCLUDE_DIRS UUID_LIBRARIES)
