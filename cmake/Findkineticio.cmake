# Try to find kinetic io library
# Once done, this will define
#
# KINETICIO_FOUND        - system has kinetic io library
# KINETICIO_INCLUDE_DIRS - the kinetic io include directories
# KINETICIO_LIBRARIES    - kinetic io libraries

if(KINETICIO_INCLUDE_DIRS AND KINETICIO_LIBRARIES)
set(KINETICIO_FIND_QUIETLY TRUE)
endif(KINETICIO_INCLUDE_DIRS AND KINETICIO_LIBRARIES)

find_path(KINETICIO_INCLUDE_DIR KineticIoFactory.hh
                    HINTS
                    /usr/include/kio/
                    /usr/local/include/kio/
                    )

find_library(KINETICIO_LIBRARY kineticio
                    PATHS /usr/ /usr/local/
                    PATH_SUFFIXES lib lib64
                    )

set(KINETICIO_INCLUDE_DIRS ${KINETICIO_INCLUDE_DIR})
set(KINETICIO_LIBRARIES ${KINETICIO_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set KINETICIO_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(kineticio DEFAULT_MSG KINETICIO_INCLUDE_DIRS KINETICIO_LIBRARIES)

mark_as_advanced(KINETICIO_INCLUDE_DIRS KINETICIO_LIBRARIES)

# set KINETICIO_FOUND as a define usable in source code for conditional includes, set library / include dir 
# variables to empty if the library isn't there, so that they can be used in CMakeLists.txt without testing
# for KINETICIO_FOUND first
if(KINETICIO_FOUND)
    add_definitions(-DKINETICIO_FOUND)
else()
    set(KINETICIO_INCLUDE_DIRS "")
    set(KINETICIO_INCLUDE_DIR "")
    set(KINETICIO_LIBRARY "")
    set(KINETICIO_LIBRARIES "")
endif()