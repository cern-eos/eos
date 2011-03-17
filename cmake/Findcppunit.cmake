# Try to find cppunit
# Once done, this will define
#
# CPPUNIT_FOUND - system has cppunit
# CPPUNIT_INCLUDE_DIRS - the cppunit include directories
# CPPUNIT_LIBRARIES - cppunit libraries directories

if(CPPUNIT_INCLUDE_DIRS AND CPPUNIT_LIBRARIES)
set(CPPUNIT_FIND_QUIETLY TRUE)
endif(CPPUNIT_INCLUDE_DIRS AND CPPUNIT_LIBRARIES)

find_path(CPPUNIT_INCLUDE_DIR cppunit/TestRunner.h)
find_library(CPPUNIT_LIBRARY cppunit)

set(CPPUNIT_INCLUDE_DIRS ${CPPUNIT_INCLUDE_DIR})
set(CPPUNIT_LIBRARIES ${CPPUNIT_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set CPPUNIT_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cppunit DEFAULT_MSG CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)

mark_as_advanced(CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)
