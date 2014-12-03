# Try to find CPPUnit
# Once done, this will define
#
# CPPUNIT_FOUND        - system has cppunit
# CPPUNIT_INCLUDE_DIRS - cppunit include directories
# CPPUNIT_LIBRARIES    - libaries needed to use cppunit

include(FindPackageHandleStandardArgs)

if (CPPUNIT_LIBRARIES AND CPPUNIT_INCLUDE_DIRS)
  set(CPPUNIT_FIND_QUIETLY TRUE)
else()
 find_path(
   CPPUNIT_INCLUDE_DIR
   NAMES cppunit/ui/text/TestRunner.h
   HINTS ${CPPUNIT_ROOT_DIR} $ENV{CPPUNIT_ROOT_DIR}
   PATH_SUFFIXES include)

 find_library(
   CPPUNIT_LIBRARY
   NAMES cppunit
   HINTS ${CPPUNIT_ROOT_DIR} $ENV{CPPUNIT_ROOT_DIR}
   PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

   set(CPPUNIT_INCLUDE_DIRS ${CPPUNIT_INCLUDE_DIR})
   set(CPPUNIT_LIBRARIES ${CPPUNIT_LIBRARY})

   find_package_handle_standard_args(
     cppunit
     DEFAULT_MSG CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)

   mark_as_advanced(CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)
endif()
