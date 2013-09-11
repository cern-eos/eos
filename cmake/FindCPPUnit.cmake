# Try to find CPPUnit
# Once done, this will define
#
# CPPUNIT_FOUND - system has cppunit
# CPPUNIT_INCLUDE_DIRS - the cppunit include directories
# CPPUNIT_LIBRARIES - cppunit libraries directories

if (CPPUNIT_LIBRARIES AND CPPUNIT_INCLUDE_DIRS)
   # Already in cache
   set(CPPUNIT_FIND_QUIETLY TRUE)
else (CPPYNIT_LIBRARIES AND CPPUNIT_INCLUDE_DIRS)
 
   find_path( CPPUNIT_INCLUDE_DIR cppunit/ui/text/TestRunner.h
     HINTS
     ${CPPUNIT_DIR}
     $ENV{CPPUNIT_DIR}
     /usr
     PATH_SUFFIXES include
   )

   find_library( CPPUNIT_LIBRARY cppunit
     HINTS
     ${CPPUNIT_DIR}
     $ENV{CPPUNIT_DIR}
     PATH_SUFFIXES lib
   )

   set(CPPUNIT_INCLUDE_DIRS ${CPPUNIT_INCLUDE_DIR})
   set(CPPUNIT_LIBRARIES ${CPPUNIT_LIBRARY})

   # handle the QUIETLY and REQUIRED arguments and set CPPUNIT_FOUND to TRUE if
   # all listed variables are TRUE
   include(FindPackageHandleStandardArgs)
   find_package_handle_standard_args(cppunit DEFAULT_MSG CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)
   mark_as_advanced(CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)

endif (CPPUNIT_LIBRARIES AND CPPUNIT_INCLUDE_DIRS)
