# Try to find libjsoncpp
# Once done, this will define
#
# JSONCPP_FOUND - system has jsoncpp
# JSONCPP_INCLUDE_DIRS - the jsoncpp include directories
# JSONCPP_LIBRARIES - jsoncpp libraries directories

if(JSONCPP_INCLUDE_DIRS AND JSONCPP_LIBRARIES)
set(JSONCPP_FIND_QUIETLY TRUE)
endif(JSONCPP_INCLUDE_DIRS AND JSONCPP_LIBRARIES)

find_path(JSONCPP_INCLUDE_DIR json/json.h PATH_SUFFIXES jsoncpp/ )
find_library(JSONCPP_LIBRARY jsoncpp)

set(JSONCPP_INCLUDE_DIRS ${JSONCPP_INCLUDE_DIR})
set(JSONCPP_LIBRARIES ${JSONCPP_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set JSONCPP_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(jsoncpp DEFAULT_MSG JSONCPP_INCLUDE_DIR JSONCPP_LIBRARY)

mark_as_advanced(JSONCPP_INCLUDE_DIR JSONCPP_LIBRARY)
