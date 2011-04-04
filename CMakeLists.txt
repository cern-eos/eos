######################################################################################################################
# Version Definition
######################################################################################################################
set(VERSION_MAJOR "0" )
set(VERSION_MINOR "1")
set(VERSION_PATCH "0" )

######################################################################################################################
# main section
######################################################################################################################
cmake_minimum_required (VERSION 2.6)
project (eos)


set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# include some generic functions
include(EosUtils)

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
find_package(SparseHash REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(uuid REQUIRED)
find_package(z REQUIRED)
find_package(CPPUnit)
find_package(XRootD REQUIRED)

######################################################################################################################
# Platform Specific
EOS_defineOperatingSystem()

if(MacOSX)
set(LIB_INSTALL_DIR lib)
else(MacOSX)
set(LIB_INSTALL_DIR lib64)
endif(MacOSX)

EOS_CheckOutOfSourceBuild()

set(CMAKE_CXX_FLAGS "-g -Wall -O2 -DVERSION=\\\"${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}\\\" ")

#set(SOFLAGS  "-m64 -dynamiclib -undefined dynamic_lookup -single_module")
#set(CMAKE_SHARED_LINKER_FLAGS ${SOFLAGS}) 

add_subdirectory (mq)
add_subdirectory (common)
add_subdirectory (authz)
add_subdirectory (fst)
add_subdirectory (mgm)
add_subdirectory (console)
add_subdirectory (namespace)
add_subdirectory (etc)

include_directories( ../ /usr/includeye/xrootd/ /usr/include/ )
link_directories( /usr/${LIB_INSTALL_DIR}/)

set(SYSCONFDIR "/etc" CACHE PATH "Directory for system configuration files" )

######################################################################################################################
# install targets
######################################################################################################################
set(CMAKE_INSTALL_PREFIX /usr/)

######################################################################################################################
# uninstall target
######################################################################################################################
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake") 

######################################################################################################################
# source packaging 
######################################################################################################################
add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source)

set(CPACK_PACKAGE_VERSION_MAJOR ${VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${VERSION_PATCH})
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_SOURCE_PACKAGE_FILE_NAME
  "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CPACK_SOURCE_IGNORE_FILES
  "/build/;/.bzr/;~$;.o$;/CMakeFiles/;Makefile;/_CPack_Packages/;.tar.gz$;.tar.bz2;CMakeCache.txt;ApMon;fuse;build;install_manifest.txt;lib;/.git/;${CPACK_SOURCE_IGNORE_FILES}")
include(CPack)

add_custom_target(gitdist
    COMMAND git archive --prefix=${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}/ HEAD
        | gzip > ${CMAKE_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}.tar.gz
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

######################################################################################################################
# source + binary rpms
######################################################################################################################

ADD_CUSTOM_TARGET(srpm COMMAND rpmbuild -ts ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}.tar.gz)
ADD_DEPENDENCIES(srpm dist)

ADD_CUSTOM_TARGET(rpm COMMAND rpmbuild -tb ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}.tar.gz)

ADD_DEPENDENCIES(rpm dist)

######################################################################################################################
# Package Finding
######################################################################################################################

# google sparse hash
