# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Geoffray Adde - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

######################################################################################################################
# Version Definition
######################################################################################################################
set(VERSION_MAJOR "0" )
set(VERSION_MINOR "3" )
set(VERSION_PATCH "0" )
set(RELEASE "1" )

######################################################################################################################
# main section
######################################################################################################################
cmake_minimum_required (VERSION 2.6)
project (xrootd-dsi)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
set(SYSCONFDIR "/etc")
#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
if (RELEASE)
else (RELEASE)
set(RELEASE "head")
endif (RELEASE)

find_package(GlobusGridFtpServer REQUIRED)
find_package(XRootD REQUIRED)

######################################################################################################################
# Platform Specific
set(LIB_INSTALL_DIR lib64)

set(CMAKE_CXX_FLAGS "-g -Wall -O2 -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}\\\" -DRELEASE=\\\"${RELEASE}\\\" -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64" )
#set(CMAKE_CXX_FLAGS "-g -Wall -O0 -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}\\\" -DRELEASE=\\\"${RELEASE}\\\" -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64" )
######################################################################################################################
# build targets
######################################################################################################################
include_directories( ../ ${XROOTD_INCLUDE_DIR} ${GLOBUSGRIDFTPSERVER_INCLUDE_DIR})
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR} )
add_library (globus_gridftp_server_xrootd_gcc64pthr SHARED
	    globus_gridftp_server_xrootd.cc
	    XrdUtils.cc
)

if (Linux)
  set_target_properties (globus_gridftp_server_xrootd_gcc64pthr PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)
target_link_libraries ( globus_gridftp_server_xrootd_gcc64pthr XrdUtils XrdCl globus_gridftp_server)

install ( TARGETS globus_gridftp_server_xrootd_gcc64pthr
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
INSTALL ( FILES ${PROJECT_SOURCE_DIR}/etc/sysconfig/xrootd-gridftp-server DESTINATION ${SYSCONFDIR}/sysconfig  PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL ( FILES ${PROJECT_SOURCE_DIR}/etc/init.d/xrootd-gridftp  DESTINATION ${SYSCONFDIR}/init.d PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL ( FILES ${PROJECT_SOURCE_DIR}/etc/logrotate.d/xrootd-gridftp  DESTINATION ${SYSCONFDIR}/logrotate.d PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)


######################################################################################################################
# install targets
######################################################################################################################
set(CMAKE_INSTALL_PREFIX /usr/)

set(SYSCONFDIR "/etc" CACHE PATH "Directory for system configuration files" )
set(MANDIR "/usr/share/man/man1" CACHE PATH "Directory where man pages get installed" ) 

include_directories( ../ /usr/include/xrootd/ /usr/include/ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})


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
  "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}-${RELEASE}")
set(CPACK_SOURCE_IGNORE_FILES "/dsi/;/build/;/.deps/;/.bzr/;~$;'.'o$;/CMakeFiles/;Makefile;/_CPack_Packages/;.tar.gz$;.tar.bz2;CMakeCache.txt;ApMon;build;install_manifest.txt;lib;/.git/;${CPACK_SOURCE_IGNORE_FILES};")

include(CPack)

add_custom_target(gitdist
    COMMAND git archive --prefix=${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}/ HEAD
        | gzip > ${CMAKE_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}-${RELEASE}.tar.gz
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

######################################################################################################################
# source + binary rpms
######################################################################################################################

ADD_CUSTOM_TARGET(srpm COMMAND rpmbuild -ts ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}-${RELEASE}.tar.gz)
ADD_DEPENDENCIES(srpm dist)

ADD_CUSTOM_TARGET(rpm COMMAND rpmbuild -tb ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}-${RELEASE}.tar.gz)

ADD_DEPENDENCIES(rpm dist)
