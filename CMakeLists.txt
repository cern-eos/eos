# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
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
set(VERSION_MINOR "2")
set(VERSION_PATCH "20")

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

if (RELEASE)
else (RELEASE)
set(RELEASE "head")
endif (RELEASE)

if (CLIENT)
find_package(XRootD REQUIRED)
find_package(fuse REQUIRED)
else (CLIENT)
find_package(SparseHash REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ncurses REQUIRED)
find_package(readline REQUIRED)
find_package(uuid REQUIRED)
find_package(z REQUIRED)
find_package(CPPUnit)
find_package(XRootD REQUIRED)
find_package(fuse REQUIRED)
find_package(attr REQUIRED)
find_package(httpd)
find_package(apr)
endif (CLIENT)

######################################################################################################################
# Platform Specific
EOS_defineOperatingSystem()

if(MacOSX)
set(LIB_INSTALL_DIR lib)
else(MacOSX)
set(LIB_INSTALL_DIR lib64)
endif(MacOSX)

set(SBIN_INSTALL_DIR sbin)

EOS_CheckOutOfSourceBuild()

if (CLIENT)
set(CMAKE_CXX_FLAGS "-g -Wall -O2 -msse4.2 -std=gnu++0x -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}\\\" -DRELEASE=\\\"${RELEASE}\\\" -DCLIENT_ONLY=1" )
else (CLIENT)
set(CMAKE_CXX_FLAGS "-g -Wall -O2 -msse4.2 -std=gnu++0x -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}\\\" -DRELEASE=\\\"${RELEASE}\\\"" )
endif (CLIENT)

#set(SOFLAGS  "-m64 -dynamiclib -undefined dynamic_lookup -single_module")
#set(CMAKE_SHARED_LINKER_FLAGS ${SOFLAGS}) 

######################################################################################################################
# install targets
######################################################################################################################
set(CMAKE_INSTALL_PREFIX /usr/)

set(SYSCONFDIR "/etc" CACHE PATH "Directory for system configuration files" )
set(MANDIR "/usr/share/man/man1" CACHE PATH "Directory where man pages get installed" ) 

if (CLIENT)
add_subdirectory (fuse)
add_subdirectory (console)
add_subdirectory (man)
add_subdirectory (test)
add_subdirectory (mq)
add_subdirectory (common)
else (CLIENT)
add_subdirectory (mq)
add_subdirectory (common)
add_subdirectory (authz)
add_subdirectory (fst)
add_subdirectory (mgm)
add_subdirectory (console)
add_subdirectory (namespace)
add_subdirectory (etc)
add_subdirectory (sync)
add_subdirectory (fuse)
add_subdirectory (srm)
add_subdirectory (var)
add_subdirectory (man)
add_subdirectory (test)
if (HTTPD_FOUND) 
if (APR_FOUND)
#add_subdirectory (httpd)
#add_subdirectory (httpd-mini)
endif (APR_FOUND)
endif (HTTPD_FOUND)
endif (CLIENT)

include_directories( ../ /usr/include/xrootd/ /usr/include/ )
link_directories( /usr/${LIB_INSTALL_DIR}/)


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

######################################################################################################################
# Package Finding
######################################################################################################################

# google sparse hash
