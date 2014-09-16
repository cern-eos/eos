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
set(VERSION_MINOR "3" )
set(VERSION_PATCH "52" )
set(VERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")

######################################################################################################################
# main section
######################################################################################################################
cmake_minimum_required (VERSION 2.6)
project (eos)


set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# force /usr/ installation prefix
set(CMAKE_INSTALL_PREFIX /usr)
set(CMAKE_INSTALL_SYSCONFDIR /etc)
set(CMAKE_INSTALL_SBINDIR /sbin)

if (PREFIX)
   set (CMAKE_INSTALL_PREFIX ${PREFIX})
endif (PREFIX)

# include some generic functions
include(EosUtils)

# include GNU path settings
include (GNUInstallDirs)

# include Python interpreter
include(FindPythonInterp)

# overwrite the /sbin directory
set(CMAKE_INSTALL_FULL_SBINDIR /usr/sbin)

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------

if (RELEASE)
else (RELEASE)
set(RELEASE "head")
endif (RELEASE)

EOS_defineOperatingSystem()

if (DEFINED PACKAGEONLY AND PACKAGEONLY)
   message(STATUS "Runing CMake in package only mode")
endif (DEFINED PACKAGEONLY AND PACKAGEONLY)

if (NOT DEFINED PACKAGEONLY)
if (CLIENT)
find_package(XRootD REQUIRED)
find_package(fuse REQUIRED)
find_package(Threads REQUIRED)
find_package(rt REQUIRED)
find_package(uuid REQUIRED)
else (CLIENT)
find_package(XRootD REQUIRED)
find_package(Threads REQUIRED)
find_package(SparseHash REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ncurses REQUIRED)
find_package(readline REQUIRED)
find_package(uuid REQUIRED)
find_package(z REQUIRED)
find_package(rt REQUIRED)
find_package(ldap REQUIRED)
find_package(ZMQ REQUIRED)
find_package(ProtocolBuffers)
find_package(leveldb)
find_package(CPPUnit)
find_package(microhttpd)
if(MacOSX)
find_package(fuse REQUIRED)
else(MacOSX)
find_package(fuse REQUIRED)
find_package(attr REQUIRED)
endif(MacOSX)
find_package(httpd)
find_package(apr)
endif (CLIENT)
endif (NOT DEFINED PACKAGEONLY)


######################################################################################################################
# Check for Gcc >=4.4
if(CMAKE_COMPILER_IS_GNUCXX)
  exec_program(
      ${CMAKE_CXX_COMPILER}
      ARGS                    --version
      OUTPUT_VARIABLE _compiler_output)
  string(REGEX REPLACE ".* ([0-9]\\.[0-9]\\.[0-9]) .*" "\\1"
	 gcc_compiler_version ${_compiler_output})
  message(STATUS "C++ compiler version: ${gcc_compiler_version} [${CMAKE_CXX_COMPILER}]")

  if(gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
    message(FATAL_ERROR "Error: you need atleast gcc 4.4.x to compile EOS!")
  endif (gcc_compiler_version MATCHES "4\\.[0-3]\\.[0-9]")
endif (CMAKE_COMPILER_IS_GNUCXX)


######################################################################################################################
# Platform Specific

#####################################################
if(MacOSX)
# ------------------------------------------
# on MAC we don't link static objects at all
# ------------------------------------------
set(FUSE_LIBRARY /usr/local/lib/libosxfuse_i64.dylib)

else(MacOSX)

# ------------------------------------------
# on Linux WE use static objects
# ------------------------------------------
set(LIB_INSTALL_DIR lib64)
set(RT_LIBS rt)

endif(MacOSX)
#####################################################

EOS_CheckOutOfSourceBuild()

if (CLIENT)
set(CMAKE_CXX_FLAGS "-O2 -g -Wall -Wno-deprecated-declarations -msse4.2 -std=gnu++0x -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION}\\\" -DRELEASE=\\\"${RELEASE}\\\" -DCLIENT_ONLY=1" )
else (CLIENT)
set(CMAKE_CXX_FLAGS "-O2 -g -Wall -Wno-deprecated-declarations -msse4.2 -std=gnu++0x -DHAVE_ATOMICS=1 -DVERSION=\\\"${VERSION}\\\" -DRELEASE=\\\"${RELEASE}\\\"" )
endif (CLIENT)
if (NOT LEVELDB_FOUND)
message("  Warning: LevelDB not found, using slower Sqlite3 DbMap implentation")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DEOS_SQLITE_DBMAP=1")
endif (NOT LEVELDB_FOUND)
if (NOT MICROHTTPD_FOUND)
message("  Warning: MicroHttpd not found, no httpd access available")
else (NOT MICROHTTPD_FOUND)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DEOS_MICRO_HTTPD=1")
endif (NOT MICROHTTPD_FOUND)

#####################################################
# generate client commands doc
#####################################################
ADD_CUSTOM_TARGET(doc COMMAND python ${PROJECT_SOURCE_DIR}/doc/generate_cli_command_docs.py)

######################################################################################################################
# install targets
######################################################################################################################


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
add_subdirectory (utils)
add_subdirectory (archive)
if (HTTPD_FOUND)
if (APR_FOUND)
#add_subdirectory (httpd)
#add_subdirectory (httpd-mini)
endif (APR_FOUND)
endif (HTTPD_FOUND)
endif (CLIENT)

include_directories( ../ /usr/include/xrootd/ /usr/include/ ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd ${CMAKE_BINARY_DIR}/xrootd/src/include/ )
link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/)


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
#####################################################################################################################
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
set(CPACK_PACKAGE_VERSION "${VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${VERSION_PATCH}")
set(CPACK_PACKAGE_RELEASE "${RELEASE}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}")
set(CPACK_SOURCE_IGNORE_FILES "xrootd-dsi/;/nginx/;/dsi/;/build/;/.deps/;/.bzr/;~$;'.'o$;/CMakeFiles/;Makefile;/_CPack_Packages/;.tar.gz$;.tar.bz2;CMakeCache.txt;ApMon;build;install_manifest.txt;/lib/;/.git/;${CPACK_SOURCE_IGNORE_FILES};")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/config_spec.cmake.in"
	       "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake" @ONLY IMMEDIATE)

add_custom_command(OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
                   COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
		   DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
		           "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec.in")

add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source
		  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec")

add_custom_command(TARGET dist POST_BUILD
		   COMMAND rm ARGS -rf "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
		   COMMENT "clean generated spec file")

include(CPack)

add_custom_target(gitdist
    COMMAND git archive --prefix=${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}/ HEAD
    | gzip > ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

######################################################################################################################
# source + binary rpms
######################################################################################################################

ADD_CUSTOM_TARGET(srpm COMMAND rpmbuild --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5" -ts ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz)
ADD_DEPENDENCIES(srpm dist)

ADD_CUSTOM_TARGET(rpm COMMAND rpmbuild -tb ${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz)

ADD_DEPENDENCIES(rpm dist)

######################################################################################################################
# Print Configuration
######################################################################################################################

message( STATUS "_________________________________________________" )
message( STATUS "Version           : eos-" ${VERSION_MAJOR} "." ${VERSION_MINOR} "." ${VERSION_PATCH} "-" ${RELEASE} )
if (CLIENT)
message( STATUS "Modules           : client " )
else (CLIENT)
if (LEVELDB_FOUND)
message( STATUS "Modules           : client+server with LevelDb" )
else (LEVELDB_FOUND)
message( STATUS "Modules           : client+server without LevelDb" )
endif (LEVELDB_FOUND)
endif (CLIENT)

message( STATUS "................................................." )
message( STATUS "prefix            : " ${CMAKE_INSTALL_PREFIX} )
message( STATUS "bin dir           : " ${CMAKE_INSTALL_FULL_BINDIR} )
message( STATUS "sbin dir          : " ${CMAKE_INSTALL_SBINDIR} )
message( STATUS "lib dir           : " ${CMAKE_INSTALL_FULL_LIBDIR} )
message( STATUS "sysconfig dir     : " ${CMAKE_INSTALL_SYSCONFDIR} )
message( STATUS "................................................." )
message( STATUS "C Compiler        : " ${CMAKE_C_COMPILER} )
message( STATUS "C++ Compiler      : " ${CMAKE_CXX_COMPILER} )
message( STATUS "Build type        : " ${CMAKE_BUILD_TYPE} )
message( STATUS "_________________________________________________" )
