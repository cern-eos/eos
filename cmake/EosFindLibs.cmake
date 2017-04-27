# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
option(PACKAGEONLY "Build without dependencies" OFF)
option(CLIENT "Build only client packages" OFF)
option(BUILD_XRDCL_RAIN_PLUGIN "Enable XrdCl RAIN plugin" OFF)
option(BUILD_TESTS "Build CppUnit tests" OFF)

set(KINETICIO_URL "http://dss-ci-repo.web.cern.ch/dss-ci-repo/kinetic/kineticio/noarch/kineticio-1.3-devel.tar.gz")
set(KINETICIO_URL_MD5 "ae1a538939ee26984d4e20f96bedb2c2")

if (NOT PACKAGEONLY)
  if(Linux)
    find_package(glibc REQUIRED)
    find_package(attr REQUIRED)
    find_package(xfs REQUIRED)
  endif()
  find_package(PythonSitePkg REQUIRED)
  find_package(XRootD REQUIRED)
  find_package(fuse REQUIRED)
  find_package(fuse3 )
  find_package(Threads REQUIRED)
  find_package(z REQUIRED)
  find_package(readline REQUIRED)
  find_package(CURL REQUIRED)
  find_package(uuid REQUIRED)
  find_package(Protobuf REQUIRED)
  find_package(openssl REQUIRED)
  find_package(ncurses REQUIRED)
  find_package(leveldb REQUIRED)
  find_package(ZMQ REQUIRED)
  find_package(krb5 REQUIRED)
  find_package(Sphinx)
  find_package(kineticio COMPONENTS headers)
  find_package(SparseHash REQUIRED)

  if(EXISTS "${CMAKE_SOURCE_DIR}/kineticio-dist.tgz" )
    set(KINETICIO_URL "${CMAKE_SOURCE_DIR}/kineticio-dist.tgz" )
  endif()
  #if kinetic headers cannot be found, provide them to allow compiling
  if(NOT KINETICIO_FOUND)
    include(ExternalProject)
    ExternalProject_add(
            kineticio-devel
            PREFIX vendor
            URL ${KINETICIO_URL}
            URL_MD5 ${KINETICIO_URL_MD5}
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND "")
    ExternalProject_Get_Property(kineticio-devel SOURCE_DIR)
    set(KINETICIO_INCLUDE_DIR ${SOURCE_DIR})
    set(KINETICIO_INCLUDE_DIRS ${KINETICIO_INCLUDE_DIR})
  endif()

  # The server build also requires
  if (NOT CLIENT)
    find_package(ldap REQUIRED)
    find_package(jsoncpp REQUIRED)
    if (BUILD_TESTS)
      find_package(CPPUnit REQUIRED)
      set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
      include(cmake/DownloadProject.cmake)
      download_project(
        PROJ                googletest
        GIT_REPOSITORY      https://github.com/google/googletest.git
        GIT_TAG             master
        ${UPDATE_DISCONNECTED_IF_AVAILABLE})
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
      add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
    else()
      find_package(CPPUnit)
    endif()
    find_package(microhttpd)
    if (NOT MICROHTTPD_FOUND)
      message ("Notice: MicroHttpd not found, no httpd access available")
    else ()
      set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DEOS_MICRO_HTTPD=1")
    endif ()
    find_package(hiredis)
  endif()
else()
  message(STATUS "Runing CMake in package only mode.")
  # Fake function for building the SRPMS in build system
  function(PROTOBUF_GENERATE_CPP SRCS HDRS)
    return()
  endfunction()
endif()
