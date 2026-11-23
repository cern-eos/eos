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

cmake_minimum_required (VERSION 3.16...3.30 FATAL_ERROR)

# Set default build type if not set. This must be done before calling project()
if(NOT CMAKE_BUILD_TYPE AND NOT GENERATOR_IS_MULTI_CONFIG)
  if(NOT CMAKE_C_FLAGS AND NOT CMAKE_CXX_FLAGS)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
      "Build type: Debug Release RelWithDebInfo MinSizeRel None(use CMAKE_CXX_FLAGS)")
  endif()
endif()

project(eos DESCRIPTION "EOS Open Storage" LANGUAGES C CXX ASM)

# Insert cmake/ before everything else in the CMake module path
list(INSERT CMAKE_MODULE_PATH 0 "${PROJECT_SOURCE_DIR}/cmake")

option(CCACHE "Use ccache for compilation" ON)

if(CCACHE)
  find_program(CCACHE_COMMAND ccache ccache-swig)
  mark_as_advanced(CCACHE_COMMAND ${CCACHE_COMMAND})

  if(EXISTS ${CCACHE_COMMAND})
    message(VERBOSE "Found ccache: ${CCACHE_COMMAND}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_COMMAND})
  else()
    message(VERBOSE "Could NOT find ccache")
    set(CCACHE OFF CACHE BOOL "Use ccache for compilation (disabled)" FORCE)
  endif()
endif()

#-------------------------------------------------------------------------------
# Activate include-what-you-use
#-------------------------------------------------------------------------------
option(ENABLE_IWYU "Enable include-what-you-use tool" OFF)

if(ENABLE_IWYU)
  find_program(IWYU_PATH NAMES include-what-you-use iwyu)
  if(NOT IWYU_PATH)
    message(FATAL_ERROR "Could not find include-what-you-use")
  endif()

  set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
  set(CMAKE_C_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif()

#-------------------------------------------------------------------------------
# Include code coverage module
#-------------------------------------------------------------------------------
option(COVERAGE "Build with test coverage reporting" OFF)

if (COVERAGE)
include(EosCoverage)
endif()

#-------------------------------------------------------------------------------
# Include generic functions and compiler definition parameters
#-------------------------------------------------------------------------------
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Default install prefix: /usr" FORCE)
endif ()

include(EosUtils)
EOS_CheckOutOfSourceBuild()
EOS_GetUidGid("daemon" "DAEMONUID" "DAEMONGID")
EOS_GetVersion("${VERSION_MAJOR}" "${VERSION_MINOR}" "${VERSION_PATCH}" "${RELEASE}")

file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/common")
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/common/BuildVersion.hh.in"
  "${CMAKE_BINARY_DIR}/common/BuildVersion.hh" @ONLY)

include(EosOSDefaults)

if (NOT PACKAGEONLY)
  include(EosCompileFlags)
endif()

if (NOT EOS_INSTALL_SYSCONFDIR)
  set(EOS_INSTALL_SYSCONFDIR /etc)
endif()

set(CMAKE_INSTALL_SYSCONFDIR ${EOS_INSTALL_SYSCONFDIR})
include(EosFindLibs)

include(CTest)

#-------------------------------------------------------------------------------
# git is required for building the source tarball (see `dist` target below)
#-------------------------------------------------------------------------------
find_package(Git REQUIRED)

#-------------------------------------------------------------------------------
# Make gtest / gmock available for all downstream CMakeLists.txt that need it
#-------------------------------------------------------------------------------
option(USE_SYSTEM_GTEST "Use GoogleTest installed in the system if found" OFF)

if(USE_SYSTEM_GTEST)
  find_package(GTest REQUIRED)
else()
  add_subdirectory(unit_tests/googletest EXCLUDE_FROM_ALL)

  # Add alias libraries to emulate same behavior as external GoogleTest
  add_library(GTest::GTest ALIAS gtest)
  add_library(GTest::Main ALIAS gtest_main)
endif()

#-------------------------------------------------------------------------------
# Generate documentation
#-------------------------------------------------------------------------------
if (Python3_Interpreter_FOUND AND SPHINX_FOUND)
  add_custom_target(doc
    COMMAND python3 generate_docs.py
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}/doc"
    COMMENT "Build HTML documentation with Sphinx ...")
endif ()

#-------------------------------------------------------------------------------
# Generate man pages
#-------------------------------------------------------------------------------
if (BUILD_MANPAGES AND HELP2MAN_FOUND)
  add_subdirectory(man)
endif()

#-------------------------------------------------------------------------------
# Build qclient static library
#-------------------------------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})

# HTTP/2 filesystem client and server from the sibling XRootD tree.
# Absent on a normal EOS build; present when ../xrootd has been built
# with the XrdHttp2 / Xio library.
set(EOS_XIOFS_ROOT "${CMAKE_SOURCE_DIR}/../xrootd" CACHE PATH
    "XRootD tree that contains the XioFS HTTP/2 library")
set(EOS_XIOFS_LIB "${EOS_XIOFS_ROOT}/build/lib/libXrdXioFS.a")
set(EOS_HAVE_XIO FALSE)
if(EXISTS "${EOS_XIOFS_ROOT}/src/XrdXioFS/http/XioClient.hh" AND
   EXISTS "${EOS_XIOFS_LIB}")
  set(EOS_HAVE_XIO TRUE)
  find_package(OpenSSL REQUIRED)
  find_library(EOS_NGHTTP2_LIBRARY nghttp2 REQUIRED)
  find_library(EOS_ZSTD_LIBRARY zstd)
  find_library(EOS_PARSOCK_LIBRARY parsock
    HINTS "${CMAKE_SOURCE_DIR}/../libparsocket/build")
  find_library(EOS_NGTCP2_LIBRARY ngtcp2)
  find_library(EOS_NGTCP2_CRYPTO_LIBRARY ngtcp2_crypto_ossl)
  if(APPLE)
    set(EOS_XIO_GSS "-framework GSS")
  else()
    find_library(EOS_GSSAPI_LIBRARY NAMES gssapi_krb5 gssapi)
    set(EOS_XIO_GSS "${EOS_GSSAPI_LIBRARY}")
  endif()
  set(EOS_XIO_LINK_LIBS
    "${EOS_XIOFS_LIB}"
    OpenSSL::SSL
    OpenSSL::Crypto
    ${EOS_NGHTTP2_LIBRARY}
    ${EOS_XIO_GSS})
  foreach(_xio_opt IN ITEMS EOS_ZSTD_LIBRARY EOS_PARSOCK_LIBRARY
                            EOS_NGTCP2_LIBRARY EOS_NGTCP2_CRYPTO_LIBRARY)
    if(${_xio_opt})
      list(APPEND EOS_XIO_LINK_LIBS ${${_xio_opt}})
    endif()
  endforeach()
  set(EOS_XIO_INCLUDE_DIRS
    "${EOS_XIOFS_ROOT}/src/XrdXioFS/http"
    "${EOS_XIOFS_ROOT}/src/XrdXioFS/include"
    "${EOS_XIOFS_ROOT}/src/XrdXioFS/server")
  message(STATUS "Xio HTTP/2 library: ${EOS_XIOFS_LIB}")
endif()

add_subdirectory(common)
add_subdirectory(proto)
add_subdirectory(fst)
add_subdirectory(console)
add_subdirectory(fusex)
add_subdirectory(misc)
add_subdirectory(test)
add_subdirectory(namespace/ns_quarkdb/qclient)

if (GRPC_FOUND)
  add_subdirectory(client)
endif()

if (NOT CLIENT)
  add_subdirectory(mgm)
  add_subdirectory(namespace)
  add_subdirectory(utils)
  add_subdirectory(archive)
  add_subdirectory(auth_plugin)
  add_subdirectory(unit_tests)
  add_subdirectory(quarkdb)
endif ()

#-------------------------------------------------------------------------------
# Uninstall target
#-------------------------------------------------------------------------------
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(
   uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake")

#-------------------------------------------------------------------------------
# Source packaging
#-------------------------------------------------------------------------------
set(EOS_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
set(EOS_PACKAGE_VERSION "${VERSION}")
set(EOS_PACKAGE_VERSION_MAJOR "${VERSION_MAJOR}")
set(EOS_PACKAGE_VERSION_MINOR "${VERSION_MINOR}")
set(EOS_PACKAGE_VERSION_PATCH "${VERSION_PATCH}")
set(EOS_PACKAGE_RELEASE "${RELEASE}")
set(EOS_SOURCE_PACKAGE_NAME "${EOS_PACKAGE_NAME}-${EOS_PACKAGE_VERSION}-${EOS_PACKAGE_RELEASE}")
set(EOS_ARCHIVE "${EOS_SOURCE_PACKAGE_NAME}.tar.gz")
set(EOS_ARCHIVE_PATH "${CMAKE_BINARY_DIR}/${EOS_ARCHIVE}")
option(EOS_INSTALL_TUI "Download and install the prebuilt eos-tui binary for server builds" ON)

# Client packages must build without downloading external release artifacts.
if (EOS_INSTALL_TUI AND NOT CLIENT)
  set(EOS_TUI_VERSION "0.2.14")
  include(EosTui)
endif()

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/config_spec.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake" @ONLY IMMEDIATE)

# Generate eos.spec in the build dir (it will be embedded into the tarball
# at <prefix>/eos.spec by the archive script).
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/eos.spec"
  COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
          "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec.in")

# Build the source archive from git (top-level repo + all submodules).
add_custom_target(
  dist
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cmake/eos-make-source-archive.sh
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${EOS_ARCHIVE_PATH}"
          "${EOS_SOURCE_PACKAGE_NAME}"
          "${CMAKE_CURRENT_BINARY_DIR}/eos.spec"
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/eos.spec"
  COMMENT "Creating source archive ${EOS_ARCHIVE} from git")

#-------------------------------------------------------------------------------
# Source and binary rpms
#-------------------------------------------------------------------------------
set(SRPM_DEFINE --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5")

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CLANG_BUILD)
  LIST(APPEND RPM_OPTIONS --with clang)
endif()

if (NOT CLIENT)
  LIST(APPEND RPM_OPTIONS --with server)

  if (WITHOUT_EOS_XROOTD)
    LIST(APPEND RPM_OPTIONS --without eos_xrootd_rh)
  endif()
endif()

if (ASAN)
  LIST(APPEND RPM_OPTIONS --with asan)
endif()

if (TSAN)
  LIST(APPEND RPM_OPTIONS --with tsan)
endif()

if (UBSAN)
  LIST(APPEND RPM_OPTIONS --with ubsan)
endif()

option(NO_SSE "Build without sse instruction set" OFF)

if (NO_SSE)
  LIST(APPEND RPM_OPTIONS --with no_sse)
endif()

option(EOS_GRPC_GW "Build without eos grpc support" OFF)

if (EOS_GRPC_GW)
  LIST(APPEND RPM_OPTIONS --with eos_grpc_gateway)
endif()


add_custom_target(
  srpm
  COMMAND rpmbuild -ts ${EOS_ARCHIVE_PATH} --define "_topdir ${CMAKE_BINARY_DIR}" ${SRPM_DEFINE} ${RPM_OPTIONS})

add_custom_target(
  rpm
  COMMAND rpmbuild -tb ${EOS_ARCHIVE_PATH} --define "_topdir ${CMAKE_BINARY_DIR}" ${RPM_OPTIONS})

add_dependencies(srpm dist)
add_dependencies(rpm dist)

#-------------------------------------------------------------------------------
# Custom target to build on OSX
#-------------------------------------------------------------------------------
add_custom_target(
  osx
  COMMAND sudo ../utils/eos-osx-package.sh ${EOS_PACKAGE_VERSION})

#-------------------------------------------------------------------------------
# Custom target to build graphviz for all target
#-------------------------------------------------------------------------------
include(EosGraphviz)

#-------------------------------------------------------------------------------
# Print project summary
#-------------------------------------------------------------------------------
include(EosSummary)
