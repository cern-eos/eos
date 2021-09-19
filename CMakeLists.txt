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

#-------------------------------------------------------------------------------
# Main section
#-------------------------------------------------------------------------------
cmake_minimum_required (VERSION 3.14)

# Set the default CMAKE_BUILD_TYPE to RelWithDebInfo before the project command
# as it can set CMAKE_BUILD_TYPE itself.
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
    "Build options: None(CMAKE_CXX_FLAGS or CMAKE_C_FLAGS used) Debug Release \
     RelWithDebInfo MinSizeRel.")
endif ()

find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK "${CCACHE_PROGRAM}")
endif ()

project (eos)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#-------------------------------------------------------------------------------
# Detect target architecture
#-------------------------------------------------------------------------------
option(AARCH64 "Build on 64bit arm platform" OFF)
execute_process(COMMAND uname -p COMMAND tr -d '\n' OUTPUT_VARIABLE FOO)
if (${FOO} STREQUAL "aarch64")
  set (AARCH64 ON)
else()
  set (AARCH64 OFF)
endif()
if (AARCH64)
 LIST(APPEND CMAKE_CXX_FLAGS "-Wall -mabi=lp64 -g -D__aarch64__")
endif()

#-------------------------------------------------------------------------------
# Make gtest / gmock available for all downstream CMakeLists.txt that need it
#-------------------------------------------------------------------------------
set(GTEST "${CMAKE_SOURCE_DIR}/unit_tests/googletest/")
set(GTEST_BINARY_DIR "${CMAKE_BINARY_DIR}/unit_tests/googletest/")
add_subdirectory("${GTEST}" "${GTEST_BINARY_DIR}" EXCLUDE_FROM_ALL)

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
include(EosOSDefaults)

if (NOT PACKAGEONLY)
  include(EosCompileFlags)
endif()

set(CMAKE_INSTALL_SYSCONFDIR /etc)
include(EosFindLibs)

if (CLIENT)
  message("BUILDING CLIENT ONLY")
else()
  message("BUILDING SERVER & CLIENT")
endif()

message("SYSTEM PATH IS ${CMAKE_SYSTEM_INCLUDE_PATH}, PREFIX ${CMAKE_SYSTEM_PREFIX_PATH} and cmake path is ${CMAKE_PREFIX_PATH} & INCLUDE ${CMAKE_INCLUDE_PATH}")
#-------------------------------------------------------------------------------
# Generate documentation
#-------------------------------------------------------------------------------
if (PYTHONINTERP_FOUND AND SPHINX_FOUND)
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
# Build the libfmt for fast conversions
#-------------------------------------------------------------------------------
if (NOT TARGET fmt)
  include_directories(common/fmt/include)
  add_subdirectory(common/fmt)
  set_target_properties(fmt PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
endif ()

#-------------------------------------------------------------------------------
# Build qclient static library
#-------------------------------------------------------------------------------
include_directories(SYSTEM ${CMAKE_SOURCE_DIR})
add_subdirectory(namespace/ns_quarkdb/qclient)
add_subdirectory(common)
add_subdirectory(proto)
add_subdirectory(misc)
add_subdirectory(fst)
add_subdirectory(mq)
add_subdirectory(fuse)
add_subdirectory(console)
add_subdirectory(fusex)
add_subdirectory(test)
if (GRPC_FOUND)
  add_subdirectory(client)
endif()

if (NOT CLIENT)
  add_subdirectory(mgm)
  add_subdirectory(namespace)
  add_subdirectory(sync)
  add_subdirectory(srm)
  add_subdirectory(utils)
  add_subdirectory(archive)
  add_subdirectory(auth_plugin)
  add_subdirectory(unit_tests)
  add_subdirectory(eos-scitokens)
  #add_subdirectory(quarkdb)
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
# Packaging
#-------------------------------------------------------------------------------
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
set(CPACK_PACKAGE_VERSION "${VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${VERSION_PATCH}")
set(CPACK_PACKAGE_RELEASE "${RELEASE}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}")
set(CPACK_SOURCE_IGNORE_FILES
"${CMAKE_CURRENT_BINARY_DIR};/ApMon/;/git/;/gitlab-ci/;/ccache/;/xrootd-dsi/;/nginx/;/dsi/;\
/microhttpd/;/grpc/grpc.spec;/.deps/;~$;'.'o$;/lib/;/.git/;eos.spec.in;elrepopackage.spec;.tar.gz$;\
.tar.bz2$;${CPACK_SOURCE_IGNORE_FILES};")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/config_spec.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake" @ONLY IMMEDIATE)

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
  COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
  "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec.in")

add_custom_target(
  dist
  COMMAND ${CMAKE_MAKE_PROGRAM} package_source
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec")

add_custom_command(
  TARGET dist POST_BUILD
  COMMAND rm ARGS -rf "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
  COMMENT "Clean generated spec file")

include(CPack)

#-------------------------------------------------------------------------------
# Source and binary rpms
#-------------------------------------------------------------------------------
set(EOS_ARCHIVE "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz")
set(SRPM_DEFINE --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5")

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CLANG_BUILD)
  LIST(APPEND RPM_OPTIONS --with clang)
endif()

if (NOT CLIENT)
  LIST(APPEND RPM_OPTIONS --with server)

  if (EOS_XROOTD)
    LIST(APPEND RPM_OPTIONS --with eos_xrootd_rh)
  endif()
endif()

if (ASAN)
  LIST(APPEND RPM_OPTIONS --with asan)
endif()

option(NO_SSE "Build without sse instruction set" OFF)
if (NO_SSE)
  LIST(APPEND RPM_OPTIONS --with no_sse)
endif()

add_custom_target(
  srpm
  COMMAND rpmbuild -ts ${EOS_ARCHIVE} --define "_topdir ${CMAKE_BINARY_DIR}" ${SRPM_DEFINE} ${RPM_OPTIONS})

add_custom_target(
  rpm
  COMMAND rpmbuild -tb ${EOS_ARCHIVE} --define "_topdir ${CMAKE_BINARY_DIR}" ${RPM_OPTIONS})

add_dependencies(srpm dist)
add_dependencies(rpm dist)

#-------------------------------------------------------------------------------
# Custom target to build on OSX
#-------------------------------------------------------------------------------
add_custom_target(
  osx
  COMMAND sudo ../utils/eos-osx-package.sh ${CPACK_PACKAGE_VERSION})

#-------------------------------------------------------------------------------
# Custom target to build graphviz for all target
#-------------------------------------------------------------------------------
include(EosGraphviz)

#-------------------------------------------------------------------------------
# Print project summary
#-------------------------------------------------------------------------------
include(EosSummary)
