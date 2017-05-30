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
cmake_minimum_required (VERSION 3.3)

# Set the default CMAKE_BUILD_TYPE to RelWithDebInfo before the project command
# as it can set CMAKE_BUILD_TYPE itself.
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
    "Build options: None(CMAKE_CXX_FLAGS or CMAKE_C_FLAGS used) Debug Release \
     RelWithDebInfo MinSizeRel.")
endif ()

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK "${CCACHE_PROGRAM}")
endif()

project (eos)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#-------------------------------------------------------------------------------
# Include some generic functions and compiler definition parameters
#-------------------------------------------------------------------------------
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Default install prefix: /usr" FORCE)
endif ()

include(EosUtils)
EOS_CheckOutOfSourceBuild()
EOS_GetUidGid("daemon" "DAEMONUID" "DAEMONGID")	 
EOS_GetVersion("${VERSION_MAJOR}" "${VERSION_MINOR}" "${VERSION_PATCH}" "${RELEASE}")
include(EosOSDefaults)

if (CLIENT)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCLIENT_ONLY=1")
endif ()

set(EOS_CXX_DEFINE "-DVERSION=\\\"${VERSION}\\\" -DRELEASE=\\\"${RELEASE}\\\"")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EOS_CXX_DEFINE} ${CPP11_FLAG} -msse4.2 -Wall")

set (CMAKE_INSTALL_SYSCONFDIR /etc)
include (EosFindLibs)

#-------------------------------------------------------------------------------
# Generate documentation
#-------------------------------------------------------------------------------
if(PYTHONINTERP_FOUND AND SPHINX_FOUND)
  add_custom_target(doc
    COMMAND ${PYTHON_EXECUTABLE} generate_docs.py
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}/doc"
    COMMENT "Build HTML documentation with Sphinx ...")
endif()

#-------------------------------------------------------------------------------
# Install targets
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# Build the libfmt for fast convertions
#-------------------------------------------------------------------------------
if (NOT TARGET fmt)
  include_directories(common/fmt)
  add_subdirectory(common/fmt)
  set_target_properties(fmt PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
endif()

add_subdirectory(common)
add_subdirectory(misc)
add_subdirectory(authz)
add_subdirectory(fst)
add_subdirectory(mq)
add_subdirectory(man)
add_subdirectory(fuse)
add_subdirectory(console)

if (NOT CLIENT)
  add_subdirectory(mgm)
  add_subdirectory(namespace)
  add_subdirectory(sync)
  add_subdirectory(srm)
  add_subdirectory(test)
  add_subdirectory(utils)
  add_subdirectory(archive)
  add_subdirectory(auth_plugin)

  if(BUILD_TESTS)
    add_subdirectory(unittests)
  endif()
endif()

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
/microhttpd/;/.deps/;~$;'.'o$;/lib/;/.git/;eos.spec.in;.tar.gz$;\
.tar.bz2$;${CPACK_SOURCE_IGNORE_FILES};")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/config_spec.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake" @ONLY IMMEDIATE)

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
  COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/cmake/config_spec.cmake"
  "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec.in")

# we get a local version of the kineticio dist so that it is embedded in the srpm because 
# it is not always possible to download it from the machine doing the build 
file(DOWNLOAD ${KINETICIO_URL} "${CMAKE_SOURCE_DIR}/kineticio-dist.tgz" 
  SHOW_PROGRESS
  EXPECTED_MD5 "${KINETICIO_URL_MD5}")

add_custom_target(
  dist
  COMMAND ${CMAKE_MAKE_PROGRAM} package_source
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
  DEPENDS "${CMAKE_SOURCE_DIR}/kineticio-dist.tgz")

add_custom_command(
  TARGET dist POST_BUILD
  COMMAND rm ARGS -rf "${CMAKE_CURRENT_SOURCE_DIR}/eos.spec"
  COMMENT "Clean generated spec file")

include(CPack)

#-------------------------------------------------------------------------------
# Source and binary rpms
#-------------------------------------------------------------------------------
set (RPM_DEFINE --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5")
set (EOS_ARCHIVE         "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz")
set (EOS_CLIENTS_ARCHIVE "${CMAKE_PROJECT_NAME}-clients-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.tar.gz")

add_custom_target(
  srpm
  COMMAND rpmbuild ${RPM_DEFINE} -ts ${EOS_ARCHIVE} --define "_topdir ${CMAKE_BINARY_DIR}" --with server)

add_custom_target(
  rpm-client
  COMMAND rpmbuild -tb ${EOS_ARCHIVE} --define "_topdir ${CMAKE_BINARY_DIR}")

add_custom_target(
  rpm
  COMMAND rpmbuild -tb ${EOS_ARCHIVE} --define "_topdir ${CMAKE_BINARY_DIR}" --with server)

add_dependencies(srpm dist)
add_dependencies(rpm dist)
add_dependencies(rpm-client dist)

#-------------------------------------------------------------------------------
# Custom target to build on OSX
#-------------------------------------------------------------------------------
add_custom_target(
  osx
  COMMAND sudo ../utils/eos-osx-package.sh ${CPACK_PACKAGE_VERSION})

#-------------------------------------------------------------------------------
# Print project summary
#-------------------------------------------------------------------------------
include(EosSummary)
