#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@cern.ch>
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2015 CERN/Switzerland                                  *
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
# EosNsCommon-Sources library
#-------------------------------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR} ${XROOTD_INCLUDE_DIR})

add_library(
  EosNsCommon-Objects OBJECT

  MDException.cc
  MDException.hh
  Constants.cc

  # Namespace interface
  interface/IView.hh
  interface/IFsView.hh
  interface/IFileMDSvc.hh
  interface/IContainerMDSvc.hh
  interface/IFileMD.hh
  interface/IContainerMD.hh
  interface/IChLogContainerMDSvc.hh
  interface/IChLogFileMDSvc.hh

  # Namespace utils
  utils/DataHelper.cc
  utils/Descriptor.cc
  utils/FileListRandomPicker.cc
  utils/ThreadUtils.cc
  utils/TestHelpers.cc
  utils/Buffer.hh)

set_target_properties(
  EosNsCommon-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE True)

add_library(EosNsCommon SHARED $<TARGET_OBJECTS:EosNsCommon-Objects> )

target_include_directories(
  EosNsCommon PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/utils/fmt>
  $<INSTALL_INTERFACE:include>)

target_link_libraries(
  EosNsCommon PUBLIC
  ${Z_LIBRARY} fmt)

set_target_properties(
  EosNsCommon
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR})

install(
  TARGETS EosNsCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# EosNsCommon-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(EosNsCommon-Static STATIC $<TARGET_OBJECTS:EosNsCommon-Objects> )

  target_include_directories(
    EosNsCommon-Static PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/utils/fmt>
    $<INSTALL_INTERFACE:include>)

  target_link_libraries(
    EosNsCommon-Static PUBLIC
    ${Z_LIBRARY_STATIC} fmt)

  set_target_properties(
    EosNsCommon-Static
    PROPERTIES
    POSITION_INDEPENDENT_CODE True)
endif ()

#-------------------------------------------------------------------------------
# text-runner used for running CPPUnit tests
#-------------------------------------------------------------------------------
if (CPPUNIT_FOUND)
  add_executable(
    text-runner
    utils/TextRunner.cc)

  target_include_directories(
    text-runner PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CPPUNIT_INCLUDE_DIRS}>
    $<INSTALL_INTERFACE:include>)

  target_link_libraries(
    text-runner PUBLIC
    ${CPPUNIT_LIBRARIES}
    ${GLIBC_DL_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

add_subdirectory(ns_in_memory)
add_subdirectory(ns_quarkdb)
