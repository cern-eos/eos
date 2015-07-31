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

include_directories(${CMAKE_SOURCE_DIR})

#-------------------------------------------------------------------------------
# EosNsCommon library sources
#-------------------------------------------------------------------------------
set(
  EOS_NS_COMMON_SRCS
  MDException.hh
  Constants.cc   Constants.hh

  # Namespace interface
  interface/IView.hh
  interface/IFileMDSvc.hh
  interface/IContainerMDSvc.hh
  interface/IFileMD.hh
  interface/IContainerMD.hh
  interface/IChLogContainerMDSvc.hh
  interface/IChLogFileMDSvc.hh

  # Namespace utils
  utils/DataHelper.cc
  utils/Descriptor.cc
  utils/ThreadUtils.cc
  utils/Buffer.hh)

add_library(
  EosNsCommon SHARED
  ${EOS_NS_COMMON_SRCS})

add_library(
  EosNsCommon-Static STATIC
  ${EOS_NS_COMMON_SRCS})

install(
  TARGETS EosNsCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

add_subdirectory(ns_in_memory)
#add_subdirectory(ns_on_filesystem)
