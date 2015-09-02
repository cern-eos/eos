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

include_directories(
o ${CMAKE_SOURCE_DIR}/namespace/
 ${XROOTD_INCLUDE_DIR})

link_directories(${XROOTD_LIB_DIR})

#-----------------------------------------------------------------------------
# EosNsOnFs library sources
#-----------------------------------------------------------------------------
set(EOS_NS_FS_SRCS
  NsOnFsPlugin.cc      NsOnFsPlugin.hh
#  FsFileMD.cc          FsFileMD.hh
#  FsFileMDSvc.cc       FsFileMDSvc.hh
  FsContainerMD.cc     FsContainerMD.hh
  FsContainerMDSvc.cc  FsContainerMDSvc.hh)

add_library(
  EosNsOnFs MODULE
  ${EOS_NS_FS_SRCS})

target_link_libraries(
  EosNsOnFs
  EosNsCommon
  ${Z_LIBRARY}
  ${GLIC_RT_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

install(
  TARGETS EosNsOnFs
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
