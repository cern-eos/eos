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
set(EOS_NS_REDIS_SRC
  NsRedisPlugin.cc      NsRedisPlugin.hh
#  RedisFileMD.cc          RedisFileMD.hh
#  RedisFileMDSvc.cc       RedisFileMDSvc.hh
  RedisContainerMD.cc     RedisContainerMD.hh
  RedisContainerMDSvc.cc  RedisContainerMDSvc.hh)

add_library(
  EosNsRedis MODULE
  ${EOS_NS_REDIS_SRCS})

target_link_libraries(
  EosNsRedis
  EosNsCommon
  ${Z_LIBRARY}
  ${GLIC_RT_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

# TODO: add hiredis client dependency

install(
  TARGETS EosNsRedis
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
