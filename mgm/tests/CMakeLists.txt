# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2017 CERN/Switzerland                                  *
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
  ${CMAKE_SOURCE_DIR}
  ${CPPUNIT_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Mgm View tests
#-------------------------------------------------------------------------------
add_executable(
  mgm_view_tests
  MgmViewTest.cc
  ${CMAKE_SOURCE_DIR}/mgm/FsView.cc
  ${CMAKE_SOURCE_DIR}/mgm/TableFormatter/TableFormatterBase.cc
  ${CMAKE_SOURCE_DIR}/mgm/TableFormatter/TableCell.cc)

target_compile_definitions(mgm_view_tests PUBLIC -DEOSMGMFSVIEWTEST)

target_link_libraries(
  mgm_view_tests
  eosCommon
  eosCommonServer
  XrdMqClient-Static
  eosCapability-Static
  ${Z_LIBRARY}
  ${ZMQ_LIBRARIES}
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${JSONCPP_LIBRARIES})
