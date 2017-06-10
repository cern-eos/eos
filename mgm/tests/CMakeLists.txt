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
  ${CPPUNIT_INCLUDE_DIRS}
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

#-------------------------------------------------------------------------------
# Proc fs mv tests
#-------------------------------------------------------------------------------
add_executable(
  test_proc_fs
  ProcFsTest.cc)

target_compile_definitions(test_proc_fs PUBLIC -DGTEST_USE_OWN_TR1_TUPLE=0)

target_link_libraries(
  test_proc_fs
  gtest
  gmock_main
  XrdEosMgm
  ${CMAKE_THREAD_LIBS_INIT})
