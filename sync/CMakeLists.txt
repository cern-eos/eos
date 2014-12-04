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

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${XROOTD_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Create sync executables
#-------------------------------------------------------------------------------
add_executable(eosfilesync eosfilesync.cc)
add_executable(eosdirsync eosdirsync.cc)
add_executable(eosha eosha.cc)

target_link_libraries(
  eosdirsync
  eosCommon-Static
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY})

target_link_libraries(
  eosfilesync
  eosCommon-Static
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY})

install(
  TARGETS eosha eosfilesync eosdirsync
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

install(
  PROGRAMS eoshapl eossh-timeout
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
