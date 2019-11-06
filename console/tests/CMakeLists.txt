# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Stefan Isidorovic <stefan.isidorovic@comtrade.com>
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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
  ${OPENSSL_INCLUDE_DIRS}
  ${CPPUNIT_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# EosConsoleTests library
#-------------------------------------------------------------------------------
add_library(EosConsoleTests SHARED
  $<TARGET_OBJECTS:EosCliProto-Objects>
  $<TARGET_OBJECTS:EosConsoleHelpers-Objects>
  $<TARGET_OBJECTS:EosConsoleCommands-Objects>)

target_link_libraries(EosConsoleTests PUBLIC
  EosFstIo
  ZMQ::ZMQ
  XROOTD::POSIX
  READLINE::READLINE)

install(TARGETS EosConsoleTests
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
