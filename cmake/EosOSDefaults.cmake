# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrailru <esindril@cern.ch> CERN
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

#-------------------------------------------------------------------------------
# Detect the operating system and define variables
#-------------------------------------------------------------------------------
# Nothing detected yet
set(Linux FALSE )
set(MacOSX FALSE )
set(Windows FALSE )
set(OSDEFINE "")

# Check if we are on Linux
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
  include(GNUInstallDirs)
  set(Linux TRUE )
  set(OSDEFINE "-D__LINUX__=1")
endif()

# Check if we are on MacOSX
if(APPLE)
  include(GNUInstallDirs)
  set(MacOSX TRUE )
  set(CLIENT TRUE )
  set(OSDEFINE "-D__APPLE__=1")
  # On MAC we don't link static objects at all
  set(FUSE_LIBRARY /usr/local/lib/libosxfuse_i64.dylib)
  set(CMAKE_MACOSX_RPATH ON)
  set(CMAKE_SKIP_BUILD_RPATH FALSE)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
  set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)

  if("${isSystemDir}" STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  endif()
endif()

# Check if we are on Windows
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Windows")
  set(Windows TRUE )
  set(OSDEFINE "-D__WINDOWS__=1")
endif()
