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
# Detect the operating system and define variables
#-------------------------------------------------------------------------------
function(EOS_defineOperatingSystem)
  # Nothing detected yet
  set(Linux FALSE PARENT_SCOPE)
  set(MacOSX FALSE PARENT_SCOPE)
  set(Windows FALSE PARENT_SCOPE)
  set(OSDEFINE "")

  # Check if we are on Linux
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    set(Linux TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__LINUX__=1" PARENT_SCOPE)
  endif(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")

  # Check if we are on MacOSX
  if(APPLE)
    set(MacOSX TRUE PARENT_SCOPE)
    set(CLIENT TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__APPLE__=1" PARENT_SCOPE)
  endif(APPLE)

  # Check if we are on Windows
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(Windows TRUE PARENT_SCOPE)
    set(OSDEFINE "-D__WINDOWS__=1" PARENT_SCOPE)
  endif(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
endfunction(EOS_defineOperatingSystem)


#-------------------------------------------------------------------------------
# Detect in source builds
#-------------------------------------------------------------------------------
macro(EOS_CheckOutOfSourceBuild)
  #Check if previous in-source build failed
  if(EXISTS ${CMAKE_SOURCE_DIR}/CMakeCache.txt OR EXISTS ${CMAKE_SOURCE_DIR}/CMakeFiles)
    message(FATAL_ERROR "CMakeCache.txt or CMakeFiles exists in source directory!")
    message(FATAL_ERROR "Please remove them before running cmake .")
  endif(EXISTS ${CMAKE_SOURCE_DIR}/CMakeCache.txt OR EXISTS ${CMAKE_SOURCE_DIR}/CMakeFiles)

  # Get real paths of the source and binary directories
  get_filename_component(srcdir "${CMAKE_SOURCE_DIR}" REALPATH)
  get_filename_component(bindir "${CMAKE_BINARY_DIR}" REALPATH)

  # Check for in-source builds
  if(${srcdir} STREQUAL ${bindir})
    message(FATAL_ERROR "EOS cannot be built in-source! Please run cmake <src-dir> outside the source directory")
  endif(${srcdir} STREQUAL ${bindir})

endmacro(EOS_CheckOutOfSourceBuild)

set(LIBRARY_PATH_PREFIX "lib")