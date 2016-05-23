# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrailru <esindril@cern.ch> CENR
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
  set(LIBRARY_PATH_PREFIX lib)
  list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)

  if("${isSystemDir}" STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  endif()

endif(APPLE)

# Check if we are on Windows
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Windows")
  set(Windows TRUE )
  set(OSDEFINE "-D__WINDOWS__=1")
endif()

#-------------------------------------------------------------------------------
# Check for Gcc >=4.4 and C++11 feature support detection
#-------------------------------------------------------------------------------
if(CMAKE_COMPILER_IS_GNUCXX)
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.4.0)
    message(FATAL_ERROR "GCC version needs to be at least 4.4!")
  endif()
endif()

include(CheckCXXCompilerFlag)

check_cxx_compiler_flag(-std=c++11 HAVE_STD_CPP11_FLAG)
if (HAVE_STD_CPP11_FLAG)
  # Check if including cmath works with -std=c++11 and -O3.
  # It may not in MinGW due to bug http://ehc.ac/p/mingw/bugs/2250/.
  set(CMAKE_REQUIRED_FLAGS "-std=c++11 -O3")
  check_cxx_source_compiles("
    #include <cmath>
    int main() {}" FMT_CPP11_CMATH)
  # Check if including <unistd.h> works with -std=c++11.
  # It may not in MinGW due to bug http://sourceforge.net/p/mingw/bugs/2024/.
  check_cxx_source_compiles("
    #include <unistd.h>
    int main() {}" FMT_CPP11_UNISTD_H)
  if (FMT_CPP11_CMATH AND FMT_CPP11_UNISTD_H)
    set(CPP11_FLAG -std=c++11)
  else ()
    check_cxx_compiler_flag(-std=gnu++11 HAVE_STD_GNUPP11_FLAG)
    if (HAVE_STD_CPP11_FLAG)
      set(CPP11_FLAG -std=gnu++11)
    endif ()
  endif ()
  set(CMAKE_REQUIRED_FLAGS )
else ()
  check_cxx_compiler_flag(-std=c++0x HAVE_STD_CPP0X_FLAG)
  if (HAVE_STD_CPP0X_FLAG)
    set(CPP11_FLAG -std=c++0x)
  endif ()
endif ()
