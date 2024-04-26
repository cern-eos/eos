# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2019 CERN/Switzerland                                  *
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
# Require C++17
#-------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ Standard")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS FALSE)

# Avoid having CMAKE treat include directories on imported libraries as systems
# includes. In newer gcc versions the systems includes are added using the
# "-isystem" flag instead of "-I". This currently breaks the build on Fedora 36
# and 37.
set(CMAKE_NO_SYSTEM_FROM_IMPORTED TRUE)

add_compile_definitions(EOSCITRINE VERSION="${VERSION}" RELEASE="${RELEASE}")

#-------------------------------------------------------------------------------
# Compile Options
#-------------------------------------------------------------------------------

add_compile_options(-Wall)

#-------------------------------------------------------------------------------
# CPU architecture flags
#-------------------------------------------------------------------------------

include(CPUArchFlags)

#-------------------------------------------------------------------------------
# Client-only flags
#-------------------------------------------------------------------------------

if (CLIENT)
  add_compile_definitions(CLIENT_ONLY=1)
endif ()

#-------------------------------------------------------------------------------
# Compiler specific flags
#-------------------------------------------------------------------------------

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # Clang requires linking with libatomic
  link_libraries(atomic)
endif()

#-------------------------------------------------------------------------------
# Sanitizer flags
#-------------------------------------------------------------------------------

include(CheckCXXCompilerFlag)

function(eos_enable_sanitizer sanitizer var)
  set(FLAG -fsanitize=${sanitizer})
  list(APPEND CMAKE_REQUIRED_FLAGS "${FLAG}")
  list(APPEND CMAKE_REQUIRED_LINK_OPTIONS "${FLAG}")
  check_cxx_compiler_flag("${FLAG}" "${var}")
  if (${${var}})
    # Set required flags in parent scope, as some sanitizers can't be used together
    set(CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS} PARENT_SCOPE)
    set(CMAKE_REQUIRED_LINK_OPTIONS ${CMAKE_REQUIRED_LINK_OPTIONS} PARENT_SCOPE)
    add_compile_options(${FLAG})
    add_link_options(${FLAG})
  else()
    message(FATAL_ERROR "Could not enable flag '${FLAG}'.\n"
      "Configure with --trace-expand to debug.")
  endif()
endfunction()

if(ASAN)
  eos_enable_sanitizer(address ASAN_SUPPORTED)
endif()

if(TSAN)
  eos_enable_sanitizer(thread TSAN_SUPPORTED)
endif()
