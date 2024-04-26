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
# Require c++17
#-------------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
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

add_compile_options(-Wall -Wextra -Wpedantic -Wno-deprecated-declarations)

#-------------------------------------------------------------------------------
# CPU architecture flags
#-------------------------------------------------------------------------------

include(CPUArchFlags)

#-------------------------------------------------------------------------------
# Client-only flags
#-------------------------------------------------------------------------------
if (CLIENT)
  add_definitions(CLIENT_ONLY=1)
endif ()

include(CheckCXXCompilerFlag)

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

if (ASAN)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
  check_cxx_compiler_flag(-fsanitize=address HAVE_FLAG_ASAN)
  unset(CMAKE_REQUIRED_FLAGS)

  if (HAVE_FLAG_ASAN)
    message(STATUS "Enabling ASAN FLAGS")
    add_compile_options(-fsanitize=address)
  else()
    message(FATAL_ERROR "A compiler with '-fsanitize=address' support is required.")
  endif()
endif()

if (TSAN)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
  check_cxx_compiler_flag(-fsanitize=thread HAVE_FLAG_TSAN)
  unset(CMAKE_REQUIRED_FLAGS)

  if (HAVE_FLAG_TSAN)
    message(STATUS "Enabling TSAN FLAGS")
    add_compile_options(-fsanitize=thread)
  else()
    message(FATAL_ERROR "A compiler with '-fsanitize=thread' support is required.")
  endif()
endif()
