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
include(CheckCXXCompilerFlag)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(EOS_CXX_DEFINE "-DEOSCITRINE -DVERSION=\\\"${VERSION}\\\" -DRELEASE=\\\"${RELEASE}\\\"")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EOS_CXX_DEFINE} ${CPP_VERSION} -Wall -Wno-error=parentheses")
# Avoid having CMAKE treat include directories on imported libraries as systems
# includes. In newer gcc versions the systems includes are added using the
# "-isystem" flag instead of "-I". This currently breaks the build on Fedora 36
# and 37.
set(CMAKE_NO_SYSTEM_FROM_IMPORTED TRUE)
check_cxx_compiler_flag(-std=c++17 HAVE_FLAG_STD_CXX17)

if(NOT HAVE_FLAG_STD_CXX17)
  message(FATAL_ERROR "A compiler with -std=c++17 support is required.")
endif()

#-------------------------------------------------------------------------------
# CPU architecture flags
#-------------------------------------------------------------------------------
include(CPUArchFlags)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPU_ARCH_FLAGS}")

#-------------------------------------------------------------------------------
# Client-only flags
#-------------------------------------------------------------------------------
if (CLIENT)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCLIENT_ONLY=1")
endif ()

#-------------------------------------------------------------------------------
# Compiler specific flags
#-------------------------------------------------------------------------------

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # Clang requires linking with libatomic
  link_libraries(atomic)

  # Add -Wno-unknown-warning-option flag when compiling with Clang to silence
  # a lot of useless noise
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-warning-option")
endif()

#-------------------------------------------------------------------------------
# Sanitizer flags
#-------------------------------------------------------------------------------
if (ASAN)
  # Copy CMAKE_CXX_FLAGS and unset them to avoid false negatives when checking
  # for support of the various flags
  set(SAVE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  unset(CMAKE_CXX_FLAGS)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
  check_cxx_compiler_flag(-fsanitize=address HAVE_FLAG_ASAN)
  unset(CMAKE_REQUIRED_FLAGS)
  set(CMAKE_CXX_FLAGS "${SAVE_CXX_FLAGS}")

  if (NOT HAVE_FLAG_ASAN)
    message(FATAL_ERROR "A compiler with '-fsanitize=address' support is required.")
  endif()

  message(STATUS "Enabling ASAN FLAGS")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

if (TSAN)
  # Copy CMAKE_CXX_FLAGS and unset them to avoid false negatives when checking
  # for support of the various flags
  set(SAVE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  unset(CMAKE_CXX_FLAGS)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
  check_cxx_compiler_flag(-fsanitize=thread HAVE_FLAG_TSAN)
  unset(CMAKE_REQUIRED_FLAGS)
  set(CMAKE_CXX_FLAGS "${SAVE_CXX_FLAGS}")

  if (NOT HAVE_FLAG_TSAN)
    message(FATAL_ERROR "A compiler with '-fsanitize=thread' support is required.")
  endif()

  message(STATUS "Enabling TSAN FLAGS")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
endif()
