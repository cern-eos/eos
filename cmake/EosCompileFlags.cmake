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
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(EOS_CXX_DEFINE "-DVERSION=\\\"${VERSION}\\\" -DRELEASE=\\\"${RELEASE}\\\"")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EOS_CXX_DEFINE} ${CPP_VERSION} -msse4.2 -Wall -Wno-error=parentheses")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EOS_CXX_DEFINE} ${CPP_VERSION} -Wall -Wno-error=parentheses")

check_cxx_compiler_flag(-std=c++17 HAVE_FLAG_STD_CXX17)
if(NOT HAVE_FLAG_STD_CXX17)
  message(FATAL_ERROR "A compiler with -std=c++17 support is required.")
endif()

#-------------------------------------------------------------------------------
# CPU architecture flags
#-------------------------------------------------------------------------------
set(CPU_ARCH_FLAGS "-msse4.2")
if (NO_SSE)
  message(NOTICE "SSE extensions not enabled")
  set(CPU_ARCH_FLAGS "-mcrc32")
endif()

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
# Clang requires linking with libatomic
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ATOMIC_LIBRARIES}")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${ATOMIC_LIBRARIES}")
endif()

#-------------------------------------------------------------------------------
# Sanitizer flags
#-------------------------------------------------------------------------------
if (ASAN)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
  check_cxx_compiler_flag(-fsanitize=address HAVE_FLAG_ASAN)
  unset(CMAKE_REQUIRED_FLAGS)

  if (NOT HAVE_FLAG_ASAN)
    message(FATAL_ERROR "A compiler with '-fsanitize=address' support is required.")
  endif()

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

if (TSAN)
  set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
  check_cxx_compiler_flag(-fsanitize=thread HAVE_FLAG_TSAN)
  unset(CMAKE_REQUIRED_FLAGS)

  if (NOT HAVE_FLAG_TSAN)
    message(FATAL_ERROR "A compiler with '-fsanitize=thread' support is required.")
  endif()

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
endif()
