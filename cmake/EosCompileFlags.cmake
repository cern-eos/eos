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
# Clang requires linking with libatomic
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ATOMIC_LIBRARIES}")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${ATOMIC_LIBRARIES}")
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

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
endif()


# from_chars will invoke __builtin_mul_overflow to check for promoting
# integer types
macro(try_compile_from_chars
        from_chars_link_flags
        from_chars_compile_result)
  set(_from_chars_compiler_args CXX_STANDARD 17)
  if (NOT from_chars_link_flags STREQUAL "")
    list(APPEND _from_chars_compiler_args
            LINK_LIBRARIES ${from_chars_link_flags})
  endif()
  try_compile(_from_chars_compile_result
          ${CMAKE_CURRENT_BINARY_DIR}
          SOURCES "${CMAKE_CURRENT_LIST_DIR}/fromchars.cpp"
          ${_from_chars_compiler_args})
  set(${from_chars_compile_result} ${_from_chars_compile_result})
  if (_from_chars_compile_result)
    message(STATUS "Compiler supports std::from_chars")
  else()
    message(STATUS "No compiler support with std::from_chars")
  endif()
endmacro()

# For clang with  fsantize=undefined will go for 4 word multiply calling _mulodi4 which
# is only implemented by compiler-rt, see https://bugs.llvm.org/show_bug.cgi?id=16404
# and https://bugs.llvm.org/show_bug.cgi?id=28629 we add these flags only for clang compiler
# Additionally clang minor versions of 7 may not be patched with https://bugzilla.redhat.com/show_bug.cgi?id=1657544
try_compile_from_chars("" from_chars_compiles)
if (NOT from_chars_compiles)
  if("${CMAKE_CXX_COMPILER_ID}" MATCHES Clang)
    message(STATUS "Trying new linker flags for from_chars")
    set(FROM_CHARS_LINKER_FLAGS "-rtlib=compiler-rt -lgcc_s")
    try_compile_from_chars("${FROM_CHARS_LINKER_FLAGS}" from_chars_compiles)
  endif()
  if (NOT from_chars_compiles)
    message(FATAL_ERROR "Cannot compile from_chars")
  else()
    message(STATUS "Adding linker flags for std::from_chars ${FROM_CHARS_LINKER_FLAGS}")
  endif()
endif()

