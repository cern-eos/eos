# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
# ----------------------------------------------------------------------
# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2021 CERN/Switzerland                                  *
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
include(CheckCXXCompilerFlag)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
  # Bypass everything if NO_SSE is set
  set(AMD64_BUILD ON)

  if (NO_SSE)
    # Some old hardware does not have sse instructions support, allow switch-off.
    message(NOTICE "SSE extensions not enabled")
    CHECK_CXX_COMPILER_FLAG(-mcrc32 HAVE_CRC32)
    if (HAVE_CRC32)
      set(CPU_ARCH_FLAGS "-mcrc32")
    endif() # HAVE_CRC32

  else()
    #find cpu features
    CHECK_CXX_COMPILER_FLAG(-msse4.2 HAVE_SSE42)
    if (HAVE_SSE42)
      set(CPU_ARCH_FLAGS "-msse4.2")
    endif() # HAVE_SSE42
    CHECK_CXX_COMPILER_FLAG(-mavx512f HAVE_AVX512F)
    CHECK_CXX_COMPILER_FLAG(-mavx512vl HAVE_AVX512L)
    if(HAVE_AVX512F AND HAVE_AVX512L)
      set(HAVE_AVX512 1)
    endif()
    CHECK_CXX_COMPILER_FLAG(-mavx2 HAVE_AVX2)
  endif() # NO_SSE

elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
  set(ARM64_BUILD ON)
  CHECK_CXX_COMPILER_FLAG(-march=armv8-a+crc+crypto HAVE_ARMV8_CRC_CRYPTO)
  CHECK_CXX_COMPILER_FLAG(-march=armv8-a+crc HAVE_ARMV8_CRC)

  if (HAVE_ARMV8_CRC_CRYPTO)
    set(CPU_ARCH_FLAGS "-march=armv8-a+crc+crypto")
  elseif (HAVE_ARMV8_CRC)
    set(CPU_ARCH_FLAGS "-march=armv8-a+crc")
  endif() # CRC/CRYPTO
  CHECK_CXX_COMPILER_FLAG(-mfpu=neon HAVE_ARM_NEON)

else()
  message(WARNING "Could not determine platform. No cpu accel. will be used ")
endif() # SYSTEM_PROCESSOR
