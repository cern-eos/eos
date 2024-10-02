# ----------------------------------------------------------------------
# File: EosCoverage.cmake
# Author: Mihai Patrascoiu - CERN
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
# Code coverage compiler flags and definitions
#-------------------------------------------------------------------------------

include(CheckCXXCompilerFlag)

list(APPEND CMAKE_REQUIRED_FLAGS --coverage)
list(APPEND CMAKE_REQUIRED_LINK_OPTIONS --coverage)
check_cxx_compiler_flag(--coverage COVERAGE_SUPPORTED)

if (COVERAGE_SUPPORTED)
  add_compile_definitions(COVERAGE_BUILD)
  add_compile_options(--coverage)
  add_link_options(--coverage)
else()
  message(FATAL_ERROR "Could not enable coverage. A compiler with '--coverage' support is required.")
endif()

# This is needed for correct results in multithreaded applications
list(APPEND CMAKE_REQUIRED_FLAGS -fprofile-update=atomic)
check_cxx_compiler_flag(-fprofile-update=atomic COVERAGE_UPDATE_ATOMIC)

if (COVERAGE_UPDATE_ATOMIC)
  add_compile_options(-fprofile-update=atomic)
else()
  message(WARNING "Could not enable atomic coverage updates, expect unreliable results.")
endif()

#-------------------------------------------------------------------------------
# Code coverage targets
#-------------------------------------------------------------------------------

add_custom_target(
  raw-code-trace
  COMMAND lcov --capture
    --base-directory ${CMAKE_SOURCE_DIR}
    --directory ${CMAKE_BINARY_DIR}
    --no-external
    --config-file ${CMAKE_SOURCE_DIR}/coverage/eoslcov.rc
    --output-file ${CMAKE_BINARY_DIR}/raw-trace.info
)

add_custom_target(
  filtered-trace-server
  COMMAND lcov --remove ${CMAKE_BINARY_DIR}/raw-trace.info
      "${CMAKE_BINARY_DIR}/\\*"
      "${CMAKE_SOURCE_DIR}/common/backward-cpp/\\*"
      "${CMAKE_SOURCE_DIR}/common/crc32c/\\*"
      "${CMAKE_SOURCE_DIR}/common/eos_cta_pb/\\*"
      "${CMAKE_SOURCE_DIR}/common/fmt/\\*"
      "${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/\\*"
      "${CMAKE_SOURCE_DIR}/console/\\*"
      "${CMAKE_SOURCE_DIR}/fst/tests/\\*"
      "${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/\\*"
      "${CMAKE_SOURCE_DIR}/test/\\*"
      "${CMAKE_SOURCE_DIR}/unit_tests/\\*"
    --config-file ${CMAKE_SOURCE_DIR}/coverage/eoslcov.rc
    --output-file ${CMAKE_BINARY_DIR}/filtered-trace-server.info
  DEPENDS raw-code-trace
)

add_custom_target(
  filtered-trace-client
  COMMAND lcov --extract ${CMAKE_BINARY_DIR}/raw-trace.info
      "${CMAKE_SOURCE_DIR}/console/\\*"
    --config-file ${CMAKE_SOURCE_DIR}/coverage/eoslcov.rc
    --output-file ${CMAKE_BINARY_DIR}/filtered-trace-client.info
  DEPENDS raw-code-trace
)

add_custom_target(
  coverage-server
  COMMAND genhtml ${CMAKE_BINARY_DIR}/filtered-trace-server.info
    --config-file ${CMAKE_SOURCE_DIR}/coverage/eoslcov.rc
    --output-directory ${CMAKE_BINARY_DIR}/coverage-report/server
  DEPENDS filtered-trace-server
)

add_custom_target(
  coverage-client
  COMMAND genhtml ${CMAKE_BINARY_DIR}/filtered-trace-client.info
    --config-file ${CMAKE_SOURCE_DIR}/coverage/eoslcov.rc
    --output-directory ${CMAKE_BINARY_DIR}/coverage-report/client
  DEPENDS filtered-trace-client
)

add_custom_target(
  coverage-report
  DEPENDS coverage-server coverage-client
)

if (COV_CROSS_PROFILE)
  install(
    DIRECTORY ${CMAKE_BINARY_DIR}
    DESTINATION "/var/eos/coverage"
    FILES_MATCHING PATTERN "*.gcno"
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )
endif()

