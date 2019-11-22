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

# This version must reflect the XRootD major version compiled against
set(PLUGIN_VERSION 4)

#-------------------------------------------------------------------------------
# Get UID/GID for an account
#-------------------------------------------------------------------------------
function(EOS_GetUidGid USERNAME UIDVARNAME GIDVARNAME)
    execute_process(
      COMMAND sh -c "id -u ${USERNAME}"
      OUTPUT_VARIABLE UID
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE RETC)

    execute_process(
      COMMAND sh -c "id -g ${USERNAME}"
      OUTPUT_VARIABLE GID
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE RETC)

  set(${UIDVARNAME} ${UID} PARENT_SCOPE)
  set(${GIDVARNAME} ${GID} PARENT_SCOPE)

  if(NOT ("${RETC}" STREQUAL "0") )
    message(FATAL_ERROR "Error calling uid, return code is ${RETC}")
  endif()
endfunction()

#-------------------------------------------------------------------------------
# Get version
#-------------------------------------------------------------------------------
function(EOS_GetVersion MAJOR MINOR PATCH RELEASE)
  if(("${MAJOR}" STREQUAL "") OR
     ("${MINOR}" STREQUAL "") OR
     ("${PATCH}" STREQUAL ""))
    execute_process(
      COMMAND ${CMAKE_SOURCE_DIR}/genversion.sh ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE VERSION_INFO
      OUTPUT_STRIP_TRAILING_WHITESPACE)

    string(REPLACE "." ";" VERSION_LIST ${VERSION_INFO})
    list(GET VERSION_LIST 0 MAJOR)
    list(GET VERSION_LIST 1 MINOR)
    list(GET VERSION_LIST 2 PATCH)

    # The patch could also contain the RELEASE value if this is a snapshot
    string(FIND "${PATCH}" "-" POS)

    if (NOT "${POS}" EQUAL "-1")
      string(REPLACE "-" ";" PR_LIST ${PATCH})
      list(GET PR_LIST 0 PATCH)

      # Set RELEASE on if not already set
      if ("${RELEASE}" STREQUAL "")
        list(GET PR_LIST 1 RELEASE)
      endif()
    endif()
  endif()

  set(VERSION_MAJOR ${MAJOR} PARENT_SCOPE)
  set(VERSION_MINOR ${MINOR} PARENT_SCOPE)
  set(VERSION_PATCH ${PATCH} PARENT_SCOPE)
  set(VERSION "${MAJOR}.${MINOR}.${PATCH}" PARENT_SCOPE)

  if("${RELEASE}" STREQUAL "")
    set(RELEASE "1")
  endif()

  set(RELEASE ${RELEASE} PARENT_SCOPE)
endfunction()

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
