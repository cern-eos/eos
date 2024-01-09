# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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
# Print Configuration
#-------------------------------------------------------------------------------
message( STATUS "_________________________________________________" )
message( STATUS "Version       : eos-" ${VERSION} "-" ${RELEASE} )
if (CLIENT)
  message( STATUS "Modules       : client" )
else ()
  message( STATUS "Modules       : client + server" )
endif ()

set(FUSE_FLOCK_STATUS "FUSE_NO_FLOCK_SUPPORT")
# We pass -DFUSE_MOUNT_VERSION0 hence the pragma for 290 in llfusexx
if (FUSE3_FOUND OR
    ("${FUSE_MOUNT_VERSION}" STREQUAL "29"))
  set(FUSE_FLOCK_STATUS "FUSE_SUPPORTS_FLOCK")
endif()

message(STATUS "................................................." )
message(STATUS "prefix        : " ${CMAKE_INSTALL_PREFIX} )
message(STATUS "bin dir       : " ${CMAKE_INSTALL_FULL_BINDIR} )
message(STATUS "sbin dir      : " ${CMAKE_INSTALL_SBINDIR} )
message(STATUS "lib dir       : " ${CMAKE_INSTALL_FULL_LIBDIR} )
message(STATUS "sysconfig dir : " ${CMAKE_INSTALL_SYSCONFDIR} )
message(STATUS "................................................." )
message(STATUS "fuse2-build   : ${FUSE_FOUND}")
message(STATUS "fuse3-build   : ${FUSE3_FOUND}")
message(STATUS "fuse-mount-ver: ${FUSE_MOUNT_VERSION}")
message(STATUS "fuse-flock    : ${FUSE_FLOCK_STATUS}")
message(STATUS "grpc-build    : ${GRPC_FOUND}")
message(STATUS "isa-l_crypto  : ${ISAL_CRYPTO_FOUND}")
message(STATUS "isa-l         : ${ISAL_FOUND}")
message(STATUS "xxhash        : ${XXHASH_FOUND}")
message(STATUS "davix         : ${DAVIX_FOUND}")
message( STATUS "................................................." )
message( STATUS "C Compiler    : " ${CMAKE_C_COMPILER} )
message( STATUS "C++ Compiler  : " ${CMAKE_CXX_COMPILER} )
message( STATUS "Protobuf      : EXE " ${PROTOBUF3_PROTOC_EXECUTABLE} " INC " ${PROTOBUF3_INCLUDE_DIR} " LIB " ${PROTOBUF3_LIBRARY} )
message( STATUS "Build type    : " ${CMAKE_BUILD_TYPE} )
message( STATUS "Code coverage : ${COVERAGE}")
message( STATUS "_________________________________________________" )

unset(FUSE_FLOCK_STATUS)
