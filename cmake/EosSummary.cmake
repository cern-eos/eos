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
  if (LEVELDB_FOUND)
    message( STATUS "Modules       : client + server with LevelDb" )
  else ()
    message( STATUS "Modules       : client + server without LevelDb" )
endif ()
endif ()

message( STATUS "................................................." )
message( STATUS "prefix        : " ${CMAKE_INSTALL_PREFIX} )
message( STATUS "bin dir       : " ${CMAKE_INSTALL_FULL_BINDIR} )
message( STATUS "sbin dir      : " ${CMAKE_INSTALL_SBINDIR} )
message( STATUS "lib dir       : " ${CMAKE_INSTALL_FULL_LIBDIR} )
message( STATUS "sysconfig dir : " ${CMAKE_INSTALL_SYSCONFDIR} )
message( STATUS "................................................." )
if (FUSE_FOUND)
message( STATUS "fuse2-build   : true")
endif ()
if (FUSE3_FOUND)
message( STATUS "fuse3-build   : true")
endif ()
IF (GRPC_FOUND)
message( STATUS "grpc-build    : true")
endif ()
IF (ISALCRYPTO_FOUND)
message( STATUS "isa-l_crypto  : true")
endif ()
IF (ISAL_FOUND)
message( STATUS "isa-l         : true")
endif ()
IF (XXHASH_FOUND)
message( STATUS "xxhash        : true")
endif ()
message( STATUS "................................................." )
message( STATUS "C Compiler    : " ${CMAKE_C_COMPILER} )
message( STATUS "C++ Compiler  : " ${CMAKE_CXX_COMPILER} )
message( STATUS "Protobuf      : EXE " ${PROTOBUF_PROTOC_EXECUTABLE} " INC " ${PROTOBUF_INCLUDE_DIRS} " LIB " ${PROTOBUF_LIBRARIES} )
message( STATUS "Build type    : " ${CMAKE_BUILD_TYPE} )
if (COVERAGE)
message( STATUS "Code coverage : true")
endif()
message( STATUS "_________________________________________________" )
