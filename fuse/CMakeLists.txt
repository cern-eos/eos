# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ------------------------------------------------------------------------------

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

include_directories(
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb)  # for the generated protobuf

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

add_subdirectory(FuseCache)

#-------------------------------------------------------------------------------
# eosd executables
#-------------------------------------------------------------------------------
add_executable(eosd
  ProcCache.cc               ProcCache.hh
  main.cc eosfuse.cc         eosfuse.hh
  filesystem.cc              filesystem.hh
  FuseCacheEntry.cc          FuseCacheEntry.hh
  AuthIdManager.cc           xrdutils.cc)

target_link_libraries(eosd PRIVATE
  XROOTD::CL
  XROOTD::UTILS
  KRB5::KRB5
  FUSE::FUSE
  ${CMAKE_THREAD_LIBS_INIT})

if(MacOSX)
  target_link_libraries(eosd PRIVATE FuseCache)
else()
  target_link_libraries(eosd PRIVATE FuseCache-Static)
endif()


target_compile_definitions(eosd PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DVERSION=\"${VERSION}\")

install(TARGETS eosd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

install(FILES eosfusebind
  DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
              GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ )

#-------------------------------------------------------------------------------
# eosd3 if fuse3 is available
#-------------------------------------------------------------------------------
if(FUSE3_FOUND)
add_executable(
  eosd3
  ProcCache.cc ProcCache.hh
  main.cc eosfuse.cc eosfuse.hh
  filesystem.cc      filesystem.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh
  AuthIdManager.cc xrdutils.cc )

  target_link_libraries(eosd3 PRIVATE
    FUSE3::FUSE3
    XROOTD::CL
    XROOTD::UTILS
    KRB5::KRB5
    FuseCache-Static
    ${CMAKE_THREAD_LIBS_INIT})

  target_compile_definitions(eosd3 PRIVATE
    -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64
    -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DVERSION=\\\"${VERSION}\\\")

install(TARGETS eosd3
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif()
