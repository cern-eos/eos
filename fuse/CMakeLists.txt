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
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR} )

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

add_subdirectory(FuseCache)

#-------------------------------------------------------------------------------
# eosd executables
#-------------------------------------------------------------------------------
add_executable(
  eosd
  ProcCache.cc ProcCache.hh
  main.cc eosfuse.cc eosfuse.hh
  filesystem.cc      filesystem.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh
  AuthIdManager.cc xrdutils.cc )

if(MacOSX)
  target_link_libraries(
    eosd
    FuseCache
    EosFstIo
    ${UUID_LIBRARIES}
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UNTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY} )

  set_target_properties(
    eosd
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"" )
else()

  target_link_libraries(
    eosd
    FuseCache-Static
    EosFstIo-Static
    ${FUSE_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY})

  set_target_properties(
    eosd 
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"")
endif()

install(
  TARGETS eosd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})


install(
  FILES eosfusebind
  DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
	      GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ )


if (FUSE3_FOUND)
add_executable(
  eosd3
  ProcCache.cc ProcCache.hh
  main.cc eosfuse.cc eosfuse.hh
  filesystem.cc      filesystem.hh
  FuseCacheEntry.cc  FuseCacheEntry.hh
  AuthIdManager.cc xrdutils.cc )

  target_link_libraries(
    eosd3
    FuseCache-Static
    EosFstIo-Static
    ${FUSE3_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY})

  set_target_properties(
    eosd3 
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FUSE3 -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"")

install(
  TARGETS eosd3
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

endif()
