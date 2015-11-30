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

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# XrdMqClient library
#-------------------------------------------------------------------------------
set(
  XRDMQCLIENT_SRCS
  XrdMqOfs.hh
  XrdMqRWMutex.hh
  XrdMqTiming.hh
  XrdMqStringConversion.hh
  XrdMqClient.cc        XrdMqClient.hh
  XrdMqMessage.cc       XrdMqMessage.hh
  XrdMqMessaging.cc     XrdMqMessaging.hh
  XrdMqSharedObject.cc  XrdMqSharedObject.hh
  ${CMAKE_SOURCE_DIR}/common/Logging.cc)

add_library(XrdMqClient SHARED ${XRDMQCLIENT_SRCS})

target_link_libraries(
  XrdMqClient
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

#-------------------------------------------------------------------------------
# XrdMqOfs library
#-------------------------------------------------------------------------------
set(
  XRDMQOFS_SRCS
  XrdMqOfsFSctl.cc
  XrdMqOfs.cc       XrdMqOfs.hh
  XrdMqMessage.cc   XrdMqMessage.hh)

add_library(XrdMqOfs MODULE ${XRDMQOFS_SRCS})

target_link_libraries(
  XrdMqOfs
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

set_target_properties(
  XrdMqClient
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES ""
  MACOSX_RPATH TRUE)

if(Linux)
  add_library(XrdMqClient-Static STATIC ${XRDMQCLIENT_SRCS})
  add_library(XrdMqOfs-Static STATIC ${XRDMQOFS_SRCS})

  set_target_properties(
    XrdMqClient-Static
    XrdMqOfs-Static
    PROPERTIES COMPILE_FLAGS -fPIC)
endif()

#-------------------------------------------------------------------------------
# Other executables
#-------------------------------------------------------------------------------
add_executable(xrdmqclienttest   XrdMqClientTest.cc)
add_executable(xrdmqdumper       XrdMqQueueDumper.cc)
add_executable(xrdmqfeeder       XrdMqQueueFeeder.cc)
add_executable(xrdmqinjection    XrdMqQueueInjection.cc)
add_executable(xrdmqclientmaster XrdMqClientMaster.cc)
add_executable(xrdmqclientworker XrdMqClientWorker.cc)
add_executable(xrdmqcryptotest   XrdMqCryptoTest.cc)
add_executable(xrdmqsharedobjectclient          XrdMqSharedObjectClient.cc)
add_executable(xrdmqsharedobjectqueueclient     XrdMqSharedObjectQueueClient.cc)
add_executable(xrdmqsharedobjectbroadcastclient XrdMqSharedObjectBroadCastClient.cc)

#-------------------------------------------------------------------------------
# Libraries that all the above executables are linked against
#-------------------------------------------------------------------------------
set(
  XRDMQ_OTHER_LINK_LIBRARIES
  XrdMqClient
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(xrdmqclienttest   ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqdumper       ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqfeeder       ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqinjection    ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqclientmaster ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqclientworker ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqcryptotest   ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqsharedobjectclient          ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqsharedobjectqueueclient     ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqsharedobjectbroadcastclient ${XRDMQ_OTHER_LINK_LIBRARIES})

install(
  TARGETS XrdMqClient XrdMqOfs xrdmqdumper
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
