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
  ${OPENSSL_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Add CppUnit tests if possible
#-------------------------------------------------------------------------------
if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif()

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
  XrdMqClient PUBLIC
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

target_compile_definitions(
  XrdMqClient PUBLIC -DHAVE_ATOMICS=1)

set_target_properties(
  XrdMqClient PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

add_library(XrdMqClient-Static STATIC ${XRDMQCLIENT_SRCS})

target_link_libraries(
  XrdMqClient-Static PRIVATE
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

set_target_properties(
  XrdMqClient-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

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
  XrdMqOfs PRIVATE
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

#-------------------------------------------------------------------------------
# Other executables
#-------------------------------------------------------------------------------
add_executable(xrdmqclienttest   XrdMqClientTest.cc  XrdMqSharedObject.cc)
add_executable(xrdmqdumper       XrdMqQueueDumper.cc XrdMqSharedObject.cc)
add_executable(xrdmqfeeder       XrdMqQueueFeeder.cc  XrdMqSharedObject.cc)
add_executable(xrdmqinjection    XrdMqQueueInjection.cc  XrdMqSharedObject.cc)
add_executable(xrdmqclientmaster XrdMqClientMaster.cc  XrdMqSharedObject.cc)
add_executable(xrdmqclientworker XrdMqClientWorker.cc XrdMqSharedObject.cc)
add_executable(xrdmqcryptotest   XrdMqCryptoTest.cc  XrdMqSharedObject.cc)
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

target_link_libraries(xrdmqclienttest PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqdumper PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqfeeder PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqinjection PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqclientmaster PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqclientworker PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqcryptotest PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqsharedobjectclient PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(xrdmqsharedobjectqueueclient PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})
target_link_libraries(
  xrdmqsharedobjectbroadcastclient
  PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})

install(
  TARGETS XrdMqClient XrdMqOfs xrdmqdumper
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
