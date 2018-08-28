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
  ${Z_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}
  ${CURL_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# XrdMqClient-Objects
#-------------------------------------------------------------------------------
add_library(XrdMqClient-Objects OBJECT
  XrdMqTiming.hh
  XrdMqClient.cc        XrdMqClient.hh
  XrdMqMessage.cc       XrdMqMessage.hh
  XrdMqMessaging.cc     XrdMqMessaging.hh
  XrdMqSharedObject.cc  XrdMqSharedObject.hh
  ${CMAKE_SOURCE_DIR}/common/RWMutex.cc
  ${CMAKE_SOURCE_DIR}/common/SharedMutex.cc
  ${CMAKE_SOURCE_DIR}/common/PthreadRWMutex.cc
  ${CMAKE_SOURCE_DIR}/common/StacktraceHere.cc
  ${CMAKE_SOURCE_DIR}/common/Logging.cc
  ${CMAKE_SOURCE_DIR}/common/StringConversion.cc
  ${CMAKE_SOURCE_DIR}/mgm/TableFormatter/TableCell.cc)

set_target_properties(XrdMqClient-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(XrdMqClient SHARED
  $<TARGET_OBJECTS:XrdMqClient-Objects>)

target_include_directories(XrdMqClient PUBLIC ${CURL_INCLUDE_DIRS})

target_link_libraries(
  XrdMqClient PUBLIC
  fmt
  ${Z_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${UUID_LIBRARIES}
  ${CURL_LIBRARIES}
  ${LIBBFD_LIBRARIES}
  ${NCURSES_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

set_target_properties(
  XrdMqClient PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

add_library(XrdMqClient-Static STATIC
  $<TARGET_OBJECTS:XrdMqClient-Objects>)

target_link_libraries(
  XrdMqClient-Static PRIVATE
  fmt
  ${Z_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARIES}
  ${LIBBFD_LIBRARIES}
  ${CURL_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

set_target_properties(
  XrdMqClient-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# XrdMqOfs library
#-------------------------------------------------------------------------------
add_library(XrdMqOfs MODULE
  XrdMqOfsFSctl.cc
  XrdMqOfs.cc       XrdMqOfs.hh
  XrdMqMessage.cc   XrdMqMessage.hh)

target_link_libraries(
  XrdMqOfs PRIVATE
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

#-------------------------------------------------------------------------------
# Other executables
#-------------------------------------------------------------------------------
add_executable(xrdmqclienttest   tests/XrdMqClientTest.cc)
add_executable(xrdmqdumper       tests/XrdMqQueueDumper.cc)
add_executable(xrdmqfeeder       tests/XrdMqQueueFeeder.cc)
add_executable(xrdmqinjection    tests/XrdMqQueueInjection.cc)
add_executable(xrdmqclientmaster tests/XrdMqClientMaster.cc)
add_executable(xrdmqclientworker tests/XrdMqClientWorker.cc)
add_executable(xrdmqcryptotest   tests/XrdMqCryptoTest.cc)
add_executable(xrdmqsharedobjectclient          tests/XrdMqSharedObjectClient.cc)
add_executable(xrdmqsharedobjectqueueclient     tests/XrdMqSharedObjectQueueClient.cc)
add_executable(xrdmqsharedobjectbroadcastclient tests/XrdMqSharedObjectBroadCastClient.cc)

#-------------------------------------------------------------------------------
# Libraries that all the above executables are linked against
#-------------------------------------------------------------------------------
set(
  XRDMQ_OTHER_LINK_LIBRARIES
  XrdMqClient-Static
  ${GLIBC_DL_LIBRARY}
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
target_link_libraries(xrdmqsharedobjectbroadcastclient PRIVATE ${XRDMQ_OTHER_LINK_LIBRARIES})

install(
  TARGETS XrdMqClient XrdMqOfs xrdmqdumper
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
