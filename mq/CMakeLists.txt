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

include_directories( ../ ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd/ ${CMAKE_BINARY_DIR}/xrootd/src/include/ ${XROOTD_INCLUDE_DIR} )
link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/ ${XROOTD_LIB_DIR})

add_library (XrdMqClient SHARED 
	     XrdMqClient.hh
	     XrdMqMessage.hh
	     XrdMqMessaging.hh
	     XrdMqOfs.hh
	     XrdMqRWMutex.hh
	     XrdMqSharedObject.hh
	     XrdMqStringConversion.hh
	     XrdMqTiming.hh
	     XrdMqClient.cc
	     XrdMqMessage.cc
	     XrdMqMessaging.cc
	     XrdMqSharedObject.cc )

add_library (XrdMqClient-Static STATIC
	     XrdMqClient.hh
	     XrdMqMessage.hh
	     XrdMqMessaging.hh
	     XrdMqOfs.hh
	     XrdMqRWMutex.hh
	     XrdMqSharedObject.hh
	     XrdMqStringConversion.hh
	     XrdMqTiming.hh
	     XrdMqClient.cc
	     XrdMqMessage.cc
	     XrdMqMessaging.cc
	     XrdMqSharedObject.cc )

add_library (XrdMqOfs MODULE
	    XrdMqOfs.cc 
	    XrdMqOfsFSctl.cc 
	    XrdMqOfs.hh 
	    XrdMqMessage.cc 
	    XrdMqMessage.hh )

add_library (XrdMqOfs-Static STATIC
	    XrdMqOfs.cc 
	    XrdMqOfsFSctl.cc 
	    XrdMqOfs.hh 
	    XrdMqMessage.cc 
	    XrdMqMessage.hh )


target_link_libraries (XrdMqClient XrdUtils  XrdCl crypto ${UUID_LIBRARIES} curses)
target_link_libraries (XrdMqOfs    XrdUtils  XrdCl XrdOfs crypto ${UUID_LIBRARIES} curses)

target_link_libraries (XrdMqClient-Static )
#depends on XrdCl XrdSys XrdOuc  XrdOuc libcrypto.a libuuid.a libcurses.a pthread dl )


add_executable ( xrdmqclienttest XrdMqClientTest.cc  )
add_executable ( xrdmqdumper     XrdMqQueueDumper.cc )
add_executable ( xrdmqfeeder     XrdMqQueueFeeder.cc )
add_executable ( xrdmqclientmaster XrdMqClientMaster.cc )
add_executable ( xrdmqclientworker XrdMqClientWorker.cc )
add_executable ( xrdmqcryptotest   XrdMqCryptoTest.cc   )
add_executable ( xrdmqsharedobjectclient XrdMqSharedObjectClient.cc )
add_executable ( xrdmqsharedobjectqueueclient     XrdMqSharedObjectQueueClient.cc     )
add_executable ( xrdmqsharedobjectbroadcastclient XrdMqSharedObjectBroadCastClient.cc )

target_link_libraries ( xrdmqclienttest XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqdumper     XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqfeeder     XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqclientmaster XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqclientworker XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqcryptotest   XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectclient XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectqueueclient XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectbroadcastclient XrdMqClient-Static XrdCl XrdUtils  libcrypto.a ${UUID_LIBRARIES} libcurses.a libz.a pthread dl)


if (Linux)
   set_target_properties (XrdMqClient XrdMqOfs PROPERTIES
     VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
     SOVERSION ${VERSION_MAJOR}
     CLEAN_DIRECT_OUTPUT 1
    )
   set_target_properties (XrdMqClient-Static PROPERTIES COMPILE_FLAGS -fPIC) 
endif(Linux)

install ( TARGETS XrdMqClient XrdMqOfs xrdmqdumper
          LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
)
