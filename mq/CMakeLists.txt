include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

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


target_link_libraries (XrdMqClient XrdUtils  XrdClient crypto uuid curses)
target_link_libraries (XrdMqOfs    XrdUtils  XrdClient XrdOfs crypto uuid curses)

target_link_libraries (XrdMqClient-Static )
#depends on XrdClient XrdSys XrdOuc  XrdOuc libcrypto.a libuuid.a libcurses.a pthread dl )


add_executable ( xrdmqclienttest XrdMqClientTest.cc  )
add_executable ( xrdmqdumper     XrdMqQueueDumper.cc )
add_executable ( xrdmqfeeder     XrdMqQueueFeeder.cc )
add_executable ( xrdmqclientmaster XrdMqClientMaster.cc )
add_executable ( xrdmqclientworker XrdMqClientWorker.cc )
add_executable ( xrdmqcryptotest   XrdMqCryptoTest.cc   )
add_executable ( xrdmqsharedobjectclient XrdMqSharedObjectClient.cc )
add_executable ( xrdmqsharedobjectqueueclient     XrdMqSharedObjectQueueClient.cc     )
add_executable ( xrdmqsharedobjectbroadcastclient XrdMqSharedObjectBroadCastClient.cc )

target_link_libraries ( xrdmqclienttest XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqdumper     XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqfeeder     XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqclientmaster XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqclientworker XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqcryptotest   XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectclient XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectqueueclient XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)
target_link_libraries ( xrdmqsharedobjectbroadcastclient XrdMqClient-Static XrdClient XrdUtils  libcrypto.a libuuid.a libcurses.a libz.a pthread dl)


if (Linux)
   set_target_properties (XrdMqClient XrdMqOfs PROPERTIES
     VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
     SOVERSION ${VERSION_MAJOR}
     CLEAN_DIRECT_OUTPUT 1
    )
   set_target_properties (XrdMqClient-Static PROPERTIES COMPILE_FLAGS -fPIC) 
endif(Linux)

install ( TARGETS XrdMqClient XrdMqOfs xrdmqdumper
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
