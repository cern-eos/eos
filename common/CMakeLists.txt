include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (eosCommon SHARED 
	    ClientLock.cc
	    FileSystem.cc
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Proc.cc
	    Statfs.cc
	    StringStore.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            TransferQueue.cc
            TransferJob.cc
            Report.cc
	    )

add_library (eosCommon-Static STATIC
	    ClientLock.cc
	    FileSystem.cc
	    Fmd.cc
	    Logging.cc
	    Mapping.cc
	    Proc.cc
	    Statfs.cc
	    StringStore.cc
	    SymKeys.cc
	    GlobalConfig.cc
            Attr.cc
            TransferQueue.cc
            TransferJob.cc
            Report.cc
)


target_link_libraries (eosCommon XrdMqClient XrdSys XrdNetUtil XrdOuc XrdClient crypto uuid curses z attr)

target_link_libraries (eosCommon-Static )
#depends on XrdMqClient-Static libcrypto.a libuuid.a libcurses.a pthread dl z)

if (Linux) 
  set_target_properties (eosCommon PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
  set_target_properties (eosCommon-Static PROPERTIES COMPILE_FLAGS -fPIC )
endif(Linux)

install ( TARGETS eosCommon 
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
