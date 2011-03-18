include_directories( ../ /usr/include/xrootd/ /usr/include/ )

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
)


target_link_libraries (eosCommon XrdMqClient XrdSys XrdNetUtil XrdOuc XrdClient crypto uuid curses z)

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
