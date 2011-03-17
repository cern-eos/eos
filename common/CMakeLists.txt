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
	    ClientAdmin.hh
	    Exception.hh
	    FileId.hh
	    FileSystem.hh
	    Fmd.hh
	    LayoutId.hh
	    Logging.hh
	    Mapping.hh
	    Path.hh
	    Proc.hh
	    RWMutex.hh
	    Statfs.hh
	    StringConversion.hh
	    StringStore.hh
	    SymKeys.hh
	    Timing.hh
	    ClientLock.hh
	    Namespace.hh
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
	    ClientAdmin.hh
	    Exception.hh
	    FileId.hh
	    FileSystem.hh
	    Fmd.hh
	    LayoutId.hh
	    Logging.hh
	    Mapping.hh
	    Path.hh
	    Proc.hh
	    RWMutex.hh
	    Statfs.hh
	    StringConversion.hh
	    StringStore.hh
	    SymKeys.hh
	    Timing.hh
	    ClientLock.hh
	    Namespace.hh
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
