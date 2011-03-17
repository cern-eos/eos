include_directories( ../ /usr/include/xrootd/ /usr/include/ )

add_library (XrdEosMgm MODULE
	    ConfigEngine.cc
	    FstNode.cc
	    Messaging.cc
	    Policy.cc  
	    ProcInterface.cc
	    Quota.cc
	    Vid.cc
	    XrdMgmOfsConfigure.cc
	    XrdMgmOfs.cc
	    )

target_link_libraries (XrdEosMgm XrdClient XrdSys XrdOuc XrdNet XrdNetUtil XrdOfs eosCommon-Static eosNamespace-Static eosCapability-Static XrdMqClient-Static crypto uuid curses pthread dl z)

if (Linux)
  set_target_properties (XrdEosMgm PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)

install ( TARGETS XrdEosMgm
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
