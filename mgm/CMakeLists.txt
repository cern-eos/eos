include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (XrdEosMgm MODULE
            Access.cc         
	    ConfigEngine.cc
	    Messaging.cc
	    Policy.cc  
	    ProcInterface.cc
	    Quota.cc
	    Vid.cc
	    FsView.cc
	    XrdMgmOfsConfigure.cc
	    XrdMgmOfs.cc
            DrainJob.cc
            FileSystem.cc
	    )

target_link_libraries (XrdEosMgm XrdClient XrdSys XrdOuc XrdNet XrdNetUtil XrdOfs eosCommon-Static eosNamespace-Static eosCapability-Static XrdMqClient-Static crypto uuid curses pthread dl z)

add_executable ( testmgmview
	         FsView.cc
	         test/MgmViewTest.cc
)

target_link_libraries ( testmgmview XrdClient XrdSys XrdOuc XrdNet XrdNetUtil eosCommon-Static XrdMqClient-Static crypto uuid curses pthread dl z)

set_target_properties ( testmgmview PROPERTIES COMPILE_FLAGS "-DEOSMGMFSVIEWTEST" )

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
