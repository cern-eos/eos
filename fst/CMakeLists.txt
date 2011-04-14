include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (XrdEosFst MODULE
	    layout/PlainLayout.cc
	    layout/Raid5Layout.cc
	    layout/ReplicaLayout.cc
	    layout/ReplicaParLayout.cc
	    checksum/CheckSum.cc
	    transfer/Transfer.cc
	    storage/Storage.cc
	    Config.cc
	    Load.cc
	    Messaging.cc
	    XrdFstOfs.cc
            ../checksum/crc32c.cc
            ../checksum/crc32ctables.cc
	    )

target_link_libraries (XrdEosFst eosCapability-Static eosCommon-Static XrdMqClient-Static XrdClient XrdSys XrdOuc XrdNet XrdNetUtil XrdOfs XrdOss crypto uuid curses pthread dl z)

add_executable ( FstLoad
                 Load.cc
                 tools/FstLoad.cc
)

target_link_libraries ( FstLoad XrdSys XrdOuc XrdNetUtil dl )

if (Linux)
  set_target_properties (XrdEosFst PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)

#INSTALL_PROGRAMS( sbin tools/eosfstregister tools/eosfstcheck tools/eosfstclean tools/eosfstmgmsync)
INSTALL_PROGRAMS( /sbin FILES tools/eosfstregister tools/eosfstcheck tools/eosfstclean tools/eosfstmgmsync)

install ( TARGETS XrdEosFst
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
