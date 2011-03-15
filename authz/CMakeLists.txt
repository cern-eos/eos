include_directories( ../ /usr/include/xrootd/ /usr/include/ )

add_library (XrdEosAuth MODULE
	    XrdCapability.cc
	    XrdCapability.hh
	    )

add_library (eosCapability SHARED
	    XrdCapability.cc
	    XrdCapability.hh
	    )

target_link_libraries (XrdEosAuth XrdSys XrdOuc XrdNet eosCommon)

target_link_libraries (eosCapability XrdSys XrdOuc XrdNet eosCommon)

if (Linux)
  set_target_properties (eosCapability XrdEosAuth PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)

install ( TARGETS eosCapability XrdEosAuth
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
