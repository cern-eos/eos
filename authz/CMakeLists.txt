include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (XrdEosAuth MODULE
	    XrdCapability.cc
	    XrdCapability.hh
	    )

add_library (eosCapability-Static STATIC
	    XrdCapability.cc
	    XrdCapability.hh
	    )

target_link_libraries (XrdEosAuth XrdUtils eosCommon)

target_link_libraries (eosCapability-Static)

if (Linux)
  set_target_properties (eosCapability-Static XrdEosAuth PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
    COMPILE_FLAGS -fPIC
  )
endif(Linux)

install ( TARGETS XrdEosAuth
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
