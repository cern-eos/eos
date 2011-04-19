include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_executable ( eosfsd
                 eosfs.c
                 xrdposix.cc
)

add_executable ( eosd
                 eosd.c
                 xrdposix.cc
)

set_target_properties ( eosfsd PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)
#target_link_libraries ( eosfsd libXrdPosix.a libXrdClient.a libXrdOuc.a libXrdSys.a libXrdNetUtil.a libXrdNet.a libXrdOuc.a fuse pthread )
target_link_libraries ( eosfsd XrdPosix XrdClient XrdOuc XrdSys XrdNetUtil XrdNet XrdOuc fuse pthread )

set_target_properties ( eosd PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)
#target_link_libraries ( eosd libXrdPosix.a libXrdClient.a libXrdOuc.a libXrdSys.a libXrdNetUtil.a libXrdNet.a libXrdOuc.a fuse pthread )
target_link_libraries ( eosd XrdPosix XrdClient XrdOuc XrdSys XrdNetUtil XrdNet XrdOuc fuse pthread )

INSTALL_PROGRAMS( /sbin FILES eosd )

install ( TARGETS eosfsd
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
