include_directories( ../ ${XROOTD_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_executable ( eosfilesync
                 eosfilesync.cc
)

add_executable ( eosdirsync
                 eosdirsync.cc
)

add_executable ( eosha
                 eosha.cc
)

INSTALL_PROGRAMS( /sbin FILES eoshapl )

target_link_libraries ( eosdirsync eosCommon-Static XrdClient XrdUtils )
target_link_libraries ( eosfilesync eosCommon-Static XrdClient XrdUtils )

install ( TARGETS eosha eosfilesync eosdirsync
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION sbin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
