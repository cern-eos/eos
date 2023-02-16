if( MacOSX )
  install(DIRECTORY DESTINATION /usr/local/lib/tmpfiles.d/)
  install(
    FILES eos-fusex-core.conf
    DESTINATION /usr/local/lib/tmpfiles.d/)
else()
  install(DIRECTORY DESTINATION /usr/lib/tmpfiles.d/)
  install(
      FILES eos-fusex-core.conf
      DESTINATION /usr/lib/tmpfiles.d/)
endif()
