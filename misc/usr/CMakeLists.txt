find_program(SYSTEMD_TMPFILES NAMES systemd-tmpfiles)

if(NOT SYSTEMD_TMPFILES)
  return()
endif()

install(FILES eosd.conf eos-fusex-core.conf DESTINATION lib/tmpfiles.d)
