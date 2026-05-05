# ----------------------------------------------------------------------
# EOS TUI packaging helpers
# ----------------------------------------------------------------------

set(EOS_TUI_LICENSE_URL "https://raw.githubusercontent.com/cern-eos/eos-tui/v${EOS_TUI_VERSION}/LICENSE")
set(EOS_TUI_README_URL "https://raw.githubusercontent.com/cern-eos/eos-tui/v${EOS_TUI_VERSION}/README.md")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
  set(EOS_TUI_BINARY_NAME "eos-tui_v${EOS_TUI_VERSION}_linux_amd64")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
  set(EOS_TUI_BINARY_NAME "eos-tui_v${EOS_TUI_VERSION}_linux_arm64")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm64|aarch64)$")
  set(EOS_TUI_BINARY_NAME "eos-tui_v${EOS_TUI_VERSION}_macos_arm64")
endif()

if(EOS_TUI_BINARY_NAME)
  set(EOS_TUI_BINARY_URL "https://github.com/cern-eos/eos-tui/releases/download/v${EOS_TUI_VERSION}/${EOS_TUI_BINARY_NAME}")
endif()

set(EOS_TUI_INSTALL_STAGING_DIR "${CMAKE_BINARY_DIR}/eos-tui/v${EOS_TUI_VERSION}")
set(EOS_TUI_LICENSE_STAGED "${EOS_TUI_INSTALL_STAGING_DIR}/LICENSE")
set(EOS_TUI_README_STAGED "${EOS_TUI_INSTALL_STAGING_DIR}/README.md")

if(EOS_TUI_BINARY_NAME)
  set(EOS_TUI_BINARY_STAGED "${EOS_TUI_INSTALL_STAGING_DIR}/${EOS_TUI_BINARY_NAME}")
  configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/EosTuiInstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/EosTuiInstall.cmake"
    @ONLY)

  install(SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/cmake/EosTuiInstall.cmake")
  install(PROGRAMS "${EOS_TUI_BINARY_STAGED}"
    DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    RENAME eos-tui)
  install(FILES "${EOS_TUI_LICENSE_STAGED}"
    DESTINATION "${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/eos-tui")
  install(FILES "${EOS_TUI_README_STAGED}"
    DESTINATION "${CMAKE_INSTALL_FULL_DATAROOTDIR}/doc/eos-tui")
else()
  message(WARNING "EOS TUI install is only configured for Linux x86_64/aarch64 and macOS arm64 builds.")
endif()
