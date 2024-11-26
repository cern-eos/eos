# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/jwker
    COMMAND ${CMAKE_COMMAND} -E echo "Creating eos-jwker"
    COMMAND go install github.com/jphastings/jwker/cmd/jwker@latest
    COMMAND cp -vf $ENV{HOME}/go/bin/jwker ${CMAKE_CURRENT_BINARY_DIR}/eos-jwker)

add_custom_target(
    build-eos-jwker ALL
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/jwker)

install(PROGRAMS
  eos-status
  eos_start.sh
  eos_start_pre.sh
  eos-reportstat
  eos-mdstat
  eos-mdreport
  eos-inspectorreport
  eos-inspectorstat
  eos-prom-push
  eos-jwker
  eos-jwk-https
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

if(TARGET eosxd)
  install(PROGRAMS mount.eosx DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
endif()

if(TARGET eosxd3)
  install(PROGRAMS umount.fuse mount.eosx3 mount.eoscfs DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

endif()
