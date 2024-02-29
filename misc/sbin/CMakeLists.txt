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
  eosjwker
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

if(TARGET eosxd)
  install(PROGRAMS mount.eosx DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
endif()

if(TARGET eosxd3)
  install(PROGRAMS umount.fuse mount.eosx3 mount.eoscfs DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

endif()
