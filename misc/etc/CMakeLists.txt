# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
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

install(DIRECTORY systemd USE_SOURCE_PERMISSIONS DESTINATION lib)

install(DIRECTORY bash_completion.d cron.d eos logrotate.d sysconfig auto.master.d
  USE_SOURCE_PERMISSIONS DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR})

install(FILES
  eos.keytab
  eos.client.keytab
  DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}
  PERMISSIONS OWNER_READ)

install(FILES
  fuse.conf.eos
  xrd.cf.auth
  xrd.cf.fed
  xrd.cf.fst
  xrd.cf.mgm
  xrd.cf.prefix
  xrd.cf.quarkdb
  xrd.cf.sync
  auto.cfsd
  DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR})
