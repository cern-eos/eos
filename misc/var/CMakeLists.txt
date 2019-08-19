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

install(DIRECTORY DESTINATION /var/eos)
install(DIRECTORY DESTINATION /var/log/eos)
install(DIRECTORY DESTINATION /var/log/eos/archive)
install(DIRECTORY DESTINATION /var/log/eos/fuse)
install(DIRECTORY DESTINATION /var/log/eos/fusex)
install(DIRECTORY DESTINATION /var/cache/eos)
install(DIRECTORY DESTINATION /var/cache/eos/fusex)
install(DIRECTORY DESTINATION /var/www/eos/cgi-bin)
install(DIRECTORY DESTINATION /var/log/eos/httpd)
install(DIRECTORY DESTINATION /var/log/eos/tx)
install(DIRECTORY DESTINATION /var/share/eos)
install(DIRECTORY DESTINATION /var/run/eosd)
install(DIRECTORY DESTINATION /var/run/eosd/credentials)
install(DIRECTORY DESTINATION /var/run/eosd/credentials/store)
install(DIRECTORY DESTINATION /var/eos/archive)
install(DIRECTORY DESTINATION /var/eos/html)
install(DIRECTORY DESTINATION /var/eos/qos)
install(DIRECTORY DESTINATION /var/eos/wfe)
install(DIRECTORY DESTINATION /var/eos/wfe/bash)
install(DIRECTORY DESTINATION /var/eos/ns-queue)
install(DIRECTORY DESTINATION /var/eos/ns-queue/default)

if( MacOSX )
  install(DIRECTORY DESTINATION /usr/local/lib/tmpfiles.d/)
else()
  install(DIRECTORY DESTINATION /usr/lib/tmpfiles.d/)
endif()

install(
  CODE "FILE ( INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/eos/wfe/bash/shell
               DESTINATION /var/eos/wfe/bash
               PERMISSIONS OWNER_READ OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE )"
  SCRIPT chown.cmake
)

install(FILES
  eos/html/error.html
  DESTINATION /var/eos/html/
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

install(FILES
  eos/qos/qos.conf
  DESTINATION /var/eos/qos/
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

install(FILES
  eos/test/fuse/untar/untar.tgz
  eos/test/fuse/untar/xrootd.tgz
  DESTINATION /var/eos/test/fuse/untar/
  PERMISSIONS OWNER_READ GROUP_READ WORLD_READ)

if (ASAN)
  install(FILES
    eos/test/LeakSanitizer.supp
    DESTINATION /var/eos/test/
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()

if( MacOSX )
  install(FILES
    eosd.conf
    DESTINATION /usr/local/lib/tmpfiles.d/
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
else()
  install(FILES
    eosd.conf
    DESTINATION /usr/lib/tmpfiles.d/
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()
