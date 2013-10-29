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

INSTALL ( FILES man1/eos.1.gz man1/eos-access.1.gz man1/eos-attr.1.gz man1/eos-cd.1.gz man1/eos-chmod.1.gz man1/eos-chown.1.gz man1/eos-clear.1.gz man1/eos-config.1.gz man1/eos-console.1.gz man1/eos-cp.1.gz man1/eos-debug.1.gz man1/eos-dropbox.1.gz man1/eos-exit.1.gz man1/eos-file.1.gz man1/eos-fileinfo.1.gz man1/eos-find.1.gz man1/eos-fs.1.gz man1/eos-fsck.1.gz man1/eos-fuse.1.gz man1/eos-group.1.gz man1/eos-help.1.gz man1/eos-io.1.gz man1/eos-json.1.gz man1/eos-license.1.gz man1/eos-ls.1.gz man1/eos-map.1.gz man1/eos-mkdir.1.gz man1/eos-motd.1.gz man1/eos-node.1.gz man1/eos-ns.1.gz man1/eos-pwd.1.gz man1/eos-quit.1.gz man1/eos-quota.1.gz man1/eos-reconnect.1.gz man1/eos-recycle.1.gz man1/eos-rm.1.gz man1/eos-rmdir.1.gz man1/eos-role.1.gz man1/eos-rtlog.1.gz man1/eos-silent.1.gz man1/eos-space.1.gz man1/eos-stat.1.gz man1/eos-timing.1.gz man1/eos-transfer.1.gz man1/eos-version.1.gz man1/eos-vid.1.gz man1/eos-who.1.gz man1/eos-whoami.1.gz DESTINATION ${CMAKE_INSTALL_MANDIR}/man1 PERMISSIONS OWNER_READ OWNER_WRITE )
