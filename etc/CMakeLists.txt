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

INSTALL ( FILES eos.keytab DESTINATION ${SYSCONFDIR}/ PERMISSIONS OWNER_READ OWNER_WRITE )
INSTALL ( FILES xrd.cf.fst xrd.cf.mgm xrd.cf.mq xrd.cf.sync xrd.cf.fed xrd.cf.prefix fuse.conf fuse.conf.eos DESTINATION ${SYSCONFDIR}/ )
INSTALL ( FILES rc.d/init.d/eos rc.d/init.d/eosha rc.d/init.d/eossync rc.d/init.d/eosd rc.d/init.d/eoshttpd DESTINATION ${SYSCONFDIR}/rc.d/init.d PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL ( FILES sysconfig/eos.example sysconfig/xrootd.example sysconfig/eoshttpd DESTINATION ${SYSCONFDIR}/sysconfig/ )
INSTALL ( FILES cron.d/eos-logs cron.d/eos-reports DESTINATION ${SYSCONFDIR}/cron.d/ )
INSTALL ( FILES logrotate.d/eos-logs logrotate.d/eos-fuse-logs DESTINATION ${SYSCONFDIR}/logrotate.d/ )
INSTALL ( FILES httpd/conf/eoshttpd.default.conf DESTINATION ${SYSCONFDIR}/httpd/conf/ )
#INSTALL ( DIRECTORY shibboleth DESTINATION ${SYSCONFDIR}/ )
