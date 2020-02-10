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

# Check if we are on a Debian system
if(EXISTS "/etc/debian_version")
  set ( DEBIAN 1 )
endif()

install(
  FILES eos.keytab
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/
  PERMISSIONS OWNER_READ OWNER_WRITE)

install(
  FILES xrd.cf.fst xrd.cf.mgm xrd.cf.mq xrd.cf.sync xrd.cf.fed xrd.cf.prefix
	xrd.cf.global-mq xrd.cf.auth xrd.cf.quarkdb fuse.conf fuse.conf.eos 
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/)

install( 
  FILES eos/fuse.home-i00.conf eos/fuse.home-i01.conf eos/fuse.home-i02.conf eos/fuse.home-i03.conf eos/fuse.home-i04.conf eos/fuse.project-a.conf eos/fuse.project-b.conf eos/fuse.project-c.conf eos/fuse.project-d.conf eos/fuse.project-e.conf eos/fuse.project-f.conf eos/fuse.project-g.conf eos/fuse.project-h.conf eos/fuse.project-i.conf eos/fuse.project-j.conf eos/fuse.project-k.conf eos/fuse.project-l.conf eos/fuse.project-m.conf eos/fuse.project-n.conf eos/fuse.project-o.conf eos/fuse.project-p.conf eos/fuse.project-q.conf eos/fuse.project-r.conf eos/fuse.project-s.conf eos/fuse.project-t.conf eos/fuse.project-u.conf eos/fuse.project-v.conf eos/fuse.project-w.conf eos/fuse.project-x.conf eos/fuse.project-y.conf eos/fuse.project-z.conf 
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/eos/
  PERMISSIONS OWNER_READ OWNER_WRITE)

install(
  FILES sysconfig/xrootd.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)

install(
  FILES bash_completion.d/eos
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/bash_completion.d/)

install(
  FILES cron.d/eos-logs cron.d/eos-reports cron.d/eos-config
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cron.d/)

install(
  FILES logrotate.d/eos-logs logrotate.d/eos-fuse-logs logrotate.d/eos-fusex-logs
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d/)

# Installing files depending on service manager (systemd or sysVinit)
set(SYSTEMD_DIR /usr/lib/systemd/)

if(EXISTS ${SYSTEMD_DIR})
 install(DIRECTORY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/)

 # TODO (esindril): Uncomment (or undebian) this when systemd support is enabled for fuse
 if(DEBIAN)
   install(
     FILES sysconfig/eosd_env.example
     DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)

   install(
     FILES systemd/system/eosd@.service
     DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/)
 endif()

 install(
   FILES sysconfig/eos_env.example
   DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)

 install(
   FILES systemd/system/eos.target
        systemd/system/eos.service
 	systemd/system/eos@.service
 	systemd/system/eos@master.service
 	systemd/system/eos@slave.service
 	systemd/system/eosfstdb@.service
 	systemd/system/eosha.service
 	systemd/system/eosslave.service
 	systemd/system/eossync.service
 	systemd/system/eossync@.service
 	systemd/system/eossync@config.service
   DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/)

#We keep for the moment the SysV scripts for eosd
install(
  FILES sysconfig/eos.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)

install(
 FILES rc.d/init.d/eosd
 DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/rc.d/init.d
 PERMISSIONS OWNER_READ OWNER_EXECUTE
	     GROUP_READ GROUP_EXECUTE
	     WORLD_READ WORLD_EXECUTE)
else()
install(
  FILES sysconfig/eos.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)

install(
 FILES rc.d/init.d/eos rc.d/init.d/eosha rc.d/init.d/eosslave
	rc.d/init.d/eossync rc.d/init.d/eosd
 DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/rc.d/init.d
 PERMISSIONS OWNER_READ OWNER_EXECUTE
	     GROUP_READ GROUP_EXECUTE
	     WORLD_READ WORLD_EXECUTE)
endif()
