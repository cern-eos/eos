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

if( MacOSX )
  install(
    FILES mount.eos mount.eosx automount.cern.eos
    DESTINATION ${CMAKE_INSTALL_PREFIX}/sbin/
    PERMISSIONS OWNER_READ OWNER_EXECUTE
	        GROUP_READ GROUP_EXECUTE
	        WORLD_READ WORLD_EXECUTE)
else()
  install(
    FILES mount.eos mount.eosx automount.cern.eos
    DESTINATION ${CMAKE_INSTALL_PREFIX}/../sbin/
    PERMISSIONS OWNER_READ OWNER_EXECUTE
	        GROUP_READ GROUP_EXECUTE
	        WORLD_READ WORLD_EXECUTE)
endif()

# Script used by systemd in the start-up phase
install(
  FILES eos_start_pre.sh eos_start.sh eos-status
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
	      GROUP_READ GROUP_EXECUTE
	      WORLD_READ WORLD_EXECUTE)
