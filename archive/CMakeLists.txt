# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru - <esindril@cern.ch>
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

INSTALL(DIRECTORY archivefile
	DESTINATION ${EOS_PYTHON_LIBDIR}
        PATTERN "test" EXCLUDE
	PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

INSTALL(FILES const.py
	DESTINATION ${EOS_PYTHON_LIBDIR}
	PERMISSIONS OWNER_READ OWNER_WRITE
		    GROUP_READ WORLD_READ)

INSTALL(PROGRAMS eosarchiverd
	DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

INSTALL(FILES eosarchiverd.init
	DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/rc.d/init.d/
	PERMISSIONS OWNER_READ OWNER_EXECUTE
		    GROUP_READ GROUP_EXECUTE
		    WORLD_READ WORLD_EXECUTE
	RENAME eosarchiverd)

INSTALL(FILES eosarchiverd.example
	DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sysconfig/)
