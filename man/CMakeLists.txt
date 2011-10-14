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

INSTALL ( FILES man1/eos.1.gz man1/eos::fs.1.gz man1/eos::access.1.gz man1/eos::attr.1.gz man1/eos::cd.1.gz man1/eos::chown.1.gz man1/eos::clear.1.gz man1/eos::config.1.gz man1/eos::debug.1.gz man1/eos::file.1.gz DESTINATION ${MANDIR}/ PERMISSIONS OWNER_READ OWNER_WRITE )
