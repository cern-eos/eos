# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Georgios Bitzes - CERN
# ------------------------------------------------------------------------------

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

add_library(
  EosFuseAuth OBJECT
  BoundIdentityProvider.cc         BoundIdentityProvider.hh
                                   CredentialCache.hh
  CredentialFinder.cc              CredentialFinder.hh
  EnvironmentReader.cc             EnvironmentReader.hh
  LoginIdentifier.cc               LoginIdentifier.hh
  ProcessCache.cc                  ProcessCache.hh
  ProcessInfo.cc                   ProcessInfo.hh
  SecurityChecker.cc               SecurityChecker.hh
  Utils.cc                         Utils.hh
)
