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
  AuthenticationGroup.cc           AuthenticationGroup.hh
  BoundIdentityProvider.cc         BoundIdentityProvider.hh
                                   CredentialCache.hh
  CredentialFinder.cc              CredentialFinder.hh
  CredentialValidator.cc           CredentialValidator.hh
  DirectoryIterator.cc             DirectoryIterator.hh
  EnvironmentReader.cc             EnvironmentReader.hh
                                   FileDescriptor.hh
  JailIdentifier.cc                JailIdentifier.hh
  Logbook.cc                       Logbook.hh
  LoginIdentifier.cc               LoginIdentifier.hh
  ProcessCache.cc                  ProcessCache.hh
  ProcessInfo.cc                   ProcessInfo.hh
  RmInfo.cc                        RmInfo.hh
                                   ScopedFsUidSetter.hh
  SecurityChecker.cc               SecurityChecker.hh
  UnixAuthenticator.cc             UnixAuthenticator.hh
  UserCredentialFactory.cc         UserCredentialFactory.hh
                                   UserCredentials.hh
  Utils.cc                         Utils.hh
  UuidStore.cc                     UuidStore.hh)

target_link_libraries(EosFuseAuth PUBLIC
  XROOTD::UTILS
  XROOTD::PRIVATE
  KRB5::KRB5
  OPENSSL::OPENSSL
  PROTOBUF::PROTOBUF)
