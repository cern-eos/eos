// ----------------------------------------------------------------------
// File: UnixAuthenticator.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef EOS_FUSEX_UNIX_AUTHENTICATOR_HH
#define EOS_FUSEX_UNIX_AUTHENTICATOR_HH

#include "CredentialFinder.hh"

class UnixAuthenticator {
public:
  //----------------------------------------------------------------------------
  // Create an identity based on unix-authentication. The uid and gid are
  // encoded in the LoginIdentifier in a way the MGM understands.
  //
  // It has to be that the MGM trusts the machine from which this request
  // originates, as this mechanism can be used to impersonate anyone.
  //----------------------------------------------------------------------------
  std::shared_ptr<const BoundIdentity> createIdentity(pid_t pid, uid_t uid,
						      gid_t gid, bool reconnect, std::string key);

private:
  std::mutex mtx;
  std::map<std::pair<uid_t, gid_t>, uint64_t> connectionCounter;

  //----------------------------------------------------------------------------
  // Get the current connection counter for the given uid, gid.
  //----------------------------------------------------------------------------
  uint64_t getUnixConnectionCounter(uid_t uid, gid_t gid, bool reconnect);
};

#endif
