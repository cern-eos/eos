// ----------------------------------------------------------------------
// File: UnixAuthenticator.cc
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

#include "UnixAuthenticator.hh"

//------------------------------------------------------------------------------
// Create an identity based on unix-authentication. The uid and gid are
// encoded in the LoginIdentifier in a way the MGM understands.
//
// It has to be that the MGM trusts the machine from which this request
// originates, as this mechanism can be used to impersonate anyone.
//------------------------------------------------------------------------------
std::shared_ptr<const BoundIdentity> UnixAuthenticator::createIdentity(
								       pid_t pid, uid_t uid, gid_t gid, bool reconnect, std::string key)
{
  std::shared_ptr<BoundIdentity> bdi(new BoundIdentity());
  bdi->getLogin() = LoginIdentifier(uid, gid, pid,
    getUnixConnectionCounter(uid, gid, reconnect));
  bdi->getCreds()->getUC().secretkey = key;
  return bdi;
}

//------------------------------------------------------------------------------
// Get the current connection counter for the given uid, gid.
//------------------------------------------------------------------------------
uint64_t UnixAuthenticator::getUnixConnectionCounter(uid_t uid, gid_t gid,
    bool reconnect)
{
  std::lock_guard<std::mutex> lock(mtx);

  if (reconnect) {
  	connectionCounter[std::make_pair(uid, gid)]++;
  }

  return connectionCounter[std::make_pair(uid, gid)];
}

