// ----------------------------------------------------------------------
// File: VirtualIdentity.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/VirtualIdentity.hh"
#include "common/Timing.hh"
#include <iostream>
#include <sstream>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// "Constructor" - return Root identity
//------------------------------------------------------------------------------
VirtualIdentity VirtualIdentity::Root()
{
  VirtualIdentity vid;
  vid.uid = 0;
  vid.gid = 0;
  vid.allowed_uids = {0};
  vid.allowed_gids = {0};
  vid.name = "root";
  vid.prot = "local";
  vid.tident = "service@localhost";
  vid.sudoer = false;
  vid.avatar = false;
  vid.host = "localhost";
  return vid;
}

//------------------------------------------------------------------------------
// "Constructor" - return Nobody identity
//------------------------------------------------------------------------------
VirtualIdentity VirtualIdentity::Nobody()
{
  VirtualIdentity vid;
  vid.uid = 99;
  vid.gid = 99;
  vid.allowed_uids = {99};
  vid.allowed_gids = {99};
  vid.name = "nobody";
  vid.sudoer = false;
  vid.avatar = false;
  vid.tident = "nobody@unknown";
  return vid;
}

//------------------------------------------------------------------------------
// Check if this client is coming from localhost
//------------------------------------------------------------------------------
bool VirtualIdentity::isLocalhost() const
{
  if (host == "localhost"               ||
      host == "localhost.localdomain"  ||
      host == "localhost6"             ||
      host == "localhost6.localdomain6") {
    return true;
  }

  return false;
}


//----------------------------------------------------------------------------
// Return user@domain string
//----------------------------------------------------------------------------
std::string
VirtualIdentity::getUserAtDomain() const
{
  return uid_string + "@" + domain;
}

//----------------------------------------------------------------------------
// Return group@domain string
//----------------------------------------------------------------------------
std::string
VirtualIdentity::getGroupAtDomain() const
{
  return gid_string + "@" + domain;
}

//----------------------------------------------------------------------------
// Return vid trace string
//----------------------------------------------------------------------------

std::string
VirtualIdentity::getTrace() const
{
  std::stringstream ss;
  time_t now = time(NULL);
  ss << "[" << eos::common::Timing::ltime(now) << "] uid:" << uid << "[" <<
     uid_string << "] gid:" << gid << "[" << gid_string << "] tident:" <<
     tident.c_str() << " name:" << name << " dn:" << dn << " prot:" << prot <<
     " host:" << host << " domain:" << domain << " geo:" << geolocation << " sudo:"
     << sudoer << " avatar:" << avatar;
  return ss.str();
}

EOSCOMMONNAMESPACE_END
