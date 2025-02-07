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
#include <cstring>
#include <pwd.h>

namespace
{
//! Method to get the uid/gid values for user 'nobody'
std::pair<uid_t, gid_t> GetNobodyUidGid()
{
  struct passwd pw_info;
  memset(&pw_info, 0, sizeof(pw_info));
  char buffer[131072];
  size_t buflen = sizeof(buffer);
  struct passwd* pw_prt = 0;
  const std::string name = "nobody";

  if (getpwnam_r(name.c_str(), &pw_info, buffer, buflen, &pw_prt) ||
      (!pw_prt)) {
    std::terminate();
  }

  return std::make_pair(pw_info.pw_uid, pw_info.pw_gid);
};
}


EOSCOMMONNAMESPACE_BEGIN

uid_t VirtualIdentity::kNobodyUid = GetNobodyUidGid().first;
gid_t VirtualIdentity::kNobodyGid = GetNobodyUidGid().second;

//----------------------------------------------------------------------------
//! Constructor - assign to "nobody" by default
//----------------------------------------------------------------------------
VirtualIdentity::VirtualIdentity() :
  uid(kNobodyUid), gid(kNobodyGid), sudoer(false), gateway(false)
{}

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
  vid.host = "localhost";
  vid.gateway = false;
  return vid;
}

//------------------------------------------------------------------------------
// "Constructor" - return Nobody identity
//------------------------------------------------------------------------------
VirtualIdentity VirtualIdentity::Nobody()
{
  VirtualIdentity vid;
  vid.uid = kNobodyUid;
  vid.gid = kNobodyGid;
  vid.allowed_uids = {kNobodyUid};
  vid.allowed_gids = {kNobodyGid};
  vid.name = "nobody";
  vid.sudoer = false;
  vid.gateway = false;
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
VirtualIdentity::getTrace(bool compact) const
{
  std::stringstream ss;
  time_t now = time(NULL);

  if (compact) {
    ss << "{uid:" << uid << ",gid:" << gid << ",tident:" << tident << ",prot:" <<
       prot << ",app:" << app << ",host:" << host << ",domain:" << domain << "trace:"
       << trace << ",onbehalf:" << onbehalf << "}";
    return ss.str();
  } else {
    ss << "[" << eos::common::Timing::ltime(now) << "] uid:" << uid << "[" <<
       uid_string << "] gid:" << gid << "[" << gid_string << "] tident:" <<
       tident.c_str() << " name:" << name << " dn:" << dn << " prot:" << prot <<
       " app:" << app << " host:" << host << " domain:" << domain << " geo:" <<
       geolocation << " sudo:"
       << sudoer << " trace:" << trace << " onbehalf:" << onbehalf;
    return ss.str();
  }
}

//----------------------------------------------------------------------------
// Set user/group to nobody
//----------------------------------------------------------------------------
void VirtualIdentity::toNobody()
{
  uid = kNobodyUid;
  gid = kNobodyGid;
  allowed_uids = {kNobodyUid};
  allowed_gids = {kNobodyGid};
  name = "nobody";
  sudoer = false;
}

EOSCOMMONNAMESPACE_END
