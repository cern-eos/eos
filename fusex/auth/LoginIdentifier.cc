//------------------------------------------------------------------------------
// File: LoginIdentifier.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "LoginIdentifier.hh"
#include "common/SymKeys.hh"
#include "common/Macros.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucString.hh>
/*----------------------------------------------------------------------------*/
#include <arpa/inet.h>

// Logic extracted from the old AuthIdManager::mapUser.

LoginIdentifier::LoginIdentifier(uid_t uid, gid_t gid, pid_t pid,
                                 uint64_t connId_)
  : connId(connId_)
{
  if (uid == 0) {
    uid = gid = 99;
  }

  bool map_only_user = false;

  if (uid > 0x3ffff) {
    eos_static_info("msg=\"unable to map uid+gid - out of range - mapping only user");
    map_only_user = true;
  }

  if (gid > 0xffff) {
    eos_static_info("msg=\"unable to map uid+gid - out of range - mapping only user");
    map_only_user = true;
  }

  // this mechanism can only transport uid's over UNIX < 1024*1024 !
  if (uid >= (1024*1024)) {
    eos_static_info("msg=\"unable to map uid+gid - out of range - requesting 99/99");
    uid = 99;
    gid = 99;
  }

  uint64_t bituser = 0;

  if (map_only_user) {
    bituser = (uid & 0xfffffffff);
    bituser <<= 6;
  } else {
    bituser = (uid & 0xfffff);
    bituser <<= 16;
    bituser |= (gid & 0xffff);
    bituser <<= 6;
  }

  // if using the gateway node, the purpose of the reamining 6 bits is just a connection counter to be able to reconnect
  if (connId) {
    bituser |= (connId & 0x3f);
  }

  if (map_only_user) {
    stringId = encode('~', bituser);
  } else {
    stringId = encode('*', bituser);
  }
}

LoginIdentifier::LoginIdentifier(uint64_t connId_) : connId(connId_)
{
  stringId = encode('A', connId);
}

//----------------------------------------------------------------------------
// Describe object as string - different than getStringID, as we also print
// the connectionID, if any
//----------------------------------------------------------------------------
std::string LoginIdentifier::describe() const
{
  if (connId == 0) {
    return stringId;
  }

  return SSTR(stringId << " - " << connId);
}

// Extracted from the old AuthIdManager::mapUser function
// TODO(gbitzes): Truncating the output from base64 encode seems.. bad? review

std::string LoginIdentifier::encode(char prefix, uint64_t bituser)
{
  XrdOucString sb64;
  bituser = h_tonll(bituser);
  // WARNING: we support only one endianess flavour by doing this
  eos::common::SymKey::Base64Encode((char*) &bituser, 8, sb64);
  size_t len = sb64.length();

  // Remove the non-informative '=' in the end
  if (len > 2) {
    sb64.erase(len - 1);
    len--;
  }

  // Reduce to 7 b64 letters
  if (len > 7) {
    sb64.erase(0, len - 7);
  }

  XrdOucString sid = prefix + sb64;
  // Encode '/' -> '_', '+' -> '-' to ensure the validity of the XRootD URL
  // if necessary.
  sid.replace('/', '_');
  sid.replace('+', '-');
  return sid.c_str();
}
