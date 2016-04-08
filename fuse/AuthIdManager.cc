//------------------------------------------------------------------------------
// File: AuthIdManager.cc
// Author: Geoffray Adde - CERN
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
#include "common/Macros.hh"
#include "AuthIdManager.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Get user name from the uid and change the effective user ID of the thread
//------------------------------------------------------------------------------

std::string AuthIdManager::mapUser (uid_t uid, gid_t gid, pid_t pid, uint64_t conid)
{
  eos_static_debug("uid=%lu gid=%lu pid=%lu", (unsigned long ) uid, (unsigned long ) gid, (unsigned long ) pid);

  XrdOucString sid = "";
  XrdOucString sb64;
  unsigned long long bituser = 0;

  if ((use_user_krb5cc || use_user_gsiproxy))
  {
    sid = "F";
    bituser=conid;
    eos_static_debug("conid = %llu", conid);
  }
  else
  {
    sid = "*";

    if (uid == 0)
    {
      uid = gid = DAEMONUID;
    }

    // Emergency mapping of too high user ids to nob
    if (uid > 0xfffff)
    {
      eos_static_err("msg=\"unable to map uid - out of 20-bit range - mapping to "
                     "nobody\" uid=%u",
                     uid);
      uid = 99;
    }
    if (gid > 0xffff)
    {
      eos_static_err("msg=\"unable to map gid - out of 16-bit range - mapping to "
                     "nobody\" gid=%u",
                     gid);
      gid = 99;
    }

    bituser = (uid & 0xfffff);
    bituser <<= 16;
    bituser |= (gid & 0xffff);
    bituser <<= 6;
    {
      // if using the gateway node, the purpose of the reamining 6 bits is just a connection counter to be able to reconnect
      XrdSysMutexHelper cLock (connectionIdMutex);
      if (connectionId) bituser |= (connectionId & 0x3f);
    }
  }

  bituser = h_tonll (bituser);

  // WARNING: we support only one endianess flavour by doing this
  eos::common::SymKey::Base64Encode ((char*) &bituser, 8, sb64);

  size_t len = sb64.length ();
  // Remove the non-informative '=' in the end
  if (len > 2)
  {
    sb64.erase (len - 1);
    len--;
  }

  // Reduce to 7 b64 letters
  if (len > 7) sb64.erase (0, len - 7);

  sid += sb64;

  // Encode '/' -> '_', '+' -> '-' to ensure the validity of the XRootD URL
  // if necessary.
  sid.replace ('/', '_');
  sid.replace ('+', '-');
  eos_static_debug("user-ident=%s", sid.c_str ());

  return sid.c_str ();
}

uint64_t AuthIdManager::sConIdCount=0;
