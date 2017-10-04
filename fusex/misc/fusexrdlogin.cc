//------------------------------------------------------------------------------
//! @file fusexrdlogin.cc
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the login user name for an XRootD fusex connection
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "fusexrdlogin.hh"
#include "eosfuse.hh"
#include "common/Macros.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include <algorithm>
/*----------------------------------------------------------------------------*/

int fusexrdlogin::loginurl ( XrdCl::URL& url,
                            fuse_req_t req,
                            fuse_ino_t ino,
                            bool root_squash,
                            int connection_id )
{
  std::string sid = "";
  std::string sb64;
  unsigned long long bituser = 0;

  EosFuse::fuse_id id(req);
  
  sid = "*";
  int rc = 0;
  
  if ( (id.uid == 0) && root_squash)
  {
    id.uid = id.gid = 99;
    rc = ECHRNG;
  }

  // Emergency mapping of too high user ids to nob
  if (id.uid > 0xfffff)
  {
    eos_static_err("msg=\"unable to map uid - out of 20-bit range - mapping to "
                   "nobody\" uid=%u",
                   id.uid);
    id.uid = 99;
    rc = ERANGE;
  }
  if (id.gid > 0xffff)
  {
    eos_static_err("msg=\"unable to map gid - out of 16-bit range - mapping to "
                   "nobody\" gid=%u",
                   id.gid);
    id.gid = 99;
    rc = ERANGE;
  }

  bituser = (id.uid & 0xfffff);
  bituser <<= 16;
  bituser |= (id.gid & 0xffff);
  bituser <<= 6;
  bituser |= connection_id & 0x3f;

  bituser = h_tonll (bituser);

  // WARNING: we support only one endianess flavour by doing this
  if (!(rc = eos::common::SymKey::Base64Encode ((char*) &bituser, 8, sb64)))
  {
    rc = EIO;   
  }

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
  std::replace ( sid.begin(), sid.end(), '/', '_');
  std::replace ( sid.begin(), sid.end(), '+', '-');

  std::string username = sid.c_str();
  url.SetUserName(username);

  eos_static_notice("%s uid=%u gid=%u rc=%d user-name=%s", 
                    EosFuse::dump(id, ino, 0, rc).c_str(),
                    id.uid,
                    id.gid,
                    rc,
                    username.c_str()
                    );
  return rc;
}
/*----------------------------------------------------------------------------*/




