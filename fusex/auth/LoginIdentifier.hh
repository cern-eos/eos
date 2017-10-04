//------------------------------------------------------------------------------
// File: LoginIdentifier.hh
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

#ifndef __LOGIN_IDENTIFIER__HH__
#define __LOGIN_IDENTIFIER__HH__

#include <string>
#include <sys/types.h>

// We have to juggle many different xrootd logins.
// This class identifies them with a unique ID, which is provided in
// the user part of an xrootd URL: root://user@host/path
// We're only limited to 8 chars..
// Each object is immutable after construction, no need for locking.
class LoginIdentifier {
public:
  LoginIdentifier() {
    connId = 0;
    stringId = "nobody";
  }
  
  LoginIdentifier(uint64_t connId);
  LoginIdentifier(uid_t uid, gid_t gid, pid_t pid, uint64_t connId);

  std::string getStringID() const {
    return stringId;
  }

  uint64_t getConnectionID() const {
    return connId;
  }

private:
  uint64_t connId;
  std::string stringId;

  static std::string encode(char prefix, uint64_t bituser);
};


#endif
