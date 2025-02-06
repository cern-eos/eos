// ----------------------------------------------------------------------
// File: VirtualIdentity.hh
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

#include "common/Namespace.hh"
#include "common/token/Token.hh"
#include <XrdOuc/XrdOucString.hh>
#include <vector>
#include <memory>
#include <string>
#include <set>
#include <cstdint>

EOSCOMMONNAMESPACE_BEGIN

#pragma once
//------------------------------------------------------------------------------
//! Struct defining the virtual identity of a client e.g. their memberships and
//! authentication information
//------------------------------------------------------------------------------
struct VirtualIdentity {
  static uid_t kNobodyUid;
  static gid_t kNobodyGid;
  uid_t uid;
  gid_t gid;
  std::string uid_string;
  std::string gid_string;
  std::set<uid_t> allowed_uids;
  std::set<gid_t> allowed_gids;
  XrdOucString tident;
  XrdOucString name;
  XrdOucString prot;
  std::string host;
  std::string domain;
  std::string grps;
  std::string role;
  std::string dn;
  std::string geolocation;
  std::string app;
  std::string key;
  std::string email;
  std::string fullname;
  std::string federation;
  std::string scope;
  std::string trace;
  std::string onbehalf;
  bool sudoer;
  bool gateway;
  std::shared_ptr<Token> token;

  //----------------------------------------------------------------------------
  //! Constructor - assign to "nobody" by default
  //----------------------------------------------------------------------------
  VirtualIdentity();

  //----------------------------------------------------------------------------
  //! "Constructor" - return Root identity
  //----------------------------------------------------------------------------
  static VirtualIdentity Root();

  //----------------------------------------------------------------------------
  //! "Constructor" - return Nobody identity
  //----------------------------------------------------------------------------
  static VirtualIdentity Nobody();

  //----------------------------------------------------------------------------
  //! Check if the uid vector contains has the requested uid
  //----------------------------------------------------------------------------
  inline bool hasUid(uid_t uid) const
  {
    return (allowed_uids.find(uid) != allowed_uids.end());
  }

  //----------------------------------------------------------------------------
  //! Check if the gid vector contains has the requested gid
  //----------------------------------------------------------------------------
  bool hasGid(gid_t gid) const
  {
    return (allowed_gids.find(gid) != allowed_gids.end());
  }

  //----------------------------------------------------------------------------
  //! Check if this client is coming from localhost
  //----------------------------------------------------------------------------
  bool isLocalhost() const;

  //----------------------------------------------------------------------------
  //! Check if this client is coming from localhost
  //----------------------------------------------------------------------------
  bool isGateway() { return gateway; }

  //----------------------------------------------------------------------------
  //! Return user@domain string
  //----------------------------------------------------------------------------
  std::string getUserAtDomain() const;

  //----------------------------------------------------------------------------
  //! Return group@domain string
  //----------------------------------------------------------------------------
  std::string getGroupAtDomain() const;

  //----------------------------------------------------------------------------
  //! Return a vid trace string
  //----------------------------------------------------------------------------
  std::string getTrace(bool compact=false) const;

  //----------------------------------------------------------------------------
  //! Set uid/gid to nobody
  //----------------------------------------------------------------------------
  void toNobody();
};

EOSCOMMONNAMESPACE_END
