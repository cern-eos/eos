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
#include <vector>
#include <string>
#include <XrdOuc/XrdOucString.hh>

EOSCOMMONNAMESPACE_BEGIN

#pragma once
//------------------------------------------------------------------------------
//! Struct defining the virtual identity of a client e.g. their memberships and
//! authentication information
//------------------------------------------------------------------------------
struct VirtualIdentity {
  uid_t uid;
  gid_t gid;
  std::string uid_string;
  std::string gid_string;
  std::vector<uid_t> uid_list;
  std::vector<gid_t> gid_list;
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
  bool sudoer;

  //----------------------------------------------------------------------------
  //! Constructor - assign to "nobody" by default
  //----------------------------------------------------------------------------
  VirtualIdentity() : uid(99), gid(99), sudoer(false) {}

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
  bool hasUid(uid_t uid) const;

  //----------------------------------------------------------------------------
  //! Check if the gid vector contains has the requested gid
  //----------------------------------------------------------------------------
  bool hasGid(gid_t gid) const;

  //----------------------------------------------------------------------------
  //! Check if this client is coming from localhost
  //----------------------------------------------------------------------------
  bool isLocalhost() const;

  //----------------------------------------------------------------------------
  //! Return user@domain string
  //----------------------------------------------------------------------------
  std::string getUserAtDomain();

  //----------------------------------------------------------------------------
  //! Return group@domain string
  //----------------------------------------------------------------------------
  std::string getGroupAtDomain();

};

EOSCOMMONNAMESPACE_END
