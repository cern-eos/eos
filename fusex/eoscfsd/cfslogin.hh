//------------------------------------------------------------------------------
//! @file cfslogin.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the username, executable from process/credentials
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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


#pragma once
#define FUSE_USE_VERSION 35

#include <memory>
#include <fuse3/fuse_lowlevel.h>
#include "auth/AuthenticationGroup.hh"
#include "auth/ProcessCache.hh"
#include "common/SymKeys.hh"
#include "cfsmapping.hh"

class cfslogin
{
public:
  static std::string fillExeName(const std::string& exename);

  static std::string executable(fuse_req_t);

  static std::string secret(fuse_req_t req);

  static std::string name(fuse_req_t req);

  static std::string translate(fuse_req_t req, uid_t& uid, gid_t& gid);

  static void initializeProcessCache(const CredentialConfig& config);
  static std::unique_ptr<AuthenticationGroup> authGroup;
  static ProcessCache* processCache; // owned by authGroup
  static std::unique_ptr<cfsmapping> cfsMap;
private:
};
