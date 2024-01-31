//------------------------------------------------------------------------------
//! @file cfslogin.cc
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing username, executable from process/credentials
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

#include "cfslogin.hh"
#include "cfsmapping.hh"
#include "auth/Logbook.hh"
#include <algorithm>
#include <regex>

std::unique_ptr<AuthenticationGroup> cfslogin::authGroup;
std::unique_ptr<cfsmapping> cfslogin::cfsMap;

ProcessCache* cfslogin::processCache = nullptr;
std::string cfslogin::k5domain = "@CERN.CH";

void cfslogin::initializeProcessCache(const CredentialConfig& config)
{
  authGroup.reset(new AuthenticationGroup(config));
  processCache = authGroup->processCache();
  cfsMap.reset(new cfsmapping());
}

std::string cfslogin::fillExeName(const std::string& execname)
{
  auto base_name = [](std::string const & path) {
    return path.substr(path.find_last_of("/\\") + 1);
  };
  std::regex safeReg("[/\\w.]+");
  std::string exe = execname;

  if (execname.length() > 32) {
    exe = base_name(execname);
  }

  if (std::regex_match(exe, safeReg)) {
    return exe;
  } else {
    std::string base64_string = "base64";
    std::string base64in = exe;
    eos::common::SymKey::Base64(base64in, base64_string);
    return base64_string;
  }
}


std::string cfslogin::executable(fuse_req_t req)
{
  Logbook logbook(true);
  ProcessSnapshot snapshot =
    (fuse_req_ctx(req)->pid) ? processCache->retrieve(fuse_req_ctx(req)->pid,
        fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid,
        false, logbook) : 0;

  if (snapshot) {
    return fillExeName(snapshot->getExe());
  } else {
    return "unknown";
  }
}

std::string cfslogin::secret(fuse_req_t req)
{
  ProcessSnapshot snapshot = processCache->retrieve(fuse_req_ctx(req)->pid,
                             fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid,
                             false);

  if (snapshot) {
    return snapshot->getBoundIdentity()->getCreds()->getKey();
  }

  return "";
}

std::string cfslogin::name(fuse_req_t req)
{
  ProcessSnapshot snapshot = processCache->retrieve(fuse_req_ctx(req)->pid,
                             fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid,
                             false);
  std::string username = "nobody";

  if (snapshot) {
    username = snapshot->getBoundIdentity()->getCreds()->toUserName();;
  }

  size_t adpos = 0;

  if ((adpos = username.find("@")) != std::string::npos) {
    if (username.find(cfslogin::k5domain) == std::string::npos) {
      return "nobody";
    } else {
      username.erase(adpos);
    }
  }

  return username;
}

std::string cfslogin::translate(fuse_req_t req, uid_t& uid, gid_t& gid)
{
  std::string name = cfslogin::name(req);
  cfsMap->translate(name, uid, gid);
  return name;
}


