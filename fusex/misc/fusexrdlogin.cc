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

#include "fusexrdlogin.hh"
#include "eosfuse.hh"
#include "common/Macros.hh"
#include "common/SymKeys.hh"
#include "misc/FuseId.hh"
#include <algorithm>
#ifdef __APPLE__
#define ECHRNG 44
#endif

std::unique_ptr<ProcessCache> fusexrdlogin::processCache;

void fusexrdlogin::initializeProcessCache(const CredentialConfig& config)
{
  processCache.reset(new ProcessCache());
  processCache->setCredentialConfig(config);
}

int fusexrdlogin::loginurl(XrdCl::URL& url,
                           XrdCl::URL::ParamsMap& paramsMap,
                           fuse_req_t req,
                           fuse_ino_t ino,
                           bool root_squash,
                           int connection_id)
{
  fuse_id id(req);
  return loginurl(url, paramsMap, id.uid, id.gid, id.pid, root_squash,
                  connection_id);
}

int fusexrdlogin::loginurl(XrdCl::URL& url,
                           XrdCl::URL::ParamsMap& paramsMap,
                           uid_t uid,
                           gid_t gid,
                           pid_t pid,
                           fuse_ino_t ino,
                           bool root_squash,
                           int connection_id)
{
  fuse_id id;
  id.uid = uid;
  id.gid = gid;
  id.pid = pid;
  ProcessSnapshot snapshot = processCache->retrieve(id.pid, id.uid, id.gid,
                             false);
  std::string username = "nobody";

  if (snapshot) {
    username = snapshot->getBoundIdentity().getLogin().getStringID();
    snapshot->getBoundIdentity().getCreds()->toXrdParams(paramsMap);
  }

  url.SetUserName(username);
  int rc = 0;
  eos_static_notice("%s uid=%u gid=%u rc=%d user-name=%s",
                    EosFuse::dump(id, ino, 0, rc).c_str(),
                    id.uid,
                    id.gid,
                    rc,
                    username.c_str()
                   );
  return rc;
}

std::string fusexrdlogin::xrd_login(fuse_req_t req)
{
  fuse_id id(req);
  ProcessSnapshot snapshot = processCache->retrieve(id.pid, id.uid, id.gid,
                             false);
  std::string login;

  if (snapshot) {
    login = snapshot->getXrdLogin();
  } else {
    login = "unix";
  }

  eos_static_notice("uid=%u gid=%u xrd-login=%s",
                    id.uid,
                    id.gid,
                    login.c_str()
                   );
  return login;
}

/*----------------------------------------------------------------------------*/
