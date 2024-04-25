//------------------------------------------------------------------------------
//! @file FuseId.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Header file providing a replacment for fuse_req_t because it has a hidden type
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


#ifndef FUSE_FUSEID_HH_
#define FUSE_FUSEID_HH_

#include <memory>
#include "llfusexx.hh"
#include "misc/fusexrdlogin.hh"
#include <XrdCl/XrdClURL.hh>

//----------------------------------------------------------------------------


struct fuse_identity {
  XrdCl::URL url;
  XrdCl::URL::ParamsMap query;
};

class fuse_id
{
public:

  uid_t uid;
  gid_t gid;
  pid_t pid;
  std::shared_ptr<struct fuse_identity> getid() const
  {
    return _id;
  }

  fuse_id()
  {
    uid = gid = pid = 0;
  }

  fuse_id(fuse_req_t req)
  {
    uid = fuse_req_ctx(req)->uid;
    gid = fuse_req_ctx(req)->gid;
    pid = fuse_req_ctx(req)->pid;
  }

  int bind()
  {
    // when called the current process environment is snapshotted for this request
    _id = std::make_shared<struct fuse_identity>();
    _id->url.FromString("root://localhost//dummy");
    return fusexrdlogin::loginurl(_id->url, _id->query, uid, gid, pid, 0);
  }
private:
  std::shared_ptr<struct fuse_identity> _id;
};


#endif
