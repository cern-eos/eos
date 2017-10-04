//------------------------------------------------------------------------------
//! @file fusexrdlogin.hh
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


#ifndef FUSE_XRDLOGIN_HH_
#define FUSE_XRDLOGIN_HH_

#include <memory>
#include "XrdCl/XrdClURL.hh"
#include "llfusexx.hh"
#include "auth/ProcessCache.hh"

class fusexrdlogin
{
public:
  static int loginurl(XrdCl::URL& url, XrdCl::URL::ParamsMap& query,
                      fuse_req_t req ,
                      fuse_ino_t ino,
                      bool root_squash = false,
                      int connectionid = 0);

  static void initializeProcessCache(const CredentialConfig& config);
  static std::unique_ptr<ProcessCache> processCache;
private:
};


#endif
