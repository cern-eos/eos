//------------------------------------------------------------------------------
//! @file backend.hh
//! @author Andreas-Joachim Peters CERN
//! @brief backend IO handling class
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

#ifndef FUSE_BACKEND_HH_
#define FUSE_BACKEND_HH_

#include "common/Logging.hh"
#include "fusex/fusex.pb.h"
#include "llfusexx.hh"

class backend
{
public:

  //----------------------------------------------------------------------------
  backend();
  virtual ~backend();

  int init(std::string& hostport, std::string& remotemountdir);

  int getMD(fuse_req_t req,
            const std::string& path,
            std::vector<eos::fusex::container>& cont,
            std::string authid=""
            );

  int getMD(fuse_req_t req,
            uint64_t inode,
            const std::string& name,
            std::vector<eos::fusex::container>& cont,
            bool listing,
            std::string authid=""
            );

  int getMD(fuse_req_t req,
            uint64_t inode,
            uint64_t myclock,
            std::vector<eos::fusex::container>& cont,
            bool listing,
            std::string authid=""
            );

  int fetchResponse(std::string& url,
                    std::vector<eos::fusex::container>& cont
                    );

  int putMD(eos::fusex::md* md, std::string authid="");

  int getCAP(fuse_req_t req,
             uint64_t inode,
             std::vector<eos::fusex::container>& cont
             );

  void set_clientuuid(std::string& s)
  {
    clientuuid = s;
  }

private:

  std::string getURL(fuse_req_t req, const std::string& path, std::string op="GET", std::string authid="");
  std::string getURL(fuse_req_t req, uint64_t inode, const std::string& name, std::string op="GET", std::string authid="");
  std::string getURL(fuse_req_t req, uint64_t inode, uint64_t clock, std::string op="GET", std::string authid="");

  std::string hostport;
  std::string mount;
  std::string clientuuid;
} ;
#endif /* FUSE_BACKEND_HH_ */
