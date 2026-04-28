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
#include "misc/FuseId.hh"
#include "llfusexx.hh"
#include <XrdCl/XrdClStatus.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClURL.hh>

#include <sys/statvfs.h>

class backend
{
public:

  //----------------------------------------------------------------------------
  backend();
  virtual ~backend();

  int init(std::string& hostport, std::string& remotemountdir, double& timeout,
           double& put_timeout);

  int getMD(fuse_req_t req,
            const std::string& path,
            std::vector<eos::fusex::container>& cont,
            bool listing,
            std::string authid = ""
           );

  int getMD(fuse_req_t req,
            uint64_t inode,
            const std::string& name,
            std::vector<eos::fusex::container>& cont,
            bool listing,
            std::string authid = ""
           );

  int getMD(fuse_req_t req,
            uint64_t inode,
            uint64_t myclock,
            std::vector<eos::fusex::container>& cont,
            bool listing,
            std::string authid = ""
           );

  int doLock(fuse_req_t req,
             eos::fusex::md& md,
             XrdSysMutex* locker);


  int fetchResponse(fuse_req_t req,
                    uint64_t inode,
                    std::string& url,
                    std::vector<eos::fusex::container>& cont,
                    bool cap = false
                   );


  int fetchQueryResponse(uint64_t inode,
                         std::string& url,
                         std::vector<eos::fusex::container>& cont
                        );

  int rmRf(fuse_req_t req, eos::fusex::md* md);

  //----------------------------------------------------------------------------
  //! Snapshot a file into its EOS version directory. Performs mkdir -p
  //! of the version directory, a server-side TPC copy of the source file
  //! into `<parent>/.sys.v#.<basename>/<ctime>.<fxid_hex>` (no atomic
  //! prefix — the FMD identity of `full_path` is left untouched), and
  //! finally a PurgeVersion call to enforce retention.
  //!
  //! @return 0 on success, errno-style error otherwise (logged).
  //----------------------------------------------------------------------------
  int versionFile(fuse_req_t req, const std::string& full_path, uint64_t src_ctime,
                  uint64_t src_fid, int max_versions);

  //----------------------------------------------------------------------------
  //! Rename `<parent>/.sys.v#.<from_basename>/` to
  //! `<parent>/.sys.v#.<to_basename>/`. Used to undo the MGM rename
  //! handler's behaviour of renaming a file's version directory
  //! alongside the file itself.
  //!
  //! @param parent_full_path Parent path (must end with '/').
  //! @return 0 on success, errno-style error otherwise (logged).
  //----------------------------------------------------------------------------
  int renameVersionDir(fuse_req_t req, const std::string& parent_full_path,
                       const std::string& from_basename, const std::string& to_basename);

  int putMD(fuse_req_t req, eos::fusex::md* md, std::string authid,
            XrdSysMutex* locker);
  int putMD(fuse_id& id, eos::fusex::md* md, std::string authid,
            XrdSysMutex* locker);

  int getCAP(fuse_req_t req,
             uint64_t inode,
             std::vector<eos::fusex::container>& cont
            );

  int getChecksum(fuse_req_t req,
                  uint64_t inode,
                  std::string& checksum);

  void set_clientuuid(std::string& s)
  {
    clientuuid = s;
  }

  int statvfs(fuse_req_t req, struct statvfs* stbuf);
private:

  std::string getURL(fuse_req_t req, const std::string& path,
                     std::string cmd = "fuseX",
                     std::string pcmd = "getfusex",
                     std::string op = "GET", std::string authid = "", bool setinline = false);
  std::string getURL(fuse_req_t req, uint64_t inode, const std::string& name,
                     std::string cmd = "fuseX",
                     std::string pcmd = "getfusex",
                     std::string op = "GET", std::string authid = "", bool setinline = false);
  std::string getURL(fuse_req_t req, uint64_t inode, uint64_t clock,
                     std::string cmd = "fuseX",
                     std::string pcmd = "getfusex",
                     std::string op = "GET", std::string authid = "", bool setinline = false);

  std::string hostport;
  std::string mount;
  std::string clientuuid;
  double timeout;
  double put_timeout;

  int mapErrCode(int retc);

  //----------------------------------------------------------------------------
  //! Issue a /proc/user/ command synchronously, with the user identity and
  //! standard fusex envelope (mgm.uuid, mgm.retc, mgm.cid, eos.app, fuse.v).
  //!
  //! @return 0 on success, errno-style error otherwise.
  //----------------------------------------------------------------------------
  int procCommand(fuse_req_t req, const XrdCl::URL::ParamsMap& params);

  XrdCl::XRootDStatus Query(XrdCl::URL& url,
                            XrdCl::QueryCode::Code query_code, XrdCl::Buffer& arg,
                            XrdCl::Buffer*& repsonse,
                            uint16_t timeout = 0,
                            bool noretry = false);

  std::string get_appname();
  bool use_mdquery();

};
#endif /* FUSE_BACKEND_HH_ */
