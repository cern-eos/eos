// ----------------------------------------------------------------------
// File: Fsctl.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
/*
 * @brief implements locate and space-ls function
 *
 * @param cmd operation to run
 * @param args arguments for cmd
 * @param error error object
 * @param client XRootD authentication object
 *
 * This function locate's files on the redirector and return's the available
 * space in XRootD fashion.
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::fsctl(const int cmd,
                 const char* args,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client)

{
  const char* tident = error.getErrUser();
  tlLogId.SetSingleShotLogId(tident);
  eos_thread_info("cmd=%d args=%s", cmd, args);
  int opcode = cmd & SFS_FSCTL_CMD;

  if (opcode == SFS_FSCTL_LOCATE) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->ManagerIp.c_str(),
            gOFS->ManagerPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    return SFS_DATA;
  }

  if (opcode == SFS_FSCTL_STATLS) {
    int blen = 0;
    char* buff = error.getMsgBuff(blen);
    XrdOucString space = "default";
    unsigned long long freebytes = 0;
    unsigned long long maxbytes = 0;
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);

    // Take the sum's from all file systems in 'default'
    std::string path = args;
    std::string opaque = args;
    if (path.find("?") != std::string::npos) {
      path.erase(path.find("?"));
      opaque.erase(0,opaque.find("?")+1);
    }
    
    XrdOucEnv env(opaque.c_str());
    bool query_space = false;
    
    if (env.Get("eos.space")) {
      query_space = true;
      space = env.Get("eos.space");
    }
    
    eos_thread_info("path=%s cgi=%s", path.c_str(), opaque.c_str());

    if (query_space || 
	(!getenv("EOS_MGM_STATVFS_ONLY_QUOTA") && ((path == "/") || (path == "")))) {
      if (FsView::gFsView.mSpaceView.count(space.c_str())) {
	freebytes =
	  FsView::gFsView.mSpaceView[space.c_str()]->SumLongLong("stat.statfs.freebytes",
								 false);
	maxbytes =
	  FsView::gFsView.mSpaceView[space.c_str()]->SumLongLong("stat.statfs.capacity",
							       false);
      }
    } else {
      if (path[path.length() - 1] != '/') {
	path += '/';
      }
      
        // Get quota group values for path and id 0
      auto map_quotas = Quota::GetGroupStatistics(path, 0);
      
      if (!map_quotas.empty()) {
	Quota::GetStatfs(path, maxbytes, freebytes);
      }
    }


    static const char* Resp = "oss.cgroup=%s&oss.space=%lld&oss.free=%lld"
                              "&oss.maxf=%lld&oss.used=%lld&oss.quota=%lld";
    blen = snprintf(buff, blen, Resp, space.c_str(), maxbytes,
                    freebytes, 64 * 1024 * 1024 * 1024LL /* fake 64GB */,
                    maxbytes - freebytes, maxbytes);
    error.setErrCode(blen + 1);
    return SFS_DATA;
  }

  return Emsg("fsctl", error, EOPNOTSUPP, "fsctl", args);
}

/*----------------------------------------------------------------------------*/
/*
 * @brief FS control funcition implementing the locate and plugin call
 *
 * @cmd operation to run (locate or plugin)
 * @args args for the operation
 * @error error object
 * @client XRootD authentication obeject
 *
 * This function locates files on the redirector. Additionally it is used in EOS
 * to implement many stateless operations like commit/drop a replica, stat
 * a file/directory, create a directory listing for FUSE, chmod, chown, access,
 * utimes, get checksum, schedule to drain/balance/delete ...
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::FSctl(const int cmd,
                 XrdSfsFSctl& args,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client)
{
  char ipath[16384];
  char iopaque[16384];
  static const char* epname = "FSctl";
  const char* tident = error.getErrUser();

  if (args.Arg1Len) {
    if (args.Arg1Len < 16384) {
      strncpy(ipath, args.Arg1, args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      return gOFS->Emsg(epname, error, EINVAL,
                        "convert path argument - string too long", "");
    }
  } else {
    ipath[0] = 0;
  }

  bool fusexset = false;

  // check if this is a protocol buffer injection
  if ((cmd == SFS_FSCTL_PLUGIN) && (args.Arg2Len > 5)) {
    std::string key;
    key.assign(args.Arg2, 6);

    if (key == "fusex:") {
      fusexset = true;
    }
  }

  if (!fusexset && args.Arg2Len) {
    if (args.Arg2Len < 16384) {
      strncpy(iopaque, args.Arg2, args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      return gOFS->Emsg(epname, error, EINVAL,
                        "convert opaque argument - string too long", "");
    }
  } else {
    iopaque[0] = 0;
  }

  const char* inpath = ipath;
  const char* ininfo = iopaque;
  // Do the id mapping with the opaque information
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, false);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  tlLogId.SetSingleShotLogId(tident);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  // ---------------------------------------------------------------------------
  // from here on we can deal with XrdOucString which is more 'comfortable'
  // ---------------------------------------------------------------------------
  XrdOucString spath = path;
  XrdOucString opaque = iopaque;
  XrdOucString result = "";
  XrdOucEnv env(opaque.c_str());
  const char* scmd = env.Get("mgm.pcmd");
  XrdOucString execmd = scmd ? scmd : "";

  // version and is_master is not submitted to access control
  // so that features of the instance can be retrieved by an authenticated user and
  // router front-ends can discover the activation state of the mgm
  if ((execmd != "is_master") && (execmd != "version") && !fusexset) {
    BOUNCE_NOT_ALLOWED;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("fusexset=%d %s %s", fusexset, args.Arg1, args.Arg2);
    eos_thread_debug("path=%s opaque=%s", spath.c_str(), opaque.c_str());
  }

  // ---------------------------------------------------------------------------
  // XRootD Locate
  // ---------------------------------------------------------------------------
  if ((cmd == SFS_FSCTL_LOCATE)) {
    ACCESSMODE_R;
    MAYSTALL;
    MAYREDIRECT;
    // check if this file exists
    XrdSfsFileExistence file_exists;

    if ((_exists(spath.c_str(), file_exists, error, client, 0)) ||
        (file_exists != XrdSfsFileExistIsFile)) {
      return SFS_ERROR;
    }

    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->ManagerIp.c_str(),
            gOFS->ManagerPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  if (cmd != SFS_FSCTL_PLUGIN) {
    return Emsg(epname, error, EOPNOTSUPP, "execute FSctl command [EOPNOTSUPP]",
                inpath);
  }

  // Fuse e(x)tension - this we always redirect to the RW master
  if (fusexset) {
    std::string protobuf;
    protobuf.assign(args.Arg2 + 6, args.Arg2Len - 6);
    return XrdMgmOfs::Fusex(path, ininfo, protobuf, env, error, vid, client);
  }

  if (scmd) {
    FsctlCommand command = lookupFsctl(execmd.c_str());

    switch (command) {
    case FsctlCommand::access: {
      return XrdMgmOfs::Access(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::adjustreplica: {
      return XrdMgmOfs::AdjustReplica(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::checksum: {
      return XrdMgmOfs::Checksum(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::chmod: {
      return XrdMgmOfs::Chmod(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::chown: {
      return XrdMgmOfs::Chown(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::commit: {
      return XrdMgmOfs::Commit(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::drop: {
      return XrdMgmOfs::Drop(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::event: {
      return XrdMgmOfs::Event(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::getfmd: {
      return XrdMgmOfs::Getfmd(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::getfusex: {
      return XrdMgmOfs::GetFusex(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::is_master: {
      return XrdMgmOfs::IsMaster(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::mastersignalbounce: {
      return XrdMgmOfs::MasterSignalBounce(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::mastersignalreload: {
      return XrdMgmOfs::MasterSignalReload(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::mkdir: {
      return XrdMgmOfs::Mkdir(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::open: {
      return XrdMgmOfs::Open(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::readlink: {
      return XrdMgmOfs::Readlink(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::redirect: {
      return XrdMgmOfs::Redirect(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::schedule2balance: {
      return XrdMgmOfs::Schedule2Balance(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::schedule2delete: {
      return XrdMgmOfs::Schedule2Delete(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::stat: {
      return XrdMgmOfs::FuseStat(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::statvfs: {
      return XrdMgmOfs::Statvfs(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::symlink: {
      return XrdMgmOfs::Symlink(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::txstate: {
      return XrdMgmOfs::Txstate(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::utimes: {
      return XrdMgmOfs::Utimes(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::version: {
      return XrdMgmOfs::Version(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::xattr: {
      return XrdMgmOfs::Xattr(path, ininfo, env, error, vid, client);
    }

    case FsctlCommand::INVALID: {
      eos_thread_err("No implementation for %s", execmd.c_str());
    }
    }
  }

  return Emsg(epname, error, EINVAL, "execute FSctl command [EINVAL]", inpath);
}
