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
int
XrdMgmOfs::fsctl (const int cmd,
                  const char *args,
                  XrdOucErrInfo &error,
                  const XrdSecEntity * client)
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
{
  const char *tident = error.getErrUser();

  eos::common::LogId ThreadLogId;
  ThreadLogId.SetSingleShotLogId(tident);

  eos_thread_info("cmd=%d args=%s", cmd, args);

  int opcode = cmd & SFS_FSCTL_CMD;
  if (opcode == SFS_FSCTL_LOCATE)
  {

    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->ManagerIp.c_str(), gOFS->ManagerPort);
    error.setErrInfo(strlen(locResp) + 3, (const char **) Resp, 2);
    return SFS_DATA;
  }

  if (opcode == SFS_FSCTL_STATLS)
  {
    int blen = 0;
    char* buff = error.getMsgBuff(blen);
    XrdOucString space = "default";

    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    unsigned long long freebytes = 0;
    unsigned long long maxbytes = 0;

    // -------------------------------------------------------------------------
    // take the sum's from all file systems in 'default'
    // -------------------------------------------------------------------------
    if (FsView::gFsView.mSpaceView.count("default"))
    {
      std::string path = args;
      if ((path == "/") || (path == ""))
      {
        space = "default";
        freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
        maxbytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
      }
      else
      {
        if (path.substr(path.length() - 1, 1) != "/")
        {
          path += "/";
        }
        eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
        SpaceQuota* space = Quota::GetResponsibleSpaceQuota(path.c_str());
        if (space)
        {
          space->Refresh();
          maxbytes = space->GetQuota(SpaceQuota::kAllGroupBytesTarget, 0);
          freebytes = maxbytes - space->GetQuota(SpaceQuota::kAllGroupBytesIs, 0);
        }
      }
    }

    static const char *Resp = "oss.cgroup=%s&oss.space=%lld&oss.free=%lld"
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
int
XrdMgmOfs::FSctl (const int cmd,
                  XrdSfsFSctl &args,
                  XrdOucErrInfo &error,
                  const XrdSecEntity * client)
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
{

  char ipath[16384];
  char iopaque[16384];

  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  if (args.Arg1Len)
  {
    if (args.Arg1Len < 16384)
    {
      strncpy(ipath, args.Arg1, args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    }
    else
    {
      return gOFS->Emsg(epname, error, EINVAL,
                        "convert path argument - string too long", "");
    }
  }
  else
  {
    ipath[0] = 0;
  }

  if (args.Arg2Len)
  {
    if (args.Arg2Len < 16384)
    {
      strncpy(iopaque, args.Arg2, args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    }
    else
    {
      return gOFS->Emsg(epname, error, EINVAL,
                        "convert opaque argument - string too long", "");
    }
  }
  else
  {
    iopaque[0] = 0;
  }

  const char* inpath = ipath;
  const char* ininfo = iopaque;

  // Do the id mapping with the opaque information
  eos::common::Mapping::VirtualIdentity vid;

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, false);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  eos::common::LogId ThreadLogId;
  ThreadLogId.SetSingleShotLogId(tident);

  NAMESPACEMAP;

  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;

  // ---------------------------------------------------------------------------
  // from here on we can deal with XrdOucString which is more 'comfortable'
  // ---------------------------------------------------------------------------
  XrdOucString spath = path;
  XrdOucString opaque = iopaque;
  XrdOucString result = "";
  XrdOucEnv env(opaque.c_str());

  eos_thread_debug("path=%s opaque=%s", spath.c_str(), opaque.c_str());

  // ---------------------------------------------------------------------------
  // XRootD Locate
  // ---------------------------------------------------------------------------
  if ((cmd == SFS_FSCTL_LOCATE))
  {

    ACCESSMODE_R;
    MAYSTALL;
    MAYREDIRECT;

    // check if this file exists
    XrdSfsFileExistence file_exists;
    if ((_exists(spath.c_str(), file_exists, error, client, 0)) || (file_exists != XrdSfsFileExistIsFile))
    {
      return SFS_ERROR;
    }

    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->ManagerIp.c_str(), gOFS->ManagerPort);
    error.setErrInfo(strlen(locResp) + 3, (const char **) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  if (cmd != SFS_FSCTL_PLUGIN)
  {
    return Emsg("fsctl", error, EOPNOTSUPP, "fsctl", inpath);
  }

  const char* scmd;

  if ((scmd = env.Get("mgm.pcmd")))
  {
    XrdOucString execmd = scmd;

    // -------------------------------------------------------------------------
    // Adjust replica (repairOnClose from FST)
    // -------------------------------------------------------------------------
    if (execmd == "adjustreplica")
    {
#include "fsctl/Adjustreplica.cc"
    }

    // -------------------------------------------------------------------------
    // Repair file (repair after scan error e.g. use the converter to rewrite)
    // -------------------------------------------------------------------------
    if (execmd == "rewrite")
    {
#include "fsctl/Rewrite.cc"
    }
    // -------------------------------------------------------------------------
    // Commit a replica
    // -------------------------------------------------------------------------
    if (execmd == "commit")
    {
#include "fsctl/Commit.cc"
    }

    // -------------------------------------------------------------------------
    // Drop a replica
    // -------------------------------------------------------------------------
    if (execmd == "drop")
    {
#include "fsctl/Drop.cc"
    }

    // -------------------------------------------------------------------------
    // Return's meta data in env representation
    // -------------------------------------------------------------------------
    if (execmd == "getfmd")
    {
#include "fsctl/Getfmd.cc"
    }

    // -------------------------------------------------------------------------
    // Stat a file/dir - this we always redirect to the RW master
    // -------------------------------------------------------------------------
    if (execmd == "stat")
    {
#include "fsctl/Stat.cc"
    }

    // -------------------------------------------------------------------------
    // Make a directory and return it's inode
    // -------------------------------------------------------------------------
    
    if (execmd == "mkdir")
    {
#include "fsctl/Mkdir.cc"
    }

    // -------------------------------------------------------------------------
    // chmod a dir
    // -------------------------------------------------------------------------
    if (execmd == "chmod")
    {
#include "fsctl/Chmod.cc"
    }

    // -------------------------------------------------------------------------
    // chown file/dir
    // -------------------------------------------------------------------------
    if (execmd == "chown")
    {
#include "fsctl/Chown.cc"
    }

    // -------------------------------------------------------------------------
    // check access rights
    // -------------------------------------------------------------------------
    if (execmd == "access")
    {
#include "fsctl/Access.cc"
    }

    // -------------------------------------------------------------------------
    // parallel IO mode open
    // -------------------------------------------------------------------------
    if (execmd == "open")
    {
#include "fsctl/Open.cc"
    }

    // -------------------------------------------------------------------------
    // utimes
    // -------------------------------------------------------------------------
    if (execmd == "utimes")
    {
#include "fsctl/Utimes.cc"
    }

    // -------------------------------------------------------------------------
    // parallel IO mode open
    // -------------------------------------------------------------------------
    if (execmd == "checksum")
    {
#include "fsctl/Checksum.cc"
    }

    // -------------------------------------------------------------------------
    // Return the virtual 'filesystem' stat
    // -------------------------------------------------------------------------
    if (execmd == "statvfs")
    {
#include "fsctl/Statvfs.cc"
    }

    // -------------------------------------------------------------------------
    // get/set/list/rm extended attributes
    // -------------------------------------------------------------------------
    if (execmd == "xattr")
    {
#include "fsctl/Xattr.cc"
    }

    // -------------------------------------------------------------------------
    // create a symbolic link
    // -------------------------------------------------------------------------
    if (execmd == "symlink")
    {
#include "fsctl/Symlink.cc"
    }

    // -------------------------------------------------------------------------
    // resolve a symbolic link
    // -------------------------------------------------------------------------
    if (execmd == "readlink")
    {
#include "fsctl/Readlink.cc"
    }

    // -------------------------------------------------------------------------
    // Schedule a balancer transfer
    // -------------------------------------------------------------------------
    if (execmd == "schedule2balance")
    {
#include "fsctl/Schedule2Balance.cc"
    }

    // -------------------------------------------------------------------------
    // Schedule a drain transfer
    // -------------------------------------------------------------------------
    if (execmd == "schedule2drain")
    {
#include "fsctl/Schedule2Drain.cc"
    }

    // -------------------------------------------------------------------------
    // Schedule deletion
    // -------------------------------------------------------------------------
    if (execmd == "schedule2delete")
    {
#include "fsctl/Schedule2Delete.cc"
    }

    // -------------------------------------------------------------------------
    // Set the transfer state (and log)
    // -------------------------------------------------------------------------
    if (execmd == "txstate")
    {
#include "fsctl/Txstate.cc"
    }

    if (execmd == "mastersignalbounce")
    {
      // -----------------------------------------------------------------------
      // a remote master signaled us to bounce everything to him
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;

      gOFS->MgmMaster.TagNamespaceInodes();
      gOFS->MgmMaster.RedirectToRemoteMaster();

      const char* ok = "OK";
      error.setErrInfo(strlen(ok) + 1, ok);
      return SFS_DATA;
    }

    if (execmd == "mastersignalreload")
    {
      // -----------------------------------------------------------------------
      // a remote master signaled us to reload our namespace now
      // -----------------------------------------------------------------------

      REQUIRE_SSS_OR_LOCAL_AUTH;

      gOFS->MgmMaster.WaitNamespaceFilesInSync();
      gOFS->MgmMaster.RebootSlaveNamespace();

      const char* ok = "OK";
      error.setErrInfo(strlen(ok) + 1, ok);
      return SFS_DATA;
    }


    eos_thread_err("No implementation for %s", execmd.c_str());
  }

  return Emsg(epname, error, EINVAL, "execute FSctl command", spath.c_str());
}
