// ----------------------------------------------------------------------
// File: XrdMgmOfs.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "common/StringConversion.hh"
#include "common/SecEntity.hh"
#include "common/StackTrace.hh"
#include "common/http/OwnCloud.hh"
#include "namespace/Constants.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/XrdMgmOfsSecurity.hh"
#include "mgm/Policy.hh"
#include "mgm/Quota.hh"
#include "mgm/Acl.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <memory>
/*----------------------------------------------------------------------------*/
#include "google/protobuf/io/zero_copy_stream_impl.h"
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#define ECOMM 70
#endif

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif


/*----------------------------------------------------------------------------*/
XrdSysError gMgmOfsEroute (0);
XrdSysError *XrdMgmOfs::eDest;
XrdOucTrace gMgmOfsTrace (&gMgmOfsEroute);
const char* XrdMgmOfs::gNameSpaceState[] = {"down", "booting", "booted", "failed", "compacting"};
XrdMgmOfs* gOFS = 0;

// Set the version information
XrdVERSIONINFO(XrdSfsGetFileSystem, MgmOfs);

//------------------------------------------------------------------------------
//! Filesystem Plugin factory function
//!
//! @param native_fs (not used)
//! @param lp the logger object
//! @param configfn the configuration file name
//!
//! @returns configures and returns our MgmOfs object
//------------------------------------------------------------------------------
extern "C"
XrdSfsFileSystem *
XrdSfsGetFileSystem (XrdSfsFileSystem *native_fs,
                     XrdSysLogger *lp,
                     const char *configfn)
{
  gMgmOfsEroute.SetPrefix("MgmOfs_");
  gMgmOfsEroute.logger(lp);

  static XrdMgmOfs myFS(&gMgmOfsEroute);

  XrdOucString vs = "MgmOfs (meta data redirector) ";
  vs += VERSION;
  gMgmOfsEroute.Say("++++++ (c) 2015 CERN/IT-DSS ", vs.c_str());

  // Initialize the subsystems
  if (!myFS.Init(gMgmOfsEroute)) return 0;

  // Disable XRootd log rotation
  lp->setRotate(0);
  gOFS = &myFS;

  // By default enable stalling and redirection
  gOFS->IsStall = true;
  gOFS->IsRedirect = true;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
  if (myFS.Configure(gMgmOfsEroute)) return 0;


  // Initialize authorization module ServerAcc
  gOFS->CapabilityEngine = (XrdCapability*) XrdAccAuthorizeObject(lp, configfn, 0);
  if (!gOFS->CapabilityEngine)
    return 0;

  return gOFS;
}


/******************************************************************************/
/* MGM Meta Data Interface                                                    */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor MGM Ofs
//------------------------------------------------------------------------------
XrdMgmOfs::XrdMgmOfs (XrdSysError *ep):
mFstGwHost(""),
mFstGwPort(0)
{
  eDest = ep;
  ConfigFN = 0;
  eos::common::LogId();
  eos::common::LogId::SetSingleShotLogId();

  fsconfiglistener_tid = stats_tid = deletion_tid = 0;
  mZmqContext = new zmq::context_t(1);
}

//------------------------------------------------------------------------------
// Init function
//------------------------------------------------------------------------------
bool
XrdMgmOfs::Init (XrdSysError &ep)
{
  return true;
}

//------------------------------------------------------------------------------
// Return a MGM directory object
//------------------------------------------------------------------------------
XrdSfsDirectory *
XrdMgmOfs::newDir (char *user, int MonID)
{
  return (XrdSfsDirectory *)new XrdMgmOfsDirectory(user, MonID);
}

//------------------------------------------------------------------------------
// Return MGM file object
//------------------------------------------------------------------------------
XrdSfsFile *
XrdMgmOfs::newFile (char *user, int MonID)
{
  return (XrdSfsFile *)new XrdMgmOfsFile(user, MonID);
}


//------------------------------------------------------------------------------
// Implementation Source Code Includes
//------------------------------------------------------------------------------
#include "XrdMgmOfs/Access.cc"
#include "XrdMgmOfs/Attr.cc"
#include "XrdMgmOfs/Chksum.cc"
#include "XrdMgmOfs/Chmod.cc"
#include "XrdMgmOfs/Chown.cc"
#include "XrdMgmOfs/DeleteExternal.cc"
#include "XrdMgmOfs/Exists.cc"
#include "XrdMgmOfs/Find.cc"
#include "XrdMgmOfs/FsConfigListener.cc"
#include "XrdMgmOfs/Fsctl.cc"
#include "XrdMgmOfs/Link.cc"
#include "XrdMgmOfs/Merge.cc"
#include "XrdMgmOfs/Mkdir.cc"
#include "XrdMgmOfs/PathMap.cc"
#include "XrdMgmOfs/Remdir.cc"
#include "XrdMgmOfs/Rename.cc"
#include "XrdMgmOfs/Rm.cc"
#include "XrdMgmOfs/SendResync.cc"
#include "XrdMgmOfs/SharedPath.cc"
#include "XrdMgmOfs/ShouldRedirect.cc"
#include "XrdMgmOfs/ShouldStall.cc"
#include "XrdMgmOfs/Shutdown.cc"
#include "XrdMgmOfs/Stacktrace.cc"
#include "XrdMgmOfs/Stat.cc"
#include "XrdMgmOfs/Stripes.cc"
#include "XrdMgmOfs/Touch.cc"
#include "XrdMgmOfs/Utimes.cc"
#include "XrdMgmOfs/Version.cc"
#include "XrdMgmOfs/Auth.cc"

//------------------------------------------------------------------------------
// Test for stall rule
//------------------------------------------------------------------------------
bool
XrdMgmOfs::HasStall (const char* path,
                     const char* rule,
                     int &stalltime,
                     XrdOucString &stallmsg)
{
  if (!rule)
    return false;
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  if (Access::gStallRules.count(std::string(rule)))
  {
    stalltime = atoi(Access::gStallRules[std::string(rule)].c_str());
    stallmsg = "Attention: you are currently hold in this instance and each request is stalled for ";
    stallmsg += (int) stalltime;
    stallmsg += " seconds after an errno of type: ";
    stallmsg += rule;
    eos_static_info("info=\"stalling\" path=\"%s\" errno=\"%s\"", path, rule);
    return true;
  }
  else
  {
    return false;
  }
}

//------------------------------------------------------------------------------
// Test for redirection rule
//------------------------------------------------------------------------------
bool
XrdMgmOfs::HasRedirect (const char* path,
                        const char* rule,
                        XrdOucString &host,
                        int &port)
{
  if (!rule)
    return false;

  std::string srule = rule;
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  if (Access::gRedirectionRules.count(srule))
  {
    std::string delimiter = ":";
    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(Access::gRedirectionRules[srule],
                                            tokens, delimiter);
    if (tokens.size() == 1)
    {
      host = tokens[0].c_str();
      port = 1094;
    }
    else
    {
      host = tokens[0].c_str();
      port = atoi(tokens[1].c_str());
      if (port == 0)
        port = 1094;
    }

    eos_static_info("info=\"redirect\" path=\"%s\" host=%s port=%d errno=%s",
                    path, host.c_str(), port, rule);

    if (srule == "ENONET")
      gOFS->MgmStats.Add("RedirectENONET", 0, 0, 1);

    if (srule == "ENOENT")
      gOFS->MgmStats.Add("redirectENOENT", 0, 0, 1);

    return true;
  }
  else
    return false;
  }


//------------------------------------------------------------------------------
// Update emulated in-memory directory modification time with the current time
//------------------------------------------------------------------------------
void
XrdMgmOfs::UpdateNowInmemoryDirectoryModificationTime (eos::IContainerMD::id_t id)
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);
  return UpdateInmemoryDirectoryModificationTime(id, ts);
}


//------------------------------------------------------------------------------
// Update emulated in-memory directory modification time with a given time
//------------------------------------------------------------------------------
void
XrdMgmOfs::UpdateInmemoryDirectoryModificationTime (eos::IContainerMD::id_t id,
                                                    eos::IContainerMD::ctime_t &mtime)
{
  XrdSysMutexHelper vLock(gOFS->MgmDirectoryModificationTimeMutex);
  {
    eos::IContainerMD::id_t cid = id;
    // mtime upstream hierarchy-up-propagation
    do
    {
      try
      {
        eos::IContainerMD* dmd = gOFS->eosDirectoryService->getContainerMD(cid);
        gOFS->MgmDirectoryModificationTime[dmd->getId()].tv_sec = mtime.tv_sec;
        gOFS->MgmDirectoryModificationTime[dmd->getId()].tv_nsec = mtime.tv_nsec;
        if (!dmd->hasAttribute("sys.mtime.propagation"))
          break;
        cid = dmd->getParentId();
      }
      catch (eos::MDException &e)
      {
        break;
      }
    }
    while (cid > 1);
  }
}


//------------------------------------------------------------------------------
// Return the version of the MGM software
//------------------------------------------------------------------------------
const char *
XrdMgmOfs::getVersion ()
{
  static XrdOucString FullVersion = XrdVERSION;
  FullVersion += " MgmOfs ";
  FullVersion += VERSION;
  return FullVersion.c_str();
}


//------------------------------------------------------------------------------
// Prepare a file (EOS does nothing, only stall/redirect if configured)
//------------------------------------------------------------------------------
int
XrdMgmOfs::prepare (XrdSfsPrep &pargs,
                    XrdOucErrInfo &error,
                    const XrdSecEntity * client)
{
  const char *tident = error.getErrUser();
  eos::common::Mapping::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, 0, tident, vid);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  return SFS_OK;
}

//------------------------------------------------------------------------------
//! Truncate a file (not supported in EOS, only via the file interface)
//------------------------------------------------------------------------------
int
XrdMgmOfs::truncate (const char*,
                     XrdSfsFileOffset,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client,
                     const char* path)
{
  static const char *epname = "truncate";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, 0, tident, vid);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Truncate", vid.uid, vid.gid, 1);
  return Emsg(epname, error, EOPNOTSUPP, "truncate", path);
}

//------------------------------------------------------------------------------
// Return error message
//------------------------------------------------------------------------------
int
XrdMgmOfs::Emsg (const char *pfx,
                 XrdOucErrInfo &einfo,
                 int ecode,
                 const char *op,
                 const char *target)
{
  char *etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
  {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // Format the error message
  snprintf(buffer, sizeof (buffer), "Unable to %s %s; %s", op, target, etext);

  if ((ecode == EIDRM) || (ecode == ENODATA))
  {
    eos_debug("Unable to %s %s; %s", op, target, etext);
  }
  else
  {
    if ((!strcmp(op, "stat")) ||
        ((!strcmp(pfx,"attr_get") || (!strcmp(pfx,"attr_ls"))) && (ecode == ENOENT)))
    {
      eos_debug("Unable to %s %s; %s", op, target, etext);
    }
    else
    {
      eos_err("Unable to %s %s; %s", op, target, etext);
    }
  }

  // Print it out if debugging is enabled
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Create stall response
//------------------------------------------------------------------------------
int
XrdMgmOfs::Stall (XrdOucErrInfo &error,
                  int stime,
                  const char *msg)

{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  EPNAME("Stall");
  const char *tident = error.getErrUser();
  ZTRACE(delay, "Stall " << stime << ": " << smessage.c_str());

  // Place the error message in the error object and return
  error.setErrInfo(0, smessage.c_str());
  return stime;
}


//------------------------------------------------------------------------------
// Create redirect response
//------------------------------------------------------------------------------
int
XrdMgmOfs::Redirect (XrdOucErrInfo &error,
                     const char* host,
                     int &port)
{
  EPNAME("Redirect");
  const char *tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host << ":" << port);

  // Place the error message in the error object and return
  error.setErrInfo(port, host);
  return SFS_REDIRECT;
}


//------------------------------------------------------------------------------
// Statistics circular buffer thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartMgmStats (void *pp)
{

  XrdMgmOfs* ofs = (XrdMgmOfs*) pp;
  ofs->MgmStats.Circulate();
  return 0;
}

//------------------------------------------------------------------------------
// Filesystem error/config listener thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartMgmFsConfigListener (void *pp)
{

  XrdMgmOfs* ofs = (XrdMgmOfs*) pp;
  ofs->FsConfigListener();
  return 0;
}

