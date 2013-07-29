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
#include "XrdOuc/XrdOucTList.hh"
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
XrdOucTrace
gMgmOfsTrace (&gMgmOfsEroute);

const char* XrdMgmOfs::gNameSpaceState[] = {"down", "booting", "booted", "failed", "compacting"};

XrdMgmOfs* gOFS = 0;

/*----------------------------------------------------------------------------*/
void
xrdmgmofs_stacktrace (int sig)
/*----------------------------------------------------------------------------*/
/* @brief static function to print a stack-trace on STDERR
 * 
 * @param sig signal catched
 * 
 * After catching 'sig' and producing a stack trace the signal handler is put
 * back to the default and the signal is send again ... this is mainly used
 * to create a stack trace and a core dump after a SEGV signal.
 *
 */
/*----------------------------------------------------------------------------*/
{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack                                                                                                                                                                 
  size = backtrace(array, 10);

  // print out all the frames to stderr                                                                                                                                                                       
  fprintf(stderr, "error: received signal %d:\n", sig);

  backtrace_symbols_fd(array, size, 2);

  eos::common::StackTrace::GdbTrace("xrootd", getpid(), "where");
  eos::common::StackTrace::GdbTrace("xrootd", getpid(), "thread apply all bt");

  if (getenv("EOS_CORE_DUMP"))
  {
    eos::common::StackTrace::GdbTrace("xrootd", getpid(), "generate-core-file");
  }

  // now we put back the initial handler ...
  signal(sig, SIG_DFL);

  // ... and send the signal again
  kill(getpid(), sig);
}

/*----------------------------------------------------------------------------*/
void
xrdmgmofs_shutdown (int sig)
/*----------------------------------------------------------------------------*/
/*
 * @brief shutdown function cleaning up running threads/objects for a clean exit
 * 
 * @param sig signal catched
 * 
 * This shutdown function tries to get a write lock before doing the namespace
 * shutdown. Since it is not guaranteed that one can always get a write lock
 * there is a timeout in requiring the write lock and then the shutdown is forced.
 * Depending on the role of the MGM it stop's the running namespace follower
 * and in all cases running sub-services of the MGM.
 */
/*----------------------------------------------------------------------------*/
{

  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);

  // avoid shutdown recursions
  if (gOFS->Shutdown)
    return;

  gOFS->Shutdown = true;

  // ---------------------------------------------------------------------------
  // handler to shutdown the daemon for valgrinding and clean server stop 
  // (e.g. let's time to finish write operations)
  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: grab write mutex");
  gOFS->eosViewRWMutex.TimeoutLockWrite();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: set stall rule");
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  Access::gStallRules[std::string("*")] = "300";

  if (gOFS->ErrorLog)
  {
    XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
    int rrc = system(errorlogkillline.c_str());
    if (WEXITSTATUS(rrc))
    {
      eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
    }
  }
  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: finalizing views ... ");
  try
  {

    if (!gOFS->MgmMaster.IsMaster())
    {
      // stop the follower thread ...
      if (gOFS->eosFileService)
      {
        gOFS->eosFileService->stopSlave();
      }
      if (gOFS->eosDirectoryService)
      {
        gOFS->eosDirectoryService->stopSlave();
      }
    }

    if (gOFS->eosFsView)
    {
      gOFS->eosFsView->finalize();
      delete gOFS->eosFsView;
    }
    if (gOFS->eosView)
    {
      gOFS->eosView->finalize();
      delete gOFS->eosView;
    }
    if (gOFS->eosDirectoryService)
    {
      gOFS->eosDirectoryService->finalize();
      delete gOFS->eosDirectoryService;
    }
    if (gOFS->eosFileService)
    {
      gOFS->eosFileService->finalize();
      delete gOFS->eosFileService;
    }

  }
  catch (eos::MDException &e)
  {
    // we don't really care about any exception here!
  }

#ifdef HAVE_ZMQ
  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop ZMQ...");
  if (gOFS->zMQ)
  {
    delete gOFS->zMQ;
    gOFS->zMQ = 0;
  }
#endif

  gOFS->ConfEngine->SetAutoSave(false);

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop egroup fetching ... ");
  gOFS->EgroupRefresh.Stop();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop LRU thread ... ");
  gOFS->LRUd.Stop();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop messaging ... ");
  if (gOFS->MgmOfsMessaging)
  {
    gOFS->MgmOfsMessaging->StopListener();
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop deletion thread ... ");
  if (gOFS->deletion_tid)
  {
    XrdSysThread::Cancel(gOFS->deletion_tid);
    XrdSysThread::Join(gOFS->deletion_tid, 0);
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop statistics thread ... ");
  if (gOFS->stats_tid)
  {
    XrdSysThread::Cancel(gOFS->stats_tid);
    XrdSysThread::Join(gOFS->stats_tid, 0);
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop fs listener thread ... ");
  if (gOFS->fsconfiglistener_tid)
  {
    XrdSysThread::Cancel(gOFS->fsconfiglistener_tid);
    XrdSysThread::Join(gOFS->fsconfiglistener_tid, 0);
  }
  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: remove messaging ... ");
  if (gOFS->MgmOfsMessaging)
  {
    delete gOFS->MgmOfsMessaging;
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: cleanup quota...");
  std::map<std::string, SpaceQuota*>::const_iterator it;
  for (it = Quota::gQuota.begin(); it != Quota::gQuota.end(); it++)
  {
    delete (it->second);
  }

  // ----------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop config engine ... ");
  if (gOFS->ConfEngine)
  {
    delete gOFS->ConfEngine;
    gOFS->ConfEngine = 0;
    FsView::ConfEngine = 0;
  }

  eos_static_warning("Shutdown complete");
  kill(getpid(), 9);
}

/*----------------------------------------------------------------------------*/
extern "C"
XrdSfsFileSystem *
XrdSfsGetFileSystem (XrdSfsFileSystem *native_fs,
                     XrdSysLogger *lp,
                     const char *configfn)
/*----------------------------------------------------------------------------*/
/*
 * The Filesystem Plugin factory function
 * 
 * @param native_fs (not used)
 * @param lp the logger object
 * @param configfn the configuration file name
 * It configures and returns our MgmOfs object
 */
/*----------------------------------------------------------------------------*/
{
  gMgmOfsEroute.SetPrefix("mgmofs_");
  gMgmOfsEroute.logger(lp);

  static XrdMgmOfs myFS(&gMgmOfsEroute);

  XrdOucString vs = "MgmOfs (meta data redirector) ";
  vs += VERSION;
  gMgmOfsEroute.Say("++++++ (c) 2012 CERN/IT-DSS ", vs.c_str());

  // ---------------------------------------------------------------------------
  // Initialize the subsystems
  // ---------------------------------------------------------------------------
  if (!myFS.Init(gMgmOfsEroute)) return 0;


  // ---------------------------------------------------------------------------
  // Disable XRootd log rotation
  // ---------------------------------------------------------------------------
  lp->setRotate(0);
  gOFS = &myFS;

  // by default enable stalling and redirection
  gOFS->IsStall = true;
  gOFS->IsRedirect = true;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
  if (myFS.Configure(gMgmOfsEroute)) return 0;


  // Initialize authorization module ServerAcc
  gOFS->CapabilityEngine = (XrdCapability*) XrdAccAuthorizeObject(lp,
                                                                  configfn, 0);
  if (!gOFS->CapabilityEngine)
  {
    return 0;
  }

  return gOFS;
}

/******************************************************************************/
/******************************************************************************/
/* MGM Meta Data Interface                                                    */
/******************************************************************************/
/******************************************************************************/

/*----------------------------------------------------------------------------*/
XrdMgmOfs::XrdMgmOfs (XrdSysError *ep)
/*----------------------------------------------------------------------------*/
/* 
 * @brief the MGM Ofs object constructor
 */
/*----------------------------------------------------------------------------*/
{
  eDest = ep;
  ConfigFN = 0;
  eos::common::LogId();
  eos::common::LogId::SetSingleShotLogId();

  fsconfiglistener_tid = stats_tid = deletion_tid = 0;

}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::Init (XrdSysError &ep)
/*----------------------------------------------------------------------------*/
/* @brief Init function
 * 
 * This is just kept to be compatible with standard OFS plugins, but it is not
 * used for the moment.
 */
/*----------------------------------------------------------------------------*/
{

  return true;
}

/*----------------------------------------------------------------------------*/
XrdSfsDirectory *
XrdMgmOfs::newDir (char *user, int MonID)
/*----------------------------------------------------------------------------*/
/* 
 * @brief return a MGM directory object
 * @param user user-name
 * @param MonID monitor ID
 */
/*----------------------------------------------------------------------------*/
{
  return (XrdSfsDirectory *)new XrdMgmOfsDirectory(user, MonID);
}

/*----------------------------------------------------------------------------*/
XrdSfsFile *
XrdMgmOfs::newFile (char *user, int MonID)
/*----------------------------------------------------------------------------*/
/* 
 * @brief return a MGM file object
 * @param user user-name
 * @param MonID monitor ID
 */
/*----------------------------------------------------------------------------*/
{
  return (XrdSfsFile *)new XrdMgmOfsFile(user, MonID);
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::ShouldStall (const char* function,
                        int __AccessMode__,
                        eos::common::Mapping::VirtualIdentity &vid,
                        int &stalltime, XrdOucString &stallmsg)
/*----------------------------------------------------------------------------*/
/*
 * @brief Function to test if a client based on the called function and his 
 * @brief identity should be stalled
 * 
 * @param function name of the function to check
 * @param __AccessMode__ macro generated parameter defining if this is a reading or writing (namespace modifying) function
 * @param stalltime returns the time for a stall
 * @param stallmsg returns the message to be displayed to a user during a stall
 * @return true if client should get a stall otherwise false
 * 
 * The stall rules are defined by globals in the Access object (see Access.cc)
 */
/*----------------------------------------------------------------------------*/
{
  // ---------------------------------------------------------------------------
  // check for user, group or host banning
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  std::string smsg = "";
  stalltime = 0;
  if ((vid.uid > 3))
  {
    if (Access::gBannedUsers.count(vid.uid))
    {
      // BANNED USER
      stalltime = 300;
      smsg = "you are banned in this instance - contact an administrator";
    }
    else
      if (Access::gBannedGroups.count(vid.gid))
    {
      // BANNED GROUP
      stalltime = 300;
      smsg = "your group is banned in this instance - contact an administrator";
    }
    else
      if (Access::gBannedHosts.count(vid.host))
    {
      // BANNED HOST
      stalltime = 300;
      smsg = "your client host is banned in this instance - contact an administrator";
    }
    else
      if (Access::gStallRules.size() && (Access::gStallGlobal))
    {
      // GLOBAL STALL
      stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
      smsg = Access::gStallComment[std::string("*")];
    }
    else
      if (IS_ACCESSMODE_R && (Access::gStallRead))
    {
      // READ STALL
      stalltime = atoi(Access::gStallRules[std::string("r:*")].c_str());
      smsg = Access::gStallComment[std::string("r:*")];
    }
    else
      if (IS_ACCESSMODE_W && (Access::gStallWrite))
    {
      stalltime = atoi(Access::gStallRules[std::string("w:*")].c_str());
      smsg = Access::gStallComment[std::string("w:*")];
    }
    else
      if (Access::gStallUserGroup)
    {
      std::string usermatch = "rate:user:";
      usermatch += vid.uid_string;
      std::string groupmatch = "rate:group:";
      groupmatch += vid.gid_string;
      std::string userwildcardmatch = "rate:user:*";
      std::string groupwildcardmatch = "rate:group:*";

      std::map<std::string, std::string>::const_iterator it;
      for (it = Access::gStallRules.begin();
        it != Access::gStallRules.end();
        it++)
      {
        std::string cmd = it->first.substr(it->first.rfind(":") + 1);
        double cutoff = strtod(it->second.c_str(), 0) * 1.33;
        if ((it->first.find(userwildcardmatch) == 0))
        {
          // catch all rule = global user rate cut
          XrdSysMutexHelper statLock(gOFS->MgmStats.Mutex);
          if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
              gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
              (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff))
          {
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(groupwildcardmatch) == 0))
        {
          // catch all rule = global user rate cut
          XrdSysMutexHelper statLock(gOFS->MgmStats.Mutex);
          if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
              gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
              (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff))
          {
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(usermatch) == 0))
        {
          // check user rule 
          if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
              gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
              (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff))
          {
            // rate exceeded
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(groupmatch) == 0))
        {
          // check group rule
          if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
              gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
              (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff))
          {
            // rate exceeded
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
      }

    }
    if (stalltime)
    {
      stallmsg = "Attention: you are currently hold in this instance and each request is stalled for ";
      stallmsg += (int) stalltime;
      stallmsg += " seconds ... ";
      stallmsg += smsg.c_str();
      eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s",
                      vid.uid, vid.gid, vid.host.c_str());
      gOFS->MgmStats.Add("Stall", vid.uid, vid.gid, 1);
      return true;
    }
  }
  else
  {
    // admin/root is only stalled for global stalls not, 
    // for write-only or read-only stalls
    if (Access::gStallRules.size())
    {
      if (Access::gStallRules.count(std::string("*")))
      {
        if ((vid.host != "localhost.localdomain") && (vid.host != "localhost"))
        {
          stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
          stallmsg = "Attention: you are currently hold in this instance and each request is stalled for ";
          stallmsg += (int) stalltime;
          stallmsg += " seconds ...";
          eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s",
                          vid.uid, vid.gid, vid.host.c_str());
          gOFS->MgmStats.Add("Stall", vid.uid, vid.gid, 1);
          return true;
        }
      }
    }
  }
  eos_static_debug("info=\"allowing access to\" uid=%u gid=%u host=%s",
                   vid.uid, vid.gid, vid.host.c_str());
  return false;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::ShouldRedirect (const char* function,
                           int __AccessMode__,
                           eos::common::Mapping::VirtualIdentity &vid,
                           XrdOucString &host,
                           int &port)
/*----------------------------------------------------------------------------*/
/*
 * @brief Function to test if a client based on the called function and his identity should be redirected
 * 
 * @param function name of the function to check
 * @param __AccessMode__ macro generated parameter defining if this is a reading or writing (namespace modifying) function
 * @param host returns the target host of a redirection
 * @param port returns the target port of a redirection
 * @return true if client should get a redirected otherwise false
 * 
 * The redirection rules are defined by globals in the Access object (see Access.cc)
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  if ((vid.host == "localhost") || (vid.host == "localhost.localdomain") || (vid.uid == 0))
  {
    if (MgmMaster.IsMaster() || (IS_ACCESSMODE_R))
    {
      // the slave is redirected to the master for everything which sort of 'writes'
      return false;
    }
  }

  if (Access::gRedirectionRules.size())
  {
    bool c1 = Access::gRedirectionRules.count(std::string("*"));
    bool c3 = (IS_ACCESSMODE_R && Access::gRedirectionRules.count(std::string("r:*")));
    bool c2 = (IS_ACCESSMODE_W && Access::gRedirectionRules.count(std::string("w:*")));
    if (c1 || c2 || c3)
    {
      // redirect
      std::string delimiter = ":";
      std::vector<std::string> tokens;
      if (c1)
      {
        eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("*")], tokens, delimiter);
        gOFS->MgmStats.Add("Redirect", vid.uid, vid.gid, 1);
      }
      else
      {
        if (c2)
        {
          eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("w:*")], tokens, delimiter);
          gOFS->MgmStats.Add("RedirectW", vid.uid, vid.gid, 1);
        }
        else
        {
          if (c3)
          {
            eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("r:*")], tokens, delimiter);
            gOFS->MgmStats.Add("RedirectR", vid.uid, vid.gid, 1);
          }
        }
      }

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
      return true;
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::HasStall (const char* path,
                     const char* rule,
                     int &stalltime,
                     XrdOucString &stallmsg)
/*----------------------------------------------------------------------------*/
/* @brief Test if there is stall configured for the given rule
 * 
 * @param path the path where the rule should be checked (currently unused)
 * @param rule the rule to check e.g. rule = "ENOENT:*" meaning we send a stall if an entry is missing
 * @param stalltime returns the configured time to stall
 * @param stallmsg returns the message displayed to the client during a stall
 * @return true if there is a stall configured otherwise false
 *
 * The interface is generic to check for individual paths, but currently we just
 * implemented global rules for any paths. See Access.cc for details.
 */
/*----------------------------------------------------------------------------*/
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

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::HasRedirect (const char* path,
                        const char* rule,
                        XrdOucString &host,
                        int &port)
/*----------------------------------------------------------------------------*/
/* @brief Test if there is redirect configured for a given rule
 * 
 * @param path the path where the rule should be checked (currently unused)
 * @param rule the rule to check e.g. rule = "ENOENT:*" meaning we send a redirect if an entry is missing
 * @param host returns the redirection target host
 * @param port returns the redirection target port
 * @return true if there is a redirection configured otherwise false
 *
 * The interface is generic to check for individual paths, but currently we just
 * implemented global rules for any paths. See Access.cc for details.
 */
/*----------------------------------------------------------------------------*/
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
    {
      gOFS->MgmStats.Add("RedirectENONET", 0, 0, 1);
    }
    if (srule == "ENOENT")
    {
      gOFS->MgmStats.Add("redirectENOENT", 0, 0, 1);
    }

    return true;
  }
  else
  {
    return false;
  }
}

void
XrdMgmOfs::UpdateNowInmemoryDirectoryModificationTime (eos::ContainerMD::id_t id)
/*----------------------------------------------------------------------------*/
/* @brief Update the modification time for a directory to the current time
 * 
 * @param id container id in the namespace
 * 
 * We don't store directory modification times persistent in the namespace for
 * performance reasonse. But to give (FUSE) clients the possiblity to do 
 * caching and see when there was a modification we keep an inmemory table
 * with this modification times.
 */
/*----------------------------------------------------------------------------*/
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);
  return UpdateInmemoryDirectoryModificationTime(id, ts);
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::UpdateInmemoryDirectoryModificationTime (eos::ContainerMD::id_t id,
                                                    eos::ContainerMD::ctime_t &mtime)
/*----------------------------------------------------------------------------*/
/* @brief Update the modification time for a directory to the given time
 * 
 * @param id container id in the namespace
 * @param mtime modification time to store
 * 
 * We don't store directory modification times persistent in the namespace for
 * performance reasonse. But to give (FUSE) clients the possiblity to do 
 * caching and see when there was a modification we keep an inmemory table
 * with this modification times.
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper vLock(gOFS->MgmDirectoryModificationTimeMutex);
  gOFS->MgmDirectoryModificationTime[id].tv_sec = mtime.tv_sec;
  gOFS->MgmDirectoryModificationTime[id].tv_nsec = mtime.tv_nsec;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::ResetPathMap ()
/*----------------------------------------------------------------------------*/
/*
 * Reset all the stored entries in the path remapping table
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  PathMap.clear();
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::AddPathMap (const char* source,
                       const char* target)
/*----------------------------------------------------------------------------*/
/*
 * Add a source/target pair to the path remapping table
 * 
 * @param source prefix path to map
 * @param target target path for substitution of prefix
 * 
 * This function allows e.g. to map paths like /store/ to /eos/instance/store/
 * to provide an unprefixed global namespace in a storage federation.
 * It is used by the Configuration Engin to apply a mapping from a configuration
 * file.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  if (PathMap.count(source))
  {
    return false;
  }
  else
  {
    PathMap[source] = target;
    ConfEngine->SetConfigValue("map", source, target);
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::PathRemap (const char* inpath,
                      XrdOucString &outpath)
/*----------------------------------------------------------------------------*/
/*
 * @brief translate a path name according to the configured mapping table
 * 
 * @param inpath path to map
 * @param outpath remapped path
 * 
 * This function does the path translation according to the configured mapping
 * table. It applies the 'longest' matching rule e.g. a rule 
 * /eos/instance/store/ => /store/
 * would win over 
 * /eos/instance/ = /global/
 * if the given path matches both prefixed like '/eos/instance/store/a'
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Path cPath(inpath);

  eos::common::RWMutexReadLock lock(PathMapMutex);
  eos_debug("mappath=%s ndir=%d dirlevel=%d", inpath, PathMap.size(), cPath.GetSubPathSize() - 1);

  outpath = inpath;

  // remove double slashes
  while (outpath.replace("//", "/"))
  {
  }

  // append a / to the path
  outpath += "/";

  if (!PathMap.size())
  {
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (PathMap.count(inpath))
  {
    outpath.replace(inpath, PathMap[inpath].c_str());
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (PathMap.count(outpath.c_str()))
  {
    outpath.replace(outpath.c_str(), PathMap[outpath.c_str()].c_str());
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (!cPath.GetSubPathSize())
  {
    outpath.erase(outpath.length() - 1);
    return;
  }

  for (size_t i = cPath.GetSubPathSize() - 1; i > 0; i--)
  {
    if (PathMap.count(cPath.GetSubPath(i)))
    {
      outpath.replace(cPath.GetSubPath(i), PathMap[cPath.GetSubPath(i)].c_str());
      outpath.erase(outpath.length() - 1);
      return;
    }
  }
  outpath.erase(outpath.length() - 1);
  return;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::chksum (XrdSfsFileSystem::csFunc Func,
                   const char *csName,
                   const char *inpath,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief retrieve a checksum
 * 
 * @param func function to be performed 'csCalc','csGet' or 'csSize'
 * @param csName name of the checksum
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * We support only checksum type 'eos' which has the maximum length of 20 bytes
 * and returns a checksum based on the defined directory policy (can be adler,
 * md5,sha1 ...). The EOS directory based checksum configuration does not map
 * 1:1 to the XRootD model where a storage system supports only one flavour.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "chksum";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  char buff[MAXPATHLEN + 8];
  int rc;

  XrdOucString CheckSumName = csName;

  // ---------------------------------------------------------------------------
  // retrieve meta data for <path>
  // ---------------------------------------------------------------------------
  // A csSize request is issued usually once to verify everything is working. We
  // take this opportunity to also verify the checksum name.
  // ---------------------------------------------------------------------------

  rc = 0;

  if (Func == XrdSfsFileSystem::csSize)
  {
    if (CheckSumName == "eos")
    {
      // just return the length
      error.setErrCode(20);
      return SFS_OK;
    }
    else
    {
      strcpy(buff, csName);
      strcat(buff, " checksum not supported.");
      error.setErrInfo(ENOTSUP, buff);
      return SFS_ERROR;
    }
  }

  gOFS->MgmStats.Add("Checksum", vid.uid, vid.gid, 1);

  NAMESPACEMAP;

  XrdOucEnv Open_Env(info);

  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  eos_info("path=%s", inpath);

  // ---------------------------------------------------------------------------
  errno = 0;
  eos::FileMD* fmd = 0;
  eos::common::Path cPath(path);

  // ---------------------------------------------------------------------------
  // Everything else requires a path
  // ---------------------------------------------------------------------------

  if (!path)
  {
    strcpy(buff, csName);
    strcat(buff, " checksum path not specified.");
    error.setErrInfo(EINVAL, buff);
    return SFS_ERROR;
  }

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try
  {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
  }

  if (!fmd)
  {
    // file does not exist
    *buff = 0;
    rc = ENOENT;

    MAYREDIRECT_ENOENT;
    MAYSTALL_ENOENT;

    error.setErrInfo(rc, "no such file or directory");
    return SFS_ERROR;
  }

  // ---------------------------------------------------------------------------
  // Now determine what to do
  // ---------------------------------------------------------------------------
  if ((Func == XrdSfsFileSystem::csCalc) ||
      (Func == XrdSfsFileSystem::csGet))
  {
  }
  else
  {
    error.setErrInfo(EINVAL, "Invalid checksum function.");
    return SFS_ERROR;
  }

  // copy the checksum buffer
  const char *hv = "0123456789abcdef";
  size_t j = 0;
  for (size_t i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
  {

    buff[j++] = hv[(fmd->getChecksum().getDataPadded(i) >> 4) & 0x0f];
    buff[j++] = hv[ fmd->getChecksum().getDataPadded(i) & 0x0f];
  }
  if (j == 0)
  {
    sprintf(buff, "NONE");
  }
  else
  {
    buff[j] = '\0';
  }
  eos_info("checksum=\"%s\"", buff);
  error.setErrInfo(0, buff);
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::chmod (const char *inpath,
                  XrdSfsMode Mode,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change the mode of a directory
 * 
 * @param inpath path to chmod
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * 
 * Function calls the internal _chmod function. See info there for details.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "chmod";
  const char *tident = error.getErrUser();
  //  mode_t acc_mode = Mode & S_IAMB;

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv chmod_Env(info);

  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _chmod(path, Mode, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_chmod (const char *path,
                   XrdSfsMode Mode,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change mode of a directory
 * 
 * @param path where to chmod
 * @param Mode mode to set
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERR
 * 
 * EOS supports mode bits only on directories, file inherit them from the parent.
 * Only the owner, the admin user, the admin group, root and an ACL chmod granted
 * user are allowed to run this operation on a directory.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "chmod";

  EXEC_TIMING_BEGIN("Chmod");

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD* cmd = 0;
  eos::ContainerMD* pcmd = 0;
  eos::ContainerMD::XAttrMap attrmap;

  errno = 0;

  gOFS->MgmStats.Add("Chmod", vid.uid, vid.gid, 1);

  eos_info("path=%s mode=%o", path, Mode);

  eos::common::Path cPath(path);

  try
  {
    cmd = gOFS->eosView->getContainer(path);
    pcmd = gOFS->eosView->getContainer(cPath.GetParentPath());

    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = pcmd->attributesBegin(); it != pcmd->attributesEnd(); ++it)
    {
      attrmap[it->first] = it->second;
    }
    // acl of the parent!
    Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
            attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

    if ((cmd->getCUid() == vid.uid) || // the owner
        (!vid.uid) || // the root user
        (vid.uid == 3) || // the admin user
        (vid.gid == 4) || // the admin group
        (acl.CanChmod()))
    { // the chmod ACL entry
      // change the permission mask, but make sure it is set to a directory
      if (Mode & S_IFREG)
        Mode ^= S_IFREG;
      if ((Mode & S_ISUID))
      {
        Mode ^= S_ISUID;
      }
      else
      {
        if (!(Mode & S_ISGID))
        {
          Mode |= S_ISGID;
        }
      }
      cmd->setMode(Mode | S_IFDIR);

      // store the in-memory modification time for parent and this directory
      UpdateNowInmemoryDirectoryModificationTime(pcmd->getId());
      UpdateNowInmemoryDirectoryModificationTime(cmd->getId());

      eosView->updateContainerStore(cmd);
      errno = 0;
    }
    else
    {
      errno = EPERM;
    }
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
  };

  // ---------------------------------------------------------------------------

  if (cmd && (!errno))
  {

    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  return Emsg(epname, error, errno, "chmod", path);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_chown (const char *path,
                   uid_t uid,
                   gid_t gid,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change the owner of a file or directory
 * 
 * @param path directory path to change
 * @param uid user id to set
 * @param gid group id to set
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * Chown has only an internal implementation because XRootD does not support
 * this operation in the Ofs interface. root can alwasy run the operation.
 * Users with the admin role can run the operation. Normal users can run the operation 
 * if they have the 'c' permissions in 'sys.acl'. File ownership can only be changed
 * with the root or admin role. 
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "chown";

  EXEC_TIMING_BEGIN("Chown");

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD* cmd = 0;
  eos::FileMD* fmd = 0;
  errno = 0;

  gOFS->MgmStats.Add("Chown", vid.uid, vid.gid, 1);

  eos_info("path=%s uid=%u gid=%u", path, uid, gid);

  // try as a directory
  try
  {
    eos::ContainerMD* pcmd = 0;
    eos::ContainerMD::XAttrMap attrmap;

    eos::common::Path cPath(path);
    cmd = gOFS->eosView->getContainer(path);
    pcmd = gOFS->eosView->getContainer(cPath.GetParentPath());

    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = pcmd->attributesBegin(); it != pcmd->attributesEnd(); ++it)
    {
      attrmap[it->first] = it->second;
    }

    // acl of the parent!
    Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""), attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

    cmd = gOFS->eosView->getContainer(path);
    if ((vid.uid) && (!eos::common::Mapping::HasUid(3, vid) &&
                      !eos::common::Mapping::HasGid(4, vid)) &&
        !acl.CanChown())

    {
      errno = EPERM;
    }
    else
    {
      // change the owner
      cmd->setCUid(uid);
      if (((!vid.uid) || (vid.uid == 3) || (vid.gid == 4)) && gid)
      {
        // change the group
        cmd->setCGid(gid);
      }
      eosView->updateContainerStore(cmd);
      errno = 0;
    }
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
  };

  if (!cmd)
  {
    errno = 0;
    try
    {
      // try as a file
      eos::common::Path cPath(path);
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());

      SpaceQuota* space = Quota::GetResponsibleSpaceQuota(cPath.GetParentPath());
      eos::QuotaNode* quotanode = 0;
      if (space)
      {
        quotanode = space->GetQuotaNode();
      }

      if ((vid.uid) && (!vid.sudoer) && (vid.uid != 3) && (vid.gid != 4))
      {
        errno = EPERM;
      }
      else
      {
        fmd = gOFS->eosView->getFile(path);

        // substract the file
        if (quotanode)
        {
          quotanode->removeFile(fmd);
        }

        // change the owner
        fmd->setCUid(uid);

        // re-add the file
        if (quotanode)
        {
          quotanode->addFile(fmd);
        }

        if (!vid.uid)
        {
          if (gid)
          {
            // change the group
            fmd->setCGid(gid);
          }
          else
          {
            if (!uid)
              fmd->setCGid(uid);
          }
        }

        eosView->updateFileStore(fmd);
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
    };
  }

  // ---------------------------------------------------------------------------
  if (cmd && (!errno))
  {

    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  return Emsg(epname, error, errno, "chown", path);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::exists (const char *inpath,
                   XrdSfsFileExistence &file_exists,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief Check for the existance of a file or directory
 * 
 * @param inpath path to check existance
 * @param file_exists return parameter specifying the type (see _exists for details)
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @result SFS_OK on success otherwise SFS_ERROR
 * 
 * The function calls the internal implementation _exists. See there for details.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "exists";
  const char *tident = error.getErrUser();


  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv exists_Env(info);

  AUTHORIZE(client, &exists_Env, AOP_Stat, "execute exists", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return _exists(path, file_exists, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_exists (const char *path,
                    XrdSfsFileExistence &file_exists,
                    XrdOucErrInfo &error,
                    const XrdSecEntity *client,
                    const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief check for the existance of a file or directory
 * 
 * @param path path to check
 * @param file_exists return the type of the checked path
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if found otherwise SFS_ERROR
 * 
 * The values of file_exists are:
 * XrdSfsFileExistIsDirectory - this is a directory
 * XrdSfsFileExistIsFile - this is a file
 * XrdSfsFileExistNo - this is neither a file nor a directory
 * 
 * This function may send a redirect response and should not be used as an 
 * internal function. The internal function has as a parameter the virtual
 * identity and not the XRootD authentication object.
 */
/*----------------------------------------------------------------------------*/
{
  // try if that is directory
  EXEC_TIMING_BEGIN("Exists");

  gOFS->MgmStats.Add("Exists", vid.uid, vid.gid, 1);

  eos::ContainerMD* cmd = 0;

  {
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      cmd = gOFS->eosView->getContainer(path);
    }
    catch (eos::MDException &e)
    {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    };
    // -------------------------------------------------------------------------
  }

  if (!cmd)
  {
    // -------------------------------------------------------------------------
    // try if that is a file
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::FileMD* fmd = 0;
    try
    {
      fmd = gOFS->eosView->getFile(path);
    }
    catch (eos::MDException &e)
    {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(),
                e.getMessage().str().c_str());
    }
    // -------------------------------------------------------------------------
    if (!fmd)
    {
      file_exists = XrdSfsFileExistNo;
    }
    else
    {
      file_exists = XrdSfsFileExistIsFile;
    }
  }
  else
  {
    file_exists = XrdSfsFileExistIsDirectory;
  }

  if (file_exists == XrdSfsFileExistNo)
  {
    // get the parent directory
    eos::common::Path cPath(path);
    eos::ContainerMD* dir = 0;
    eos::ContainerMD::XAttrMap attrmap;

    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      dir = eosView->getContainer(cPath.GetParentPath());
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }
    }
    catch (eos::MDException &e)
    {
      dir = 0;
    }
    // -------------------------------------------------------------------------

    if (dir)
    {
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;

      XrdOucString redirectionhost = "invalid?";
      int ecode = 0;
      int rcode = SFS_OK;
      if (attrmap.count("sys.redirect.enoent"))
      {
        // there is a redirection setting here
        redirectionhost = "";
        redirectionhost = attrmap["sys.redirect.enoent"].c_str();
        int portpos = 0;
        if ((portpos = redirectionhost.find(":")) != STR_NPOS)
        {
          XrdOucString port = redirectionhost;
          port.erase(0, portpos + 1);
          ecode = atoi(port.c_str());
          redirectionhost.erase(portpos);
        }
        else
        {

          ecode = 1094;
        }
        rcode = SFS_REDIRECT;
        error.setErrInfo(ecode, redirectionhost.c_str());
        gOFS->MgmStats.Add("RedirectENOENT", vid.uid, vid.gid, 1);
        return rcode;
      }
    }
  }

  EXEC_TIMING_END("Exists");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_exists (const char *path,
                    XrdSfsFileExistence &file_exists,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief check for the existance of a file or directory
 * 
 * @param path path to check
 * @param file_exists return the type of the checked path
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK if found otherwise SFS_ERROR
 * 
 * The values of file_exists are:
 * XrdSfsFileExistIsDirectory - this is a directory
 * XrdSfsFileExistIsFile - this is a file
 * XrdSfsFileExistNo - this is neither a file nor a directory
 * 
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("Exists");

  gOFS->MgmStats.Add("Exists", vid.uid, vid.gid, 1);

  eos::ContainerMD* cmd = 0;

  // try if that is directory
  {
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      cmd = gOFS->eosView->getContainer(path);
    }
    catch (eos::MDException &e)
    {
      cmd = 0;
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    };
    // -------------------------------------------------------------------------  
  }

  if (!cmd)
  {
    // try if that is a file
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::FileMD* fmd = 0;
    try
    {
      fmd = gOFS->eosView->getFile(path);
    }
    catch (eos::MDException &e)
    {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
    // -------------------------------------------------------------------------

    if (!fmd)
    {
      file_exists = XrdSfsFileExistNo;
    }
    else
    {
      file_exists = XrdSfsFileExistIsFile;
    }
  }
  else
  {

    file_exists = XrdSfsFileExistIsDirectory;
  }

  EXEC_TIMING_END("Exists");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
const char *
XrdMgmOfs::getVersion ()
/*----------------------------------------------------------------------------*/
/*
 * Return the version of the MGM software
 * 
 * @return return a version string
 */
/*----------------------------------------------------------------------------*/
{

  static XrdOucString FullVersion = XrdVERSION;
  FullVersion += " MgmOfs ";
  FullVersion += VERSION;
  return FullVersion.c_str();
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::mkdir (const char *inpath,
                  XrdSfsMode Mode,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief create a directory with the given mode
 * 
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "mkdir";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv mkdir_Env(info);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  eos_info("path=%s", path);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _mkdir(path, Mode, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_mkdir (const char *path,
                   XrdSfsMode Mode,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief create a directory with the given mode
 * 
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 * 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  errno = 0;

  EXEC_TIMING_BEGIN("Mkdir");

  gOFS->MgmStats.Add("Mkdir", vid.uid, vid.gid, 1);

  //  const char *tident = error.getErrUser();

  XrdOucString spath = path;

  eos_info("path=%s\n", spath.c_str());

  if (!spath.beginswith("/"))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL,
                "create directory - you have to specifiy an absolute pathname",
                path);
  }

  bool recurse = false;

  eos::common::Path cPath(path);
  bool noParent = false;

  eos::ContainerMD* dir = 0;
  eos::ContainerMD::XAttrMap attrmap;
  eos::ContainerMD* copydir = 0;

  {
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    // check for the parent directory
    if (spath != "/")
    {
      try
      {
        dir = eosView->getContainer(cPath.GetParentPath());
        copydir = new eos::ContainerMD(*dir);
        dir = copydir;
        eos::ContainerMD::XAttrMap::const_iterator it;
        for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
        {
          attrmap[it->first] = it->second;
        }
      }
      catch (eos::MDException &e)
      {
        dir = 0;
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
        noParent = true;
      }
    }

    // check permission
    if (dir)
    {
      uid_t d_uid = dir->getCUid();
      gid_t d_gid = dir->getCGid();

      // ACL and permission check
      Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
              attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

      // Check for sys.owner.auth entries, which let people operate as the owner of the directory
      if (attrmap.count("sys.owner.auth"))
      {
        attrmap["sys.owner.auth"] += ",";
        std::string ownerkey = vid.prot.c_str();
        ownerkey += ":";
        if (vid.prot == "gsi")
        {
          ownerkey += vid.dn.c_str();
        }
        else
        {
          ownerkey += vid.name.c_str();
        }
        if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos)
        {
          eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                   path, vid.uid, vid.gid, d_uid, d_gid);
          // yes the client can operate as the owner, we rewrite the virtual identity to the directory uid/gid pair
          vid.uid = d_uid;
          vid.gid = d_gid;
        }
      }
      bool stdpermcheck = true;
      ;
      if (acl.HasAcl())
      {
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce()))
        {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      }

      // admin's can always create a directory
      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK)))
      {
        if (copydir) delete copydir;

        errno = EPERM;
        return Emsg(epname, error, EPERM, "create parent directory", cPath.GetParentPath());
      }
    }
  }

  // check if the path exists anyway
  if (Mode & SFS_O_MKPTH)
  {
    recurse = true;
    eos_debug("SFS_O_MKPATH set", path);
    // short cut if it exists already
    eos::ContainerMD* fulldir = 0;
    if (dir)
    {
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      // only if the parent exists, the full path can exist!
      try
      {
        fulldir = eosView->getContainer(path);
      }
      catch (eos::MDException &e)
      {
        fulldir = 0;
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }
      if (fulldir)
      {
        if (copydir) delete copydir;
        EXEC_TIMING_END("Exists");
        return SFS_OK;
      }
    }
  }

  eos_debug("mkdir path=%s deepness=%d dirname=%s basename=%s",
            path, cPath.GetSubPathSize(), cPath.GetParentPath(), cPath.GetName());
  eos::ContainerMD* newdir = 0;

  if (noParent)
  {
    if (recurse)
    {
      int i, j;
      // go the paths up until one exists!
      for (i = cPath.GetSubPathSize() - 1; i >= 0; i--)
      {
        eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        attrmap.clear();
        eos_debug("testing path %s", cPath.GetSubPath(i));
        try
        {
          if (copydir) delete copydir;
          dir = eosView->getContainer(cPath.GetSubPath(i));
          copydir = new eos::ContainerMD(*dir);
          eos::ContainerMD::XAttrMap::const_iterator it;
          for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
          {
            attrmap[it->first] = it->second;
          }
        }
        catch (eos::MDException &e)
        {
          dir = 0;
        }
        if (dir)
          break;
      }
      // that is really a serious problem!
      if (!dir)
      {
        if (copydir) delete copydir;
        eos_crit("didn't find any parent path traversing the namespace");
        errno = ENODATA;
        // ---------------------------------------------------------------------
        return Emsg(epname, error, ENODATA, "create directory", cPath.GetSubPath(i));
      }

      // ACL and permission check
      Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
              attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup());
      bool stdpermcheck = false;
      if (acl.HasAcl())
      {
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce()))
        {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      }
      else
      {
        stdpermcheck = true;
      }

      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK)))
      {
        if (copydir) delete copydir;
        errno = EPERM;

        return Emsg(epname, error, EPERM, "create parent directory",
                    cPath.GetParentPath());
      }


      for (j = i + 1; j < (int) cPath.GetSubPathSize(); j++)
      {
        eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        try
        {
          eos_debug("creating path %s", cPath.GetSubPath(j));
          newdir = eosView->createContainer(cPath.GetSubPath(j), recurse);
          newdir->setCUid(vid.uid);
          newdir->setCGid(vid.gid);
          newdir->setMode(dir->getMode());

          if (dir->getMode() & S_ISGID)
          {
            // inherit the attributes
            eos::ContainerMD::XAttrMap::const_iterator it;
            for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
            {
              newdir->setAttribute(it->first, it->second);
            }
          }
          // commit
          eosView->updateContainerStore(newdir);
        }
        catch (eos::MDException &e)
        {
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                    e.getErrno(), e.getMessage().str().c_str());
        }

        dir = newdir;
        if (dir)
        {
          if (copydir) delete copydir;
          copydir = new eos::ContainerMD(*dir);
          dir = copydir;
        }

        if (!newdir)
        {
          if (copydir) delete copydir;
          return Emsg(epname, error, errno, "mkdir", path);
        }
      }
    }
    else
    {
      if (copydir) delete copydir;
      errno = ENOENT;
      return Emsg(epname, error, errno, "mkdir", path);
    }
  }

  // this might not be needed, but it is detected by coverty
  if (!dir)
  {
    return Emsg(epname, error, errno, "mkdir", path);
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    newdir = eosView->createContainer(path);
    newdir->setCUid(vid.uid);
    newdir->setCGid(vid.gid);
    newdir->setMode(acc_mode);
    newdir->setMode(dir->getMode());

    // store the in-memory modification time
    eos::ContainerMD::ctime_t ctime;
    newdir->getCTime(ctime);
    UpdateInmemoryDirectoryModificationTime(dir->getId(), ctime);

    if (dir->getMode() & S_ISGID)
    {
      // inherit the attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
      {
        newdir->setAttribute(it->first, it->second);
      }
    }
    // commit on disk
    eosView->updateContainerStore(newdir);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (copydir) delete copydir;

  if (!newdir)
  {

    return Emsg(epname, error, errno, "mkdir", path);
  }

  EXEC_TIMING_END("Mkdir");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::prepare (XrdSfsPrep &pargs,
                    XrdOucErrInfo &error,
                    const XrdSecEntity * client)
/*----------------------------------------------------------------------------*/
/*
 * Prepare a file (EOS does nothing, only stall/redirect if configured)
 * 
 * @return always SFS_OK 
 */
/*----------------------------------------------------------------------------*/
{
  //  static const char *epname = "prepare";

  const char *tident = error.getErrUser();

  eos::common::Mapping::VirtualIdentity vid;

  eos::common::Mapping::IdMap(client, 0, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::rem (const char *inpath,
                XrdOucErrInfo &error,
                const XrdSecEntity *client,
                const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a file from the namespace
 * 
 * @param inpath file to delete
 * @param error error object
 * @param client XRootD authenticiation object
 * @param ininfo CGI
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Deletion supports a recycle bin. See internal implementation of _rem for details.
 */
/*----------------------------------------------------------------------------*/

{

  static const char *epname = "rem";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv env(info);

  AUTHORIZE(client, &env, AOP_Delete, "remove", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _rem(path, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_rem (const char *path,
                 XrdOucErrInfo &error,
                 eos::common::Mapping::VirtualIdentity &vid,
                 const char *ininfo,
                 bool simulate)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a file from the namespace
 * 
 * @param inpath file to delete
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @param simulate indicates 'simulate deletion' e.g. it can be used as a test if a deletion would succeed
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Deletion supports the recycle bin if configured on the parent directory of 
 * the file to be deleted. The simulation mode is used to test if there is 
 * enough space in the recycle bin to move the object. If the simulation succeeds
 * the real deletion is executed. 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "rem";

  EXEC_TIMING_BEGIN("Rm");

  eos_info("path=%s vid.uid=%u vid.gid=%u", path, vid.uid, vid.gid);

  if (!simulate)
  {
    gOFS->MgmStats.Add("Rm", vid.uid, vid.gid, 1);
  }
  // Perform the actual deletion
  //
  errno = 0;

  XrdSfsFileExistence file_exists;
  if ((_exists(path, file_exists, error, vid, 0)))
  {
    return SFS_ERROR;
  }

  if (file_exists != XrdSfsFileExistIsFile)
  {
    if (file_exists == XrdSfsFileExistIsDirectory)
      errno = EISDIR;
    else
      errno = ENOENT;

    return Emsg(epname, error, errno, "remove", path);
  }

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);

  gOFS->eosViewRWMutex.LockWrite();

  // free the booked quota
  eos::FileMD* fmd = 0;
  eos::ContainerMD* container = 0;

  eos::ContainerMD::XAttrMap attrmap;
  Acl acl;

  bool doRecycle = false; // indicating two-step deletion via recycle-bin

  try
  {
    fmd = gOFS->eosView->getFile(path);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (fmd)
  {
    eos_info("got fmd=%lld", (unsigned long long) fmd);
    try
    {
      container = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      eos_info("got container=%lld", (unsigned long long) container);
      // get the attributes out
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = container->attributesBegin(); it != container->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }
    }
    catch (eos::MDException &e)
    {
      container = 0;
    }

    // ACL and permission check
    acl.Set(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
            attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);
    bool stdpermcheck = false;
    if (acl.HasAcl())
    {
      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup());

      if ((!acl.CanWrite()) && (!acl.CanWriteOnce()))
      {
        // we have to check the standard permissions
        stdpermcheck = true;
      }
    }
    else
    {
      stdpermcheck = true;
    }

    if (container)
    {
      if (stdpermcheck && (!container->access(vid.uid, vid.gid, W_OK | X_OK)))
      {
        errno = EPERM;
        gOFS->eosViewRWMutex.UnLockWrite();
        return Emsg(epname, error, errno, "remove file", path);
      }

      // check if this directory is write-once for the mapped user
      if (acl.CanWriteOnce() && (fmd->getSize()))
      {
        gOFS->eosViewRWMutex.UnLockWrite();
        errno = EPERM;
        // this is a write once user
        return Emsg(epname, error, EPERM, "remove existing file - you are write-once user");
      }

      if ((vid.uid) && (vid.uid != container->getCUid()) &&
          (vid.uid != 3) && (vid.gid != 4) && (acl.CanNotDelete()))
      {
        gOFS->eosViewRWMutex.UnLockWrite();
        errno = EPERM;
        // deletion is forbidden for not-owner
        return Emsg(epname, error, EPERM, "remove existing file - ACL forbids file deletion");
      }
      if ((!stdpermcheck) && (!acl.CanWrite()))
      {
        gOFS->eosViewRWMutex.UnLockWrite();
        errno = EPERM;
        // this user is not allowed to write
        return Emsg(epname, error, EPERM, "remove existing file - you don't have write permissions");
      }

      // -----------------------------------------------------------------------
      // check if there is a recycling bin specified and avoid recycling of the 
      // already recycled files/dirs
      // -----------------------------------------------------------------------
      XrdOucString sPath = path;
      if (attrmap.count(Recycle::gRecyclingAttribute) &&
          (!sPath.beginswith(Recycle::gRecyclingPrefix.c_str())))
      {
        // ---------------------------------------------------------------------
        // this is two-step deletion via a recyle bin
        // ---------------------------------------------------------------------
        doRecycle = true;
      }
      else
      {
        // ---------------------------------------------------------------------
        // this is one-step deletion just removing files 'forever' and now
        // ---------------------------------------------------------------------
        if (!simulate)
        {
          eos::QuotaNode* quotanode = 0;
          try
          {
            quotanode = gOFS->eosView->getQuotaNode(container);
            eos_info("got quotanode=%lld", (unsigned long long) quotanode);
            if (quotanode)
            {
              quotanode->removeFile(fmd);
            }
          }
          catch (eos::MDException &e)
          {
            quotanode = 0;
          }
        }
      }
    }
  }

  if (!doRecycle)
  {
    try
    {
      if (!simulate)
      {
        eos_info("unlinking from view %s", path);
        gOFS->eosView->unlinkFile(path);
        if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation()))
        {
          gOFS->eosView->removeFile(fmd);
        }

        if (container)
        {
          // update the in-memory modification time
          UpdateNowInmemoryDirectoryModificationTime(container->getId());
        }
      }
      errno = 0;
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    };
  }

  if (doRecycle && (!simulate))
  {
    // -------------------------------------------------------------------------
    // two-step deletion re-cycle logic
    // -------------------------------------------------------------------------

    // copy the meta data to be able to unlock
    eos::FileMD fmdCopy(*fmd);
    fmd = &fmdCopy;
    gOFS->eosViewRWMutex.UnLockWrite();

    SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(attrmap[Recycle::gRecyclingAttribute].c_str());
    eos_info("%llu %s", namespacequota, attrmap[Recycle::gRecyclingAttribute].c_str());
    if (namespacequota)
    {
      // there is quota defined on that recycle path
      if (!namespacequota->CheckWriteQuota(fmd->getCUid(),
                                           fmd->getCGid(),
                                           fmd->getSize(),
                                           fmd->getNumLocation()))
      {
        // ---------------------------------------------------------------------
        // this is the very critical case where we have to reject to delete
        // since the recycle space is full
        // ---------------------------------------------------------------------
        errno = ENOSPC;
        return Emsg(epname,
                    error,
                    ENOSPC,
                    "remove existing file - the recycle space is full");
      }
      else
      {
        // ---------------------------------------------------------------------
        // move the file to the recycle bin
        // ---------------------------------------------------------------------
        eos::common::Mapping::VirtualIdentity rootvid;
        eos::common::Mapping::Root(rootvid);
        int rc = 0;

        Recycle lRecycle(path, attrmap[Recycle::gRecyclingAttribute].c_str(),
                         &vid, fmd->getCUid(), fmd->getCGid(), fmd->getId());

        if ((rc = lRecycle.ToGarbage(epname, error)))
        {
          return rc;
        }
      }
    }
    else
    {
      // -----------------------------------------------------------------------
      // there is no quota defined on that recycle path
      // -----------------------------------------------------------------------
      errno = ENODEV;
      return Emsg(epname,
                  error,
                  ENODEV,
                  "remove existing file - the recycle space has no quota configuration"
                  );
    }
  }
  else
  {
    gOFS->eosViewRWMutex.UnLockWrite();
  }

  EXEC_TIMING_END("Rm");

  if (errno)
    return Emsg(epname, error, errno, "remove", path);

  else
    return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::remdir (const char *inpath,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 * 
 * @param inpath directory to delete
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "remdir";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv remdir_Env(info);

  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _remdir(path, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_remdir (const char *path,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *ininfo,
                    bool simulate)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 * 
 * @param inpath directory to delete
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * We support a special ACL to forbid deletion if it would be allowed by the
 * normal POSIX settings (ACL !d flag).
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "remdir";
  errno = 0;

  eos_info("path=%s", path);

  EXEC_TIMING_BEGIN("RmDir");

  gOFS->MgmStats.Add("RmDir", vid.uid, vid.gid, 1);

  eos::ContainerMD* dhpar = 0;
  eos::ContainerMD* dh = 0;

  eos::ContainerMD::id_t dh_id = 0;
  eos::ContainerMD::id_t dhpar_id = 0;

  eos::common::Path cPath(path);
  eos::ContainerMD::XAttrMap attrmap;

  // ---------------------------------------------------------------------------
  // make sure this is not a quota node
  // ---------------------------------------------------------------------------
  {
    eos::common::RWMutexReadLock qlock(Quota::gQuotaMutex);
    if (Quota::GetSpaceQuota(path, true))
    {
      errno = EBUSY;
      return Emsg(epname, error, errno, "rmdir - this is a quota node", path);
    }
  }

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  try
  {
    dhpar = gOFS->eosView->getContainer(cPath.GetParentPath());
    dhpar_id = dhpar->getId();
    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = dhpar->attributesBegin(); it != dhpar->attributesEnd(); ++it)
    {
      attrmap[it->first] = it->second;
    }
    dh = gOFS->eosView->getContainer(path);
    dh_id = dh->getId();
  }
  catch (eos::MDException &e)
  {
    dhpar = 0;
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check existence
  if (!dh)
  {
    errno = ENOENT;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""), attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

  bool stdpermcheck = false;
  bool aclok = false;
  if (acl.HasAcl())
  {
    if ((dh->getCUid() != vid.uid) &&
        (vid.uid) && // not the root user
        (vid.uid != 3) && // not the admin user
        (vid.gid != 4) && // not the admin group
        (acl.CanNotDelete()))
    {
      // deletion is explicitly forbidden
      errno = EPERM;
      return Emsg(epname, error, EPERM, "rmdir by ACL", path);
    }

    if ((!acl.CanWrite()))
    {
      // we have to check the standard permissions
      stdpermcheck = true;
    }
    else
    {
      aclok = true;
    }
  }
  else
  {
    stdpermcheck = true;
  }


  // check permissions
  bool permok = stdpermcheck ? (dhpar ? (dhpar->access(vid.uid, vid.gid, X_OK | W_OK)) : false) : aclok;

  if (!permok)
  {
    errno = EPERM;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  if ((dh->getFlags() && eos::QUOTA_NODE_FLAG) && (vid.uid))
  {
    errno = EADDRINUSE;
    eos_err("%s is a quota node - deletion canceled", path);
    return Emsg(epname, error, errno, "rmdir", path);
  }

  if (!simulate)
  {
    try
    {
      // remove the in-memory modification time of the deleted directory
      gOFS->MgmDirectoryModificationTimeMutex.Lock();
      gOFS->MgmDirectoryModificationTime.erase(dh_id);
      gOFS->MgmDirectoryModificationTimeMutex.UnLock();
      // update the in-memory modification time of the parent directory
      UpdateNowInmemoryDirectoryModificationTime(dhpar_id);

      eosView->removeContainer(path);
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("RmDir");

  if (errno)
  {
    return Emsg(epname, error, errno, "rmdir", path);
  }
  else
  {

    return SFS_OK;
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::rename (const char *old_name,
                   const char *new_name,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *infoO,
                   const char *infoN)
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 * 
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param client XRootD authentication object
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * There are three flavours of rename function, two external and one internal
 * implementation. See the internal implementation _rename for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "rename";
  const char *tident = error.getErrUser();

  eos_info("old-name=%s new-name=%s", old_name, new_name);
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;


  eos::common::Mapping::IdMap(client, infoO, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  errno = 0;

  XrdOucString source, destination;
  XrdOucString oldn, newn;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);

  oldn = old_name;
  newn = new_name;

  {
    const char* inpath = old_name;
    const char* ininfo = infoO;
    AUTHORIZE(client, &renameo_Env, AOP_Delete, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    if (info)info = 0;
  }

  {
    const char* inpath = new_name;
    const char* ininfo = infoN;
    AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;

    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN);

}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::rename (const char *old_name,
                   const char *new_name,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity& vid,
                   const char *infoO,
                   const char *infoN)
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 * 
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * There are three flavours of rename function, two external and one internal
 * implementation. See the internal implementation _rename for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "rename";
  errno = 0;

  XrdOucString source, destination;
  XrdOucString oldn, newn;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);


  oldn = old_name;
  newn = new_name;

  {
    const char* inpath = old_name;
    const char* ininfo = infoO;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    if (info)info = 0;
  }

  {
    const char* inpath = new_name;
    const char* ininfo = infoN;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;

    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  // check access permissions on source
  if ((_access(oldn.c_str(), W_OK, error, vid, infoO) != SFS_OK))
  {
    return SFS_ERROR;
  }

  // check access permissions on target
  if ((_access(newn.c_str(), W_OK, error, vid, infoN) != SFS_OK))
  {

    return SFS_ERROR;
  }
  return _rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_rename (const char *old_name,
                    const char *new_name,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity& vid,
                    const char *infoO,
                    const char *infoN,
                    bool updateCTime,
                    bool checkQuota
                    )
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 * 
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @param updateCTime indicates to update the change time of a directory
 * @param checkQuota indicates to check the quota during a rename operation
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * There are three flavours of rename function, two external and one internal
 * implementation. 
 * Rename within a directory is simple since the quota accounting has not to 
 * be modified. Rename of directories between quota nodes need to recompute
 * all the quota of the subtree which is moving and in case reject the operation
 * if there is not enough quota left. Overall it is a quite complex function.
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "_rename";
  errno = 0;

  eos_info("source=%s target=%s", old_name, new_name);

  EXEC_TIMING_BEGIN("Rename");

  eos::common::Path oPath(old_name);
  eos::common::Path nPath(new_name);
  std::string oP = oPath.GetParentPath();
  std::string nP = nPath.GetParentPath();

  if ((!old_name) || (!new_name))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "rename - 0 source or target name");
  }

  gOFS->MgmStats.Add("Rename", vid.uid, vid.gid, 1);

  XrdSfsFileExistence file_exists;

  if (!_exists(new_name, file_exists, error, vid, infoN))
  {
    if (file_exists == XrdSfsFileExistIsFile)
    {
      errno = EEXIST;
      return Emsg(epname, error, EEXIST, "rename - target file name exists");
    }
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      errno = EEXIST;
      return Emsg(epname, error, EEXIST,
                  "rename - target directory name exists");
    }
  }

  eos::ContainerMD* dir = 0;
  eos::ContainerMD* newdir = 0;
  eos::ContainerMD* rdir = 0;
  eos::FileMD* file = 0;
  bool renameFile = false;
  bool renameDir = false;
  bool findOk = false;

  if (_exists(old_name, file_exists, error, vid, infoN))
  {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename - source does not exist");
  }
  else
  {
    if (file_exists == XrdSfsFileExistIsFile)
    {
      renameFile = true;
    }
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      renameDir = true;
    }
  }

  std::map<std::string, std::set<std::string> > found; //< list of source files if a directory is renamed
  if (renameDir)
  {
    // for directory renaming which move into a different directory 
    // we build the list of files which we are moving
    if (oP != nP)
    {
      XrdOucString stdErr;

      if (!gOFS->_find(oPath.GetFullPath().c_str(), error, stdErr, vid, found))
      {
        findOk = true;
      }
      else
      {
        return Emsg(epname, error, errno, "rename - cannot do 'find' inside the source tree");
      }
    }
  }
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock qlock(Quota::gQuotaMutex);
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dir = eosView->getContainer(oPath.GetParentPath());
    newdir = eosView->getContainer(nPath.GetParentPath());
    if (renameFile)
    {
      if (oP == nP)
      {
        file = dir->findFile(oPath.GetName());
        if (file)
        {
          eosView->renameFile(file, nPath.GetName());
          UpdateNowInmemoryDirectoryModificationTime(dir->getId());
        }
      }
      else
      {
        file = dir->findFile(oPath.GetName());
        if (file)
        {
          // move to a new directory
          dir->removeFile(oPath.GetName());
          UpdateNowInmemoryDirectoryModificationTime(dir->getId());
          UpdateNowInmemoryDirectoryModificationTime(newdir->getId());
          file->setName(nPath.GetName());
          file->setContainerId(newdir->getId());
          if (updateCTime)
          {
            file->setCTimeNow();
          }
          newdir->addFile(file);
          eosView->updateFileStore(file);
          // adjust the quota
          SpaceQuota* oldspace = Quota::GetResponsibleSpaceQuota(oP.c_str());
          SpaceQuota* newspace = Quota::GetResponsibleSpaceQuota(nP.c_str());
          eos::QuotaNode* oldquotanode = 0;
          eos::QuotaNode* newquotanode = 0;
          if (oldspace)
          {
            oldquotanode = oldspace->GetQuotaNode();
            // remove quota
            if (oldquotanode)
            {
              oldquotanode->removeFile(file);
            }
          }
          if (newspace)
          {
            newquotanode = newspace->GetQuotaNode();
            // add quota
            if (newquotanode)
            {
              newquotanode->addFile(file);
            }
          }
        }
      }
    }
    if (renameDir)
    {
      rdir = dir->findContainer(oPath.GetName());
      if (rdir)
      {
        // remove all the quota from the source node and add to the target node

        std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
        std::set<std::string>::const_iterator fileit;
        // loop over all the files and subtract them from their quota node
        if (findOk)
        {
          if (checkQuota)
          {
            std::map<uid_t, unsigned long long> user_deletion_size;
            std::map<gid_t, unsigned long long> group_deletion_size;
            // -----------------------------------------------------------------
            // compute the total quota we need to rename by uid/gid
            // -----------------------------------------------------------------s
            for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
            {
              for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
              {
                std::string fspath = rfoundit->first;
                fspath += *fileit;
                eos::FileMD* fmd = 0;
                // stat this file and add to the deletion maps

                try
                {
                  fmd = gOFS->eosView->getFile(fspath.c_str());
                }
                catch (eos::MDException &e)
                {
                  errno = e.getErrno();
                  eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                            e.getErrno(), e.getMessage().str().c_str());
                }

                if (!fmd)
                {
                  return Emsg(epname, error, errno, "rename - cannot stat file in subtree", fspath.c_str());
                }
                user_deletion_size[fmd->getCUid()] += (fmd->getSize() * fmd->getNumLocation());
                group_deletion_size[fmd->getCGid()] += (fmd->getSize() * fmd->getNumLocation());
              }
            }
            // -----------------------------------------------------------------
            // verify for each uid/gid that there is enough quota to rename
            // -----------------------------------------------------------------
            bool userok = true;
            bool groupok = true;

            // either all have user quota then userok is true
            for (auto it = user_deletion_size.begin(); it != user_deletion_size.end(); it++)
            {
              SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(nP.c_str());

              if (namespacequota)
              {
                // there is quota defined on that recycle path
                if (!namespacequota->CheckWriteQuota(it->first,
                                                     Quota::gProjectId,
                                                     it->second,
                                                     1))
                {
                  userok = false;
                }
              }
            }

            // or all have group quota then groupok is true
            for (auto it = group_deletion_size.begin(); it != group_deletion_size.end(); it++)
            {
              SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(nP.c_str());

              if (namespacequota)
              {
                // there is quota defined on that recycle path
                if (!namespacequota->CheckWriteQuota(Quota::gProjectId,
                                                     it->first,
                                                     it->second,
                                                     1))
                {
                  groupok = false;
                }
              }
            }

            if ((!userok) && (!groupok))
            {
              // deletion has to fail there is not enough quota on the target
              return Emsg(epname, error, ENOSPC, "rename - cannot get all the needed quota for the target directory");

            }
          } // if (checkQuota)

          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
          {
            for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
            {
              std::string fspath = rfoundit->first;
              fspath += *fileit;
              file = gOFS->eosView->getFile(fspath.c_str());
              if (file)
              {
                SpaceQuota* oldspace = Quota::GetResponsibleSpaceQuota(fspath.c_str()); // quota node from file path
                SpaceQuota* newspace = Quota::GetResponsibleSpaceQuota(nP.c_str()); // quota node of target directory
                eos::QuotaNode* oldquotanode = 0;
                eos::QuotaNode* newquotanode = 0;
                if (oldspace)
                {
                  oldquotanode = oldspace->GetQuotaNode();
                  // remove quota
                  if (oldquotanode)
                  {
                    oldquotanode->removeFile(file);
                  }
                }
                if (newspace)
                {
                  newquotanode = newspace->GetQuotaNode();
                  // add quota
                  if (newquotanode)
                  {
                    newquotanode->addFile(file);
                  }
                }
              }
            }
          }
        }
        if (nP == oP)
        {
          // -------------------------------------------------------------------
          // rename within a container
          // -------------------------------------------------------------------
          eosView->renameContainer(rdir, nPath.GetName());
          UpdateNowInmemoryDirectoryModificationTime(rdir->getId());
        }
        else
        {
          // -------------------------------------------------------------------
          // move from one container to another one
          // -------------------------------------------------------------------
          dir->removeContainer(oPath.GetName());
          UpdateNowInmemoryDirectoryModificationTime(dir->getId());
          rdir->setName(nPath.GetName());
          if (updateCTime)
          {
            rdir->setCTimeNow();
          }
          newdir->addContainer(rdir);
          UpdateNowInmemoryDirectoryModificationTime(newdir->getId());
          eosView->updateContainerStore(rdir);
        }
      }
      file = 0;
    }
  }
  catch (eos::MDException &e)
  {
    dir = 0;
    file = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if ((!dir) || ((!file) && (!rdir)))
  {

    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename", old_name);
  }

  EXEC_TIMING_END("Rename");

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::stat (const char *inpath,
                 struct stat *buf,
                 XrdOucErrInfo &error,
                 const XrdSecEntity *client,
                 const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief return stat information for a given path
 * 
 * @param inpath path to stat
 * @param buf stat buffer where to store the stat information
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * See the internal implemtation _stat for details.
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "stat";
  const char *tident = error.getErrUser();


  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv Open_Env(info);

  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid, false);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  errno = 0;
  int rc = _stat(path, buf, error, vid, info);
  if (rc && (errno == ENOENT))
  {

    MAYREDIRECT_ENOENT;
    MAYSTALL_ENOENT;
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_stat (const char *path,
                  struct stat *buf,
                  XrdOucErrInfo &error,
                  eos::common::Mapping::VirtualIdentity &vid,
                  const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief return stat information for a given path
 * 
 * @param inpath path to stat
 * @param buf stat buffer where to store the stat information
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 * 
 * We don't apply any access control on stat calls for performance reasons.
 * Modification times of directories are only emulated and returned from an 
 * in-memory map.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_stat";

  EXEC_TIMING_BEGIN("Stat");


  gOFS->MgmStats.Add("Stat", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  // try if that is a file
  errno = 0;
  eos::FileMD* fmd = 0;
  eos::common::Path cPath(path);

  // ---------------------------------------------------------------------------
  // a stat on the master proc entry succeeds
  // only, if this MGM is in RW master mode
  // ---------------------------------------------------------------------------

  if (cPath.GetFullPath() == gOFS->MgmProcMasterPath)
  {
    if (!gOFS->MgmMaster.IsMaster())
    {
      return Emsg(epname, error, ENOENT, "stat", cPath.GetPath());
    }
  }
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try
  {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // ---------------------------------------------------------------------------
  if (fmd)
  {
    eos::FileMD fmdCopy(*fmd);
    fmd = &fmdCopy;
    memset(buf, 0, sizeof (struct stat));

    buf->st_dev = 0xcaff;
    buf->st_ino = fmd->getId() << 28;
    buf->st_mode = S_IFREG;
    buf->st_mode |= (S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR);
    buf->st_nlink = fmd->getNumLocation();
    buf->st_uid = fmd->getCUid();
    buf->st_gid = fmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_size = fmd->getSize();
    buf->st_blksize = 512;
    buf->st_blocks = Quota::MapSizeCB(fmd) / 512; // including layout factor
    eos::FileMD::ctime_t atime;

    // adding also nanosecond to stat struct
    fmd->getCTime(atime);
#ifdef __APPLE__
    buf->st_ctimespec.tv_sec = atime.tv_sec;
    buf->st_ctimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_ctime = atime.tv_sec;
    buf->st_ctim.tv_sec = atime.tv_sec;
    buf->st_ctim.tv_nsec = atime.tv_nsec;
#endif

    fmd->getMTime(atime);

#ifdef __APPLE__
    buf->st_mtimespec.tv_sec = atime.tv_sec;
    buf->st_mtimespec.tv_nsec = atime.tv_nsec;

    buf->st_atimespec.tv_sec = atime.tv_sec;
    buf->st_atimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_mtime = atime.tv_sec;
    buf->st_mtim.tv_sec = atime.tv_sec;
    buf->st_mtim.tv_nsec = atime.tv_nsec;

    buf->st_atime = atime.tv_sec;
    buf->st_atim.tv_sec = atime.tv_sec;
    buf->st_atim.tv_nsec = atime.tv_nsec;
#endif
    EXEC_TIMING_END("Stat");
    return SFS_OK;
  }

  // try if that is directory
  eos::ContainerMD* cmd = 0;
  errno = 0;

  // ---------------------------------------------------------------------------
  try
  {
    cmd = gOFS->eosView->getContainer(cPath.GetPath());

    memset(buf, 0, sizeof (struct stat));

    buf->st_dev = 0xcaff;
    buf->st_ino = cmd->getId();
    buf->st_mode = cmd->getMode();
    if (cmd->attributesBegin() != cmd->attributesEnd())
    {
      buf->st_mode |= S_ISVTX;
    }
    buf->st_nlink = cmd->getNumContainers() + cmd->getNumFiles() + 1;
    buf->st_uid = cmd->getCUid();
    buf->st_gid = cmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_size = cmd->getNumContainers();
    buf->st_blksize = 0;
    buf->st_blocks = 0;
    eos::ContainerMD::ctime_t atime;
    cmd->getCTime(atime);

#ifdef __APPLE__
    buf->st_atimespec.tv_sec = atime.tv_sec;
    buf->st_mtimespec.tv_sec = atime.tv_sec;
    buf->st_ctimespec.tv_sec = atime.tv_sec;
    buf->st_atimespec.tv_nsec = atime.tv_nsec;
    buf->st_mtimespec.tv_nsec = atime.tv_nsec;
    buf->st_ctimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_atime = atime.tv_sec;
    buf->st_mtime = atime.tv_sec;
    buf->st_ctime = atime.tv_sec;

    buf->st_atim.tv_sec = atime.tv_sec;
    buf->st_mtim.tv_sec = atime.tv_sec;
    buf->st_ctim.tv_sec = atime.tv_sec;
    buf->st_atim.tv_nsec = atime.tv_nsec;
    buf->st_mtim.tv_nsec = atime.tv_nsec;
    buf->st_ctim.tv_nsec = atime.tv_nsec;
#endif    

    // if we have a cached modification time, return that one
    // -->
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    if (gOFS->MgmDirectoryModificationTime.count(buf->st_ino))
    {
#ifdef __APPLE__
      buf->st_mtimespec.tv_sec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_sec;
      buf->st_mtimespec.tv_nsec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_nsec;
#else
      buf->st_mtime = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_sec;
      buf->st_mtim.tv_sec = buf->st_mtime;
      buf->st_mtim.tv_nsec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_nsec;
#endif
    }
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();
    // --|
    return SFS_OK;
  }
  catch (eos::MDException &e)
  {

    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
    return Emsg(epname, error, errno, "stat", cPath.GetPath());
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::lstat (const char *path,
                  struct stat *buf,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief stat following links (not existing in EOS - behaves like stat)
 */
/*----------------------------------------------------------------------------*/
{
  return stat(path, buf, error, client, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::truncate (const char*,
                     XrdSfsFileOffset,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client,
                     const char* path)
/*----------------------------------------------------------------------------*/
/*
 * @brief truncate a file ( not supported in EOS, only via the file interface )
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "truncate";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  eos::common::Mapping::IdMap(client, 0, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Truncate", vid.uid, vid.gid, 1);
  return Emsg(epname, error, EOPNOTSUPP, "truncate", path);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::access (const char *inpath,
                   int mode,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 * 
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK or F_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 * 
 * See the internal implementation _access for details
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "access";
  const char *tident = error.getErrUser();


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return _access(path, mode, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_access (const char *path,
                    int mode,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 * 
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK or F_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 * 
 * If F_OK is specified we just check for the existance of the path, which can
 * be a file or directory. We don't support X_OK since it cannot be mapped
 * in case of files (we don't have explicit execution permissions).
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_access";

  eos_info("path=%s mode=%x uid=%u gid=%u", path, mode, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);

  eos::common::Path cPath(path);

  eos::ContainerMD* dh = 0;
  eos::FileMD* fh = 0;
  bool permok = false;
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  // check for existing file
  try
  {
    fh = gOFS->eosView->getFile(cPath.GetPath());
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  errno = 0;
  try
  {
    eos::ContainerMD::XAttrMap attrmap;
    if (fh || (!dh))
    {
      // if this is a file or a not existing directory we check the access on the parent directory
      eos_debug("path=%s", cPath.GetParentPath());
      dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    }

    permok = dh->access(vid.uid, vid.gid, mode);

    if (!permok)
    {
      // get attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }
      // ACL and permission check
      Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
              attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid);

      eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.CanBrowse(), acl.HasEgroup());

      // browse permission by ACL
      if (acl.HasAcl())
      {
        if ((mode & W_OK) && acl.CanWrite())
        {
          permok = true;
        }

        if ((mode & R_OK) && acl.CanRead())
        {
          permok = true;
        }

        if ((mode & R_OK) && acl.CanBrowse())
        {
          permok = true;
        }
        else
        {
          permok = false;
        }

      }
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions

  if (!dh)
  {
    eos_debug("msg=\"access\" errno=ENOENT");
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "access", path);
  }

  // root/daemon can always access
  if ((vid.uid == 0) || (vid.uid == 2))
    permok = true;

  if (dh)
  {
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=%o",
              vid.uid, vid.gid, permok, dh->getMode());
  }

  if (dh && (mode & F_OK))
  {
    return SFS_OK;
  }

  if (dh && permok)
  {
    return SFS_OK;
  }

  if (dh && (!permok))
  {

    errno = EACCES;
    return Emsg(epname, error, EACCES, "access", path);
  }

  errno = EOPNOTSUPP;
  return Emsg(epname, error, EOPNOTSUPP, "access", path);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::utimes (const char *inpath,
                   struct timespec *tvp,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief set change time for a given file/directory
 * 
 * @param inpath path to set
 * @param tvp timespec structure
 * @param error error object
 * @client XRootD authentication object
 * @ininfo CGI
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "utimes";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv utimes_Env(info);

  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _utimes(path, tvp, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_utimes (const char *path,
                    struct timespec *tvp,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief set change time for a given file/directory
 * 
 * @param path path to set
 * @param tvp timespec structure
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * For directories this routine set's the creation time and the in-memory
 * modification time to the specified modificationt time. For files it 
 * set's the modification time.
 */
/*----------------------------------------------------------------------------*/
{
  bool done = false;
  eos::ContainerMD* cmd = 0;

  EXEC_TIMING_BEGIN("Utimes");

  gOFS->MgmStats.Add("Utimes", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    cmd = gOFS->eosView->getContainer(path);
    UpdateInmemoryDirectoryModificationTime(cmd->getId(), tvp[1]);
    eosView->updateContainerStore(cmd);
    done = true;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!cmd)
  {
    eos::FileMD* fmd = 0;
    // try as a file
    try
    {
      fmd = gOFS->eosView->getFile(path);
      fmd->setMTime(tvp[1]);
      eosView->updateFileStore(fmd);
      done = true;
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("Utimes");

  if (!done)
  {


  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_find (const char *path,
                  XrdOucErrInfo &out_error,
                  XrdOucString &stdErr,
                  eos::common::Mapping::VirtualIdentity &vid,
                  std::map<std::string, std::set<std::string> > &found,
                  const char* key,
                  const char* val,
                  bool nofiles,
                  time_t millisleep
                  )
/*----------------------------------------------------------------------------*/
/*
 * @brief low-level namespace find command
 * 
 * @param path path to start the sub-tree find
 * @param stdErr stderr output string
 * @param vid virtual identity of the client
 * @param found result map/set of the find
 * @param key search for a certain key in the extended attributes
 * @param val search for a certain value in the extended attributes (requires key)
 * @param nofiles if true returns only directories, otherwise files and directories
 * @param millisleep milli seconds to sleep between each directory scan
 * 
 * The find command distinuishes 'power' and 'normal' users. If the virtual
 * identity indicates the root or admin user queries are unlimited.
 * For others queries are limited to 50k directories and 100k files and an 
 * appropriate error/warning message is written to stdErr. Note that currently
 * find does not do a 'full' permission check including ACLs in every
 * subdirectory but checks only the POSIX permission R_OK/X_OK bits.
 * If 'key' contains a wildcard character in the end find produces a list of 
 * directories containing an attribute starting with that key match like
 * var=sys.policy.*
 * The millisleep variable allows to slow down full scans to decrease impact
 * when doing large scans.
 * 
 */
/*----------------------------------------------------------------------------*/
{
  std::vector< std::vector<std::string> > found_dirs;

  // try if that is directory
  eos::ContainerMD* cmd = 0;
  std::string Path = path;
  XrdOucString sPath = path;
  errno = 0;
  XrdSysTimer snooze;

  EXEC_TIMING_BEGIN("Find");

  gOFS->MgmStats.Add("Find", vid.uid, vid.gid, 1);

  if (!(sPath.endswith('/')))
    Path += "/";

  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = Path.c_str();
  int deepness = 0;

  // users cannot return more than 100k files and 50k dirs with one find

  static unsigned long long finddiruserlimit = 50000;
  static unsigned long long findfileuserlimit = 100000;

  unsigned long long filesfound = 0;
  unsigned long long dirsfound = 0;

  bool limitresult = false;
  bool limited = false;

  if ((vid.uid != 0) && (!eos::common::Mapping::HasUid(3, vid.uid_list)) &&
      (!eos::common::Mapping::HasGid(4, vid.gid_list)) && (!vid.sudoer))
  {
    limitresult = true;
  }

  do
  {
    bool permok = false;

    found_dirs.resize(deepness + 2);
    // loop over all directories in that deepness
    for (unsigned int i = 0; i < found_dirs[deepness].size(); i++)
    {
      Path = found_dirs[deepness][i].c_str();
      eos_static_debug("Listing files in directory %s", Path.c_str());

      if (millisleep)
      {
        // slow down the find command without having locks
        snooze.Wait(millisleep);
      }
      // -----------------------------------------------------------------------
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      try
      {
        cmd = gOFS->eosView->getContainer(Path.c_str());
        permok = cmd->access(vid.uid, vid.gid, R_OK | X_OK);
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        cmd = 0;
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }

      if (cmd)
      {
        if (!permok)
        {
          stdErr += "error: no permissions to read directory ";
          stdErr += Path.c_str();
          stdErr += "\n";
          continue;
        }

        // add all children into the 2D vectors
        eos::ContainerMD::ContainerMap::iterator dit;
        for (dit = cmd->containersBegin(); dit != cmd->containersEnd(); ++dit)
        {
          std::string fpath = Path.c_str();
          fpath += dit->second->getName();
          fpath += "/";
          // check if we select by tag
          if (key)
          {
            XrdOucString wkey = key;
            if (wkey.find("*") != STR_NPOS)
            {
              // this is a search for 'beginswith' match
              eos::ContainerMD::XAttrMap attrmap;
              if (!gOFS->_attr_ls(fpath.c_str(),
                                  out_error,
                                  vid,
                                  (const char*) 0,
                                  attrmap))
              {
                for (auto it = attrmap.begin(); it != attrmap.end(); it++)
                {
                  XrdOucString akey = it->first.c_str();
                  if (akey.matches(wkey.c_str()))
                  {
                    found[fpath].size();
                  }
                }
              }
              found_dirs[deepness + 1].push_back(fpath.c_str());
            }
            else
            {
              // this is a search for a full match 

              std::string sval = val;
              XrdOucString attr = "";
              if (!gOFS->_attr_get(fpath.c_str(), out_error, vid,
                                   (const char*) 0, key, attr, true))
              {
                found_dirs[deepness + 1].push_back(fpath.c_str());
                if (attr == val)
                {
                  found[fpath].size();
                } 
              }
            }
          }
          else
          {
            if (limitresult)
            {
              // apply the user limits for non root/admin/sudoers
              if (dirsfound >= finddiruserlimit)
              {
                stdErr += "warning: find results are limited for users to ndirs=";
                stdErr += (int) finddiruserlimit;
                stdErr += " -  result is truncated!\n";
                limited = true;
                break;
              }
            }
            found_dirs[deepness + 1].push_back(fpath.c_str());
            found[fpath].size();
            dirsfound++;
          }
        }

        if (!nofiles)
        {
          eos::ContainerMD::FileMap::iterator fit;
          for (fit = cmd->filesBegin(); fit != cmd->filesEnd(); ++fit)
          {
            if (limitresult)
            {
              // apply the user limits for non root/admin/sudoers
              if (filesfound >= findfileuserlimit)
              {
                stdErr += "warning: find results are limited for users to nfiles=";
                stdErr += (int) findfileuserlimit;
                stdErr += " -  result is truncated!\n";
                limited = true;
                break;
              }
            }
            found[Path].insert(fit->second->getName());
            filesfound++;
          }
        }
      }
      if (limited)
      {
        break;
      }
    }

    deepness++;
    if (limited)
    {
      break;
    }
  }
  while (found_dirs[deepness].size());
  // ---------------------------------------------------------------------------  
  if (!nofiles)
  {
    // if the result is empty, maybe this was a find by file
    if (!found.size())
    {
      XrdSfsFileExistence file_exists;
      if (((_exists(Path.c_str(), file_exists, out_error, vid, 0)) == SFS_OK) &&
          (file_exists == XrdSfsFileExistIsFile))
      {
        eos::common::Path cPath(Path.c_str());
        found[cPath.GetParentPath()].insert(cPath.GetName());
      }
    }
  }
  // ---------------------------------------------------------------------------  
  // include also the directory which was specified in the query if it is 
  // accessible and a directory since it can evt. be missing if it is empty
  // ---------------------------------------------------------------------------
  XrdSfsFileExistence dir_exists;
  if (((_exists(found_dirs[0][0].c_str(), dir_exists, out_error, vid, 0)) == SFS_OK)
      && (dir_exists == XrdSfsFileExistIsDirectory))
  {

    eos::common::Path cPath(found_dirs[0][0].c_str());
    found[found_dirs[0][0].c_str()].size();
  }

  EXEC_TIMING_END("Find");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_touch (const char *path,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief create(touch) a no-replica file in the namespace
 * 
 * @param path file to touch
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * 
 * Access control is not fully done here, just the POSIX write flag is checked,
 * no ACLs ...
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("Touch");

  eos_info("path=%s vid.uid=%u vid.gid=%u", path, vid.uid, vid.gid);


  gOFS->MgmStats.Add("Touch", vid.uid, vid.gid, 1);

  // Perform the actual deletion
  //
  errno = 0;
  eos::FileMD* fmd = 0;

  if (_access(path, W_OK, error, vid, ininfo))
  {
    return SFS_ERROR;
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    fmd = gOFS->eosView->getFile(path);
    errno = 0;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  try
  {
    if (!fmd)
    {
      fmd = gOFS->eosView->createFile(path, vid.uid, vid.gid);
      fmd->setCUid(vid.uid);
      fmd->setCGid(vid.gid);
      fmd->setCTimeNow();
      fmd->setSize(0);
    }
    fmd->setMTimeNow();
    gOFS->eosView->updateFileStore(fmd);
    errno = 0;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }
  if (errno)
  {
    return Emsg("utimes", error, errno, "touch", path);
  }
  EXEC_TIMING_END("Touch");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Emsg (const char *pfx,
                 XrdOucErrInfo &einfo,
                 int ecode,
                 const char *op,
                 const char *target)
/*----------------------------------------------------------------------------*/
/*
 * @brief create an error message
 * 
 * @param pfx message prefix value
 * @param einfo error text/code object
 * @param ecode error code
 * @param op name of the operation performed
 * @param target target of the operation e.g. file name etc.
 * 
 * @return SFS_ERROR in all cases
 * 
 * This routines prints also an error message into the EOS log if it was not
 * due to a stat call or the error codes EIDRM or ENODATA
 */
/*----------------------------------------------------------------------------*/
{
  char *etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  //
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
  {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // Format the error message
  //


  snprintf(buffer, sizeof (buffer), "Unable to %s %s; %s", op, target, etext);

  if ((ecode == EIDRM) || (ecode == ENODATA))
  {
    eos_debug("Unable to %s %s; %s", op, target, etext);
  }
  else
  {
    if ((!strcmp(op, "stat")))
    {
      eos_debug("Unable to %s %s; %s", op, target, etext);
    }
    else
    {

      eos_err("Unable to %s %s; %s", op, target, etext);
    }
  }

  // ---------------------------------------------------------------------------
  // Print it out if debugging is enabled
  // ---------------------------------------------------------------------------
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsDirectory::Emsg (const char *pfx,
                          XrdOucErrInfo &einfo,
                          int ecode,
                          const char *op,
                          const char *target)
/*----------------------------------------------------------------------------*/
/*
 * @brief create an error message for a directory object
 * 
 * @param pfx message prefix value
 * @param einfo error text/code object
 * @param ecode error code
 * @param op name of the operation performed
 * @param target target of the operation e.g. file name etc.
 * 
 * @return SFS_ERROR in all cases
 * 
 * This routines prints also an error message into the EOS log.
 */
/*----------------------------------------------------------------------------*/
{
  char *etext, buffer[4096], unkbuff[64];

  // ---------------------------------------------------------------------------
  // Get the reason for the error
  // ---------------------------------------------------------------------------
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
  {

    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // ---------------------------------------------------------------------------
  // Format the error message
  // ---------------------------------------------------------------------------
  snprintf(buffer, sizeof (buffer), "Unable to %s %s; %s", op, target, etext);

  eos_err("Unable to %s %s; %s", op, target, etext);

  // ---------------------------------------------------------------------------
  // Print it out if debugging is enabled
  // ---------------------------------------------------------------------------
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::Emsg (const char *pfx,
                     XrdOucErrInfo &einfo,
                     int ecode,
                     const char *op,
                     const char *target)
/*----------------------------------------------------------------------------*/
/*
 * @brief create an error message for a file object
 * 
 * @param pfx message prefix value
 * @param einfo error text/code object
 * @param ecode error code
 * @param op name of the operation performed
 * @param target target of the operation e.g. file name etc.
 * 
 * @return SFS_ERROR in all cases
 * 
 * This routines prints also an error message into the EOS log.
 */
/*----------------------------------------------------------------------------*/
{
  char *etext, buffer[4096], unkbuff[64];

  // ---------------------------------------------------------------------------
  // Get the reason for the error
  // ---------------------------------------------------------------------------
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
  {

    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // ---------------------------------------------------------------------------
  // Format the error message
  // ---------------------------------------------------------------------------
  snprintf(buffer, sizeof (buffer), "Unable to %s %s; %s", op, target, etext);

  eos_err("Unable to %s %s; %s", op, target, etext);

  // ---------------------------------------------------------------------------
  // Print it out if debugging is enabled
  // ---------------------------------------------------------------------------
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif

  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  einfo.setErrInfo(ecode, buffer);

  return SFS_ERROR;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Stall (XrdOucErrInfo &error,
                  int stime,
                  const char *msg)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a stall response to the client
 * 
 * @param error error object with text/code
 * @param stime seconds to stall
 * @param msg message for the client
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";

  EPNAME("Stall");
  const char *tident = error.getErrUser();

  ZTRACE(delay, "Stall " << stime << ": " << smessage.c_str());

  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  error.setErrInfo(0, smessage.c_str());

  // ---------------------------------------------------------------------------
  // All done
  // ---------------------------------------------------------------------------
  return stime;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Redirect (XrdOucErrInfo &error,
                     const char* host,
                     int &port)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a redirect response to the client
 * 
 * @param error error object with text/code
 * @param host redirection target host
 * @param port redirection target port
 */
/*----------------------------------------------------------------------------*/
{

  EPNAME("Redirect");
  const char *tident = error.getErrUser();

  ZTRACE(delay, "Redirect " << host << ":" << port);

  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  error.setErrInfo(port, host);

  // ---------------------------------------------------------------------------
  // All done
  // ---------------------------------------------------------------------------
  return SFS_REDIRECT;
}

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

    eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);

    unsigned long long freebytes = 0;
    unsigned long long maxbytes = 0;

    // -------------------------------------------------------------------------
    // take the sum's from all file systems in 'default'
    // -------------------------------------------------------------------------
    if (FsView::gFsView.mSpaceView.count("default"))
    {

      space = "default";
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
      freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
      maxbytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
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

  eos::common::Mapping::VirtualIdentity vid;

  eos::common::Mapping::IdMap(client, "", tident, vid, false);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  eos::common::LogId ThreadLogId;
  ThreadLogId.SetSingleShotLogId(tident);

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

  NAMESPACEMAP;
  if (info) info = 0; // for compiler happyness;

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

      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("AdjustReplica");

      // execute adjust replica                                                                                                                                                                            
      eos::common::Mapping::VirtualIdentity vid;
      eos::common::Mapping::Root(vid);

      // execute a proc command                                                                                                                                                                            
      ProcCommand Cmd;
      XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
      char* spath = env.Get("mgm.path");
      if (spath)
      {
        info += spath;
        info += "&mgm.format=fuse";
        Cmd.open("/proc/user", info.c_str(), vid, &error);
        Cmd.close();
        gOFS->MgmStats.Add("AdjustReplica", 0, 0, 1);
      }
      if (Cmd.GetRetc())
      {
        // the adjustreplica failed
        return Emsg(epname, error, EIO, "[EIO] repair", spath);
      }
      else
      {
        // the adjustreplica succeede!
        const char* ok = "OK";
        error.setErrInfo(strlen(ok) + 1, ok);
        EXEC_TIMING_END("AdjustReplica");
        return SFS_DATA;
      }
    }
    // -------------------------------------------------------------------------
    // Commit a replica
    // -------------------------------------------------------------------------
    if (execmd == "commit")
    {

      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Commit");

      char* asize = env.Get("mgm.size");
      char* spath = env.Get("mgm.path");
      char* afid = env.Get("mgm.fid");
      char* afsid = env.Get("mgm.add.fsid");
      char* amtime = env.Get("mgm.mtime");
      char* amtimensec = env.Get("mgm.mtime_ns");
      char* alogid = env.Get("mgm.logid");

      if (alogid)
      {
        ThreadLogId.SetLogId(alogid, tident);
      }

      XrdOucString averifychecksum = env.Get("mgm.verify.checksum");
      XrdOucString acommitchecksum = env.Get("mgm.commit.checksum");
      XrdOucString averifysize = env.Get("mgm.verify.size");
      XrdOucString acommitsize = env.Get("mgm.commit.size");
      XrdOucString adropfsid = env.Get("mgm.drop.fsid");
      XrdOucString areplication = env.Get("mgm.replication");
      XrdOucString areconstruction = env.Get("mgm.reconstruction");

      bool verifychecksum = (averifychecksum == "1");
      bool commitchecksum = (acommitchecksum == "1");
      bool verifysize = (averifysize == "1");
      bool commitsize = (acommitsize == "1");
      bool replication = (areplication == "1");
      bool reconstruction = (areconstruction == "1");

      char* checksum = env.Get("mgm.checksum");
      char binchecksum[SHA_DIGEST_LENGTH];
      memset(binchecksum, 0, sizeof (binchecksum));
      unsigned long dropfsid = 0;
      if (adropfsid.length())
      {
        dropfsid = strtoul(adropfsid.c_str(), 0, 10);
      }

      if (reconstruction)
      {
        // remove the checksum we don't care about it
        checksum = 0;
        verifysize = false;
        verifychecksum = false;
        commitsize = false;
        commitchecksum = false;
        replication = false;
      }

      if (checksum)
      {
        for (unsigned int i = 0; i < strlen(checksum); i += 2)
        {
          // hex2binary conversion
          char hex[3];
          hex[0] = checksum[i];
          hex[1] = checksum[i + 1];
          hex[2] = 0;
          binchecksum[i / 2] = strtol(hex, 0, 16);
        }
      }
      if (asize && afid && spath && afsid && amtime && amtimensec)
      {
        unsigned long long size = strtoull(asize, 0, 10);
        unsigned long long fid = strtoull(afid, 0, 16);
        unsigned long fsid = strtoul(afsid, 0, 10);
        unsigned long mtime = strtoul(amtime, 0, 10);
        unsigned long mtimens = strtoul(amtimensec, 0, 10);

        {
          // ---------------------------------------------------------------
          // check that the filesystem is still allowed to accept replica's
          // ---------------------------------------------------------------
          eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
          eos::mgm::FileSystem* fs = 0;
          if (FsView::gFsView.mIdView.count(fsid))
          {
            fs = FsView::gFsView.mIdView[fsid];
          }
          if ((!fs) || (fs->GetConfigStatus() < eos::common::FileSystem::kDrain))
          {
            eos_thread_err("msg=\"commit suppressed\" configstatus=%s subcmd=commit path=%s size=%s fid=%s fsid=%s dropfsid=%llu checksum=%s mtime=%s mtime.nsec=%s",
                           fs ? eos::common::FileSystem::GetConfigStatusAsString(fs->GetConfigStatus()) : "deleted",
                           spath,
                           asize,
                           afid,
                           afsid,
                           dropfsid,
                           checksum,
                           amtime,
                           amtimensec);

            return Emsg(epname, error, EIO, "commit file metadata - filesystem is in non-operational state [EIO]", "");
          }
        }

        eos::Buffer checksumbuffer;
        checksumbuffer.putData(binchecksum, SHA_DIGEST_LENGTH);

        if (checksum)
        {
          eos_thread_info("subcmd=commit path=%s size=%s fid=%s fsid=%s dropfsid=%llu checksum=%s mtime=%s mtime.nsec=%s", spath, asize, afid, afsid, dropfsid, checksum, amtime, amtimensec);
        }
        else
        {
          eos_thread_info("subcmd=commit path=%s size=%s fid=%s fsid=%s dropfsid=%llu mtime=%s mtime.nsec=%s",
                          spath, asize, afid, afsid, dropfsid, amtime, amtimensec);
        }

        // get the file meta data if exists
        eos::FileMD *fmd = 0;
        eos::ContainerMD::id_t cid = 0;

        // ---------------------------------------------------------------------
        // keep the lock order View=>Quota=>Namespace
        // ---------------------------------------------------------------------
        eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
        eos::common::RWMutexWriteLock nslock(gOFS->eosViewRWMutex);
        XrdOucString emsg = "";
        try
        {
          fmd = gOFS->eosFileService->getFileMD(fid);
        }
        catch (eos::MDException &e)
        {
          errno = e.getErrno();
          eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
          emsg = "retc=";
          emsg += e.getErrno();
          emsg += " msg=";
          emsg += e.getMessage().str().c_str();
        }

        if (!fmd)
        {
          // uups, no such file anymore
          if (errno == ENOENT)
          {
            return Emsg(epname, error, ENOENT, "commit filesize change - file is already removed [EIDRM]", "");
          }
          else
          {
            emsg.insert("commit filesize change [EIO] ", 0);
            return Emsg(epname, error, errno, emsg.c_str(), spath);
          }
        }
        else
        {
          unsigned long lid = fmd->getLayoutId();

          // check if fsid and fid are ok 
          if (fmd->getId() != fid)
          {
            eos_thread_notice("commit for fid=%lu but fid=%lu", fmd->getId(), fid);
            gOFS->MgmStats.Add("CommitFailedFid", 0, 0, 1);
            return Emsg(epname, error, EINVAL, "commit filesize change - file id is wrong [EINVAL]", spath);
          }

          // check if this file is already unlinked from the visible namespace
          if (!(cid = fmd->getContainerId()))
          {

            eos_thread_warning("commit for fid=%lu but file is disconnected from any container", fmd->getId());
            gOFS->MgmStats.Add("CommitFailedUnlinked", 0, 0, 1);
            return Emsg(epname, error, EIDRM, "commit filesize change - file is already removed [EIDRM]", "");
          }
          else
          {
            // store the in-memory modification time
            // we get the current time, but we don't update the creation time
            UpdateNowInmemoryDirectoryModificationTime(cid);
            // -----------------------------------------------------------------
          }

          // check if this commit comes from a transfer and if the size/checksum is ok
          if (replication)
          {
            eos_debug("fmd size=%lli, size=%lli", fmd->getSize(), size);
            if (fmd->getSize() != size)
            {
              eos_thread_err("replication for fid=%lu resulted in a different file "
                             "size on fsid=%llu - rejecting replica", fmd->getId(), fsid);

              gOFS->MgmStats.Add("ReplicaFailedSize", 0, 0, 1);
              return Emsg(epname, error, EBADE, "commit replica - file size is wrong [EBADE]", "");
            }

            if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica)
            {
              // we check the checksum only for replica layouts
              bool cxError = false;
              size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
              for (size_t i = 0; i < cxlen; i++)
              {
                if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i))
                {
                  cxError = true;
                }
              }
              if (cxError)
              {
                eos_thread_err("replication for fid=%lu resulted in a different checksum "
                               "on fsid=%llu - rejecting replica", fmd->getId(), fsid);

                gOFS->MgmStats.Add("ReplicaFailedChecksum", 0, 0, 1);
                return Emsg(epname, error, EBADR, "commit replica - file checksum is wrong [EBADR]", "");
              }
            }
          }

          if (verifysize)
          {
            // check if we saw a file size change or checksum change
            if (fmd->getSize() != size)
            {
              eos_thread_err("commit for fid=%lu gave a file size change after "
                             "verification on fsid=%llu", fmd->getId(), fsid);
            }
          }

          if (checksum)
          {
            if (verifychecksum)
            {
              bool cxError = false;
              size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
              for (size_t i = 0; i < cxlen; i++)
              {
                if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i))
                {
                  cxError = true;
                }
              }
              if (cxError)
              {
                eos_thread_err("commit for fid=%lu gave a different checksum after "
                               "verification on fsid=%llu", fmd->getId(), fsid);
              }
            }
          }

          // -----------------------------------------------------------------
          // for changing the modification time we have to figure out if we
          // just attach a new replica or if we have a change of the contents
          // -----------------------------------------------------------------
          bool isUpdate;

          {
            SpaceQuota* space = Quota::GetResponsibleSpaceQuota(spath);
            eos::QuotaNode* quotanode = 0;
            if (space)
            {
              quotanode = space->GetQuotaNode();
              // free previous quota
              if (quotanode)
                quotanode->removeFile(fmd);
            }
            fmd->addLocation(fsid);
            // if fsid is in the deletion list, we try to remove it if there is something in the deletion list
            if (fmd->getNumUnlinkedLocation())
            {
              fmd->removeLocation(fsid);
            }

            if (dropfsid)
            {
              eos_thread_debug("commit: dropping replica on fs %lu", dropfsid);
              fmd->unlinkLocation((unsigned short) dropfsid);
            }

            if (commitsize)
            {
              if (fmd->getSize() != size)
              {
                isUpdate = true;
              }
              fmd->setSize(size);
            }

            if (quotanode)
            {
              quotanode->addFile(fmd);
            }
          }

          if (commitchecksum)
          {
            if (!isUpdate)
            {
              for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
              {
                if (fmd->getChecksum().getDataPadded(i) != checksumbuffer.getDataPadded(i))
                {
                  isUpdate = true;
                }
              }
            }
            fmd->setChecksum(checksumbuffer);
          }

          eos::FileMD::ctime_t mt;
          mt.tv_sec = mtime;
          mt.tv_nsec = mtimens;

          if (isUpdate)
          {
            // update the modification time only if the file contents changed
            fmd->setMTime(mt);
          }

          eos_thread_debug("commit: setting size to %llu", fmd->getSize());
          try
          {
            gOFS->eosView->updateFileStore(fmd);
          }
          catch (eos::MDException &e)
          {
            errno = e.getErrno();
            std::string errmsg = e.getMessage().str();
            eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                             e.getErrno(), e.getMessage().str().c_str());
            gOFS->MgmStats.Add("CommitFailedNamespace", 0, 0, 1);
            return Emsg(epname, error, errno, "commit filesize change",
                        errmsg.c_str());
          }
        }
      }
      else
      {
        int envlen = 0;
        eos_thread_err("commit message does not contain all meta information: %s",
                       env.Env(envlen));
        gOFS->MgmStats.Add("CommitFailedParameters", 0, 0, 1);
        if (spath)
        {
          return Emsg(epname, error, EINVAL,
                      "commit filesize change - size,fid,fsid,mtime not complete", spath);
        }
        else
        {
          return Emsg(epname, error, EINVAL,
                      "commit filesize change - size,fid,fsid,mtime,path not complete", "unknown");
        }
      }
      gOFS->MgmStats.Add("Commit", 0, 0, 1);
      const char* ok = "OK";
      error.setErrInfo(strlen(ok) + 1, ok);
      EXEC_TIMING_END("Commit");
      return SFS_DATA;
    }

    if (execmd == "drop")
    {
      // -----------------------------------------------------------------------
      // Drop a replica
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Drop");
      // drops a replica
      int envlen;
      eos_thread_info("drop request for %s", env.Env(envlen));
      char* afid = env.Get("mgm.fid");
      char* afsid = env.Get("mgm.fsid");

      if (afid && afsid)
      {
        unsigned long fsid = strtoul(afsid, 0, 10);

        // ---------------------------------------------------------------------
        eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        eos::FileMD* fmd = 0;
        eos::ContainerMD* container = 0;
        eos::QuotaNode* quotanode = 0;

        try
        {
          fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
        }
        catch (...)
        {
          eos_thread_warning("no meta record exists anymore for fid=%s", afid);
          fmd = 0;
        }

        if (fmd)
        {
          try
          {
            container = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
          }
          catch (eos::MDException &e)
          {
            container = 0;
          }
        }

        if (container)
        {
          try
          {
            quotanode = gOFS->eosView->getQuotaNode(container);
            if (quotanode)
            {
              quotanode->removeFile(fmd);
            }
          }
          catch (eos::MDException &e)
          {
            quotanode = 0;
          }
        }

        if (fmd)
        {
          try
          {
            // If mgm.dropall flag is set then it means we got a deleteOnClose
            // at the gateway node and we need to delete all replicas
            char* drop_all = env.Get("mgm.dropall");
            std::vector<unsigned int> drop_fsid;
            bool updatestore = false;

            if (drop_all)
            {
              for (unsigned int i = 0; i < fmd->getNumLocation(); i++)
              {
                drop_fsid.push_back(fmd->getLocation(i));
              }
            }
            else
            {
              drop_fsid.push_back(fsid);
            }

            // Drop the selected replicas
            for (auto id = drop_fsid.begin(); id != drop_fsid.end(); id++)
            {
              eos_thread_debug("removing location %u of fid=%s", *id, afid);
              updatestore = false;

              if (fmd->hasLocation(*id))
              {
                fmd->unlinkLocation(*id);
                updatestore = true;
              }

              if (fmd->hasUnlinkedLocation(*id))
              {
                fmd->removeLocation(*id);
                updatestore = true;
              }

              if (updatestore)
              {
                gOFS->eosView->updateFileStore(fmd);
                // After update we have to get the new address - who knows ...
                fmd = eosFileService->getFileMD(eos::common::FileId::Hex2Fid(afid));
              }

              if (quotanode)
              {
                quotanode->addFile(fmd);
              }
            }

            // Finally delete the record if all replicas are dropped
            if ((!fmd->getNumUnlinkedLocation()) && (!fmd->getNumLocation()))
            {
              gOFS->eosView->removeFile(fmd);
              if (quotanode)
              {
                // If we were still attached to a container, we can now detach
                // and count the file as removed
                quotanode->removeFile(fmd);
              }
            }
          }
          catch (...)
          {
            eos_thread_warning("no meta record exists anymore for fid=%s", afid);
          };
        }

        gOFS->MgmStats.Add("Drop", vid.uid, vid.gid, 1);

        const char* ok = "OK";
        error.setErrInfo(strlen(ok) + 1, ok);
        EXEC_TIMING_END("Drop");
        return SFS_DATA;
      }
    }

    if (execmd == "getfmd")
    {
      // -----------------------------------------------------------------------
      // Return's meta data in env representatino
      // -----------------------------------------------------------------------

      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("GetMd", 0, 0, 1);

      char* afid = env.Get("mgm.getfmd.fid"); // decimal fid

      eos::common::FileId::fileid_t fid = afid ? strtoull(afid, 0, 10) : 0;

      if (!fid)
      {
        // illegal request
        XrdOucString response = "getfmd: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }

      eos::FileMD* fmd = 0;
      std::string fullpath;
      eos::common::RWMutexReadLock(gOFS->eosViewRWMutex);

      try
      {
        fmd = gOFS->eosFileService->getFileMD(fid);
        fullpath = gOFS->eosView->getUri(fmd);

      }
      catch (eos::MDException &e)
      {
        XrdOucString response = "getfmd: retc=";
        response += e.getErrno();
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }

      eos::common::Path cPath(fullpath.c_str());
      std::string fmdenv = "";
      fmd->getEnv(fmdenv);
      fmdenv += "&container=";
      fmdenv += cPath.GetParentPath();
      XrdOucString response = "getfmd: retc=0 ";
      response += fmdenv.c_str();
      response.replace("checksum=&", "checksum=none&"); // XrdOucEnv does not deal with empty values ... sigh ...
      error.setErrInfo(response.length() + 1, response.c_str());
      return SFS_DATA;
    }

    if (execmd == "stat")
    {
      // -----------------------------------------------------------------------
      // Stat a file/dir
      // -----------------------------------------------------------------------
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Dirlist", vid.uid, vid.gid, 1);

      struct stat buf;

      int retc = lstat(spath.c_str(),
                       &buf,
                       error,
                       client,
                       0);

      if (retc == SFS_OK)
      {
        char statinfo[16384];
        // convert into a char stream
        sprintf(statinfo, "stat: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu\n",
                (unsigned long long) buf.st_dev,
                (unsigned long long) buf.st_ino,
                (unsigned long long) buf.st_mode,
                (unsigned long long) buf.st_nlink,
                (unsigned long long) buf.st_uid,
                (unsigned long long) buf.st_gid,
                (unsigned long long) buf.st_rdev,
                (unsigned long long) buf.st_size,
                (unsigned long long) buf.st_blksize,
                (unsigned long long) buf.st_blocks,
#ifdef __APPLE__
          (unsigned long long) buf.st_atimespec.tv_sec,
                (unsigned long long) buf.st_mtimespec.tv_sec,
                (unsigned long long) buf.st_ctimespec.tv_sec,
                (unsigned long long) buf.st_atimespec.tv_nsec,
                (unsigned long long) buf.st_mtimespec.tv_nsec,
                (unsigned long long) buf.st_ctimespec.tv_nsec
#else
          (unsigned long long) buf.st_atime,
                (unsigned long long) buf.st_mtime,
                (unsigned long long) buf.st_ctime,
                (unsigned long long) buf.st_atim.tv_nsec,
                (unsigned long long) buf.st_mtim.tv_nsec,
                (unsigned long long) buf.st_ctim.tv_nsec
#endif
          );

        error.setErrInfo(strlen(statinfo) + 1, statinfo);
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "stat: retc=";
        response += errno;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "chmod")
    {
      // -----------------------------------------------------------------------
      // chmod a dir
      // -----------------------------------------------------------------------
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Chmod", vid.uid, vid.gid, 1);

      char* smode;
      if ((smode = env.Get("mode")))
      {
        struct stat buf;

        // check if it is a file or directory ....
        int retc = lstat(spath.c_str(),
                         &buf,
                         error,
                         client,
                         0);

        // if it is a file ....
        if (!retc && S_ISREG(buf.st_mode))
        {
          // since we don't have permissions on files, we just acknoledge as ok
          XrdOucString response = "chmod: retc=0";
          error.setErrInfo(response.length() + 1, response.c_str());
          return SFS_DATA;
        }


        XrdSfsMode newmode = atoi(smode);
        retc = _chmod(spath.c_str(),
                      newmode,
                      error,
                      vid);

        XrdOucString response = "chmod: retc=";
        response += errno;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "chmod: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "chown")
    {
      // -----------------------------------------------------------------------
      // chown file/dir
      // -----------------------------------------------------------------------
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Chown", vid.uid, vid.gid, 1);

      char* suid;
      char* sgid;
      if ((suid = env.Get("uid")) && (sgid = env.Get("gid")))
      {
        uid_t uid = atoi(suid);
        gid_t gid = atoi(sgid);

        int retc = _chown(spath.c_str(),
                          uid,
                          gid,
                          error,
                          vid);
        XrdOucString response = "chmod: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "chmod: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "access")
    {
      // -----------------------------------------------------------------------
      // check access rights
      // -----------------------------------------------------------------------
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Access", vid.uid, vid.gid, 1);

      char* smode;
      if ((smode = env.Get("mode")))
      {
        int newmode = atoi(smode);
        int retc = 0;
        if (access(spath.c_str(), newmode, error, client, 0))
        {
          retc = error.getErrInfo();
        }
        XrdOucString response = "access: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "access: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "open")
    {
      // -----------------------------------------------------------------------
      // parallel IO mode open
      // -----------------------------------------------------------------------
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("OpenLayout", vid.uid, vid.gid, 1);

      XrdMgmOfsFile* file = new XrdMgmOfsFile();
      if (file)
      {
        opaque += "&eos.cli.access=pio";
        int rc = file->open(spath.c_str(), SFS_O_RDONLY, 0, client, opaque.c_str());
        error.setErrInfo(strlen(file->error.getErrText()) + 1, file->error.getErrText());
        if (rc == SFS_REDIRECT)
        {
          delete file;
          return SFS_DATA;
        }
        else
        {
          error.setErrCode(file->error.getErrInfo());
          delete file;
          return SFS_ERROR;
        }
      }
      else
      {
        error.setErrInfo(ENOMEM, "allocate file object");
        return SFS_ERROR;
      }
    }


    if (execmd == "utimes")
    {
      // -----------------------------------------------------------------------
      // set modification times
      // -----------------------------------------------------------------------
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Utimes", vid.uid, vid.gid, 1);

      char* tv1_sec;
      char* tv1_nsec;
      char* tv2_sec;
      char* tv2_nsec;

      tv1_sec = env.Get("tv1_sec");
      tv1_nsec = env.Get("tv1_nsec");
      tv2_sec = env.Get("tv2_sec");
      tv2_nsec = env.Get("tv2_nsec");

      struct timespec tvp[2];
      if (tv1_sec && tv1_nsec && tv2_sec && tv2_nsec)
      {
        tvp[0].tv_sec = strtol(tv1_sec, 0, 10);
        tvp[0].tv_nsec = strtol(tv1_nsec, 0, 10);
        tvp[1].tv_sec = strtol(tv2_sec, 0, 10);
        tvp[1].tv_nsec = strtol(tv2_nsec, 0, 10);

        int retc = utimes(spath.c_str(),
                          tvp,
                          error,
                          client,
                          0);

        XrdOucString response = "utimes: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "utimes: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "checksum")
    {
      // -----------------------------------------------------------------------
      // Return a file checksum
      // -----------------------------------------------------------------------
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Checksum", vid.uid, vid.gid, 1);

      // get the checksum 
      XrdOucString checksum = "";
      eos::FileMD* fmd = 0;
      int retc = 0;

      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      try
      {
        fmd = gOFS->eosView->getFile(spath.c_str());
        size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
        for (unsigned int i = 0; i < SHA_DIGEST_LENGTH; i++)
        {
          char hb[3];
          sprintf(hb, "%02x", (i < cxlen) ? (unsigned char) (fmd->getChecksum().getDataPadded(i)) : 0);
          checksum += hb;
        }
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
      }

      if (!fmd)
      {
        retc = errno;
      }
      else
      {
        retc = 0;
      }

      XrdOucString response = "checksum: ";
      response += checksum;
      response += " retc=";
      response += retc;
      error.setErrInfo(response.length() + 1, response.c_str());
      return SFS_DATA;
    }

    if (execmd == "statvfs")
    {
      // -----------------------------------------------------------------------
      // Return the virtual 'filesystem' stat
      // -----------------------------------------------------------------------
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-Statvfs", vid.uid, vid.gid, 1);

      XrdOucString space = env.Get("path");
      static XrdSysMutex statvfsmutex;
      static unsigned long long freebytes = 0;
      static unsigned long long freefiles = 0;
      static unsigned long long maxbytes = 0;
      static unsigned long long maxfiles = 0;

      static time_t laststat = 0;

      XrdOucString response = "";

      if (!space.length())
      {
        response = "df: retc=";
        response += EINVAL;
      }
      else
      {
        statvfsmutex.Lock();

        // here we put some cache to avoid too heavy space recomputations
        if ((time(NULL) - laststat) > (10 + (int) rand() / RAND_MAX))
        {
          SpaceQuota* spacequota = 0;
          {
            eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);
            spacequota = Quota::GetResponsibleSpaceQuota(space.c_str());
          }

          if (!spacequota)
          {
            // take the sum's from all file systems in 'default'
            if (FsView::gFsView.mSpaceView.count("default"))
            {
              eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
              freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
              freefiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.ffree");

              maxbytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
              maxfiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.files");
            }
          }
          else
          {
            freebytes = spacequota->GetPhysicalFreeBytes();
            freefiles = spacequota->GetPhysicalFreeFiles();
            maxbytes = spacequota->GetPhysicalMaxBytes();
            maxfiles = spacequota->GetPhysicalMaxFiles();
          }
          laststat = time(NULL);
        }
        statvfsmutex.UnLock();
        response = "statvfs: retc=0";
        char val[1025];
        snprintf(val, 1024, "%llu", freebytes);
        response += " f_avail_bytes=";
        response += val;
        snprintf(val, 1024, "%llu", freefiles);
        response += " f_avail_files=";
        response += val;
        snprintf(val, 1024, "%llu", maxbytes);
        response += " f_max_bytes=";
        response += val;
        snprintf(val, 1024, "%llu", maxfiles);
        response += " f_max_files=";
        response += val;
        error.setErrInfo(response.length() + 1, response.c_str());
      }
      return SFS_DATA;
    }

    if (execmd == "xattr")
    {
      // -----------------------------------------------------------------------
      // get/set/list/rm extended attributes
      // -----------------------------------------------------------------------
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      gOFS->MgmStats.Add("Fuse-XAttr", vid.uid, vid.gid, 1);

      eos_thread_debug("cmd=xattr subcmd=%s path=%s", env.Get("mgm.subcmd"), spath.c_str());

      const char* sub_cmd;
      struct stat buf;

      // check if it is a file or directory ....
      int retc = lstat(spath.c_str(),
                       &buf,
                       error,
                       client,
                       0);

      if (!retc && S_ISDIR(buf.st_mode))
      { //extended attributes for directories
        if ((sub_cmd = env.Get("mgm.subcmd")))
        {
          XrdOucString subcmd = sub_cmd;
          if (subcmd == "ls")
          { //listxattr
            eos::ContainerMD::XAttrMap map;
            int rc = gOFS->attr_ls(spath.c_str(), error, client, (const char *) 0, map);

            XrdOucString response = "lsxattr: retc=";
            response += rc;
            response += " ";
            if (rc == SFS_OK)
            {
              for (std::map<std::string,
                std::string>::iterator iter = map.begin();
                iter != map.end(); iter++)
              {
                response += iter->first.c_str();
                response += "&";
              }
              response += "\0";
              while (response.replace("user.", "tmp."))
              {
              }
              while (response.replace("tmp.", "user.eos."))
              {
              }
              while (response.replace("sys.", "user.admin."))
              {
              }
            }
            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "get")
          { //getxattr
            XrdOucString value;
            XrdOucString key = env.Get("mgm.xattrname");
            key.replace("user.admin.", "sys.");
            key.replace("user.eos.", "user.");
            int rc = gOFS->attr_get(spath.c_str(), error, client,
                                    (const char*) 0, key.c_str(), value);

            XrdOucString response = "getxattr: retc=";
            response += rc;

            if (rc == SFS_OK)
            {
              response += " value=";
              response += value;
            }

            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "set")
          { //setxattr
            XrdOucString key = env.Get("mgm.xattrname");
            XrdOucString value = env.Get("mgm.xattrvalue");
            key.replace("user.admin.", "sys.");
            key.replace("user.eos.", "user.");
            int rc = gOFS->attr_set(spath.c_str(), error, client,
                                    (const char *) 0, key.c_str(), value.c_str());

            XrdOucString response = "setxattr: retc=";
            response += rc;

            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
          else if (subcmd == "rm")
          { // rmxattr
            XrdOucString key = env.Get("mgm.xattrname");
            key.replace("user.admin.", "sys.");
            key.replace("user.eos.", "user.");
            int rc = gOFS->attr_rem(spath.c_str(), error, client,
                                    (const char *) 0, key.c_str());

            XrdOucString response = "rmxattr: retc=";
            response += rc;

            error.setErrInfo(response.length() + 1, response.c_str());
            return SFS_DATA;
          }
        }
      }
      else if (!retc && S_ISREG(buf.st_mode))
      { //extended attributes for files

        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        eos::FileMD* fmd = 0;
        try
        {
          fmd = gOFS->eosView->getFile(spath.c_str());
        }
        catch (eos::MDException &e)
        {
          eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                           e.getErrno(), e.getMessage().str().c_str());
        }

        if ((sub_cmd = env.Get("mgm.subcmd")))
        {
          XrdOucString subcmd = sub_cmd;
          char* char_key = NULL;
          XrdOucString key;
          XrdOucString response;

          if (subcmd == "ls")
          { //listxattr
            response = "lsxattr: retc=0 ";
            response += "user.eos.cid";
            response += "&";
            response += "user.eos.fid";
            response += "&";
            response += "user.eos.lid";
            response += "&";
            response += "user.eos.XStype";
            response += "&";
            response += "user.eos.XS";
            response += "&";
            error.setErrInfo(response.length() + 1, response.c_str());
          }
          else if (subcmd == "get")
          { //getxattr
            char_key = env.Get("mgm.xattrname");
            key = char_key;
            response = "getxattr: retc=";

            if (key.find("eos.cid") != STR_NPOS)
            {
              XrdOucString sizestring;
              response += "0 ";
              response += "value=";
              response += eos::common::StringConversion::GetSizeString(sizestring,
                                                                       (unsigned long long) fmd->getContainerId());
            }
            else if (key.find("eos.fid") != STR_NPOS)
            {
              char fid[32];
              response += "0 ";
              response += "value=";
              snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());
              response += fid;
            }
            else if (key.find("eos.XStype") != STR_NPOS)
            {
              response += "0 ";
              response += "value=";
              response += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            }
            else if (key.find("eos.XS") != STR_NPOS)
            {
              response += "0 ";
              response += "value=";
              char hb[3];
              size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
              for (unsigned int i = 0; i < cxlen; i++)
              {
                if ((i + 1) == cxlen)
                  sprintf(hb, "%02x ", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
                else
                  sprintf(hb, "%02x_", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
                response += hb;
              }

            }
            else if (key.find("eos.lid") != STR_NPOS)
            {
              response += "0 ";
              response += "value=";
              response += eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
            }
            else
              response += "1 ";

            error.setErrInfo(response.length() + 1, response.c_str());
          }
          else if (subcmd == "rm")
          { //rmxattr
            response = "rmxattr: retc=0"; //error
            error.setErrInfo(response.length() + 1, response.c_str());
          }
          else if (subcmd == "set")
          { //setxattr
            response = "setxattr: retc=0"; //error
            error.setErrInfo(response.length() + 1, response.c_str());
          }

          return SFS_DATA;
        }
        return SFS_DATA;
      }
    }

    if (execmd == "schedule2balance")
    {
      // -----------------------------------------------------------------------
      // Schedule a balancer transfer
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Scheduled2Balance");
      gOFS->MgmStats.Add("Schedule2Balance", 0, 0, 1);

      XrdOucString sfsid = env.Get("mgm.target.fsid");
      XrdOucString sfreebytes = env.Get("mgm.target.freebytes");
      char* alogid = env.Get("mgm.logid");
      char* simulate = env.Get("mgm.simulate"); // used to test the routing

      // static map with iterator position for the next group scheduling and it's mutex
      static std::map<std::string, size_t> sGroupCycle;
      static XrdSysMutex sGroupCycleMutex;
      static XrdSysMutex sScheduledFidMutex;
      static std::map<eos::common::FileSystem::fsid_t, time_t> sScheduledFid;
      static time_t sScheduledFidCleanupTime = 0;

      if (alogid)
      {
        ThreadLogId.SetLogId(alogid, tident);
      }

      if ((!sfsid.length()) || (!sfreebytes.length()))
      {
        gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
        return Emsg(epname, error, EINVAL, "unable to schedule - missing parameters [EINVAL]");
      }

      eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
      eos::common::FileSystem::fsid_t source_fsid = 0;
      eos::common::FileSystem::fs_snapshot target_snapshot;
      eos::common::FileSystem::fs_snapshot source_snapshot;
      eos::common::FileSystem* target_fs = 0;

      unsigned long long freebytes = (sfreebytes.c_str()) ? strtoull(sfreebytes.c_str(), 0, 10) : 0;

      eos_thread_info("cmd=schedule2balance fsid=%d freebytes=%llu logid=%s",
                      target_fsid, freebytes, alogid ? alogid : "");

      while (1)
        // lock the view and get the filesystem information for the target where be balance to
      {
        eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
        target_fs = FsView::gFsView.mIdView[target_fsid];
        if (!target_fs)
        {
          eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
          gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
          return Emsg(epname, error, EINVAL,
                      "unable to schedule - filesystem ID is not known");
        }

        target_fs->SnapShotFileSystem(target_snapshot);
        FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

        size_t groupsize = FsView::gFsView.mGroupView[target_snapshot.mGroup]->size();
        if (!group)
        {
          eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
          gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
          return Emsg(epname, error, EINVAL,
                      "unable to schedule - group is not known [EINVAL]");
        }

        eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

        // select the next fs in the group to get a file to move
        size_t gposition = 0;
        sGroupCycleMutex.Lock();
        if (sGroupCycle.count(target_snapshot.mGroup))
        {
          gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
        }
        else
        {
          gposition = 0;
          sGroupCycle[target_snapshot.mGroup] = 0;
        }
        // shift the iterator for the next schedule call to the following filesystem in the group
        sGroupCycle[target_snapshot.mGroup]++;
        sGroupCycle[target_snapshot.mGroup] %= groupsize;
        sGroupCycleMutex.UnLock();

        eos_thread_debug("group=%s cycle=%lu",
                         target_snapshot.mGroup.c_str(), gposition);
        // try to find a file, which is smaller than the free bytes and has no replica on the target filesystem
        // we start at a random position not to move data of the same period to a single disk

        group = FsView::gFsView.mGroupView[target_snapshot.mGroup];
        FsGroup::const_iterator group_iterator;
        group_iterator = group->begin();
        std::advance(group_iterator, gposition);

        eos::common::FileSystem* source_fs = 0;
        for (size_t n = 0; n < group->size(); n++)
        {
          // skip over the target file system, that isn't usable
          if (*group_iterator == target_fsid)
          {
            source_fs = 0;
            group_iterator++;
            if (group_iterator == group->end()) group_iterator = group->begin();
            continue;
          }
          source_fs = FsView::gFsView.mIdView[*group_iterator];
          if (!source_fs)
            continue;
          source_fs->SnapShotFileSystem(source_snapshot);
          source_fsid = *group_iterator;
          if ((source_snapshot.mDiskFilled < source_snapshot.mNominalFilled) || // this is not a source since it is empty
              (source_snapshot.mStatus != eos::common::FileSystem::kBooted) || // this filesystem is not readable
              (source_snapshot.mConfigStatus < eos::common::FileSystem::kRO) ||
              (source_snapshot.mErrCode != 0) ||
              (source_fs->GetActiveStatus(source_snapshot) == eos::common::FileSystem::kOffline))
          {
            source_fs = 0;
            group_iterator++;
            if (group_iterator == group->end()) group_iterator = group->begin();
            continue;
          }
          break;
        }

        if (!source_fs)
        {
          eos_thread_debug("no source available");
          gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
          error.setErrInfo(0, "");
          return SFS_DATA;
        }
        source_fs->SnapShotFileSystem(source_snapshot);

        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

        eos::FileSystemView::FileList source_filelist;
        eos::FileSystemView::FileList target_filelist;

        try
        {
          source_filelist = gOFS->eosFsView->getFileList(source_fsid);
        }
        catch (eos::MDException &e)
        {
          source_filelist.set_deleted_key(0);
          source_filelist.set_empty_key(0xffffffffffffffff);
        }

        try
        {
          target_filelist = gOFS->eosFsView->getFileList(target_fsid);
        }
        catch (eos::MDException &e)
        {
          target_filelist.set_deleted_key(0);
          target_filelist.set_empty_key(0xffffffffffffffff);
        }

        unsigned long long nfids = (unsigned long long) source_filelist.size();

        eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u n_source_fids=%llu",
                         target_snapshot.mGroup.c_str(), gposition, source_fsid, target_fsid, nfids);
        unsigned long long rpos = (unsigned long long) ((0.999999 * random() * nfids) / RAND_MAX);
        eos::FileSystemView::FileIterator fit = source_filelist.begin();
        std::advance(fit, rpos);
        while (fit != source_filelist.end())
        {
          // check that the target does not have this file
          eos::FileMD::id_t fid = *fit;
          if (target_filelist.count(fid))
          {
            // iterate to the next file, we have this file already
            fit++;
            continue;
          }
          else
          {
            // check that this file has not been scheduled during the 1h period
            XrdSysMutexHelper sLock(sScheduledFidMutex);
            time_t now = time(NULL);
            if (sScheduledFidCleanupTime < now)
            {
              // next clean-up in 10 minutes
              sScheduledFidCleanupTime = now + 600;
              // do some cleanup
              std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
              std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
              it1 = it2 = sScheduledFid.begin();
              while (it2 != sScheduledFid.end())
              {
                it1 = it2;
                it2++;
                if (it1->second < now)
                {
                  sScheduledFid.erase(it1);
                }
              }
            }
            if ((sScheduledFid.count(fid) && ((sScheduledFid[fid] > (now)))))
            {
              // iterate to the next file, we have scheduled this file during the last hour or anyway it is empty
              fit++;
              continue;
            }
            else
            {
              eos::FileMD* fmd = 0;
              unsigned long long cid = 0;
              unsigned long long size = 0;
              long unsigned int lid = 0;
              uid_t uid = 0;
              gid_t gid = 0;
              std::string fullpath = "";

              try
              {
                fmd = gOFS->eosFileService->getFileMD(fid);
                fullpath = gOFS->eosView->getUri(fmd);
                fmd = gOFS->eosFileService->getFileMD(fid);
                lid = fmd->getLayoutId();
                cid = fmd->getContainerId();
                size = fmd->getSize();
                uid = fmd->getCUid();
                gid = fmd->getCGid();
              }
              catch (eos::MDException &e)
              {
                fmd = 0;
              }

              if (fmd)
              {
                if ((size > 0) && (size < freebytes))
                {
                  // we can schedule fid from source => target_it
                  eos_thread_info("subcmd=scheduling fid=%llx source_fsid=%u target_fsid=%u",
                                  fid, source_fsid, target_fsid);

                  XrdOucString source_capability = "";
                  XrdOucString sizestring;
                  source_capability += "mgm.access=read";
                  source_capability += "&mgm.lid=";
                  source_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                    (unsigned long long) lid & 0xffffff0f);
                  // make's it a plain replica
                  source_capability += "&mgm.cid=";
                  source_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
                  source_capability += "&mgm.ruid=";
                  source_capability += (int) 1;
                  source_capability += "&mgm.rgid=";
                  source_capability += (int) 1;
                  source_capability += "&mgm.uid=";
                  source_capability += (int) 1;
                  source_capability += "&mgm.gid=";
                  source_capability += (int) 1;
                  source_capability += "&mgm.path=";
                  source_capability += fullpath.c_str();
                  source_capability += "&mgm.manager=";
                  source_capability += gOFS->ManagerId.c_str();
                  source_capability += "&mgm.fid=";
                  XrdOucString hexfid;
                  eos::common::FileId::Fid2Hex(fid, hexfid);
                  source_capability += hexfid;

                  source_capability += "&mgm.sec=";
                  source_capability += eos::common::SecEntity::ToKey(0, "eos/balancing").c_str();

                  source_capability += "&mgm.drainfsid=";
                  source_capability += (int) source_fsid;

                  // build the source_capability contents
                  source_capability += "&mgm.localprefix=";
                  source_capability += source_snapshot.mPath.c_str();
                  source_capability += "&mgm.fsid=";
                  source_capability += (int) source_snapshot.mId;
                  source_capability += "&mgm.sourcehostport=";
                  source_capability += source_snapshot.mHostPort.c_str();

                  XrdOucString target_capability = "";
                  target_capability += "mgm.access=write";
                  target_capability += "&mgm.lid=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                    (unsigned long long) lid & 0xffffff0f);
                  // make's it a plain replica
                  target_capability += "&mgm.source.lid=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                    (unsigned long long) lid);
                  target_capability += "&mgm.source.ruid=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                    (unsigned long long) uid);
                  target_capability += "&mgm.source.rgid=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                    (unsigned long long) gid);

                  target_capability += "&mgm.cid=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
                  target_capability += "&mgm.ruid=";
                  target_capability += (int) 1;
                  target_capability += "&mgm.rgid=";
                  target_capability += (int) 1;
                  target_capability += "&mgm.uid=";
                  target_capability += (int) 1;
                  target_capability += "&mgm.gid=";
                  target_capability += (int) 1;
                  target_capability += "&mgm.path=";
                  target_capability += fullpath.c_str();
                  target_capability += "&mgm.manager=";
                  target_capability += gOFS->ManagerId.c_str();
                  target_capability += "&mgm.fid=";
                  target_capability += hexfid;
                  target_capability += "&mgm.sec=";
                  target_capability += eos::common::SecEntity::ToKey(0, "eos/balancing").c_str();

                  target_capability += "&mgm.drainfsid=";
                  target_capability += (int) source_fsid;

                  // build the target_capability contents
                  target_capability += "&mgm.localprefix=";
                  target_capability += target_snapshot.mPath.c_str();
                  target_capability += "&mgm.fsid=";
                  target_capability += (int) target_snapshot.mId;
                  target_capability += "&mgm.targethostport=";
                  target_capability += target_snapshot.mHostPort.c_str();
                  target_capability += "&mgm.bookingsize=";
                  target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
                  // issue a source_capability
                  XrdOucEnv insource_capability(source_capability.c_str());
                  XrdOucEnv intarget_capability(target_capability.c_str());
                  XrdOucEnv* source_capabilityenv = 0;
                  XrdOucEnv* target_capabilityenv = 0;
                  XrdOucString fullcapability = "";
                  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

                  int caprc = 0;
                  if ((caprc = gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
                      (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey)))
                  {
                    eos_thread_err("unable to create source/target capability - errno=%u", caprc);
                    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
                    return Emsg(epname, error, caprc, "create source/target capability [EADV]");
                  }
                  else
                  {
                    int caplen = 0;
                    XrdOucString source_cap = source_capabilityenv->Env(caplen);
                    XrdOucString target_cap = target_capabilityenv->Env(caplen);
                    source_cap.replace("cap.sym", "source.cap.sym");
                    target_cap.replace("cap.sym", "target.cap.sym");
                    source_cap.replace("cap.msg", "source.cap.msg");
                    target_cap.replace("cap.msg", "target.cap.msg");
                    source_cap += "&source.url=root://";
                    source_cap += source_snapshot.mHostPort.c_str();
                    source_cap += "//replicate:";
                    source_cap += hexfid;
                    target_cap += "&target.url=root://";
                    target_cap += target_snapshot.mHostPort.c_str();
                    target_cap += "//replicate:";
                    target_cap += hexfid;
                    fullcapability += source_cap;
                    fullcapability += target_cap;

                    // send submitted response
                    XrdOucString response = "submitted";
                    error.setErrInfo(response.length() + 1, response.c_str());

                    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

                    if (!simulate)
                    {
                      if (target_fs->GetBalanceQueue()->Add(txjob))
                      {
                        eos_thread_info("cmd=queued fid=%x source_fs=%u target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
                        eos_thread_debug("job=%s", fullcapability.c_str());
                      }
                    }

                    if (txjob)
                    {
                      delete txjob;
                    }

                    if (source_capabilityenv)
                      delete source_capabilityenv;
                    if (target_capabilityenv)
                      delete target_capabilityenv;

                    gOFS->MgmStats.Add("Scheduled2Balance", 0, 0, 1);
                    EXEC_TIMING_END("Scheduled2Balance");
                    return SFS_DATA;
                  }
                }
                else
                {
                  fit++;
                  continue;
                }
              }
              else
              {
                fit++;
                continue;
              }
            }
          }
        }
        break;
      }
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      error.setErrInfo(0, "");
      return SFS_DATA;
    }

    if (execmd == "schedule2drain")
    {
      // -----------------------------------------------------------------------
      // Schedule a drain transfer
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Scheduled2Drain");
      gOFS->MgmStats.Add("Schedule2Drain", 0, 0, 1);

      XrdOucString sfsid = env.Get("mgm.target.fsid");
      XrdOucString sfreebytes = env.Get("mgm.target.freebytes");
      char* alogid = env.Get("mgm.logid");
      char* simulate = env.Get("mgm.simulate"); // used to test the routing

      // static map with iterator position for the next group scheduling and it's mutex
      static std::map<std::string, size_t> sGroupCycle;
      static XrdSysMutex sGroupCycleMutex;
      static XrdSysMutex sScheduledFidMutex;
      static std::map<eos::common::FileSystem::fsid_t, time_t> sScheduledFid;
      static time_t sScheduledFidCleanupTime = 0;

      if (alogid)
      {
        ThreadLogId.SetLogId(alogid, tident);
      }

      if ((!sfsid.length()) || (!sfreebytes.length()))
      {
        gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
        return Emsg(epname, error, EINVAL, "unable to schedule - missing parameters [EINVAL]");
      }

      eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
      eos::common::FileSystem::fsid_t source_fsid = 0;
      eos::common::FileSystem::fs_snapshot target_snapshot;
      eos::common::FileSystem::fs_snapshot source_snapshot;
      eos::common::FileSystem::fs_snapshot replica_source_snapshot;
      eos::common::FileSystem* target_fs = 0;

      unsigned long long freebytes = (sfreebytes.c_str()) ? strtoull(sfreebytes.c_str(), 0, 10) : 0;

      eos_thread_info("cmd=schedule2drain fsid=%d freebytes=%llu logid=%s", target_fsid, freebytes, alogid ? alogid : "");

      while (1)
        // lock the view and get the filesystem information for the target where be balance to
      {
        eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
        target_fs = FsView::gFsView.mIdView[target_fsid];
        if (!target_fs)
        {
          eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
          gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
          return Emsg(epname, error, EINVAL, "unable to schedule - filesystem ID is not known");
        }
        target_fs->SnapShotFileSystem(target_snapshot);
        FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

        size_t groupsize = FsView::gFsView.mGroupView.size();
        if (!group)
        {
          eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
          gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
          return Emsg(epname, error, EINVAL, "unable to schedule - group is not known [EINVAL]");
        }

        eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

        // select the next fs in the group to get a file to move
        size_t gposition = 0;
        {
          XrdSysMutexHelper(sGroupCycleMutex);
          if (sGroupCycle.count(target_snapshot.mGroup))
          {
            gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
          }
          else
          {
            gposition = 0;
            sGroupCycle[target_snapshot.mGroup] = 0;
          }
          // shift the iterator for the next schedule call to the following filesystem in the group
          sGroupCycle[target_snapshot.mGroup]++;
          sGroupCycle[target_snapshot.mGroup] %= groupsize;
        }

        eos_thread_debug("group=%s cycle=%lu", target_snapshot.mGroup.c_str(), gposition);
        // try to find a file, which is smaller than the free bytes and has no replica on the target filesystem
        // we start at a random position not to move data of the same period to a single disk

        group = FsView::gFsView.mGroupView[target_snapshot.mGroup];
        FsGroup::const_iterator group_iterator;
        group_iterator = group->begin();
        std::advance(group_iterator, gposition);

        eos::common::FileSystem* source_fs = 0;
        for (size_t n = 0; n < group->size(); n++)
        {
          // look for a filesystem in drain mode
          if ((eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*group_iterator]->GetString("stat.drain").c_str()) != eos::common::FileSystem::kDraining) &&
              (eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*group_iterator]->GetString("stat.drain").c_str()) != eos::common::FileSystem::kDrainStalling))
          {
            source_fs = 0;
            group_iterator++;
            if (group_iterator == group->end()) group_iterator = group->begin();
            continue;
          }
          source_fs = FsView::gFsView.mIdView[*group_iterator];
          if (!source_fs)
            continue;
          source_fs->SnapShotFileSystem(source_snapshot);
          source_fsid = *group_iterator;
        }

        if (!source_fs)
        {
          eos_thread_debug("no source available");
          gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
          error.setErrInfo(0, "");
          return SFS_DATA;
        }
        source_fs->SnapShotFileSystem(source_snapshot);

        // ---------------------------------------------------------------------
        // keep the lock order View=>Quota=>Namespace
        // ---------------------------------------------------------------------
        eos::common::RWMutexReadLock gLock(Quota::gQuotaMutex);
        // ---------------------------------------------------------------------
        eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
        // ---------------------------------------------------------------------

        eos::FileSystemView::FileList source_filelist;
        eos::FileSystemView::FileList target_filelist;

        try
        {
          source_filelist = gOFS->eosFsView->getFileList(source_fsid);
        }
        catch (eos::MDException &e)
        {
          source_filelist.set_deleted_key(0);
          source_filelist.set_empty_key(0xffffffffffffffff);
        }

        try
        {
          target_filelist = gOFS->eosFsView->getFileList(target_fsid);
        }
        catch (eos::MDException &e)
        {
          target_filelist.set_deleted_key(0);
          target_filelist.set_empty_key(0xffffffffffffffff);
        }

        unsigned long long nfids = (unsigned long long) source_filelist.size();

        eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u n_source_fids=%llu",
                         target_snapshot.mGroup.c_str(), gposition, source_fsid, target_fsid, nfids);

        // give the oldest file first
        eos::FileSystemView::FileIterator fit = source_filelist.begin();
        while (fit != source_filelist.end())
        {
          eos_thread_debug("checking fid %llx", *fit);
          // check that the target does not have this file
          eos::FileMD::id_t fid = *fit;
          if (target_filelist.count(fid))
          {
            // iterate to the next file, we have this file already
            fit++;
            continue;
          }
          else
          {
            // check that this file has not been scheduled during the 1h period
            XrdSysMutexHelper sLock(sScheduledFidMutex);
            time_t now = time(NULL);
            if (sScheduledFidCleanupTime < now)
            {
              // next clean-up in 10 minutes
              sScheduledFidCleanupTime = now + 600;
              // do some cleanup
              std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
              std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
              it1 = it2 = sScheduledFid.begin();
              while (it2 != sScheduledFid.end())
              {
                it1 = it2;
                it2++;
                if (it1->second < now)
                {
                  sScheduledFid.erase(it1);
                }
              }
              while (it2 != sScheduledFid.end());
            }

            if ((sScheduledFid.count(fid) && ((sScheduledFid[fid] > (now)))))
            {
              // iterate to the next file, we have scheduled this file during the last hour or anyway it is empty
              fit++;
              eos_thread_debug("file %llx has already been scheduled at %lu", fid, sScheduledFid[fid]);
              continue;
            }
            else
            {
              eos::FileMD* fmd = 0;
              unsigned long long cid = 0;
              unsigned long long size = 0;
              long unsigned int lid = 0;
              uid_t uid = 0;
              gid_t gid = 0;
              std::string fullpath = "";
              std::vector<unsigned int> locationfs;
              try
              {
                fmd = gOFS->eosFileService->getFileMD(fid);
                fullpath = gOFS->eosView->getUri(fmd);
                fmd = gOFS->eosFileService->getFileMD(fid);
                lid = fmd->getLayoutId();
                cid = fmd->getContainerId();
                size = fmd->getSize();
                uid = fmd->getCUid();
                gid = fmd->getCGid();


                eos::FileMD::LocationVector::const_iterator lociter;
                for (lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter)
                {
                  // ignore filesystem id 0
                  if ((*lociter))
                  {
                    if (source_snapshot.mId == *lociter)
                    {
                      if (source_snapshot.mConfigStatus == eos::common::FileSystem::kDrain)
                      {
                        // only add filesystems which are not in drain dead to the possible locations
                        locationfs.push_back(*lociter);
                      }
                    }
                    else
                    {
                      locationfs.push_back(*lociter);
                    }
                  }
                }
              }
              catch (eos::MDException &e)
              {
                fmd = 0;
              }

              if (fmd)
              {
                XrdOucString fullcapability = "";
                XrdOucString hexfid = "";

                if ((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) ||
                    (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kArchive) ||
                    (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaid6))
                {
                  // -----------------------------------------------------------
                  // RAIN layouts (not replica) drain by running a 
                  // reconstruction 'eoscp -c' ...
                  // they are easy to configure, they just call an open with 
                  // reconstruction/replacement option and the real scheduling
                  // is done when 'eoscp' is executed.
                  // -----------------------------------------------------------
                  eos_thread_info(
                                  "msg=\"creating RAIN reconstruction job\" path=%s", fullpath.c_str()
                                  );

                  fullcapability += "source.url=root://";
                  fullcapability += gOFS->ManagerId;
                  fullcapability += "/";
                  fullcapability += fullpath.c_str();
                  fullcapability += "&target.url=/dev/null";
                  XrdOucString source_env;
                  source_env = "eos.pio.action=reconstruct&";
                  source_env += "eos.pio.recfs=";
                  source_env += (int) source_snapshot.mId;
                  fullcapability += "&source.env=";
                  fullcapability += XrdMqMessage::Seal(source_env, "_AND_");
                  fullcapability += "&tx.layout.reco=true";
                }
                else
                {
                  // -----------------------------------------------------------
                  // Plain/replica layouts get source/target scheduled here
                  // -----------------------------------------------------------

                  XrdOucString sizestring = "";
                  long unsigned int fsindex = 0;

                  // get the responsible quota space
                  SpaceQuota* space = Quota::GetSpaceQuota(source_snapshot.mSpace.c_str(), false);
                  if (space)
                  {
                    eos_thread_debug("space=%s", space->GetSpaceName());
                  }
                  else
                  {
                    eos_thread_err("cmd=schedule2drain msg=\"no responsible space for |%s|\"", source_snapshot.mSpace.c_str());
                  }

                  // schedule access to that file with the original layout
                  int retc = 0;
                  std::vector<unsigned int> unavailfs; // not used
                  eos::common::Mapping::VirtualIdentity_t h_vid;
                  eos::common::Mapping::Root(h_vid);
                  if ((!space) || (retc = space->FileAccess(h_vid,
                                                            (long unsigned int) 0,
                                                            (const char*) 0,
                                                            lid,
                                                            locationfs,
                                                            fsindex,
                                                            false, (
                                                                    long long unsigned) 0,
                                                            unavailfs)))
                  {
                    // inaccessible files we let retry after 60 seconds
                    eos_thread_err("cmd=schedule2drain msg=\"no access to file %llx retc=%d\"", fid, retc);
                    sScheduledFid[fid] = time(NULL) + 60;
                    // try with next file
                    fit++;
                    continue;
                  }

                  if ((size < freebytes))
                  {
                    eos::common::FileSystem* replica_source_fs = 0;
                    replica_source_fs = FsView::gFsView.mIdView[locationfs[fsindex]];
                    if (!replica_source_fs)
                    {
                      fit++;
                      continue;
                    }
                    replica_source_fs->SnapShotFileSystem(replica_source_snapshot);

                    // we can schedule fid from replica_source => target_it
                    eos_thread_info("cmd=schedule2drain subcmd=scheduling fid=%llx drain_fsid=%u replica_source_fsid=%u target_fsid=%u", fid, source_fsid, locationfs[fsindex], target_fsid);

                    XrdOucString replica_source_capability = "";
                    XrdOucString sizestring;
                    replica_source_capability += "mgm.access=read";
                    replica_source_capability += "&mgm.lid=";
                    replica_source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
                    // make's it a plain replica
                    replica_source_capability += "&mgm.cid=";
                    replica_source_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
                    replica_source_capability += "&mgm.ruid=";
                    replica_source_capability += (int) 1;
                    replica_source_capability += "&mgm.rgid=";
                    replica_source_capability += (int) 1;
                    replica_source_capability += "&mgm.uid=";
                    replica_source_capability += (int) 1;
                    replica_source_capability += "&mgm.gid=";
                    replica_source_capability += (int) 1;
                    replica_source_capability += "&mgm.path=";
                    replica_source_capability += fullpath.c_str();
                    replica_source_capability += "&mgm.manager=";
                    replica_source_capability += gOFS->ManagerId.c_str();
                    replica_source_capability += "&mgm.fid=";

                    eos::common::FileId::Fid2Hex(fid, hexfid);
                    replica_source_capability += hexfid;

                    replica_source_capability += "&mgm.sec=";
                    replica_source_capability += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();

                    replica_source_capability += "&mgm.drainfsid=";
                    replica_source_capability += (int) source_fsid;

                    // build the replica_source_capability contents
                    replica_source_capability += "&mgm.localprefix=";
                    replica_source_capability += replica_source_snapshot.mPath.c_str();
                    replica_source_capability += "&mgm.fsid=";
                    replica_source_capability += (int) replica_source_snapshot.mId;
                    replica_source_capability += "&mgm.sourcehostport=";
                    replica_source_capability += replica_source_snapshot.mHostPort.c_str();

                    XrdOucString target_capability = "";
                    target_capability += "mgm.access=write";
                    target_capability += "&mgm.lid=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
                    // make's it a plain replica
                    target_capability += "&mgm.source.lid=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid);
                    target_capability += "&mgm.source.ruid=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) uid);
                    target_capability += "&mgm.source.rgid=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) gid);

                    target_capability += "&mgm.cid=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
                    target_capability += "&mgm.ruid=";
                    target_capability += (int) 1;
                    target_capability += "&mgm.rgid=";
                    target_capability += (int) 1;
                    target_capability += "&mgm.uid=";
                    target_capability += (int) 1;
                    target_capability += "&mgm.gid=";
                    target_capability += (int) 1;
                    target_capability += "&mgm.path=";
                    target_capability += fullpath.c_str();
                    target_capability += "&mgm.manager=";
                    target_capability += gOFS->ManagerId.c_str();
                    target_capability += "&mgm.fid=";
                    target_capability += hexfid;
                    target_capability += "&mgm.sec=";
                    target_capability += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();

                    target_capability += "&mgm.drainfsid=";
                    target_capability += (int) source_fsid;

                    // build the target_capability contents
                    target_capability += "&mgm.localprefix=";
                    target_capability += target_snapshot.mPath.c_str();
                    target_capability += "&mgm.fsid=";
                    target_capability += (int) target_snapshot.mId;
                    target_capability += "&mgm.targethostport=";
                    target_capability += target_snapshot.mHostPort.c_str();
                    target_capability += "&mgm.bookingsize=";
                    target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
                    // issue a replica_source_capability
                    XrdOucEnv insource_capability(replica_source_capability.c_str());
                    XrdOucEnv intarget_capability(target_capability.c_str());
                    XrdOucEnv* source_capabilityenv = 0;
                    XrdOucEnv* target_capabilityenv = 0;
                    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

                    int caprc = 0;
                    if ((caprc = gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
                        (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey)))
                    {
                      eos_thread_err("unable to create source/target capability - errno=%u", caprc);
                      gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
                      return Emsg(epname, error, caprc, "create source/target capability [EADV]");
                    }
                    else
                    {
                      int caplen = 0;
                      XrdOucString source_cap = source_capabilityenv->Env(caplen);
                      XrdOucString target_cap = target_capabilityenv->Env(caplen);
                      source_cap.replace("cap.sym", "source.cap.sym");
                      target_cap.replace("cap.sym", "target.cap.sym");
                      source_cap.replace("cap.msg", "source.cap.msg");
                      target_cap.replace("cap.msg", "target.cap.msg");
                      source_cap += "&source.url=root://";
                      source_cap += replica_source_snapshot.mHostPort.c_str();
                      source_cap += "//replicate:";
                      source_cap += hexfid;
                      target_cap += "&target.url=root://";
                      target_cap += target_snapshot.mHostPort.c_str();
                      target_cap += "//replicate:";
                      target_cap += hexfid;
                      fullcapability += source_cap;
                      fullcapability += target_cap;
                    }

                    if (source_capabilityenv)
                      delete source_capabilityenv;
                    if (target_capabilityenv)
                      delete target_capabilityenv;
                  }
                  else
                  {
                    fit++;
                    continue;
                  }
                }

                eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

                if (!simulate)
                {
                  if (target_fs->GetDrainQueue()->Add(txjob))
                  {
                    eos_thread_info("cmd=schedule2drain msg=queued fid=%x source_fs=%u target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
                    eos_thread_debug("cmd=schedule2drain job=%s", fullcapability.c_str());

                    if (!simulate)
                    {
                      // this file fits
                      sScheduledFid[fid] = time(NULL) + 3600;
                    }

                    // send submitted response
                    XrdOucString response = "submitted";
                    error.setErrInfo(response.length() + 1, response.c_str());
                  }
                  else
                  {
                    eos_thread_err("cmd=schedule2drain msg=\"failed to submit job\" job=%s", fullcapability.c_str());
                    error.setErrInfo(0, "");
                  }
                }

                if (txjob)
                {
                  delete txjob;
                }
                gOFS->MgmStats.Add("Scheduled2Drain", 0, 0, 1);
                EXEC_TIMING_END("Scheduled2Drain");
                return SFS_DATA;
              }
              else
              {
                fit++;
                continue;
              }
            }
          }
        }
        break;
      }
      gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
      error.setErrInfo(0, "");
      return SFS_DATA;
    }

    if (execmd == "schedule2delete")
    {
      // -----------------------------------------------------------------------
      // Schedule deletion
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      EXEC_TIMING_BEGIN("Scheduled2Delete");
      gOFS->MgmStats.Add("Schedule2Delete", 0, 0, 1);

      XrdOucString nodename = env.Get("mgm.target.nodename");

      eos_static_debug("nodename=%s", nodename.c_str() ? nodename.c_str() : "-none-");
      std::vector <unsigned int> fslist;
      // get a list of file Ids

      std::map<eos::common::FileSystem::fsid_t, eos::mgm::FileSystem*>::const_iterator it;

      {
        std::set<eos::common::FileSystem::fsid_t>::const_iterator set_it;

        // get all the filesystem's of that node
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        std::string snodename = nodename.c_str() ? nodename.c_str() : "-none-";
        if (!FsView::gFsView.mNodeView.count(snodename))
        {
          eos_static_warning("msg=\"node is not configured\" name=%s", snodename.c_str());
          return Emsg(epname, error, EINVAL, "unable to schedule - node is not existing");
        }

        for (set_it = FsView::gFsView.mNodeView[snodename]->begin(); set_it != FsView::gFsView.mNodeView[snodename]->end(); ++set_it)
        {
          fslist.push_back(*set_it);
        }
      }

      size_t totaldeleted = 0;

      for (unsigned int i = 0; i < fslist.size(); i++)
      {
        // ---------------------------------------------------------------------
        // loop over all file systems
        // ---------------------------------------------------------------------
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
        std::pair<eos::FileSystemView::FileIterator, eos::FileSystemView::FileIterator> unlinkpair;
        try
        {
          unlinkpair = eosFsView->getUnlinkedFiles(fslist[i]);
          XrdMqMessage message("deletion");
          eos::FileSystemView::FileIterator it;
          int ndeleted = 0;

          eos::mgm::FileSystem* fs = 0;
          XrdOucString receiver = "";
          XrdOucString msgbody = "mgm.cmd=drop";
          XrdOucString capability = "";
          XrdOucString idlist = "";
          for (it = unlinkpair.first; it != unlinkpair.second; ++it)
          {
            eos_static_info("msg=\"add to deletion message\" fxid=%08llx fsid=%lu",
                            *it, (unsigned long) fslist[i]);

            // loop over all files and emit a deletion message
            if (!fs)
            {
              // set the file system only for the first file to relax the mutex contention
              if (!fslist[i])
              {
                eos_err("no filesystem in deletion list");
                continue;
              }

              if (FsView::gFsView.mIdView.count(fslist[i]))
              {
                fs = FsView::gFsView.mIdView[fslist[i]];
              }
              else
              {
                fs = 0;
              }

              if (fs)
              {
                eos::common::FileSystem::fsstatus_t bootstatus = fs->GetStatus();
                // check the state of the filesystem (if it can actually delete in this moment!)
                if ((fs->GetConfigStatus() <= eos::common::FileSystem::kOff) ||
                    (bootstatus != eos::common::FileSystem::kBooted))
                {
                  // we don't need to send messages, this one is anyway down or currently booting
                  break;
                }

                if ((fs->GetActiveStatus() == eos::common::FileSystem::kOffline))
                {
                  break;
                }

                capability += "&mgm.access=delete";
                capability += "&mgm.manager=";
                capability += gOFS->ManagerId.c_str();
                capability += "&mgm.fsid=";
                capability += (int) fs->GetId();
                capability += "&mgm.localprefix=";
                capability += fs->GetPath().c_str();
                capability += "&mgm.fids=";
                receiver = fs->GetQueue().c_str();
              }
            }

            ndeleted++;
            totaldeleted++;

            XrdOucString sfid = "";
            XrdOucString hexfid = "";
            eos::common::FileId::Fid2Hex(*it, hexfid);
            idlist += hexfid;
            idlist += ",";

            if (ndeleted > 1024)
            {
              XrdOucString refcapability = capability;
              refcapability += idlist;
              XrdOucEnv incapability(refcapability.c_str());
              XrdOucEnv* capabilityenv = 0;
              eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

              int caprc = 0;
              if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
              {
                eos_static_err("unable to create capability - errno=%u", caprc);
              }
              else
              {
                int caplen = 0;
                msgbody += capabilityenv->Env(caplen);
                // we send deletions in bunches of max 1024 for efficiency
                message.SetBody(msgbody.c_str());

                if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
                {
                  eos_static_err("unable to send deletion message to %s", receiver.c_str());
                }
              }
              idlist = "";
              ndeleted = 0;
              msgbody = "mgm.cmd=drop";
              if (capabilityenv)
                delete capabilityenv;
            }
          }

          // send the remaining ids
          if (idlist.length())
          {
            XrdOucString refcapability = capability;
            refcapability += idlist;
            XrdOucEnv incapability(refcapability.c_str());
            XrdOucEnv* capabilityenv = 0;
            eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

            int caprc = 0;
            if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
            {
              eos_static_err("unable to create capability - errno=%u", caprc);
            }
            else
            {
              int caplen = 0;
              msgbody += capabilityenv->Env(caplen);
              // we send deletions in bunches of max 1000 for efficiency
              message.SetBody(msgbody.c_str());
              if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
              {
                eos_static_err("unable to send deletion message to %s", receiver.c_str());
              }
            }
            if (capabilityenv)
              delete capabilityenv;
          }
        }
        catch (...)
        {
          eos_static_debug("nothing to delete in fs %lu", (unsigned long) fslist[i]);
        }
      }
      // -----------------------------------------------------------------------
      if (totaldeleted)
      {
        EXEC_TIMING_END("Scheduled2Delete");
        gOFS->MgmStats.Add("Scheduled2Delete", 0, 0, totaldeleted);
        error.setErrInfo(0, "submitted");
        return SFS_DATA;
      }
      else
      {
        error.setErrInfo(0, "");
        return SFS_DATA;
      }
    }


    if (execmd == "txstate")
    {
      // -----------------------------------------------------------------------
      // Set the transfer state (and log)
      // -----------------------------------------------------------------------
      REQUIRE_SSS_OR_LOCAL_AUTH;
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;

      int envlen;
      EXEC_TIMING_BEGIN("TxStateLog");
      eos_thread_debug("Transfer state + log received for %s", env.Env(envlen));

      char* txid = env.Get("tx.id");
      char* sstate = env.Get("tx.state");
      char* logb64 = env.Get("tx.log.b64");
      char* sprogress = env.Get("tx.progress");

      if (txid)
      {
        long long id = strtoll(txid, 0, 10);
        if (sprogress)
        {
          if (sprogress)
          {
            float progress = atof(sprogress);
            if (!gTransferEngine.SetProgress(id, progress))
            {
              eos_thread_err("unable to set progress for transfer id=%lld progress=%.02f", id, progress);
              return Emsg(epname, error, ENOENT, "set transfer state - transfer has been canceled [EIDRM]", "");
            }
            else
            {
              eos_thread_info("id=%lld progress=%.02f", id, progress);
            }
          }
        }

        if (sstate)
        {
          char* logout = 0;
          unsigned loglen = 0;
          if (logb64)
          {
            XrdOucString slogb64 = logb64;

            if (eos::common::SymKey::Base64Decode(slogb64, logout, loglen))
            {
              logout[loglen] = 0;
              if (!gTransferEngine.SetLog(id, logout))
              {
                eos_thread_err("unable to set log for transfer id=%lld", id);
              }
            }
          }

          int state = atoi(sstate);
          if (!gTransferEngine.SetState(id, state))
          {
            eos_thread_err("unable to set state for transfer id=%lld state=%s",
                           id, TransferEngine::GetTransferState(state));
          }
          else
          {
            eos_thread_info("id=%lld state=%s", id, TransferEngine::GetTransferState(state));
          }
        }
      }

      gOFS->MgmStats.Add("TxState", vid.uid, vid.gid, 1);

      const char* ok = "OK";
      error.setErrInfo(strlen(ok) + 1, ok);
      EXEC_TIMING_END("TxState");
      return SFS_DATA;
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_ls (const char *inpath,
                    XrdOucErrInfo &error,
                    const XrdSecEntity *client,
                    const char *ininfo,
                    eos::ContainerMD::XAttrMap & map)
/*----------------------------------------------------------------------------*/
/*
 * @brief list extended attributes for a given directory
 *  
 * @param inpath directory name to list attributes
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param map return object with the extended attribute key-value map
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * See _attr_ls for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_ls";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_ls(path, error, vid, info, map);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_set (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key,
                     const char *value)
/*----------------------------------------------------------------------------*/
/*
 * @brief set an extended attribute for a given directory to key=value
 *  
 * @param inpath directory name to set attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to set
 * @param value value to set for key
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * See _attr_set for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_set";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Update, "update", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_set(path, error, vid, info, key, value);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_get (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key,
                     XrdOucString & value)
/*----------------------------------------------------------------------------*/
/*
 * @brief get an extended attribute for a given directory by key
 *  
 * @param inpath directory name to get attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to retrieve
 * @param value variable to store the value
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * See _attr_get for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_get";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_get(path, error, vid, info, key, value);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_rem (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete an extended attribute for a given directory by key
 *  
 * @param inpath directory name to delete attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to delete
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * See _attr_rem for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_rm";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;

  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Delete, "delete", inpath, error);

  eos::common::Mapping::IdMap(client, info, tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_rem(path, error, vid, info, key);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_ls (const char *path,
                     XrdOucErrInfo &error,
                     eos::common::Mapping::VirtualIdentity &vid,
                     const char *info,
                     eos::ContainerMD::XAttrMap & map)
/*----------------------------------------------------------------------------*/
/*
 * @brief list extended attributes for a given directory
 *  
 * @param path directory name to list attributes
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param map return object with the extended attribute key-value map
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Normal unix permissions R_OK & X_OK are needed to list attributes.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_ls";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrLs");

  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(path);
    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
    {
      XrdOucString key = it->first.c_str();
      map[it->first] = it->second;
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }
  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | R_OK)))
    if (!errno)errno = EPERM;


  EXEC_TIMING_END("AttrLs");

  if (errno)
    return Emsg(epname, error, errno, "list attributes", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_set (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key,
                      const char *value)
/*----------------------------------------------------------------------------*/
/*
 * @brief set an extended attribute for a given directory with key=value
 *  
 * @param path directory name to set attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to set
 * @param value value for key
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Only the owner of a directory can set extended attributes with user prefix.
 * sys prefix attributes can be set only by sudo'ers or root.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_set";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrSet");

  gOFS->MgmStats.Add("AttrSet", vid.uid, vid.gid, 1);

  if (!key || !value)
    return Emsg(epname, error, EINVAL, "set attribute", path);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid)))
      errno = EPERM;
    else
    {
      // check permissions in case of user attributes
      if (dh && Key.beginswith("user.") && (vid.uid != dh->getCUid())
          && (!vid.sudoer))
      {
        errno = EPERM;
      }
      else
      {
        dh->setAttribute(key, value);
        eosView->updateContainerStore(dh);
        errno = 0;
      }
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("AttrSet");

  if (errno)
    return Emsg(epname, error, errno, "set attributes", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_get (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key,
                      XrdOucString &value,
                      bool islocked)
/*----------------------------------------------------------------------------*/
/*
 * @brief get an extended attribute for a given directory by key
 *  
 * @param path directory name to get attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to get
 * @param value value returned
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Normal POSIX R_OK & X_OK permissions are required to retrieve a key.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_get";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrGet");

  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);

  if (!key)
    return Emsg(epname, error, EINVAL, "get attribute", path);

  value = "";

  // ---------------------------------------------------------------------------
  if (!islocked) gOFS->eosViewRWMutex.LockRead();
  try
  {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    value = (dh->getAttribute(key)).c_str();
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }
  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | R_OK)))
    if (!errno) errno = EPERM;

  if (!islocked) gOFS->eosViewRWMutex.UnLockRead();

  EXEC_TIMING_END("AttrGet");

  if (errno)
    return Emsg(epname, error, errno, "list attributes", path);
  ;

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_rem (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete an extended attribute for a given directory by key
 *  
 * @param path directory name to set attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to delete
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * Only the owner of a directory can delete an extended attributes with user prefix.
 * sys prefix attributes can be deleted only by sudo'ers or root.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_rm";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrRm");

  gOFS->MgmStats.Add("AttrRm", vid.uid, vid.gid, 1);

  if (!key)
    return Emsg(epname, error, EINVAL, "delete attribute", path);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid)))
      errno = EPERM;
    else
    {
      dh->removeAttribute(key);
      eosView->updateContainerStore(dh);
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }
  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | R_OK)))
    if (!errno) errno = EPERM;

  EXEC_TIMING_END("AttrRm");

  if (errno)
    return Emsg(epname, error, errno, "remove attribute", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_verifystripe (const char *path,
                          XrdOucErrInfo &error,
                          eos::common::Mapping::VirtualIdentity &vid,
                          unsigned long fsid,
                          XrdOucString option)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a verification message to a file system for a given file
 * 
 * @param path file name to verify
 * @param error error object
 * @param vid virtual identity of the client
 * @param fsid filesystem id where to run the verification 
 * @param option pass-through string for the verification
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed. 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "verifystripe";
  eos::ContainerMD *dh = 0;
  eos::FileMD *fmd = 0;

  EXEC_TIMING_BEGIN("VerifyStripe");

  errno = 0;
  unsigned long long fid = 0;
  unsigned long long cid = 0;
  int lid = 0;

  eos::ContainerMD::XAttrMap attrmap;

  gOFS->MgmStats.Add("VerifyStripe", vid.uid, vid.gid, 1);

  eos_debug("verify");
  eos::common::Path cPath(path);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
    {
      attrmap[it->first] = it->second;
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;


  if (errno)
  {
    return Emsg(epname, error, errno, "verify stripe", path);
  }

  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    fid = fmd->getId();
    lid = fmd->getLayoutId();
    cid = fmd->getContainerId();
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!errno)
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;
    if (FsView::gFsView.mIdView.count(fsid))
    {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }
    if (!verifyfilesystem)
    {
      errno = EINVAL;
      return Emsg(epname, error, ENOENT,
                  "verify stripe - filesystem does not exist",
                  fmd->getName().c_str());
    }

    XrdOucString receiver = verifyfilesystem->GetQueue().c_str();
    XrdOucString opaquestring = "";
    // build the opaquestring contents
    opaquestring += "&mgm.localprefix=";
    opaquestring += verifyfilesystem->GetPath().c_str();
    opaquestring += "&mgm.fid=";
    XrdOucString hexfid;
    eos::common::FileId::Fid2Hex(fid, hexfid);
    opaquestring += hexfid;
    opaquestring += "&mgm.manager=";
    opaquestring += gOFS->ManagerId.c_str();
    opaquestring += "&mgm.access=verify";
    opaquestring += "&mgm.fsid=";
    opaquestring += (int) verifyfilesystem->GetId();
    if (attrmap.count("user.tag"))
    {
      opaquestring += "&mgm.container=";
      opaquestring += attrmap["user.tag"].c_str();
    }
    XrdOucString sizestring = "";
    opaquestring += "&mgm.cid=";
    opaquestring += eos::common::StringConversion::GetSizeString(sizestring, cid);
    opaquestring += "&mgm.path=";
    opaquestring += path;
    opaquestring += "&mgm.lid=";
    opaquestring += lid;

    if (option.length())
    {
      opaquestring += option;
    }

    XrdMqMessage message("verifycation");
    XrdOucString msgbody = "mgm.cmd=verify";

    msgbody += opaquestring;

    message.SetBody(msgbody.c_str());

    if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
    {
      eos_static_err("unable to send verification message to %s", receiver.c_str());
      errno = ECOMM;
    }
    else
    {
      errno = 0;
    }
  }

  EXEC_TIMING_END("VerifyStripe");

  if (errno)
    return Emsg(epname, error, errno, "verify stripe", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropstripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long fsid,
                        bool forceRemove)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a drop message to a file system for a given file
 * 
 * @param path file name to drop stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param fsid filesystem id where to run the drop 
 * @param forceRemove if true the stripe is immediatly dropped 
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed. 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "dropstripe";
  eos::ContainerMD *dh = 0;
  eos::FileMD *fmd = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("DropStripe");

  gOFS->MgmStats.Add("DropStripe", vid.uid, vid.gid, 1);

  eos_debug("drop");
  eos::common::Path cPath(path);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;

  if (errno)
  {
    return Emsg(epname, error, errno, "drop stripe", path);
  }

  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    if (!forceRemove)
    {
      // we only unlink a location
      if (fmd->hasLocation(fsid))
      {
        fmd->unlinkLocation(fsid);
        gOFS->eosView->updateFileStore(fmd);
        eos_debug("unlinking location %u", fsid);
      }
      else
      {
        errno = ENOENT;
      }
    }
    else
    {
      // we unlink and remove a location by force
      if (fmd->hasLocation(fsid))
      {
        fmd->unlinkLocation(fsid);
      }
      fmd->removeLocation(fsid);
      gOFS->eosView->updateFileStore(fmd);
      eos_debug("removing/unlinking location %u", fsid);
    }
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("DropStripe");

  if (errno)
    return Emsg(epname, error, errno, "drop stripe", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_movestripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long sourcefsid,
                        unsigned long targetfsid,
                        bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a move message for a given file from source to target file system
 * 
 * @param path file name to move stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param expressflag if true the move is put in front of the queue on the FST
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 * It calls _replicatestripe internally. 
 */
/*----------------------------------------------------------------------------*/
{

  EXEC_TIMING_BEGIN("MoveStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, true, expressflag);
  EXEC_TIMING_END("MoveStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_copystripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long sourcefsid,
                        unsigned long targetfsid,
                        bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a copy message for a given file from source to target file system
 * 
 * @param path file name to copy stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param expressflag if true the copy is put in front of the queue on the FST
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed. 
 * It calls _replicatestripe internally.
 */
/*----------------------------------------------------------------------------*/
{

  EXEC_TIMING_BEGIN("CopyStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, false, expressflag);
  EXEC_TIMING_END("CopyStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe (const char *path,
                             XrdOucErrInfo &error,
                             eos::common::Mapping::VirtualIdentity &vid,
                             unsigned long sourcefsid,
                             unsigned long targetfsid,
                             bool dropsource,
                             bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a replication message for a given file from source to target file system
 * 
 * @param path file name to copy stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param dropsource indicates if the source is deleted(dropped) after successfull replication
 * @param expressflag if true the copy is put in front of the queue on the FST
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed. 
 * It calls _replicatestripe with a file meta data object.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "replicatestripe";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("ReplicateStripe");

  eos::common::Path cPath(path);

  eos_debug("replicating %s from %u=>%u [drop=%d]", path, sourcefsid, targetfsid, dropsource);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;

  eos::FileMD * fmd = 0;

  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    if (fmd->hasLocation(sourcefsid))
    {
      if (fmd->hasLocation(targetfsid))
      {
        errno = EEXIST;
      }
    }
    else
    {
      // this replica does not exist!
      errno = ENODATA;
    }
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  if (errno)
  {
    // -------------------------------------------------------------------------

    return Emsg(epname, error, errno, "replicate stripe", path);
  }

  // make a copy of the file meta data to release the lock
  eos::FileMD fmdCopy(*fmd);
  fmd = &fmdCopy;

  // ------------------------------------------

  int retc = _replicatestripe(fmd, path, error, vid, sourcefsid, targetfsid, dropsource, expressflag);

  EXEC_TIMING_END("ReplicateStripe");

  return retc;

}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe (eos::FileMD *fmd,
                             const char* path,
                             XrdOucErrInfo &error,
                             eos::common::Mapping::VirtualIdentity &vid,
                             unsigned long sourcefsid,
                             unsigned long targetfsid,
                             bool dropsource,
                             bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a replication message for a given file from source to target file system
 * 
 * @param fmd namespace file meta data object
 * @param path file name 
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param dropsource indicates if the source is deleted(dropped) after successfull replication
 * @param expressflag if true the copy is put in front of the queue on the FST
 * 
 * @return SFS_OK if success otherwise SFS_ERROR
 * 
 * The function sends an appropriate message to the target FST. 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "replicatestripe";
  unsigned long long fid = fmd->getId();
  unsigned long long cid = fmd->getContainerId();
  long unsigned int lid = fmd->getLayoutId();
  uid_t uid = fmd->getCUid();
  gid_t gid = fmd->getCGid();

  unsigned long long size = fmd->getSize();

  if (dropsource)
    gOFS->MgmStats.Add("MoveStripe", vid.uid, vid.gid, 1);
  else
    gOFS->MgmStats.Add("CopyStripe", vid.uid, vid.gid, 1);

  if ((!sourcefsid) || (!targetfsid))
  {
    eos_err("illegal fsid sourcefsid=%u targetfsid=%u", sourcefsid, targetfsid);
    return Emsg(epname, error, EINVAL,
                "illegal source/target fsid", fmd->getName().c_str());
  }

  eos::mgm::FileSystem* sourcefilesystem = 0;
  eos::mgm::FileSystem* targetfilesystem = 0;


  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  if (FsView::gFsView.mIdView.count(sourcefsid))
  {
    sourcefilesystem = FsView::gFsView.mIdView[sourcefsid];
  }

  if (FsView::gFsView.mIdView.count(targetfsid))
  {
    targetfilesystem = FsView::gFsView.mIdView[targetfsid];
  }

  if (!sourcefilesystem)
  {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - source filesystem does not exist",
                fmd->getName().c_str());
  }

  if (!targetfilesystem)
  {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - target filesystem does not exist",
                fmd->getName().c_str());
  }

  // snapshot the filesystems
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  sourcefilesystem->SnapShotFileSystem(source_snapshot);
  targetfilesystem->SnapShotFileSystem(target_snapshot);

  // build a transfer capability
  XrdOucString source_capability = "";
  XrdOucString sizestring;
  source_capability += "mgm.access=read";
  source_capability += "&mgm.lid=";
  source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
  // make's it a plain replica
  source_capability += "&mgm.cid=";
  source_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  source_capability += "&mgm.ruid=";
  source_capability += (int) 1;
  source_capability += "&mgm.rgid=";
  source_capability += (int) 1;
  source_capability += "&mgm.uid=";
  source_capability += (int) 1;
  source_capability += "&mgm.gid=";
  source_capability += (int) 1;
  source_capability += "&mgm.path=";
  source_capability += path;
  source_capability += "&mgm.manager=";
  source_capability += gOFS->ManagerId.c_str();
  source_capability += "&mgm.fid=";

  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(fid, hexfid);
  source_capability += hexfid;

  source_capability += "&mgm.sec=";
  source_capability += eos::common::SecEntity::ToKey(0, "eos/replication").c_str();


  // this is a move of a replica
  if (dropsource)
  {
    source_capability += "&mgm.drainfsid=";
    source_capability += (int) source_snapshot.mId;
  }

  // build the source_capability contents
  source_capability += "&mgm.localprefix=";
  source_capability += source_snapshot.mPath.c_str();
  source_capability += "&mgm.fsid=";
  source_capability += (int) source_snapshot.mId;
  source_capability += "&mgm.sourcehostport=";
  source_capability += source_snapshot.mHostPort.c_str();

  XrdOucString target_capability = "";
  target_capability += "mgm.access=write";
  target_capability += "&mgm.lid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
  // make's it a plain replica
  target_capability += "&mgm.cid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  target_capability += "&mgm.ruid=";
  target_capability += (int) 1;
  target_capability += "&mgm.rgid=";
  target_capability += (int) 1;
  target_capability += "&mgm.uid=";
  target_capability += (int) 1;
  target_capability += "&mgm.gid=";
  target_capability += (int) 1;
  target_capability += "&mgm.path=";
  target_capability += path;
  target_capability += "&mgm.manager=";
  target_capability += gOFS->ManagerId.c_str();
  target_capability += "&mgm.fid=";
  target_capability += hexfid;

  target_capability += "&mgm.sec=";
  target_capability += eos::common::SecEntity::ToKey(0, "eos/replication").c_str();
  if (dropsource)
  {
    target_capability += "&mgm.drainfsid=";
    target_capability += (int) source_snapshot.mId;
  }

  target_capability += "&mgm.source.lid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid);
  target_capability += "&mgm.source.ruid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) uid);
  target_capability += "&mgm.source.rgid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) gid);

  // build the target_capability contents
  target_capability += "&mgm.localprefix=";
  target_capability += target_snapshot.mPath.c_str();
  target_capability += "&mgm.fsid=";
  target_capability += (int) target_snapshot.mId;
  target_capability += "&mgm.targethostport=";
  target_capability += target_snapshot.mHostPort.c_str();
  target_capability += "&mgm.bookingsize=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
  // issue a source_capability
  XrdOucEnv insource_capability(source_capability.c_str());
  XrdOucEnv intarget_capability(target_capability.c_str());
  XrdOucEnv* source_capabilityenv = 0;
  XrdOucEnv* target_capabilityenv = 0;
  XrdOucString fullcapability = "";
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  int caprc = 0;
  if ((caprc = gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
      (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey)))
  {
    eos_err("unable to create source/target capability - errno=%u", caprc);
    errno = caprc;
  }
  else
  {
    errno = 0;
    int caplen = 0;
    XrdOucString source_cap = source_capabilityenv->Env(caplen);
    XrdOucString target_cap = target_capabilityenv->Env(caplen);
    source_cap.replace("cap.sym", "source.cap.sym");
    target_cap.replace("cap.sym", "target.cap.sym");
    source_cap.replace("cap.msg", "source.cap.msg");
    target_cap.replace("cap.msg", "target.cap.msg");
    source_cap += "&source.url=root://";
    source_cap += source_snapshot.mHostPort.c_str();
    source_cap += "//replicate:";
    source_cap += hexfid;
    target_cap += "&target.url=root://";
    target_cap += target_snapshot.mHostPort.c_str();
    target_cap += "//replicate:";
    target_cap += hexfid;
    fullcapability += source_cap;
    fullcapability += target_cap;

    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

    bool sub = targetfilesystem->GetExternQueue()->Add(txjob);
    eos_info("info=\"submitted transfer job\" subretc=%d fxid=%s fid=%llu cap=%s\n",
             sub, hexfid.c_str(), fid, fullcapability.c_str());

    if (!sub)
      errno = ENXIO;
    else
      errno = 0;

    if (txjob)
      delete txjob;
    else
    {
      eos_err("Couldn't create transfer job to replicate stripe of %s", path);
      errno = ENOMEM;
    }

    if (source_capabilityenv)
      delete source_capabilityenv;
    if (target_capabilityenv)
      delete target_capabilityenv;
  }

  if (errno)
    return Emsg(epname, error, errno, "replicate stripe", fmd->getName().c_str());

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::merge (
                  const char* src,
                  const char* dst,
                  XrdOucErrInfo &error,
                  eos::common::Mapping::VirtualIdentity & vid
                  )
/*----------------------------------------------------------------------------*/
/**
 * @brief merge one file into another one 
 * @param src to merge
 * @param dst to merge into
 * @return SFS_OK if success 
 * 
 * This command act's like a rename and keeps the ownership and creation time
 * of the target file.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  eos::common::RWMutexReadLock(gOFS->eosViewRWMutex);
  eos::FileMD* src_fmd = 0;
  eos::FileMD* dst_fmd = 0;

  if (!src || !dst)
  {
    return Emsg("merge", error, EINVAL, "merge source into destination path - source or target missing");
  }

  std::string src_path = src;
  std::string dst_path = dst;
  try
  {
    src_fmd = gOFS->eosView->getFile(src_path);
    dst_fmd = gOFS->eosView->getFile(dst_path);

    // -------------------------------------------------------------------------
    // inherit some core meta data, the checksum must be right by construction,
    // so we don't copy it
    // -------------------------------------------------------------------------

    // inherit the previous ownership
    src_fmd->setCUid(dst_fmd->getCUid());
    src_fmd->setCGid(dst_fmd->getCGid());
    // inherit the creation time
    eos::FileMD::ctime_t ctime;
    dst_fmd->getCTime(ctime);
    src_fmd->setCTime(ctime);
    // change the owner of the source file
    eosView->updateFileStore(src_fmd);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("caught exception %d %s\n",
              e.getErrno(),
              e.getMessage().str().c_str());
  }

  int rc = SFS_OK;

  if (src_fmd && dst_fmd)
  {
    // remove the destination file
    rc |= gOFS->_rem(dst_path.c_str(),
                     error,
                     rootvid,
                     "");

    // rename the source to destination
    rc |= gOFS->_rename(src_path.c_str(),
                        dst_path.c_str(),
                        error,
                        rootvid,
                        "",
                        "",
                        true,
                        false
                        );
  }
  else
  {
    return Emsg("merge", error, EINVAL, "merge source into destination path - "
                "cannot get file meta data ", src_path.c_str());
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::SendResync (eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a resync command for a file identified by id and filesystem
 * 
 * @param fid file id to be resynced
 * @param fsid filesystem id where the file should be resynced
 * 
 * @return true if successfully send otherwise false
 * 
 * A resync synchronizes the cache DB on the FST with the meta data on disk 
 * and on the MGM and flags files accordingly with size/checksum errors.
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("SendResync");

  gOFS->MgmStats.Add("SendResync", vid.uid, vid.gid, 1);

  XrdMqMessage message("resync");
  XrdOucString msgbody = "mgm.cmd=resync";

  char payload[4096];
  snprintf(payload, sizeof (payload) - 1, "&mgm.fsid=%lu&mgm.fid=%llu",
           (unsigned long) fsid, (unsigned long long) fid);
  msgbody += payload;

  message.SetBody(msgbody.c_str());

  // figure out the receiver
  XrdOucString receiver;

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;
    if (FsView::gFsView.mIdView.count(fsid))
    {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }
    if (!verifyfilesystem)
    {
      eos_err("fsid=%lu is not in the configuration - cannot send resync message",
              fsid);
      return false;
    }
    receiver = verifyfilesystem->GetQueue().c_str();
  }


  if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
  {

    eos_err("unable to send resync message to %s", receiver.c_str());
    return false;
  }

  EXEC_TIMING_END("SendResync");

  return true;
}

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StartMgmStats (void *pp)
{

  XrdMgmOfs* ofs = (XrdMgmOfs*) pp;
  ofs->MgmStats.Circulate();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StartMgmFsConfigListener (void *pp)
{

  XrdMgmOfs* ofs = (XrdMgmOfs*) pp;
  ofs->FsConfigListener();
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::DeleteExternal (eos::common::FileSystem::fsid_t fsid,
                           unsigned long long fid)
/*----------------------------------------------------------------------------*/
/*
 * @brief send an explicit deletion message to a fsid/fid pair
 * 
 * @param fsid file system id where to run a deletion
 * @param fid file id to be deleted
 * 
 * @result true if successfully sent otherwise false
 * 
 * This routine signs a deletion message for the given file id and sends it 
 * to the referenced file system. 
 */
/*----------------------------------------------------------------------------*/
{
  // ---------------------------------------------------------------------------
  // send an explicit deletion message to any fsid/fid pair
  // ---------------------------------------------------------------------------


  XrdMqMessage message("deletion");

  eos::mgm::FileSystem* fs = 0;
  XrdOucString receiver = "";
  XrdOucString msgbody = "mgm.cmd=drop";
  XrdOucString capability = "";
  XrdOucString idlist = "";

  // get the filesystem from the FS view
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mIdView.count(fsid))
    {
      fs = FsView::gFsView.mIdView[fsid];
      if (fs)
      {
        capability += "&mgm.access=delete";
        capability += "&mgm.manager=";
        capability += gOFS->ManagerId.c_str();
        capability += "&mgm.fsid=";
        capability += (int) fs->GetId();
        capability += "&mgm.localprefix=";
        capability += fs->GetPath().c_str();
        capability += "&mgm.fids=";
        XrdOucString hexfid = "";
        eos::common::FileId::Fid2Hex(fid, hexfid);
        capability += hexfid;
        receiver = fs->GetQueue().c_str();
      }
    }
  }

  bool ok = false;

  if (fs)
  {
    XrdOucEnv incapability(capability.c_str());
    XrdOucEnv* capabilityenv = 0;
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    int caprc = 0;
    if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
    {
      eos_static_err("unable to create capability - errno=%u", caprc);
    }
    else
    {
      int caplen = 0;
      msgbody += capabilityenv->Env(caplen);
      message.SetBody(msgbody.c_str());
      if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
      {
        eos_static_err("unable to send deletion message to %s", receiver.c_str());
      }
      else
      {

        ok = true;
      }
    }
  }
  return ok;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::FsConfigListener ()
/*----------------------------------------------------------------------------*/
/*
 * @brief file system listener agent starting drain jobs when receving opserror
 * and applying remote master configuration changes to the local configuration
 * object.
 * 
 * This thread agent catches 'opserror' states on filesystems and executes the
 * drain job start routine on the referenced filesystem. If a filesystem
 * is removing the error code it also run's a stop drain job routine.
 * Additionally it applies changes in the MGM configuration which have been
 * broadcasted by a remote master MGM.
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysTimer sleeper;
  sleeper.Snooze(5);
  // thread listening on filesystem errors and configuration changes
  do
  {
    gOFS->ObjectManager.SubjectsSem.Wait();

    XrdSysThread::SetCancelOff();

    // we always take a lock to take something from the queue and then release it
    gOFS->ObjectManager.SubjectsMutex.Lock();

    // listens on modifications on filesystem objects
    while (gOFS->ObjectManager.NotificationSubjects.size())
    {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS->ObjectManager.NotificationSubjects.front();
      gOFS->ObjectManager.NotificationSubjects.pop_front();
      gOFS->ObjectManager.SubjectsMutex.UnLock();

      std::string newsubject = event.mSubject.c_str();


      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation)
      {
        // ---------------------------------------------------------------------
        // handle subject creation
        // ---------------------------------------------------------------------
        eos_static_debug("received creation on subject %s\n", newsubject.c_str());
        gOFS->ObjectManager.SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion)
      {
        // ---------------------------------------------------------------------
        // handle subject deletion
        // ---------------------------------------------------------------------
        eos_static_debug("received deletion on subject %s\n", newsubject.c_str());

        gOFS->ObjectManager.SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification)
      {
        // ---------------------------------------------------------------------
        // handle subject modification
        // ---------------------------------------------------------------------

        eos_static_info("received modification on subject %s", newsubject.c_str());
        // if this is an error status on a file system, check if the filesystem is > drained state and in this case launch a drain job with
        // the opserror flag by calling StartDrainJob
        // We use directly the ObjectManager Interface because it is more handy with the available information we have at this point

        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;
        if ((dpos = queue.find(";")) != std::string::npos)
        {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == MgmConfigQueue.c_str())
        {
          // -------------------------------------------------------------------
          // this is an MGM configuration modification
          // -------------------------------------------------------------------
          if (!gOFS->MgmMaster.IsMaster())
          {
            // only an MGM slave needs to aplly this

            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              XrdOucString err;
              XrdOucString value = hash->Get(key).c_str();
              if (value.c_str())
              {
                gOFS->ConfEngine->ApplyEachConfig(key.c_str(), &value, (void*) &err);
              }
              gOFS->ObjectManager.HashMutex.UnLockRead();
            }
          }
        }
        else
        {
          // -------------------------------------------------------------------
          // this is a filesystem status error
          // ------------------------------------------------------------------- 
          if (gOFS->MgmMaster.IsMaster())
          {
            // only an MGM master needs to initiate draining
            eos::common::FileSystem::fsid_t fsid = 0;
            FileSystem* fs = 0;
            long long errc = 0;
            std::string configstatus = "";
            std::string bootstatus = "";
            int cfgstatus = 0;
            int bstatus = 0;

            // read the id from the hash and the current error value
            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
              errc = (int) hash->GetLongLong("stat.errc");
              configstatus = hash->Get("configstatus");
              bootstatus = hash->Get("stat.boot");
              cfgstatus = eos::common::FileSystem::GetConfigStatusFromString(configstatus.c_str());
              bstatus = eos::common::FileSystem::GetStatusFromString(bootstatus.c_str());
            }
            gOFS->ObjectManager.HashMutex.UnLockRead();

            if (fsid && errc && (cfgstatus >= eos::common::FileSystem::kRO) && (bstatus == eos::common::FileSystem::kOpsError))
            {
              // this is the case we take action and explicitly ask to start a drain job
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              if (FsView::gFsView.mIdView.count(fsid))
                fs = FsView::gFsView.mIdView[fsid];
              else
                fs = 0;
              if (fs)
              {
                fs->StartDrainJob();
              }
            }
            if (fsid && (!errc))
            {
              // make sure there is no drain job triggered by a previous filesystem errc!=0
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              if (FsView::gFsView.mIdView.count(fsid))
                fs = FsView::gFsView.mIdView[fsid];
              else
                fs = 0;
              if (fs)
              {
                fs->StopDrainJob();
              }
            }
          }
        }
        gOFS->ObjectManager.SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectKeyDeletion)
      {
        // ---------------------------------------------------------------------
        // handle subject key deletion
        // ---------------------------------------------------------------------
        eos_static_debug("received deletion on subject %s\n", newsubject.c_str());

        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;
        if ((dpos = queue.find(";")) != std::string::npos)
        {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        gOFS->ConfEngine->ApplyKeyDeletion(key.c_str());

        gOFS->ObjectManager.SubjectsMutex.Lock();
        continue;
      }
      eos_static_warning("msg=\"don't know what to do with subject\" subject=%s", newsubject.c_str());
      gOFS->ObjectManager.SubjectsMutex.Lock();
      continue;
    }
    gOFS->ObjectManager.SubjectsMutex.UnLock();
    XrdSysThread::SetCancelOff();
  }
  while (1);
}
