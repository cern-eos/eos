// ----------------------------------------------------------------------
// File: WFE.cc
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/ShellCmd.hh"
#include "mgm/Quota.hh"
#include "mgm/WFE.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "XrdSys/XrdSysTimer.hh"

#define EOS_WFE_BASH_PREFIX "/var/eos/wfe/bash/"

XrdSysMutex eos::mgm::WFE::gSchedulerMutex;
XrdScheduler* eos::mgm::WFE::gScheduler;

/*----------------------------------------------------------------------------*/
extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

EOSMGMNAMESPACE_BEGIN

using namespace eos::common;

/*----------------------------------------------------------------------------*/
WFE::WFE()
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor of the work flow engine
 */
/*----------------------------------------------------------------------------*/
{
  mThread = 0;
  mMs = 0;
  mActiveJobs = 0;
  eos::common::Mapping::Root(mRootVid);
  XrdSysMutexHelper sLock(gSchedulerMutex);
  gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
  gScheduler->Start();
}

/*----------------------------------------------------------------------------*/
bool
WFE::Start()
/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous WFE thread startup function
 */
/*----------------------------------------------------------------------------*/
{
  // run an asynchronous WFE thread
  mThread = 0;
  XrdSysThread::Run(&mThread,
                    WFE::StartWFEThread,
                    static_cast<void*>(this),
                    XRDSYSTHREAD_HOLD,
                    "WFE engine Thread");
  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
void
WFE::Stop()
/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous WFE thread stop function
 */
/*----------------------------------------------------------------------------*/
{
  // cancel the asynchronous WFE thread
  if (mThread) {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }

  mThread = 0;
}

void*
WFE::StartWFEThread(void* arg)
{
  return reinterpret_cast<WFE*>(arg)->WFEr();
}

/*----------------------------------------------------------------------------*/
void*
WFE::WFEr()
/*----------------------------------------------------------------------------*/
/**
 * @brief WFE method doing the actual workflow
 *
 * This thread method loops in regular intervals over all workflow jobs in the
 * workflow directory /eos/<instance>/proc/workflow/
 */
/*----------------------------------------------------------------------------*/
{
  // ---------------------------------------------------------------------------
  // wait that the namespace is initialized
  // ---------------------------------------------------------------------------
  bool go = false;

  do {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);

      if (gOFS->Initialized == gOFS->kBooted) {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  } while (!go);

  XrdSysTimer sleeper;
  sleeper.Snooze(10);
  //----------------------------------------------------------------------------
  // Eternal thread doing WFE scans
  //----------------------------------------------------------------------------
  time_t snoozetime = 10;
  size_t lWFEntx = 0;
  time_t cleanuptime = 0;
  eos_static_info("msg=\"async WFE thread started\"");

  while (1) {
    // -------------------------------------------------------------------------
    // every now and then we wake up
    // -------------------------------------------------------------------------
    XrdSysThread::SetCancelOff();
    bool IsEnabledWFE;
    time_t lWFEInterval;
    time_t lStartTime = time(NULL);
    time_t lStopTime;
    time_t lKeepTime = 7 * 86400;
    std::map<std::string, std::set<std::string> > wfedirs;
    XrdOucString stdErr;
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      if (FsView::gFsView.mSpaceView.count("default") &&
          (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") == "on")) {
        IsEnabledWFE = true;
      } else {
        IsEnabledWFE = false;
      }

      if (FsView::gFsView.mSpaceView.count("default")) {
        lWFEInterval =
          atoi(FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe.interval").c_str());
        lWFEntx =
          atoi(FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe.ntx").c_str());
        lKeepTime = atoi(
                      FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe.keepTIME").c_str());

        if (!lKeepTime) {
          lKeepTime = 7 * 86400;
        }
      } else {
        lWFEInterval = 0;
        lWFEntx = 0;
      }
    }

    // only a master needs to run WFE
    if (gOFS->MgmMaster.IsMaster() && IsEnabledWFE) {
      // -------------------------------------------------------------------------
      // do a find
      // -------------------------------------------------------------------------
      eos_static_info("msg=\"start WFE scan\"");
      // -------------------------------------------------------------------------
      // find all directories defining an WFE policy
      // -------------------------------------------------------------------------
      gOFS->MgmStats.Add("WFEFind", 0, 0, 1);
      EXEC_TIMING_BEGIN("WFEFind");
      // prepare four queries today, yestereday for queued and error jobs
      std::string queries[4];

      for (size_t i = 0; i < 4; ++i) {
        queries[i] = gOFS->MgmProcWorkflowPath.c_str();
        queries[i] += "/";
      }

      {
        // today
        time_t when = time(NULL);
        std::string day = eos::common::Timing::UnixTimstamp_to_Day(when);
        queries[0] += day;
        queries[0] += "/q/";
        queries[1] += day;
        queries[1] += "/e/";
        //yesterday
        when -= (24 * 3600);
        day = eos::common::Timing::UnixTimstamp_to_Day(when);
        queries[2] += day;
        queries[2] += "/q/";
        queries[3] += day;
        queries[3] += "/e/";
      }

      for (size_t i = 0; i < 4; ++i) {
        eos_static_info("query-path=%s", queries[i].c_str());
        gOFS->_find(queries[i].c_str(),
                    mError,
                    stdErr,
                    mRootVid,
                    wfedirs,
                    0,
                    0,
                    false,
                    0,
                    false,
                    0
                   );
      }

      {
        eos_static_info("msg=\"finished WFE find\" WFE-dirs=%llu %s",
                        wfedirs.size(), stdErr.c_str()
                       );
        time_t now = time(NULL);

        for (auto it = wfedirs.begin(); it != wfedirs.end(); it++) {
          // -------------------------------------------------------------------
          // get workflows
          // -------------------------------------------------------------------
          if (it->second.size()) {
            for (auto wit = it->second.begin(); wit != it->second.end(); ++wit) {
              eos_static_info("wfe-dir=\"%s\" wfe-job=\"%s\"", it->first.c_str(),
                              wit->c_str());
              std::string f = it->first;
              f += *wit;
              Job* job = new Job();

              if (!job || job->Load(f)) {
                eos_static_err("msg=\"cannot load workflow entry\" value=\"%s\"", f.c_str());

                if (job) {
                  delete job;
                }
              } else {
                // don't schedule jobs for the future
                if ((!job->mActions.size()) || (now < job->mActions[0].mTime)) {
                  delete job;
                  continue;
                }

                // stop scheduling if there are too many jobs running
                if ((lWFEntx - GetActiveJobs()) <= 0) {
                  if (lWFEntx > 0) {
                    mDoneSignal.Wait(10);

                    if ((lWFEntx - GetActiveJobs()) <= 0) {
                      delete job;
                      break;
                    }
                  }
                }

                if (!job->IsSync()) {
                  // use the shared scheduler for asynchronous jobs
                  XrdSysMutexHelper sLock(gSchedulerMutex);
                  time_t storetime = 0;
                  // move job into the scheduled queue
                  job->Move("q", "s", storetime);
                  job->mActions[0].mQueue = "s";
                  job->mActions[0].mTime = storetime;
                  XrdOucString tst;
                  job->mActions[0].mWhen = eos::common::StringConversion::GetSizeString(tst,
                                           (unsigned long long) storetime);
                  gScheduler->Schedule((XrdJob*) job);
                  IncActiveJobs();
                  eos_static_info("msg=\"scheduled workflow\" job=\"%s\"",
                                  job->mDescription.c_str());
                } else {
                  delete job;
                }
              }
            }
          }
        }
      }

      EXEC_TIMING_END("WFEFind");
      eos_static_info("msg=\"finished WFE application\" WFE-dirs=%llu",
                      wfedirs.size()
                     );
    }

    lStopTime = time(NULL);

    if ((lStopTime - lStartTime) < lWFEInterval) {
      snoozetime = lWFEInterval - (lStopTime - lStartTime);
    }

    eos_static_info("snooze-time=%llu enabled=%d", snoozetime, IsEnabledWFE);
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    time_t snoozeinterval = 1;
    size_t snoozeloop = snoozetime / 1;

    for (size_t i = 0; i < snoozeloop; i++) {
      sleeper.Snooze(snoozeinterval);
      {
        // check if the setting changes
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        if (FsView::gFsView.mSpaceView.count("default") &&
            (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") == "on")) {
          if (!IsEnabledWFE) {
            break;
          }
        } else {
          if (IsEnabledWFE) {
            break;
          }
        }
      }
    }

    if (gOFS->MgmMaster.IsMaster() && (!cleanuptime ||
                                       (cleanuptime < time(NULL)))) {
      time_t now = time(NULL);
      eos_static_info("msg=\"clean old workflows\"");
      XrdMgmOfsDirectory dir;

      if (dir.open(gOFS->MgmProcWorkflowPath.c_str(), mRootVid, "") != SFS_OK) {
        eos_static_err("msg=\"failed to open proc workflow directory\"");
        continue;
      }

      const char* entry;

      while ((entry = dir.nextEntry())) {
        std::string when = entry;

        if ((when == ".") ||
            (when == "..")) {
          continue;
        }

        time_t tst = eos::common::Timing::Day_to_UnixTimestamp(when);

        if (!tst || (tst < (now - lKeepTime))) {
          eos_static_info("msg=\"cleaning\" dir=\"%s\"", entry);
          ProcCommand Cmd;
          XrdOucString info;
          XrdOucString out;
          XrdOucString err;
          info = "mgm.cmd=rm&eos.ruid=0&eos.rgid=0&mgm.deletion=deep&mgm.option=r&mgm.path=";
          info += gOFS->MgmProcWorkflowPath;
          info += "/";
          info += entry;
          Cmd.open("/proc/user", info.c_str(), mRootVid, &mError);
          Cmd.AddOutput(out, err);

          if (err.length()) {
            eos_static_err("msg=\"cleaning failed\" errmsg=\"%s\"", err.c_str());
          } else {
            eos_static_info("msg=\"cleaned\" dri=\"%s\"");
          }

          Cmd.close();
        }
      }

      cleanuptime = now + 3600;
    }
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
/**
 * @brief store a workflow jobs in the workflow queue
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
WFE::Job::Save(std::string queue, time_t& when, int action, int retry)
{
  if (mActions.size() != 1) {
    return -1;
  }

  std::string workflowdir = gOFS->MgmProcWorkflowPath.c_str();
  workflowdir += "/";
  workflowdir += mActions[action].mDay;
  workflowdir += "/";
  workflowdir += queue;
  workflowdir += "/";
  workflowdir += mActions[action].mWorkflow;
  workflowdir += "/";
  std::string entry;
  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(mFid, hexfid);
  entry = hexfid.c_str();
  eos_static_info("workflowdir=\"%s\" retry=%d when=%u job-time=%s",
                  workflowdir.c_str(),
                  retry, when, mActions[action].mWhen.c_str());
  XrdOucErrInfo lError;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  // check that the workflow directory exists
  struct stat buf;

  if (gOFS->_stat(workflowdir.c_str(),
                  &buf,
                  lError,
                  rootvid,
                  "")) {
    // create the workflow sub directory
    if (gOFS->_mkdir(workflowdir.c_str(),
                     S_IRWXU | SFS_O_MKPTH,
                     lError,
                     rootvid,
                     "")) {
      eos_static_err("msg=\"failed to create workflow directory\" path=\"%s\"",
                     workflowdir.c_str());
      return -1;
    }
  }

  // write a workflow file
  std::string workflowpath = workflowdir;

  // evt. store with the current time

  if (!when) {
    when = time(NULL);
  }

  XrdOucString tst;
  workflowpath += eos::common::StringConversion::GetSizeString(tst,
                  (unsigned long long) when);
  workflowpath += ":";
  workflowpath += entry;
  workflowpath += ":";
  workflowpath += mActions[action].mEvent;
  mWorkflowPath = workflowpath;

  if (gOFS->_touch(workflowpath.c_str(), lError, rootvid, 0)) {
    eos_static_err("msg=\"failed to create workflow entry\" path=\"%s\"",
                   workflowpath.c_str());
    return -1;
  }

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.action",
                      mActions[0].mAction.c_str())) {
    eos_static_err("msg=\"failed to store workflow action\" path=\"%s\" action=\"%s\"",
                   workflowpath.c_str(),
                   mActions[0].mAction.c_str());
    return -1;
  }

  std::string vids = eos::common::Mapping::VidToString(mVid).c_str();

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.vid",
                      vids.c_str())) {
    eos_static_err("msg=\"failed to store workflow vid\" path=\"%s\" vid=\"%s\"",
                   workflowpath.c_str(),
                   vids.c_str());
    return -1;
  }

  XrdOucString sretry;
  sretry += (int) retry;

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.wfe.retry",
                      sretry.c_str())) {
    eos_static_err("msg=\"failed to store workflow retry count\" path=\"%s\" retry=\"%d\"",
                   workflowpath.c_str(),
                   retry);
    return -1;
  }

  mRetry = retry;
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::Load(std::string path2entry)
/*----------------------------------------------------------------------------*/
/**
 * @brief load a workflow job from the given path
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  XrdOucErrInfo lError;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  std::string f = path2entry;
  f.erase(0, path2entry.rfind("/") + 1);
  std::string workflow = path2entry;
  workflow.erase(path2entry.rfind("/"));
  workflow.erase(0, workflow.rfind("/") + 1);
  std::string q = path2entry;
  q.erase(q.rfind("/"));
  q.erase(q.rfind("/"));
  q.erase(0, q.rfind("/") + 1);
  std::string when;
  std::string idevent;
  std::string id;
  std::string event;
  bool s1 = eos::common::StringConversion::SplitKeyValue(f, when, idevent, ":");
  bool s2 = eos::common::StringConversion::SplitKeyValue(idevent, id, event, ":");
  mWorkflowPath = path2entry;

  if (s1 && s2) {
    mFid = eos::common::FileId::Hex2Fid(id.c_str());
    eos_static_info("workflow=\"%s\" fid=%lx", workflow.c_str(), mFid);
    XrdOucString action;

    if (!gOFS->_attr_get(path2entry.c_str(), lError, rootvid, 0,
                         "sys.action", action)) {
      time_t t_when = strtoull(when.c_str(), 0, 10);
      AddAction(action.c_str(), event, t_when, workflow, q);
    } else {
      eos_static_err("msg=\"no action stored\" path=\"%s\"", f.c_str());
    }

    XrdOucString vidstring;

    if (!gOFS->_attr_get(path2entry.c_str(), lError, rootvid, 0,
                         "sys.vid", vidstring)) {
      if (!eos::common::Mapping::VidFromString(mVid, vidstring.c_str())) {
        eos_static_crit("parsing of %s failed - setting nobody\n", vidstring.c_str());
        eos::common::Mapping::Nobody(mVid);
      }
    } else {
      eos::common::Mapping::Nobody(mVid);
      eos_static_err("msg=\"no vid stored\" path=\"%s\"", f.c_str());
    }

    XrdOucString sretry;

    if (!gOFS->_attr_get(path2entry.c_str(), lError, rootvid, 0,
                         "sys.wfe.retry", sretry)) {
      mRetry = (int)strtoul(sretry.c_str(), 0, 10);
    } else {
      eos_static_err("msg=\"no retry stored\" path=\"%s\"", f.c_str());
    }
  } else {
    eos_static_err("msg=\"illegal workflow entry\" key=\"%s\"", f.c_str());
    return SFS_ERROR;
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::Move(std::string from_queue, std::string to_queue, time_t& when,
               int retry)
/*----------------------------------------------------------------------------*/
/**
 * @brief move workflow jobs between qeueus
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  if (Save(to_queue, when, 0, retry) == SFS_OK) {
    if ((from_queue != to_queue) && (Delete(from_queue) == SFS_ERROR)) {
      eos_static_err("msg=\"failed to remove for move from queue\"%s\" to  queue=\"%s\"",
                     from_queue.c_str(), to_queue.c_str());
    }
  } else {
    eos_static_err("msg=\"failed to save for move to queue\" queue=\"%s\"",
                   to_queue.c_str());
    return SFS_ERROR;
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
WFE::Job::Results(std::string queue, int retc, XrdOucString log, time_t when)
/*----------------------------------------------------------------------------*/
{
  std::string workflowdir = gOFS->MgmProcWorkflowPath.c_str();
  workflowdir += "/";
  workflowdir += mActions[0].mDay;
  workflowdir += "/";
  workflowdir += queue;
  workflowdir += "/";
  workflowdir += mActions[0].mWorkflow;
  workflowdir += "/";
  std::string entry;
  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(mFid, hexfid);
  entry = hexfid.c_str();
  eos_static_info("workflowdir=\"%s\" entry=%s", workflowdir.c_str(),
                  entry.c_str());
  XrdOucErrInfo lError;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  // check that the workflow directory exists
  struct stat buf;

  if (gOFS->_stat(workflowdir.c_str(),
                  &buf,
                  lError,
                  rootvid,
                  "")) {
    eos_static_err("msg=\"failed to find the workflow dir\" path=\"%s\"",
                   workflowdir.c_str());
    return -1;
  }

  // write a workflow file
  std::string workflowpath = workflowdir;
  XrdOucString tst;
  workflowpath += eos::common::StringConversion::GetSizeString(tst,
                  (unsigned long long) when);
  workflowpath += ":";
  workflowpath += entry;
  workflowpath += ":";
  workflowpath += mActions[0].mEvent;
  mWorkflowPath = workflowpath;
  XrdOucString sretc;
  sretc += retc;

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.wfe.retc",
                      sretc.c_str())) {
    eos_static_err("msg=\"failed to store workflow return code\" path=\"%s\" retc=\"%s\"",
                   workflowpath.c_str(),
                   sretc.c_str());
    return -1;
  }

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.wfe.log",
                      log.c_str())) {
    eos_static_err("msg=\"failed to store workflow log\" path=\"%s\" log=\"%s\"",
                   workflowpath.c_str(),
                   log.c_str());
    return -1;
  }

  return SFS_OK;
}



/*----------------------------------------------------------------------------*/
int
WFE::Job::Delete(std::string queue)
/*----------------------------------------------------------------------------*/
/**
 * @brief delete a workflow job from a queue
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  if (mActions.size() != 1) {
    return SFS_ERROR;
  }

  std::string workflowdir = gOFS->MgmProcWorkflowPath.c_str();
  workflowdir += "/";
  workflowdir += mActions[0].mDay;
  workflowdir += "/";
  workflowdir += queue;
  workflowdir += "/";
  workflowdir += mActions[0].mWorkflow;
  workflowdir += "/";
  std::string entry;
  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(mFid, hexfid);
  entry = hexfid.c_str();
  eos_static_info("workflowdir=\"%s\"", workflowdir.c_str());
  XrdOucErrInfo lError;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  // write a workflow file
  std::string workflowpath = workflowdir;
  workflowpath += mActions[0].mWhen;
  workflowpath += ":";
  workflowpath += entry;
  workflowpath += ":";
  workflowpath += mActions[0].mEvent;

  if (!gOFS->_rem(workflowpath.c_str(),
                  lError,
                  rootvid,
                  "",
                  false
                  , false, true)) {
    return SFS_OK;
  } else {
    eos_static_err("msg=\"failed to delete job\" job=\"%s\"", mDescription.c_str());
    return SFS_ERROR;
  }
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::DoIt(bool issync)
/*----------------------------------------------------------------------------*/
/**
 * @brief execute a workflow
 * @return
 *  */
/*----------------------------------------------------------------------------*/
{
  std::string method;
  std::string args;
  eos::common::Mapping::VirtualIdentity lRootVid;
  XrdOucErrInfo lError;
  eos::common::Mapping::Root(lRootVid);
  eos_static_info("queue=\"%s\"", mActions[0].mQueue.c_str());
  int retc = 0;
  time_t storetime = 0;

  if (mActions[0].mQueue == "s" || mActions[0].mQueue == "e") {
    if (eos::common::StringConversion::SplitKeyValue(mActions[0].mAction, method,
        args, ":")) {
      if (method == "mail") {
        std::string recipient;
        std::string freetext;

        if (!eos::common::StringConversion::SplitKeyValue(args, recipient, freetext,
            ":")) {
          recipient = args;
          freetext = "EOS workflow notification";
        }

        std::string topic = gOFS->MgmOfsInstanceName.c_str();
        topic += " ( ";
        topic += gOFS->HostName;
        topic += " ) ";
        topic += " ";
        topic += " event=";
        topic += mActions[0].mEvent.c_str();
        topic += " fxid=";
        XrdOucString hexid;
        eos::common::FileId::Fid2Hex(mFid, hexid);
        topic += hexid.c_str();
        std::string do_mail = "echo ";
        do_mail += "\"";
        do_mail += freetext;
        do_mail += "\"";
        do_mail += "| mail -s \"";
        do_mail += topic;
        do_mail += "\" ";
        do_mail += recipient;
        eos_static_info("shell-cmd=\"%s\"", do_mail.c_str());
        eos::common::ShellCmd cmd(do_mail.c_str());
        eos::common::cmd_status rc = cmd.wait(5);

        if (rc.exit_code) {
          eos_static_err("msg=\"failed to send workflow notification mail\" job=\"%s\"",
                         mDescription.c_str());
          storetime = 0;
          Move("s", "f", storetime);
          XrdOucString log = "failed to send workflow notification mail";
          Results("f", -1, log, storetime);
        } else {
          eos_static_info("msg=\"done notification\" job=\"%s\"",
                          mDescription.c_str());
          storetime = 0;
          Move("s", "d", storetime);
          XrdOucString log = "notified by email";
          Results("d", 0, log, storetime);
        }
      } else if (method == "bash") {
        std::string executable;
        std::string executableargs;

        if (!eos::common::StringConversion::SplitKeyValue(args, executable,
            executableargs, ":")) {
          executable = args;
          executableargs = "";
        }

        XrdOucString execargs = executableargs.c_str();
        std::string fullpath;
        bool format_error = false;

        if (executable.find("/") == std::string::npos) {
          std::shared_ptr<eos::IFileMD> fmd ;
          std::shared_ptr<eos::IContainerMD> cmd ;
          // do meta replacement
          gOFS->eosViewRWMutex.LockRead();

          try {
            fmd = gOFS->eosFileService->getFileMD(mFid);
            cmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
            fullpath = gOFS->eosView->getUri(fmd.get());
          } catch (eos::MDException& e) {
            eos_static_debug("caught exception %d %s\n", e.getErrno(),
                             e.getMessage().str().c_str());
          }

          if (fmd.get() && cmd.get()) {
            std::shared_ptr<eos::IFileMD> cfmd  = fmd;
            std::shared_ptr<eos::IContainerMD> ccmd = cmd;
            gOFS->eosViewRWMutex.UnLockRead();
            std::string cv;
            eos::IFileMD::ctime_t ctime;
            eos::IFileMD::ctime_t mtime;
            cfmd->getCTime(ctime);
            cfmd->getMTime(mtime);
            std::string checksum;
            size_t cxlen = eos::common::LayoutId::GetChecksumLen(cfmd->getLayoutId());

            for (unsigned int i = 0; i < cxlen; i++) {
              char hb[3];
              sprintf(hb, "%02x", (i < cxlen) ? (unsigned char)(
                        cfmd->getChecksum().getDataPadded(i)) : 0);
              checksum += hb;
            }

            // translate uid/gid to username/groupname
            std::string user_name;
            std::string group_name;
            int errc;
            errc = 0;
            user_name  = Mapping::UidToUserName(cfmd->getCUid(), errc);

            if (errc) {
              user_name = "nobody";
            }

            errc = 0;
            group_name = Mapping::GidToGroupName(cfmd->getCGid(), errc);

            if (errc) {
              group_name = "nobody";
            }

            XrdOucString unbase64;
            XrdOucString base64;
            unbase64 = fullpath.c_str();
            eos::common::SymKey::Base64(unbase64, base64);

            while (execargs.replace("<eos::wfe::path>", unbase64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::base64:path>", base64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::uid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getCUid()))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::gid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getCGid()))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::ruid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) mVid.uid))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::rgid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) mVid.gid))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::username>",
                                    user_name.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::groupname>",
                                    group_name.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::rusername>",
                                    mVid.uid_string.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::rgroupname>",
                                    mVid.gid_string.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::host>",
                                    mVid.host.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::sec.app>",
                                    mVid.app.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::sec.name>",
                                    mVid.name.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::sec.prot>",
                                    mVid.prot.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::sec.grps>",
                                    mVid.grps.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::instance>",
                                    gOFS->MgmOfsInstanceName)) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::ctime.s>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::mtime.s>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::ctime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_nsec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::mtime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_nsec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::ctime>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::mtime>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::size>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getSize()))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::cid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getContainerId()))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::fid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) mFid))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            XrdOucString hexfid;
            eos::common::FileId::Fid2Hex(mFid, hexfid);

            while (execargs.replace("<eos::wfe::fxid>",
                                    hexfid)) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            std::string turl = "root://";
            turl += gOFS->MgmOfsAlias.c_str();
            turl += "/";
            turl += fullpath.c_str();
            turl += "?eos.lfn=fxid:";
            turl += hexfid.c_str();

            while (execargs.replace("<eos::wfe::turl>",
                                    turl.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            unbase64 = cfmd->getName().c_str();
            eos::common::SymKey::Base64(unbase64, base64);

            while (execargs.replace("<eos::wfe::name>", unbase64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::base64:name>", base64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            unbase64 = cfmd->getLink().c_str();
            eos::common::SymKey::Base64(unbase64, base64);

            while (execargs.replace("<eos::wfe::link>", unbase64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::base64:link>", base64.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::checksum>", checksum.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::checksumtype>",
                                    eos::common::LayoutId::GetChecksumString(cfmd->getLayoutId()))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::event>", mActions[0].mEvent.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::queue>", mActions[0].mQueue.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::workflow>",
                                    mActions[0].mWorkflow.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            while (execargs.replace("<eos::wfe::vpath>", mWorkflowPath.c_str())) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            time_t now = time(NULL);

            while (execargs.replace("<eos::wfe::now>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) now))) {
              int cnt = 0;
              cnt++;

              if (cnt > 16) {
                break;
              }
            }

            int xstart = 0;

            while ((xstart = execargs.find("<eos::wfe::fxattr:")) != STR_NPOS) {
              int cnt = 0;
              cnt++;

              if (cnt > 256) {
                break;
              }

              int xend = execargs.find(">", xstart);

              if (xend == STR_NPOS) {
                format_error = true;
                break;
              } else {
                bool b64encode = false;
                std::string key;
                std::string value;
                key.assign(execargs.c_str() + xstart + 18, xend - xstart - 18);
                execargs.erase(xstart, xend + 1 - xstart);
                XrdOucString skey = key.c_str();

                if (skey.beginswith("base64:")) {
                  key.erase(0, 7);
                  b64encode = true;
                }

                if (gOFS->_attr_get(cfmd->getId(), key, value)) {
                  if (b64encode) {
                    unbase64 = value.c_str();
                    eos::common::SymKey::Base64(unbase64, base64);
                    value = base64.c_str();
                  }

                  if (xstart == execargs.length()) {
                    execargs += value.c_str();
                  } else {
                    execargs.insert(value.c_str(), xstart);
                  }
                } else {
                  execargs.insert("UNDEF", xstart);
                }
              }
            }

            xstart = 0;

            while ((xstart = execargs.find("<eos::wfe::cxattr:")) != STR_NPOS) {
              int cnt = 0;
              cnt++;

              if (cnt > 256) {
                break;
              }

              int xend = execargs.find(">", xstart);

              if (xend == STR_NPOS) {
                format_error = true;
                break;
              } else {
                bool b64encode = false;
                std::string key;
                std::string value;
                key.assign(execargs.c_str() + xstart + 18, xend - xstart - 18);
                execargs.erase(xstart, xend + 1 - xstart);
                XrdOucString skey = key.c_str();

                if (skey.beginswith("base64:")) {
                  key.erase(0, 7);
                  b64encode = true;
                }

                if (gOFS->_attr_get(ccmd->getId(), key, value)) {
                  if (b64encode) {
                    unbase64 = value.c_str();
                    eos::common::SymKey::Base64(unbase64, base64);
                    value = base64.c_str();
                  }

                  if (xstart == execargs.length()) {
                    execargs += value.c_str();
                  } else {
                    execargs.insert(value.c_str(), xstart);
                  }
                } else {
                  execargs.insert("UNDEF", xstart);
                }
              }
            }

            if (execargs.find("<eos::wfe::base64:metadata>") != STR_NPOS) {
              XrdOucString out = "";
              XrdOucString err = "";
              // ---------------------------------------------------------------------------------
              // run file info to get file md
              // ---------------------------------------------------------------------------------
              XrdOucString file_metadata;
              ProcCommand Cmd;
              XrdOucString info;
              info = "mgm.cmd=fileinfo&mgm.path=fid:";
              info += eos::common::StringConversion::GetSizeString(cv,
                      (unsigned long long) mFid);
              info += "&mgm.file.info.option=-m";
              Cmd.open("/proc/user", info.c_str(), lRootVid, &lError);
              Cmd.AddOutput(out, err);
              Cmd.close();
              file_metadata = out;

              if (err.length()) {
                eos_static_err("msg=\"file info returned error\" err=\"%s\"", err.c_str());
              }

              while (file_metadata.replace("\"", "'")) {
                int cnt = 0;
                cnt++;

                if (cnt > 16) {
                  break;
                }
              }

              out = err = "";
              // ---------------------------------------------------------------------------------
              // run container info to get container md
              // ---------------------------------------------------------------------------------
              XrdOucString container_metadata;
              info = "mgm.cmd=fileinfo&mgm.path=pid:";
              info += eos::common::StringConversion::GetSizeString(cv,
                      (unsigned long long) cfmd->getContainerId());
              info += "&mgm.file.info.option=-m";
              Cmd.open("/proc/user", info.c_str(), lRootVid, &lError);
              Cmd.AddOutput(out, err);
              Cmd.close();
              container_metadata = out;

              if (err.length()) {
                eos_static_err("msg=\"container info returned error\" err=\"%s\"", err.c_str());
              }

              while (container_metadata.replace("\"", "'")) {
                int cnt = 0;
                cnt++;

                if (cnt > 16) {
                  break;
                }
              }

              std::string metadata = "\"fmd={ ";
              metadata += file_metadata.c_str();
              metadata += "} dmd={ ";
              metadata += container_metadata.c_str();
              metadata += "}\"";
              unbase64 = metadata.c_str();
              eos::common::SymKey::Base64(unbase64, base64);
              execargs.replace("<eos::wfe::base64:metadata>", base64.c_str());
            }

            execargs.replace("<eos::wfe::action>", mActions[0].mAction.c_str());
            std::string bashcmd = EOS_WFE_BASH_PREFIX + executable + " " + execargs.c_str();

            if (!format_error) {
              eos::common::ShellCmd cmd(bashcmd.c_str());
              storetime = (time_t) mActions[0].mTime;
              Move(mActions[0].mQueue, "r", storetime, mRetry);
              eos_static_info("shell-cmd=\"%s\"", bashcmd.c_str());
              eos::common::cmd_status rc = cmd.wait(1800);
              // retrieve the stderr of this command
              XrdOucString outerr;
              char buff[65536];
              int end;
              memset(buff, 0, sizeof(buff));

              while ((end = ::read(cmd.errfd, buff, sizeof(buff))) > 0) {
                outerr += buff;
                memset(buff, 0, sizeof(buff));
              }

              eos_static_info("shell-cmd-stderr=%s", outerr.c_str());
              // scan for result tags referencing the trigger path
              xstart = 0;

              while ((xstart = outerr.find("<eos::wfe::path::fxattr:", xstart)) != STR_NPOS) {
                int cnt = 0;
                cnt++;

                if (cnt > 256) {
                  break;
                }

                int xend = outerr.find(">", xstart);

                if (xend == STR_NPOS) {
                  eos_static_err("malformed shell stderr tag");
                  break;
                } else {
                  std::string key;
                  std::string value;
                  key.assign(outerr.c_str() + xstart + 24, xend - xstart - 24);
                  int vend = outerr.find(" ", xend + 1);

                  if (vend > 0) {
                    value.assign(outerr.c_str(), xend + 1, vend - (xend + 1));
                  } else {
                    value.assign(outerr.c_str(), xend + 1, string::npos);
                  }

                  // remove a possible line feed from the value
                  while (value.length() && (value[value.length() - 1] == '\n')) {
                    value.erase(value.length() - 1);
                  }

                  eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

                  try {
                    fmd = gOFS->eosFileService->getFileMD(mFid);
                    base64 = value.c_str();
                    eos::common::SymKey::DeBase64(base64, unbase64);
                    fmd->setAttribute(key.c_str(), unbase64.c_str());
                    fmd->setMTimeNow();
                    gOFS->eosView->updateFileStore(fmd.get());
                    errno = 0;
                    eos_static_info("msg=\"stored extended attribute\" key=%s value=%s",
                                    key.c_str(), value.c_str());
                  } catch (eos::MDException& e) {
                    eos_static_err("msg=\"failed set extended attribute\" key=%s value=%s",
                                   key.c_str(), value.c_str());
                  }
                }

                xstart++;
              }

              retc = rc.exit_code;

              if (rc.exit_code) {
                eos_static_err("msg=\"failed to run bash workflow\" job=\"%s\" retc=%d",
                               mDescription.c_str(), rc.exit_code);
                int retry = 0;
                time_t delay = 0;

                if (rc.exit_code == EAGAIN) {
                  try {
                    std::string retryattr = "sys.workflow." + mActions[0].mEvent + "." +
                                            mActions[0].mWorkflow + ".retry.max";
                    std::string delayattr = "sys.workflow." + mActions[0].mEvent + "." +
                                            mActions[0].mWorkflow + ".retry.delay";
                    eos_static_info("%s %s", retryattr.c_str(), delayattr.c_str());
                    std::string value = ccmd->getAttribute(retryattr);
                    retry = (int)strtoul(value.c_str(), 0, 10);
                    value = ccmd->getAttribute(delayattr);
                    delay = (int)strtoul(value.c_str(), 0, 10);
                  } catch (eos::MDException& e) {
                    execargs.insert("UNDEF", xstart);
                  }

                  if (!IsSync() && (mRetry < retry)) {
                    storetime = (time_t) mActions[0].mTime + delay;
                    // can retry
                    Move("r", "e", storetime, ++mRetry);
                    XrdOucString log = "scheduled for retry";
                    Results("e", EAGAIN , log, storetime);
                  } else {
                    storetime = (time_t) mActions[0].mTime;
                    // can not retry
                    Move("r", "f", storetime, mRetry);
                    XrdOucString log = "workflow failed without possibility to retry";
                    Results("f", rc.exit_code , log, storetime);
                  }
                } else {
                  storetime = 0;
                  // can not retry
                  Move("r", "f", storetime);
                  XrdOucString log = "workflow failed without possibility to retry";
                  Results("f", rc.exit_code , log, storetime);
                }
              } else {
                eos_static_info("msg=\"done bash workflow\" job=\"%s\"",
                                mDescription.c_str());
                storetime = 0;
                Move("r", "d", storetime);
                XrdOucString log = "workflow succeeded";
                Results("d", rc.exit_code , log, storetime);
              }

              // scan for result tags referencing the workflow path
              xstart = 0;

              while ((xstart = outerr.find("<eos::wfe::vpath::fxattr:",
                                           xstart)) != STR_NPOS) {
                int cnt = 0;
                cnt++;

                if (cnt > 256) {
                  break;
                }

                int xend = outerr.find(">", xstart);

                if (xend == STR_NPOS) {
                  eos_static_err("malformed shell stderr tag");
                  break;
                } else {
                  std::string key;
                  std::string value;
                  key.assign(outerr.c_str() + xstart + 25, xend - xstart - 25);
                  int vend = outerr.find(" ", xend + 1);

                  if (vend > 0) {
                    value.assign(outerr.c_str(), xend + 1, vend - (xend + 1));
                  } else {
                    value.assign(outerr.c_str(), xend + 1, string::npos);
                  }

                  eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

                  try {
                    fmd = gOFS->eosView->getFile(mWorkflowPath.c_str());
                    base64 = value.c_str();
                    eos::common::SymKey::DeBase64(base64, unbase64);
                    fmd->setAttribute(key.c_str(), unbase64.c_str());
                    fmd->setMTimeNow();
                    gOFS->eosView->updateFileStore(fmd.get());
                    errno = 0;
                    eos_static_info("msg=\"stored extended attribute on vpath\" vpath=%s key=%s value=%s",
                                    mWorkflowPath.c_str(), key.c_str(), value.c_str());
                  } catch (eos::MDException& e) {
                    eos_static_err("msg=\"failed set extended attribute\" key=%s value=%s",
                                   key.c_str(), value.c_str());
                  }
                }

                xstart++;
              }
            } else {
              retc = EINVAL;
              storetime = 0;
              // cannot retry
              Move(mActions[0].mQueue, "f", storetime);
              XrdOucString log = "workflow failed to invalid arguments";
              Results("f", retc , log, storetime);
            }
          } else {
            storetime = 0;
            retc = EINVAL;
            gOFS->eosViewRWMutex.UnLockRead();
            eos_static_err("msg=\"failed to run bash workflow - file gone\" job=\"%s\"",
                           mDescription.c_str());
            Move(mActions[0].mQueue, "g", storetime);
            XrdOucString log = "workflow failed to invalid arguments - file is gone";
            Results("g", retc , log, storetime);
          }
        } else {
          storetime = 0;
          retc = EINVAL;
          eos_static_err("msg=\"failed to run bash workflow - executable name modifies path\" job=\"%s\"",
                         mDescription.c_str());
          Move(mActions[0].mQueue, "g", storetime);
        }
      } else {
        storetime = 0;
        eos_static_err("msg=\"moving unkown workflow\" job=\"%s\"",
                       mDescription.c_str());
        Move(mActions[0].mQueue, "g", storetime);
        XrdOucString log = "workflow is not known";
        Results("g", EINVAL , log, storetime);
      }
    } else {
      storetime = 0;
      retc = EINVAL;
      eos_static_err("msg=\"moving illegal workflow\" job=\"%s\"",
                     mDescription.c_str());
      Move(mActions[0].mQueue, "g", storetime);
      XrdOucString log = "workflow illegal";
      Results("g", retc , log, storetime);
    }
  } else {
    //Delete(mActions[0].mQueue);
  }

  if (!IsSync()) {
    gOFS->WFEd.GetSignal()->Signal();
    gOFS->WFEd.DecActiveJobs();
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
void
WFE::PublishActiveJobs()
/*----------------------------------------------------------------------------*/
/**
 * @brief publish the active job number in the space view
 *
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  char sactive[256];
  snprintf(sactive, sizeof(sactive) - 1, "%lu", GetActiveJobs());
  FsView::gFsView.mSpaceView["default"]->SetConfigMember
  ("stat.wfe.active",
   sactive,
   true,
   "/eos/*/mgm",
   true);
}

EOSMGMNAMESPACE_END
