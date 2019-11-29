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

#include "common/Path.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/ShellCmd.hh"
#include "common/StringTokenizer.hh"
#include "common/Constants.hh"
#include "mgm/Quota.hh"
#include "common/CtaCommon.hh"
#include "common/eos_cta_pb/EosCtaAlertHandler.hh"
#include "mgm/XattrSet.hh"
#include "mgm/WFE.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/proc/admin/StagerRmCmd.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Checksum.hh"
#include "Xrd/XrdScheduler.hh"

#define EOS_WFE_BASH_PREFIX "/var/eos/wfe/bash/"

XrdSysMutex eos::mgm::WFE::gSchedulerMutex;
XrdScheduler* eos::mgm::WFE::gScheduler;

/*----------------------------------------------------------------------------*/
extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

EOSMGMNAMESPACE_BEGIN

using namespace eos::common;

/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor of the work flow engine
 */
/*----------------------------------------------------------------------------*/
WFE::WFE()
{
  mMs = 0;
  mActiveJobs = 0;
  mRootVid = eos::common::VirtualIdentity::Root();
  XrdSysMutexHelper sLock(gSchedulerMutex);
  gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 10, 500, 100);
  gScheduler->Start();
}

/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous WFE thread startup function
 */
/*----------------------------------------------------------------------------*/
bool
WFE::Start()
{
  try {
    mThread.reset(&WFE::WFEr, this);
  } catch (const std::system_error& e) {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous WFE thread stop function
 */
/*----------------------------------------------------------------------------*/
void
WFE::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// @brief WFE method doing the actual workflow
//
// This thread method loops in regular intervals over all workflow jobs in the
// workflow directory /eos/<instance>/proc/workflow/
//------------------------------------------------------------------------------/
void
WFE::WFEr(ThreadAssistant& assistant) noexcept
{
  // Eternal thread doing WFE scans
  time_t snoozetime = 10;
  size_t lWFEntx = 0;
  time_t cleanuptime = 0;
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  if (assistant.terminationRequested()) {
    return;
  }

  assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("msg=\"async WFE thread started\"");

  while (!assistant.terminationRequested()) {
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

    // Only a master needs to run WFE
    if (gOFS->mMaster->IsMaster() && IsEnabledWFE) {
      eos_static_debug("msg=\"start WFE scan\"");
      // Find all directories defining an WFE policy
      gOFS->MgmStats.Add("WFEFind", 0, 0, 1);
      EXEC_TIMING_BEGIN("WFEFind");
      // prepare four queries today, yesterday for queued and error jobs
      std::string queries[4];

      for (size_t i = 0; i < 4; ++i) {
        queries[i] = gOFS->MgmProcWorkflowPath.c_str();
        queries[i] += "/";
      }

      {
        // today
        time_t when = time(NULL);
        std::string day = eos::common::Timing::UnixTimestamp_to_Day(when);
        queries[0] += day;
        queries[0] += "/q/";
        queries[1] += day;
        queries[1] += "/e/";
        //yesterday
        when -= (24 * 3600);
        day = eos::common::Timing::UnixTimestamp_to_Day(when);
        queries[2] += day;
        queries[2] += "/q/";
        queries[3] += day;
        queries[3] += "/e/";
      }

      for (size_t i = 0; i < 4; ++i) {
        eos_static_debug("query-path=%s", queries[i].c_str());
        gOFS->_find(queries[i].c_str(), mError, stdErr, mRootVid, wfedirs, 0,
                    0, false, 0, false, 0);
      }

      {
        eos_static_debug("msg=\"finished WFE find\" WFE-dirs=%llu %s",
                         wfedirs.size(), stdErr.c_str()
                        );
        time_t now = time(NULL);

        for (auto it = wfedirs.begin(); it != wfedirs.end(); it++) {
          // -------------------------------------------------------------------
          // get workflows
          // -------------------------------------------------------------------
          if (it->second.size()) {
            for (auto wit = it->second.begin(); wit != it->second.end(); ++wit) {
              eos_static_debug("wfe-dir=\"%s\" wfe-job=\"%s\"", it->first.c_str(),
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
                if (lWFEntx <= GetActiveJobs()) {
                  if (lWFEntx > 0) {
                    mDoneSignal.WaitMS(100);

                    if (lWFEntx <= GetActiveJobs()) {
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
                  job->Move(job->mActions[0].mQueue, "r", storetime);
                  job->mActions[0].mQueue = "r";
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
      eos_static_debug("msg=\"finished WFE application\" WFE-dirs=%llu",
                       wfedirs.size());
    }

    lStopTime = time(NULL);

    if ((lStopTime - lStartTime) < lWFEInterval) {
      snoozetime = lWFEInterval - (lStopTime - lStartTime);
    }

    if (!IsEnabledWFE) {
      snoozetime = 6000;
    }

    eos_static_debug("snooze-time=%llu enabled=%d", snoozetime, IsEnabledWFE);
    size_t snoozeloop = snoozetime * 10;

    for (size_t i = 0; i < snoozeloop; i++) {
      assistant.wait_for(std::chrono::milliseconds(100));

      if (assistant.terminationRequested()) {
        break;
      }

      // Check if the setting changed
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

    if (gOFS->mMaster->IsMaster() &&
        (!cleanuptime || (cleanuptime < time(NULL)))) {
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
}

/*----------------------------------------------------------------------------*/
/**
 * @brief store a workflow jobs in the workflow queue
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
int
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
  std::string entry = eos::common::FileId::Fid2Hex(mFid);
  eos_static_info("workflowdir=\"%s\" retry=%d when=%u job-time=%s",
                  workflowdir.c_str(),
                  retry, when, mActions[action].mWhen.c_str());
  XrdOucErrInfo lError;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
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
      // check if it has been created in the meanwhile as the stat and mkdir are not atomic together
      if (gOFS->_stat(workflowdir.c_str(),
                      &buf,
                      lError,
                      rootvid,
                      "")) {
        eos_static_err("msg=\"failed to create workflow directory\" path=\"%s\"",
                       workflowdir.c_str());
        return -1;
      }
    }
  }

  // write a workflow file
  std::string workflowpath = workflowdir;

  // evt. store with the current time

  if (!when) {
    when = time(nullptr);
  }

  XrdOucString tst;
  workflowpath += eos::common::StringConversion::GetSizeString(tst,
                  (unsigned long long) when);
  workflowpath += ":";
  workflowpath += entry;
  workflowpath += ":";
  workflowpath += mActions[action].mEvent;
  mWorkflowPath = workflowpath;
  //Store which day it is stored for
  mActions[action].mSavedOnDay = mActions[action].mDay;
  std::string vids = eos::common::Mapping::VidToString(mVid);

  try {
    // The point of prefetching here is to get the chunks preceeding the final
    // one, so that createFile is guaranteed not to wait on network requests.
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, workflowpath);
    eos::common::RWMutexWriteLock wLock {gOFS->eosViewRWMutex};
    auto fmd = gOFS->eosView->createFile(workflowpath, 0, 0);
    auto cid = fmd->getContainerId();
    auto cmd = gOFS->eosDirectoryService->getContainerMD(cid);
    cmd->setMTimeNow();
    cmd->notifyMTimeChange(gOFS->eosDirectoryService);
    gOFS->eosView->updateContainerStore(cmd.get());
    fmd->setAttribute("sys.action", mActions[0].mAction);
    fmd->setAttribute("sys.vid", vids);
    fmd->setAttribute("sys.wfe.errmsg", mErrorMesssage);
    fmd->setAttribute("sys.wfe.retry", std::to_string(retry));
    gOFS->eosView->updateFileStore(fmd.get());
  } catch (eos::MDException& ex) {
    eos_static_err("msg=\"failed to save workflow entry\" path=\"%s\" error=\"%s\"",
                   workflowpath.c_str(),
                   ex.what());
    return -1;
  }

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
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  std::string f = path2entry;
  f.erase(0, path2entry.rfind('/') + 1);
  std::string workflow = path2entry;
  workflow.erase(path2entry.rfind('/'));
  workflow.erase(0, workflow.rfind('/') + 1);
  std::string q = path2entry;
  q.erase(q.rfind('/'));
  q.erase(q.rfind('/'));
  q.erase(0, q.rfind('/') + 1);
  std::string savedAtDay = path2entry;
  savedAtDay.erase(savedAtDay.rfind('/'));
  savedAtDay.erase(savedAtDay.rfind('/'));
  savedAtDay.erase(savedAtDay.rfind('/'));
  savedAtDay.erase(0, savedAtDay.rfind('/') + 1);
  std::string when;
  std::string idevent;
  std::string id;
  std::string event;
  bool s1 = eos::common::StringConversion::SplitKeyValue(f, when, idevent, ":");
  bool s2 = eos::common::StringConversion::SplitKeyValue(idevent, id, event, ":");
  mWorkflowPath = path2entry;

  if (s1 && s2) {
    mFid = eos::common::FileId::Hex2Fid(id.c_str());
    eos_static_info("workflow=\"%s\" fxid=%08llx", workflow.c_str(), mFid);
    {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path2entry);
      eos::common::RWMutexReadLock rLock {gOFS->eosViewRWMutex};
      auto fmd = gOFS->eosView->getFile(path2entry);

      try {
        time_t t_when = strtoull(when.c_str(), 0, 10);
        AddAction(fmd->getAttribute("sys.action"), event, t_when, savedAtDay, workflow,
                  q);
      } catch (eos::MDException& ex) {
        eos_static_err("msg=\"no action stored\" path=\"%s\"", f.c_str());
      }

      try {
        auto vidstring = fmd->getAttribute("sys.vid").c_str();

        if (!eos::common::Mapping::VidFromString(mVid, vidstring)) {
          eos_static_crit("parsing of %s failed - setting nobody\n", vidstring);
          mVid = eos::common::VirtualIdentity::Nobody();
        }
      } catch (eos::MDException& ex) {
        mVid = eos::common::VirtualIdentity::Nobody();
        eos_static_err("msg=\"no vid stored\" path=\"%s\"", f.c_str());
      }

      try {
        mRetry = (int)strtoul(fmd->getAttribute("sys.wfe.retry").c_str(), nullptr, 10);
      } catch (eos::MDException& ex) {
        eos_static_err("msg=\"no retry stored\" path=\"%s\"", f.c_str());
      }

      try {
        mErrorMesssage = fmd->getAttribute("sys.wfe.errmsg");
      } catch (eos::MDException& ex) {
        eos_static_info("msg=\"no error message stored\" path=\"%s\"", f.c_str());
      }
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
 * @brief move workflow jobs between queues
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  auto fromDay = mActions[0].mSavedOnDay;

  if (Save(to_queue, when, 0, retry) == SFS_OK) {
    mActions[0].mQueue = to_queue;

    if ((from_queue != to_queue) && (Delete(from_queue, fromDay) == SFS_ERROR)) {
      eos_static_err("msg=\"failed to remove for move from queue=\"%s\" to queue=\"%s\"",
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
  std::string entry = eos::common::FileId::Fid2Hex(mFid);
  eos_static_info("workflowdir=\"%s\" entry=%s", workflowdir.c_str(),
                  entry.c_str());
  XrdOucErrInfo lError;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
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
                      nullptr,
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
                      nullptr,
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
WFE::Job::Delete(std::string queue, std::string fromDay)
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
  // We have to remove from the day when it was saved
  workflowdir += fromDay;
  workflowdir += "/";
  workflowdir += queue;
  workflowdir += "/";
  workflowdir += mActions[0].mWorkflow;
  workflowdir += "/";
  std::string entry = eos::common::FileId::Fid2Hex(mFid);
  eos_static_info("workflowdir=\"%s\"", workflowdir.c_str());
  XrdOucErrInfo lError;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
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
                  false,
                  false,
                  true)) {
    return SFS_OK;
  } else {
    eos_static_err("msg=\"failed to delete job\" job=\"%s\"", mDescription.c_str());
    return SFS_ERROR;
  }
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::DoIt(bool issync, std::string& errorMsg, const char* const ininfo)
/*----------------------------------------------------------------------------*/
/**
 * @brief execute a workflow
 * @return
 *  */
/*----------------------------------------------------------------------------*/
{
  // RAII: Async jobs reduce counter on all paths
  auto decrementJobs = [this](void*) {
    if (!IsSync()) {
      gOFS->WFEd.DecActiveJobs();
      gOFS->WFEd.GetSignal()->Signal();
    }
  };
  std::unique_ptr<void, decltype(decrementJobs)> activeJobsGuard {
    static_cast<void*>(this),
    decrementJobs
  };
  std::string method;
  std::string args;
  eos::common::VirtualIdentity lRootVid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo lError;
  int retc = 0;
  time_t storetime = 0;

  if (mActions[0].mQueue == "r" || mActions[0].mQueue == "e") {
    bool actionParsed = false;

    if (mActions[0].mAction.find(':') == std::string::npos) {
      method = mActions[0].mAction;
      actionParsed = true;
    } else {
      actionParsed = eos::common::StringConversion::SplitKeyValue(mActions[0].mAction,
                     method,
                     args, ":");
    }

    if (actionParsed) {
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
        topic += mActions[0].mEvent;
        topic += " fid=";
        topic += eos::common::FileId::Fid2Hex(mFid).c_str();
        std::string do_mail = "echo ";
        do_mail += "\"";
        do_mail += freetext;
        do_mail += "\"";
        do_mail += "| mail -s \"";
        do_mail += topic;
        do_mail += "\" ";
        do_mail += recipient;
        eos_static_info("shell-cmd=\"%s\"", do_mail.c_str());
        eos::common::ShellCmd cmd(do_mail);
        eos::common::cmd_status rc = cmd.wait(5);

        if (rc.exit_code) {
          eos_static_err("msg=\"failed to send workflow notification mail\" job=\"%s\"",
                         mDescription.c_str());
          storetime = 0;
          Move(mActions[0].mQueue, "f", storetime);
          XrdOucString log = "failed to send workflow notification mail";
          Results("f", -1, log, storetime);
        } else {
          eos_static_info("msg=\"done notification\" job=\"%s\"",
                          mDescription.c_str());
          storetime = 0;
          Move(mActions[0].mQueue, "d", storetime);
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

        if (executable.find('/') == std::string::npos) {
          std::shared_ptr<eos::IFileMD> fmd ;
          std::shared_ptr<eos::IContainerMD> cmd ;
          // do meta replacement
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
          eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);

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
            viewReadLock.Release();
            std::string cv;
            eos::IFileMD::ctime_t ctime;
            eos::IFileMD::ctime_t mtime;
            cfmd->getCTime(ctime);
            cfmd->getMTime(mtime);
            std::string checksum;
            eos::appendChecksumOnStringAsHex(cfmd.get(), checksum);
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
            int cnt = 0;

            while (execargs.replace("<eos::wfe::path>", unbase64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::base64:path>", base64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::uid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getCUid()))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::gid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getCGid()))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::ruid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) mVid.uid))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::rgid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) mVid.gid))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::username>",
                                    user_name.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::groupname>",
                                    group_name.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::rusername>",
                                    mVid.uid_string.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::rgroupname>",
                                    mVid.gid_string.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::host>",
                                    mVid.host.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::sec.app>",
                                    mVid.app.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::sec.name>",
                                    mVid.name.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::sec.prot>",
                                    mVid.prot.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::sec.grps>",
                                    mVid.grps.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::instance>",
                                    gOFS->MgmOfsInstanceName)) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::ctime.s>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::mtime.s>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::ctime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_nsec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::mtime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_nsec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::ctime>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::mtime>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) ctime.tv_sec))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::size>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getSize()))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::cid>",
                                    eos::common::StringConversion::GetSizeString(cv,
                                        (unsigned long long) cfmd->getContainerId()))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::fid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) mFid))) {
              if (++cnt > 16) {
                break;
              }
            }

            const std::string hex_fid = eos::common::FileId::Fid2Hex(mFid);
            cnt = 0;

            while (execargs.replace("<eos::wfe::fxid>", hex_fid.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            std::string turl = "root://";
            turl += gOFS->MgmOfsAlias.c_str();
            turl += "/";
            turl += fullpath;
            turl += "?eos.lfn=fxid:";
            turl += hex_fid.c_str();
            cnt = 0;

            while (execargs.replace("<eos::wfe::turl>",
                                    turl.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            unbase64 = cfmd->getName().c_str();
            eos::common::SymKey::Base64(unbase64, base64);
            cnt = 0;

            while (execargs.replace("<eos::wfe::name>", unbase64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::base64:name>", base64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            unbase64 = cfmd->getLink().c_str();
            eos::common::SymKey::Base64(unbase64, base64);
            cnt = 0;

            while (execargs.replace("<eos::wfe::link>", unbase64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::base64:link>", base64.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::checksum>", checksum.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::checksumtype>",
                                    eos::common::LayoutId::GetChecksumString(cfmd->getLayoutId()))) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::event>", mActions[0].mEvent.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::queue>", mActions[0].mQueue.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::workflow>",
                                    mActions[0].mWorkflow.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            cnt = 0;

            while (execargs.replace("<eos::wfe::vpath>", mWorkflowPath.c_str())) {
              if (++cnt > 16) {
                break;
              }
            }

            time_t now = time(NULL);
            cnt = 0;

            while (execargs.replace("<eos::wfe::now>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) now))) {
              if (++cnt > 16) {
                break;
              }
            }

            int xstart = 0;
            cnt = 0;

            while ((xstart = execargs.find("<eos::wfe::fxattr:")) != STR_NPOS) {
              if (++cnt > 256) {
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

                if (gOFS->_attr_get(*cfmd, key, value)) {
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
            cnt = 0;

            while ((xstart = execargs.find("<eos::wfe::cxattr:")) != STR_NPOS) {
              if (++cnt > 256) {
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

                if (gOFS->_attr_get(*ccmd, key, value)) {
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

              cnt = 0;

              while (file_metadata.replace("\"", "'")) {
                if (++cnt > 16) {
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

              cnt = 0;

              while (container_metadata.replace("\"", "'")) {
                if (++cnt > 16) {
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
              eos::common::ShellCmd cmd(bashcmd);
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
              cnt = 0;

              while ((xstart = outerr.find("<eos::wfe::path::fxattr:", xstart)) != STR_NPOS) {
                if (++cnt > 256) {
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

                  eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
                  eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

                  try {
                    fmd = gOFS->eosFileService->getFileMD(mFid);
                    base64 = value.c_str();
                    eos::common::SymKey::DeBase64(base64, unbase64);
                    fmd->setAttribute(key, unbase64.c_str());
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
                    Results("e", EAGAIN, log, storetime);
                  } else {
                    storetime = (time_t) mActions[0].mTime;
                    // can not retry
                    Move("r", "f", storetime, mRetry);
                    XrdOucString log = "workflow failed without possibility to retry";
                    Results("f", rc.exit_code, log, storetime);
                  }
                } else {
                  storetime = 0;
                  // can not retry
                  Move("r", "f", storetime);
                  XrdOucString log = "workflow failed without possibility to retry";
                  Results("f", rc.exit_code, log, storetime);
                }
              } else {
                eos_static_info("msg=\"done bash workflow\" job=\"%s\"",
                                mDescription.c_str());
                storetime = 0;
                Move("r", "d", storetime);
                XrdOucString log = "workflow succeeded";
                Results("d", rc.exit_code, log, storetime);
              }

              // scan for result tags referencing the workflow path
              xstart = 0;
              cnt = 0;

              while ((xstart = outerr.find("<eos::wfe::vpath::fxattr:",
                                           xstart)) != STR_NPOS) {
                if (++cnt > 256) {
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

                  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mWorkflowPath);
                  eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

                  try {
                    fmd = gOFS->eosView->getFile(mWorkflowPath);
                    base64 = value.c_str();
                    eos::common::SymKey::DeBase64(base64, unbase64);
                    fmd->setAttribute(key, unbase64.c_str());
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
              Results("f", retc, log, storetime);
            }
          } else {
            storetime = 0;
            retc = EINVAL;
            viewReadLock.Release();
            eos_static_err("msg=\"failed to run bash workflow - file gone\" job=\"%s\"",
                           mDescription.c_str());
            Move(mActions[0].mQueue, "g", storetime);
            XrdOucString log = "workflow failed to invalid arguments - file is gone";
            Results("g", retc, log, storetime);
          }
        } else {
          storetime = 0;
          retc = EINVAL;
          eos_static_err("msg=\"failed to run bash workflow - executable name modifies path\" job=\"%s\"",
                         mDescription.c_str());
          Move(mActions[0].mQueue, "g", storetime);
        }
      } else if (method == "proto") {
        return HandleProtoMethodEvents(errorMsg, ininfo);
      } else {
        storetime = 0;
        eos_static_err("msg=\"moving unknown workflow\" job=\"%s\"",
                       mDescription.c_str());
        Move(mActions[0].mQueue, "g", storetime);
        XrdOucString log = "workflow is not known";
        Results("g", EINVAL, log, storetime);
      }
    } else {
      storetime = 0;
      retc = EINVAL;
      eos_static_err("msg=\"moving illegal workflow\" job=\"%s\"",
                     mDescription.c_str());
      Move(mActions[0].mQueue, "g", storetime);
      XrdOucString log = "workflow illegal";
      Results("g", retc, log, storetime);
    }
  } else {
    //Delete(mActions[0].mQueue);
  }

  return retc;
}


/*----------------------------------------------------------------------------*/
/**
 * @brief Handles "proto" method events
 * @param errorMsg out parameter giving the text of any error
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
int WFE::Job::HandleProtoMethodEvents(std::string& errorMsg,
                                      const char* const ininfo)
{
  const auto event = mActions[0].mEvent;
  std::string fullPath;

  try {
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    fullPath = gOFS->eosView->getUri(fmd.get());
  } catch (eos::MDException& e) {
    eos_static_err("Could not get metadata for file %u. Reason: %s", mFid,
                   e.getMessage().str().c_str());
    MoveWithResults(ENOENT);
    return ENOENT;
  }

  {
    auto eventUpperCase = event;
    std::transform(eventUpperCase.begin(), eventUpperCase.end(),
                   eventUpperCase.begin(),
    [](unsigned char c) {
      return std::toupper(c);
    }
                  );
    eos_static_info("%s %s %s %s fxid=%08llx", mActions[0].mWorkflow.c_str(),
                    eventUpperCase.c_str(),
                    fullPath.c_str(), gOFS->ProtoWFEndPoint.c_str(), mFid);
  }

  if (event == "sync::prepare" || event == "prepare") {
    return HandleProtoMethodPrepareEvent(fullPath, ininfo, errorMsg);
  } else if (event == "sync::abort_prepare" || event == "abort_prepare") {
    return HandleProtoMethodAbortPrepareEvent(fullPath, ininfo, errorMsg);
  } else if (event == "sync::evict_prepare" || event == "evict_prepare") {
    return HandleProtoMethodEvictPrepareEvent(fullPath, ininfo, errorMsg);
  } else if (event == "sync::create" || event == "create") {
    return HandleProtoMethodCreateEvent(fullPath, errorMsg);
  } else if (event == "sync::delete" || event == "delete") {
    return HandleProtoMethodDeleteEvent(fullPath, errorMsg);
  } else if (event == "sync::closew" || event == "closew") {
    return HandleProtoMethodCloseEvent(event, fullPath);
  } else if (event == "sync::archived" || event == "archived") {
    return HandleProtoMethodArchivedEvent(event, fullPath, ininfo);
  } else if (event == RETRIEVE_FAILED_WORKFLOW_NAME) {
    return HandleProtoMethodRetrieveFailedEvent(fullPath);
  } else if (event == ARCHIVE_FAILED_WORKFLOW_NAME) {
    return HandleProtoMethodArchiveFailedEvent(fullPath);
  } else {
    eos_static_err("Unknown event %s for proto workflow", event.c_str());
    MoveWithResults(SFS_ERROR);
    return SFS_ERROR;
  }
}


int
WFE::Job::HandleProtoMethodPrepareEvent(const std::string& fullPath,
                                        const char* const ininfo,
                                        std::string& errorMsg)
{
  using namespace std::chrono;
  struct stat buf;
  XrdOucErrInfo errInfo;
  bool onDisk;
  bool onTape;
  EXEC_TIMING_BEGIN("Proto::Prepare");
  gOFS->MgmStats.Add("Proto::Prepare", 0, 0, 1);

  // Check if we have a disk replica and if not, whether it's on tape
  if (gOFS->_stat(fullPath.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                  false) == 0) {
    onDisk = ((buf.st_mode & EOS_TAPE_MODE_T) ? buf.st_nlink - 1 : buf.st_nlink) >
             0;
    onTape = (buf.st_mode & EOS_TAPE_MODE_T) != 0;
  } else {
    eos_static_err("Cannot determine file and disk replicas, not doing the prepare. Reason: %s",
                   errInfo.getErrText());
    MoveWithResults(EAGAIN);
    return EAGAIN;
  }

  uid_t cuid = 99;
  gid_t cgid = 99;

  if (onDisk) {
    eos_static_info("File %s is already on disk, nothing to prepare.",
                    fullPath.c_str());
    MoveWithResults(SFS_OK);
    return SFS_OK;
  } else if (!onTape) {
    eos_static_err("File %s is not on disk nor on tape, cannot prepare it.",
                   fullPath.c_str());
    MoveWithResults(ENODATA);
    return ENODATA;
  } else {
    eos::common::RWMutexWriteLock lock;
    lock.Grab(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    // Get the list of in-flight Prepare requests for this file (if any)
    XattrSet prepareReqIds;

    if (fmd->hasAttribute(RETRIEVE_REQID_ATTR_NAME)) {
      prepareReqIds.deserialize(fmd->getAttribute(RETRIEVE_REQID_ATTR_NAME));
    }

    bool isFirstPrepare = prepareReqIds.values.empty();

    try {
      // Get the new request ID from the opaque data and add it to the list
      XrdOucEnv opaque(ininfo);
      const char* const opaqueRequestId = opaque.Get("mgm.reqid");

      if(opaqueRequestId == nullptr) {
        throw_mdexception(EINVAL, "mgm.reqid does not exist in opaque data.");
      } else if(*opaqueRequestId == '\0') {
        throw_mdexception(EINVAL, "mgm.reqid has no value set in opaque data.");
      }

      prepareReqIds.values.insert(opaqueRequestId);
      fmd->setAttribute(RETRIEVE_REQID_ATTR_NAME, prepareReqIds.serialize());

      // if we are the first to retrieve the file
      if (isFirstPrepare) {
        fmd->setAttribute(RETRIEVE_ERROR_ATTR_NAME, "");
        // Read these attributes here to optimize locking
        cuid = fmd->getCUid();
        cgid = fmd->getCGid();
        gOFS->eosView->updateFileStore(fmd.get());
      } else {
        eos_static_info("File %s is already being retrieved by %u clients.",
                        fullPath.c_str(), prepareReqIds.values.size() - 1);
        MoveWithResults(SFS_OK);
        return SFS_OK;
      }
    } catch (eos::MDException& ex) {
      lock.Release();
      eos_static_err("Could not write attributes %s and %s for file %s. Not doing the retrieve.",
                     RETRIEVE_REQID_ATTR_NAME, RETRIEVE_ERROR_ATTR_NAME, fullPath.c_str());
      MoveWithResults(EAGAIN);
      return EAGAIN;
    }
  }

  // If we reached this point: the file is not on disk, it is on tape, and this is the first Prepare
  // request for this file. Proceed with issuing the Prepare request to the tape back-end.
  cta::xrd::Request request;
  auto notification = request.mutable_notification();
  notification->mutable_cli()->mutable_user()->set_username(GetUserName(
        mVid.uid));
  notification->mutable_cli()->mutable_user()->set_groupname(GetGroupName(
        mVid.gid));

  for (const auto& attribute : CollectAttributes(fullPath)) {
    google::protobuf::MapPair<std::string, std::string> attr(attribute.first,
        attribute.second);
    notification->mutable_file()->mutable_xattr()->insert(attr);
  }

  notification->mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
  notification->mutable_file()->set_lpath(fullPath);
  notification->mutable_wf()->mutable_instance()->set_name(
    gOFS->MgmOfsInstanceName.c_str());
  notification->mutable_file()->set_fid(mFid);
  notification->mutable_file()->mutable_owner()->set_uid(cuid);
  notification->mutable_file()->mutable_owner()->set_gid(cgid);
  auto fxidString = StringConversion::FastUnsignedToAsciiHex(mFid);
  std::ostringstream destStream;
  destStream << "root://" << gOFS->HostName << "/" << fullPath << "?eos.lfn=fxid:"
             << fxidString;
  destStream << "&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=" <<
             RETRIEVE_WRITTEN_WORKFLOW_NAME <<
             "&eos.space=" << gOFS->mPrepareDestSpace;
  notification->mutable_transport()->set_dst_url(destStream.str());
  std::ostringstream errorReportStream;
  errorReportStream << "eosQuery://" << gOFS->HostName
                    << "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" << fxidString
                    << "&mgm.logid=cta&mgm.event=" << RETRIEVE_FAILED_WORKFLOW_NAME
                    << "&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&mgm.rgid=0&mgm.errmsg=";
  notification->mutable_transport()->set_error_report_url(
    errorReportStream.str());
  auto sendResult = SendProtoWFRequest(this, fullPath, request, errorMsg);
  // Create timestamp
  std::string ctimestr(std::to_string(system_clock::to_time_t(
                                        system_clock::now())));

  if (sendResult == 0) {
    // Update the timestamp of the last Prepare request that was successfully sent
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);

    try {
      fmd->setAttribute(RETRIEVE_REQTIME_ATTR_NAME, ctimestr);
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& ex) {
      // fail silently if we couldn't update the timestamp
    }
  } else {
    if (errorMsg.empty()) {
      errorMsg = "Prepare handshake failed";
    }

    std::string errorMsgAttr = ctimestr + " -> " + errorMsg;
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);

    try {
      // Delete the request ID from the extended attributes so it can be retried
      fmd->setAttribute(RETRIEVE_REQID_ATTR_NAME, "");
      fmd->setAttribute(RETRIEVE_REQTIME_ATTR_NAME, "");
      fmd->setAttribute(RETRIEVE_ERROR_ATTR_NAME, errorMsgAttr);
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& ex) {}
  }

  EXEC_TIMING_END("Proto::Prepare");
  return sendResult;
}


int
WFE::Job::HandleProtoMethodAbortPrepareEvent(const std::string& fullPath,
    const char* const ininfo,
    std::string& errorMsg)
{
  EXEC_TIMING_BEGIN("Proto::Prepare::Abort");
  gOFS->MgmStats.Add("Proto::Prepare::Abort", 0, 0, 1);
  XattrSet prepareReqIds;
  {
    eos::common::RWMutexWriteLock lock;
    lock.Grab(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);

    try {
      if (fmd->hasAttribute(RETRIEVE_REQID_ATTR_NAME)) {
        prepareReqIds.deserialize(fmd->getAttribute(RETRIEVE_REQID_ATTR_NAME));
      }
    } catch (...) {
      lock.Release();
      eos_static_err("Could not determine ongoing retrieves for file %s. Check the %s extended attribute",
                     fullPath.c_str(), RETRIEVE_REQID_ATTR_NAME);
      MoveWithResults(EAGAIN);
      return EAGAIN;
    }

    try {
      // Remove the request ID from the list in the Xattr
      XrdOucEnv opaque(ininfo);
      const char* const opaqueRequestId = opaque.Get("mgm.reqid");

      if (opaqueRequestId == nullptr) {
        throw_mdexception(EINVAL, "mgm.reqid does not exist in opaque data.");
      } else if(*opaqueRequestId == '\0') {
        throw_mdexception(EINVAL, "mgm.reqid has no value set in opaque data.");
      }

      if (prepareReqIds.values.erase(opaqueRequestId) != 1) {
        throw_mdexception(EINVAL, "Request ID not found in extended attributes");
      }

      fmd->setAttribute(RETRIEVE_REQID_ATTR_NAME, prepareReqIds.serialize());
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& ex) {
      lock.Release();
      eos_static_err("Error accessing attribute %s for file %s: %s. Not doing the abort retrieve.",
                     RETRIEVE_REQID_ATTR_NAME, fullPath.c_str(), ex.what());
      MoveWithResults(EAGAIN);
      return EAGAIN;
    }
  }

  if (!prepareReqIds.values.empty()) {
    // There are other pending Prepare requests on this file, just return OK
    MoveWithResults(SFS_OK);
    return SFS_OK;
  }

  // optimization for reduced memory IO during write lock
  cta::xrd::Request request;
  auto notification = request.mutable_notification();
  notification->mutable_cli()->mutable_user()->set_username(GetUserName(
        mVid.uid));
  notification->mutable_cli()->mutable_user()->set_groupname(GetGroupName(
        mVid.gid));

  for (const auto& attribute : CollectAttributes(fullPath)) {
    google::protobuf::MapPair<std::string, std::string> attr(attribute.first,
        attribute.second);
    notification->mutable_file()->mutable_xattr()->insert(attr);
  }

  uid_t cuid = 99;
  gid_t cgid = 99;
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    cuid = fmd->getCUid();
    cgid = fmd->getCGid();
  }
  notification->mutable_file()->mutable_owner()->set_uid(cuid);
  notification->mutable_file()->mutable_owner()->set_gid(cgid);
  notification->mutable_wf()->set_event(cta::eos::Workflow::ABORT_PREPARE);
  notification->mutable_file()->set_lpath(fullPath);
  notification->mutable_wf()->mutable_instance()->set_name(
    gOFS->MgmOfsInstanceName.c_str());
  notification->mutable_file()->set_fid(mFid);
  auto s_ret = SendProtoWFRequest(this, fullPath, request, errorMsg);

  if (s_ret == 0) {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);

    try {
      // All Prepare requests cancelled by the user:
      // Delete the request time and error message from the extended attributes
      fmd->setAttribute(RETRIEVE_REQTIME_ATTR_NAME, "");
      fmd->setAttribute(RETRIEVE_ERROR_ATTR_NAME, "");
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& ex) {}
  }

  EXEC_TIMING_END("Proto::Prepare::Abort");
  return s_ret;
}

int
WFE::Job::HandleProtoMethodEvictPrepareEvent(const std::string& fullPath,
    const char* const ininfo,
    std::string& errorMsg)
{
  using namespace std::chrono;
  struct stat buf;
  XrdOucErrInfo errInfo;
  bool onDisk;
  bool onTape;
  EXEC_TIMING_BEGIN("Proto::EvictPrepare");
  gOFS->MgmStats.Add("Proto::EvictPrepare", 0, 0, 1);
  std::ostringstream preamble;
  preamble << "fxid=" << std::hex << mFid << " file=" << fullPath;

  // Check if we have a disk replica and if not, whether it's on tape
  if (gOFS->_stat(fullPath.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                  false) == 0) {
    onDisk = ((buf.st_mode & EOS_TAPE_MODE_T) ? buf.st_nlink - 1 : buf.st_nlink) >
             0;
    onTape = (buf.st_mode & EOS_TAPE_MODE_T) != 0;
  } else {
    std::ostringstream msg;
    msg << preamble.str() <<
        " msg=\"Cannot determine file and disk replicas, not doing the evict. Reason: "
        << errInfo.getErrText() << "\"";
    eos_static_err(msg.str().c_str());
    MoveWithResults(EAGAIN);
    return EAGAIN;
  }

  if (!onDisk) {
    std::ostringstream msg;
    msg << preamble.str() << " msg=\"File is not on disk, nothing to evict.\"";
    eos_static_info(msg.str().c_str());
  } else if (!onTape) {
    std::ostringstream msg;
    msg << preamble.str() << " msg=\"File is not on tape, cannot evict it.\"";
    eos_static_err(msg.str().c_str());
    MoveWithResults(ENODATA);
    return ENODATA;
  } else {
    const auto result = StagerrmAsRoot(mFid);

    if (0 == result.retc()) {
      std::ostringstream msg;
      msg << preamble.str() <<
          " msg=\"Successfully issued stagerrm for evict_prepare event\"";
      eos_static_info(msg.str().c_str());
    } else {
      std::ostringstream msg;
      msg << preamble.str() <<
          " msg=\"Failed to issue stagerrm for evict_prepare event\"";
      eos_static_info(msg.str().c_str());
      MoveWithResults(EAGAIN);
      return EAGAIN;
    }
  }

  MoveWithResults(SFS_OK);
  EXEC_TIMING_END("Proto::EvictPrepare");
  return SFS_OK;
}

int
WFE::Job::HandleProtoMethodCreateEvent(const std::string& fullPath,
                                       std::string& errorMsg)
{
  EXEC_TIMING_BEGIN("Proto::Create");
  gOFS->MgmStats.Add("Proto::Create", 0, 0, 1);
  cta::xrd::Request request;
  auto notification = request.mutable_notification();
  notification->mutable_cli()->mutable_user()->set_username(GetUserName(
        mVid.uid));
  notification->mutable_cli()->mutable_user()->set_groupname(GetGroupName(
        mVid.gid));

  for (const auto& attribute : CollectAttributes(fullPath)) {
    google::protobuf::MapPair<std::string, std::string> attr(attribute.first,
        attribute.second);
    notification->mutable_file()->mutable_xattr()->insert(attr);
  }

  uid_t cuid = 99;
  gid_t cgid = 99;
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    cuid = fmd->getCUid();
    cgid = fmd->getCGid();
  }
  notification->mutable_file()->mutable_owner()->set_uid(cuid);
  notification->mutable_file()->mutable_owner()->set_gid(cgid);
  notification->mutable_wf()->set_event(cta::eos::Workflow::CREATE);
  notification->mutable_wf()->mutable_instance()->set_name(
    gOFS->MgmOfsInstanceName.c_str());
  notification->mutable_file()->set_lpath(fullPath);
  notification->mutable_file()->set_fid(mFid);
  auto s_ret = SendProtoWFRequest(this, fullPath, request, errorMsg);
  EXEC_TIMING_END("Proto::Create");
  return s_ret;
}

int
WFE::Job::HandleProtoMethodDeleteEvent(const std::string& fullPath,
                                       std::string& errorMsg)
{
  EXEC_TIMING_BEGIN("Proto::Delete");
  gOFS->MgmStats.Add("Proto::Delete", 0, 0, 1);
  cta::xrd::Request request;
  auto notification = request.mutable_notification();
  notification->mutable_cli()->mutable_user()->set_username(GetUserName(
        mVid.uid));
  notification->mutable_cli()->mutable_user()->set_groupname(GetGroupName(
        mVid.gid));

  for (const auto& attribute : CollectAttributes(fullPath)) {
    google::protobuf::MapPair<std::string, std::string> attr(attribute.first,
        attribute.second);
    notification->mutable_file()->mutable_xattr()->insert(attr);
  }

  notification->mutable_wf()->set_event(cta::eos::Workflow::DELETE);
  notification->mutable_wf()->mutable_instance()->set_name(
    gOFS->MgmOfsInstanceName.c_str());
  notification->mutable_file()->set_lpath(fullPath);
  notification->mutable_file()->set_fid(mFid);
  const int sendRc = SendProtoWFRequest(this, fullPath, request, errorMsg);

  if (SFS_OK != sendRc) {
    eos_static_err("msg=\"Failed to notify protocol buffer endpoint about the deletion of file %s: %s\" sendRc=%d",
                   fullPath.c_str(), errorMsg.c_str(), sendRc);
  }

  try {
    // remove tape location
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    fmd->unlinkLocation(TAPE_FS_ID);
    fmd->removeLocation(TAPE_FS_ID);
    gOFS->eosView->updateFileStore(fmd.get());
  } catch (eos::MDException& ex) {
    eos_static_err("msg=\"Failed to unlink tape location for file %s",
		   fullPath.c_str());
  }

  EXEC_TIMING_END("Proto::Delete");
  return SFS_OK; // Ignore any failure in notifying the protocol buffer endpoint
}

int
WFE::Job::HandleProtoMethodCloseEvent(const std::string& event,
                                      const std::string& fullPath)
{
  EXEC_TIMING_BEGIN("Proto::Close");
  gOFS->MgmStats.Add("Proto::Close", 0, 0, 1);

  if (mActions[0].mWorkflow == RETRIEVE_WRITTEN_WORKFLOW_NAME) {
    resetRetrieveIdListAndErrorMsg(fullPath);
  }

  MoveWithResults(SFS_OK);
  EXEC_TIMING_END("Proto::Close");
  return SFS_OK;
}

void
WFE::Job::resetRetrieveIdListAndErrorMsg(const std::string& fullPath)
{
  std::string errorMsg;

  try {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    fmd->setAttribute(RETRIEVE_REQID_ATTR_NAME, "");
    fmd->setAttribute(RETRIEVE_REQTIME_ATTR_NAME, "");
    fmd->setAttribute(RETRIEVE_ERROR_ATTR_NAME, "");
    gOFS->eosView->updateFileStore(fmd.get());
    return;
  } catch (std::exception& se) {
    errorMsg = se.what();
  } catch (...) {
    errorMsg = "Caught an unknown exception";
  }

  // Reaching this point means an exception was thrown and caught
  eos_static_err("Could not reset retrieves counter and error attribute for file %s: %s",
                 fullPath.c_str(), errorMsg.c_str());
}

int
WFE::Job::HandleProtoMethodArchivedEvent(const std::string& event,
    const std::string& fullPath,
    const char* const ininfo)
{
  EXEC_TIMING_BEGIN("Proto::Archive");
  gOFS->MgmStats.Add("Proto::Archive", 0, 0, 1);
  std::string xattrCtaArchiveFileId;
  bool hasXattrCtaArchiveFileId = false;
  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    hasXattrCtaArchiveFileId = fmd->hasAttribute("sys.archive.file_id");

    if (hasXattrCtaArchiveFileId) {
      xattrCtaArchiveFileId = fmd->getAttribute("sys.archive.file_id");
    }
  }
  bool onlyTapeCopy = false;
  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    onlyTapeCopy = fmd->hasLocation(TAPE_FS_ID) && fmd->getLocations().size() == 1;
  }
  XrdOucEnv opaque(ininfo);
  const char* const opaqueCtaArchiveFileId = opaque.Get("cta_archive_file_id");

  if (event == "archived") {
    eos_static_err("The archived message for file %s is asynchronous when it should be synchronous."
                   " Ignoring request", fullPath.c_str());
  } else if (onlyTapeCopy) {
    eos_static_info("File %s already has a tape copy. Ignoring request.",
                    fullPath.c_str());
  } else if (!hasXattrCtaArchiveFileId) {
    eos_static_err("File %s does not have the sys.archive.file_id attribute. Ignoring request.",
                   fullPath.c_str());
  } else if (xattrCtaArchiveFileId.empty()) {
    eos_static_err("The sys.archive.file_id attribute of file %s is an empty string. Ignoring request.",
                   fullPath.c_str());
  } else if (nullptr == opaqueCtaArchiveFileId) {
    eos_static_err("The opaque data of the archived message for file %s does not contain cta_archive_file_id."
                   " Ignoring request.", fullPath.c_str());
  } else if (xattrCtaArchiveFileId != opaqueCtaArchiveFileId) {
    eos_static_err("The sys.archive.file_id attribute of file %s does not match cta_archive_file_id in the"
                   " opaque data of the archived message. xattrCtaArchiveFileId=%s opaqueCtaArchiveFileId=%s."
                   " Ignoring request.",
                   fullPath.c_str(), xattrCtaArchiveFileId.c_str(), opaqueCtaArchiveFileId);
  } else {
    eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
    {
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      auto fmd = gOFS->eosFileService->getFileMD(mFid);
      try {
	fmd->addLocation(TAPE_FS_ID);
	// Reset the error message
	fmd->setAttribute(ARCHIVE_ERROR_ATTR_NAME, "");
	gOFS->eosView->updateFileStore(fmd.get());
      } catch (eos::MDException& ex) {
	// fail silently - file could have been removed already
      }
    }

    if (GetFileArchivedGCEnabled("default")) {
      bool dropAllStripes = true;
      IContainerMD::XAttrMap parentDirAttributes;
      XrdOucErrInfo errInfo;

      if (gOFS->_attr_ls(eos::common::Path{fullPath.c_str()} .GetParentPath(),
                         errInfo, root_vid, nullptr, parentDirAttributes, true, true) == 0) {
        for (const auto& attrPair : parentDirAttributes) {
          if (attrPair.first == "sys.wfe.archived.dropdiskreplicas" &&
              attrPair.second == "0") {
            dropAllStripes = false;
          }
        }
      }
      errInfo.clear();

      if (dropAllStripes &&
          gOFS->_dropallstripes(fullPath.c_str(), errInfo, root_vid, false) != 0) {
        eos_static_err("Could not delete all file replicas of %s. Reason: %s",
                       fullPath.c_str(), errInfo.getErrText());
        MoveToRetry(fullPath);
        return EAGAIN;
      }
    }
  }

  MoveWithResults(SFS_OK);
  EXEC_TIMING_END("Proto::Archive");
  return SFS_OK;
}

bool
WFE::Job::GetFileArchivedGCEnabled(const std::string& space)
{
  const bool defaultValue =
    false; // The default value of filearchivedgc is 'false'
  std::string valueStr;

  try {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    const auto spaceItor = FsView::gFsView.mSpaceView.find(space);

    if (FsView::gFsView.mSpaceView.end() == spaceItor) {
      return defaultValue;
    }

    if (nullptr == spaceItor->second) {
      return defaultValue;
    }

    const auto& space = *(spaceItor->second);
    valueStr = space.GetConfigMember("filearchivedgc");
  } catch (...) {
    return defaultValue;
  }

  if (valueStr.empty()) {
    return defaultValue;
  } else {
    return valueStr == "on";
  }
}

int
WFE::Job::HandleProtoMethodRetrieveFailedEvent(const std::string& fullPath)
{
  EXEC_TIMING_BEGIN("Proto::Retrieve::Failed");
  gOFS->MgmStats.Add("Proto::Retrieve::Failed", 0, 0, 1);

  try {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    fmd->setAttribute(RETRIEVE_REQID_ATTR_NAME, "");
    fmd->setAttribute(RETRIEVE_REQTIME_ATTR_NAME, "");
    fmd->setAttribute(RETRIEVE_ERROR_ATTR_NAME, mErrorMesssage);
    gOFS->eosView->updateFileStore(fmd.get());
  } catch (eos::MDException& ex) {
    eos_static_err("Could not reset retrieves counter and set retrieve error attribute for file %s.",
                   fullPath.c_str());
    MoveWithResults(SFS_ERROR);
    return SFS_ERROR;
  }

  MoveWithResults(SFS_OK);
  EXEC_TIMING_END("Proto::Retrieve::Failed");
  return SFS_OK;
}

int
WFE::Job::HandleProtoMethodArchiveFailedEvent(const std::string& fullPath)
{
  EXEC_TIMING_BEGIN("Proto::Archive::Failed");
  gOFS->MgmStats.Add("Proto::Archive::Failed", 0, 0, 1);

  try {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mFid);
    fmd->setAttribute(ARCHIVE_ERROR_ATTR_NAME, mErrorMesssage);
    gOFS->eosView->updateFileStore(fmd.get());
  } catch (eos::MDException& ex) {
    eos_static_err("Could not set archive error attribute for file %s.",
                   fullPath.c_str());
    MoveWithResults(SFS_ERROR);
    return SFS_ERROR;
  }

  MoveWithResults(SFS_OK);
  EXEC_TIMING_END("Proto::Archive::Failed");
  return SFS_OK;
}

int
WFE::Job::SendProtoWFRequest(Job* jobPtr, const std::string& fullPath,
                             const cta::xrd::Request& request, std::string& errorMsg, bool retry)
{
  const std::string& event = jobPtr->mActions[0].mEvent;
  std::string exec_tag = "Proto::Send::";
  exec_tag += event;
  EXEC_TIMING_BEGIN(exec_tag.c_str());
  gOFS->MgmStats.Add(exec_tag.c_str(), 0, 0, 1);

  if (gOFS->ProtoWFEndPoint.empty() || gOFS->ProtoWFResource.empty()) {
    eos_static_err("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" "
                   "msg=\"You are running proto wf jobs without specifying mgmofs.protowfendpoint or "
                   "mgmofs.protowfresource in the MGM config file.\"",
                   gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                   event.c_str());
    jobPtr->MoveWithResults(ENOTCONN);
    return ENOTCONN;
  }

  XrdSsiPb::Config config;

  if (getenv("XRDDEBUG")) {
    config.set("log", "all");
  } else {
    config.set("log", "info");
  }

  config.set("request_timeout", "120");
  // Instantiate service object only once, static is thread-safe
  static XrdSsiPbServiceType service(gOFS->ProtoWFEndPoint, gOFS->ProtoWFResource,
                                     config);
  cta::xrd::Response response;

  // Send the request
  try {
    const auto sentAt = std::chrono::steady_clock::now();
    service.Send(request, response);
    const auto receivedAt = std::chrono::steady_clock::now();
    const auto timeSpentMilliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds> (receivedAt - sentAt);
    eos_static_info("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" timeSpentMs=%ld "
                    "msg=\"Sent SSI protocol buffer request\"",
                    gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                    event.c_str(),
                    timeSpentMilliseconds.count());
  } catch (std::runtime_error& error) {
    eos_static_err("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" "
                   "msg=\"Could not send SSI protocol buffer request to outside service.\" reason=\"%s\"",
                   gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                   event.c_str(),
                   error.what());
    errorMsg = error.what();
    retry ? jobPtr->MoveToRetry(fullPath) : jobPtr->MoveWithResults(ENOTCONN);
    return ENOTCONN;
  }

  // Handle the response
  int retval = EPROTO;

  switch (response.type()) {
  case cta::xrd::Response::RSP_SUCCESS: {
    // Set all attributes for file from response
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
    XrdOucErrInfo errInfo;

    for (const auto& attrPair : response.xattr()) {
      errInfo.clear();

      if (gOFS->_attr_set(fullPath.c_str(), errInfo, rootvid,
                          nullptr, attrPair.first.c_str(), attrPair.second.c_str()) != 0) {
        eos_static_err("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" "
                       "msg=\"Could not set attribute\" attrName=\"%s\" attrValue=\"%s\" reason=\"%s\"",
                       gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                       event.c_str(),
                       attrPair.first.c_str(), attrPair.second.c_str(), errInfo.getErrText());
      }
    }

    jobPtr->MoveWithResults(SFS_OK);
    EXEC_TIMING_END(exec_tag.c_str());
    return SFS_OK;
  }

  // CTA Frontend error; return operation cancelled
  case cta::xrd::Response::RSP_ERR_CTA:
    retval = ECANCELED;
    break;

  // User error; return operation not permitted
  case cta::xrd::Response::RSP_ERR_USER:
    retval = EPERM;
    break;

  // Error in Google Protocol buffers; return protocol error
  case cta::xrd::Response::RSP_ERR_PROTOBUF:
    retval = EPROTO;
    break;

  // Response type was not set or was set to an unknown value; return not a data message
  case cta::xrd::Response::RSP_INVALID:
  default:
    retval = EBADMSG;
    eos_static_err("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" "
                   "msg=\"Invalid or unknown response\" response=\"%s\"",
                   gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                   event.c_str(),
                   response.DebugString().c_str());
  }

  eos_static_err("protoWFEndPoint=\"%s\" protoWFResource=\"%s\" fullPath=\"%s\" event=\"%s\" "
                 "msg=\"Received an error response\" response=\"%s\" reason=\"%s\"",
                 gOFS->ProtoWFEndPoint.c_str(), gOFS->ProtoWFResource.c_str(), fullPath.c_str(),
                 event.c_str(),
                 CtaCommon::ctaResponseCodeToString(response.type()).c_str(),
                 response.message_txt().c_str());
  retry ? jobPtr->MoveToRetry(fullPath) : jobPtr->MoveWithResults(retval);
  errorMsg = response.message_txt();
  return retval;
}

console::ReplyProto
WFE::Job::StagerrmAsRoot(const eos::IFileMD::id_t fid)
{
  eos::common::VirtualIdentity rootVid = eos::common::VirtualIdentity::Root();
  eos::console::RequestProto req;
  eos::console::StagerRmProto* stagerRm = req.mutable_stagerrm();
  auto file = stagerRm->add_file();
  file->set_fid(fid);
  StagerRmCmd cmd(std::move(req), rootVid);
  return cmd.ProcessRequest();
}

void
WFE::Job::MoveToRetry(const std::string& filePath)
{
  if (!IsSync()) {
    int retry = 0, delay = 0;
    std::string retryattr = "sys.workflow." + mActions[0].mEvent + "." +
                            mActions[0].mWorkflow + ".retry.max";
    std::string delayattr = "sys.workflow." + mActions[0].mEvent + "." +
                            mActions[0].mWorkflow + ".retry.delay";
    eos_static_info("%s %s", retryattr.c_str(), delayattr.c_str());
    {
      eos::common::Path cPath(filePath.c_str());
      auto parentPath = cPath.GetParentPath();
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, parentPath);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      auto cmd = gOFS->eosView->getContainer(parentPath);

      try {
        retry = std::stoi(cmd->getAttribute(retryattr));
      } catch (...) {
        // retry 25 times by default
        retry = 25;
      }

      try {
        delay = std::stoi(cmd->getAttribute(delayattr));
      } catch (...) {
        // retry after 1 hour by default and one final longer wait
        delay = mRetry == retry - 1 ? 7200 : 3600;
      }
    }

    if (mRetry < retry) {
      time_t storetime = (time_t) mActions[0].mTime + delay;
      Move("r", "e", storetime, ++mRetry);
      Results("e", EAGAIN, "scheduled for retry", storetime);
    } else {
      eos_static_err("WF event finally failed for %s event of %s file after %d retries.",
                     mActions[0].mEvent.c_str(), filePath.c_str(), mRetry);
      MoveWithResults(SFS_ERROR, "e");
    }
  }
}

void
WFE::Job::MoveWithResults(int rcode, std::string fromQueue)
{
  if (!IsSync()) {
    time_t storetime = 0;

    if (rcode == 0) {
      Move(fromQueue, "d", storetime);
      Results("d", rcode, "moved to done", storetime);
    } else {
      Move(fromQueue, "f", storetime);
      Results("f", rcode, "moved to failed", storetime);
    }
  }
}

std::string
WFE::GetGroupName(gid_t gid)
{
  int errc = 0;
  auto group_name  = Mapping::GidToGroupName(gid, errc);

  if (errc) {
    group_name = "nobody";
  }

  return group_name;
}

std::string
WFE::GetUserName(uid_t uid)
{
  int errc = 0;
  auto user_name  = Mapping::UidToUserName(uid, errc);

  if (errc) {
    user_name = "nobody";
  }

  return user_name;
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
  snprintf(sactive, sizeof(sactive) - 1, "%u", GetActiveJobs());
  FsView::gFsView.mSpaceView["default"]->SetConfigMember("stat.wfe.active", sactive, true);
}

IContainerMD::XAttrMap
WFE::CollectAttributes(const std::string& fullPath)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo errInfo;
  IContainerMD::XAttrMap fileAttributes, parentDirAttributes, result;

  if (gOFS->_attr_ls(fullPath.c_str(),
                     errInfo, rootvid, nullptr, fileAttributes, true, true) == 0) {
    for (const auto& fileAttrPair : fileAttributes) {
      if (fileAttrPair.first.find("sys.archive.") == 0 ||
          fileAttrPair.first.find("CTA_") == 0) {
        result.insert(fileAttrPair);
      }
    }
  }

  errInfo.clear();

  if (gOFS->_attr_ls(eos::common::Path{fullPath.c_str()} .GetParentPath(),
                     errInfo, rootvid, nullptr, parentDirAttributes, true, true) == 0) {
    for (const auto& dirAttrPair : parentDirAttributes) {
      if (dirAttrPair.first.find("sys.archive.") == 0 ||
          dirAttrPair.first.find("CTA_") == 0) {
        result.insert(dirAttrPair);
      }
    }
  }
  return result;
}

void
WFE::MoveFromRBackToQ()
{
  std::string queries[2];

  for (auto& query : queries) {
    query = gOFS->MgmProcWorkflowPath.c_str();
    query += "/";
  }

  {
    // today
    time_t when = time(nullptr);
    std::string day = eos::common::Timing::UnixTimestamp_to_Day(when);
    queries[0] += day;
    queries[0] += "/r/";
    //yesterday
    when -= (24 * 3600);
    day = eos::common::Timing::UnixTimestamp_to_Day(when);
    queries[1] += day;
    queries[1] += "/r/";
  }

  std::map<std::string, std::set<std::string>> wfedirs;
  XrdOucErrInfo errInfo;
  XrdOucString stdErr;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  for (const auto& query : queries) {
    gOFS->_find(query.c_str(),
                errInfo,
                stdErr,
                rootvid,
                wfedirs,
                nullptr,
                nullptr,
                false,
                0,
                false,
                0
               );
  }

  for (const auto& wfedir : wfedirs) {
    auto wfEntry = wfedir.first;

    for (const auto& entry : wfedir.second) {
      wfEntry += entry;
      Job job;

      if (job.Load(wfEntry) == 0) {
        if (!job.IsSync()) {
          job.Move("r", "q", job.mActions[0].mTime);
        }
      } else {
        eos_static_err("msg=\"cannot load workflow entry during recycling from r queue\" value=\"%s\"",
                       wfEntry.c_str());
      }
    }
  }
}

EOSMGMNAMESPACE_END
