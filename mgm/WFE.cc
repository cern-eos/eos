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

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/ShellCmd.hh"
#include "mgm/Quota.hh"
#include "mgm/WFE.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/

#define EOS_WFE_BASH_PREFIX "/var/eos/wfe/bash/"

XrdSysMutex eos::mgm::WFE::gSchedulerMutex;
XrdScheduler* eos::mgm::WFE::gScheduler;
/*----------------------------------------------------------------------------*/
extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN
;

using namespace eos::common;

/*----------------------------------------------------------------------------*/
WFE::WFE ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor of the work flow engine
 */
/*----------------------------------------------------------------------------*/
{
  mThread = 0;
  mMs = 0;
  eos::common::Mapping::Root(mRootVid);

  XrdSysMutexHelper sLock(gSchedulerMutex);
  gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
  gScheduler->Start();
}

/*----------------------------------------------------------------------------*/
bool
WFE::Start ()
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
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "WFE engine Thread");

  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
void
WFE::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief asynchronous WFE thread stop function
 */
/*----------------------------------------------------------------------------*/
{
  // cancel the asynchronous WFE thread
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }
  mThread = 0;
}

void*
WFE::StartWFEThread (void* arg)
{
  return reinterpret_cast<WFE*> (arg)->WFEr();
}

/*----------------------------------------------------------------------------*/
void*
WFE::WFEr ()
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
  do
  {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  while (!go);

  XrdSysTimer sleeper;
  sleeper.Snooze(10);

  //----------------------------------------------------------------------------
  // Eternal thread doing WFE scans
  //----------------------------------------------------------------------------

  time_t snoozetime = 10;
  size_t lWFEntx = 0;

  eos_static_info("msg=\"async WFE thread started\"");
  while (1)
  {
    // -------------------------------------------------------------------------
    // every now and then we wake up
    // -------------------------------------------------------------------------

    XrdSysThread::SetCancelOff();
    bool IsEnabledWFE;
    time_t lWFEInterval;
    time_t lStartTime = time(NULL);
    time_t lStopTime;
    std::map<std::string, std::set<std::string> > wfedirs;
    XrdOucString stdErr;

    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      if (FsView::gFsView.mSpaceView.count("default") && (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") == "on"))
        IsEnabledWFE = true;
      else
        IsEnabledWFE = false;
      if (FsView::gFsView.mSpaceView.count("default"))
      {
        lWFEInterval =
                atoi(FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe.interval").c_str());
	lWFEntx =
	  atoi(FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe.ntx").c_str());
      }
      else
      {
        lWFEInterval = 0;
	lWFEntx = 0;
      }
    }

    // only a master needs to run WFE
    if (gOFS->MgmMaster.IsMaster() && IsEnabledWFE)
    {
      // -------------------------------------------------------------------------
      // do a find
      // -------------------------------------------------------------------------

      eos_static_info("msg=\"start WFE scan\"");

      // -------------------------------------------------------------------------
      // find all directories defining an WFE policy
      // -------------------------------------------------------------------------
      gOFS->MgmStats.Add("WFEFind", 0, 0, 1);

      EXEC_TIMING_BEGIN("WFEFind");

      if (!gOFS->_find(gOFS->MgmProcWorkflowPath.c_str(),
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
                       )
          )
      {
        eos_static_info("msg=\"finished WFE find\" WFE-dirs=%llu %s",
                        wfedirs.size(), stdErr.c_str()
                        );

        for (auto it = wfedirs.begin(); it != wfedirs.end(); it++)
        {
          // -------------------------------------------------------------------
          // get workflows
          // -------------------------------------------------------------------

          if (it->second.size())
          {
            for (auto wit = it->second.begin(); wit != it->second.end(); ++wit)
            {
              eos_static_info("wfe-dir=\"%s\" wfe-job=\"%s\"", it->first.c_str(), wit->c_str());
              std::string f = it->first;
              f += *wit;
              Job* job = new Job();
              if (!job || job->Load(f))
              {
                eos_static_err("msg=\"cannot load workflow entry\" value=\"%s\"", f.c_str());
                if (job)
                  delete job;
              }
              else
              {
                // stop scheduling if there are too many jobs running
                if ((lWFEntx - GetActiveJobs()) <= 0)
                {
                  if (lWFEntx > 0)
                  {
                    mDoneSignal.Wait(10);
                    if ((lWFEntx - GetActiveJobs()) <= 0)
                      break;
                  }
                }

                // use the shared scheduler
                XrdSysMutexHelper sLock(gSchedulerMutex);
                gScheduler->Schedule((XrdJob*) job);
                IncActiveJobs();
                eos_static_info("msg=\"scheduled workflow\" job=\"%s\"", job->mDescription.c_str());
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

    if ((lStopTime - lStartTime) < lWFEInterval)
    {
      snoozetime = lWFEInterval - (lStopTime - lStartTime);
    }

    eos_static_info("snooze-time=%llu enabled=%d", snoozetime, IsEnabledWFE);
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    time_t snoozeinterval = 10;
    size_t snoozeloop = snoozetime / 10;
    for (size_t i = 0; i < snoozeloop; i++)
    {
      sleeper.Snooze(snoozeinterval);
      {
        // check if the setting changes
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (FsView::gFsView.mSpaceView.count("default") && (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") == "on"))
        {
          if (!IsEnabledWFE)
            break;
        }
        else
        {
          if (IsEnabledWFE)
            break;
        }
      }
    }
  };
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
WFE::Job::Save (std::string queue, bool time_now, int action)
{

  if (mActions.size() != 1)
    return -1;

  std::string workflowdir = gOFS->MgmProcWorkflowPath.c_str();
  workflowdir += "/";
  workflowdir += mActions[action].mWorkflow;
  workflowdir += "/";
  workflowdir += mActions[action].mDay;
  workflowdir += "/";
  workflowdir += queue;
  workflowdir += "/";

  std::string entry;

  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(mFid, hexfid);
  entry = hexfid.c_str();

  eos_static_info("workflowdir=\"%s\"", workflowdir.c_str());

  XrdOucErrInfo lError;
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);

  // check that the workflow directory exists
  struct stat buf;
  if (gOFS->_stat(workflowdir.c_str(),
                  &buf,
                  lError,
                  rootvid,
                  ""))
  {
    // create the workflow sub directory
    if (gOFS->_mkdir(workflowdir.c_str(),
                     S_IRWXU | SFS_O_MKPTH,
                     lError,
                     rootvid,
                     ""))
    {
      eos_static_err("msg=\"failed to create workflow directory\" path=\"%s\"", workflowdir.c_str());
      return -1;
    }
  }

  // write a workflow file
  std::string workflowpath = workflowdir;

  // evt. store with the current time
  if (time_now)
  {
    std::string tst;
    workflowpath += eos::common::StringConversion::GetSizeString(tst, (unsigned long long) time(NULL));
  }
  else
  {
    workflowpath += mActions[action].mWhen;
  }

  workflowpath += ":";
  workflowpath += entry;

  workflowpath += ":";
  workflowpath += mActions[action].mEvent;

  if (gOFS->_touch(workflowpath.c_str(), lError, rootvid, 0))
  {
    eos_static_err("msg=\"failed to create workflow entry\" path=\"%s\"", workflowpath.c_str());
    return -1;
  }

  if (gOFS->_attr_set(workflowpath.c_str(),
                      lError,
                      rootvid,
                      0,
                      "sys.action",
                      mActions[0].mAction.c_str()))
  {
    eos_static_err("msg=\"failed to store workflow action\" path=\"%s\" action=\"%s\"",
                   workflowpath.c_str(),
                   mActions[0].mAction.c_str());
    return -1;
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::Load (std::string path2entry)
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
  std::string q = path2entry;
  q.erase(path2entry.rfind("/"));
  q.erase(0, q.rfind("/") + 1);

  std::string workflow = path2entry;
  workflow.erase(0, gOFS->MgmProcWorkflowPath.length() + 1);
  workflow.erase(workflow.find("/"));

  std::string when;
  std::string idevent;
  std::string id;
  std::string event;

  bool s1 = eos::common::StringConversion::SplitKeyValue(f, when, idevent, ":");
  bool s2 = eos::common::StringConversion::SplitKeyValue(idevent, id, event, ":");

  if (s1 && s2)
  {
    mFid = eos::common::FileId::Hex2Fid(id.c_str());

    eos_static_info("workflow=\"%s\" fid=%lx", workflow.c_str(), mFid);
    XrdOucString action;
    if (!gOFS->_attr_get(path2entry.c_str(),
                         lError,
                         rootvid,
                         0,
                         "sys.action",
                         action, false))
    {
      time_t t_when = strtoull(when.c_str(), 0, 10);
      AddAction(action.c_str(), event, t_when, workflow, q);
    }
    else
    {
      eos_static_err("msg=\"no action stored\" path=\"%s\"", f.c_str());
    }
  }
  else
  {
    eos_static_err("msg=\"illegal workflow entry\" key=\"%s\"", f.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::Move (std::string from_queue, std::string to_queue, bool time_now)
/*----------------------------------------------------------------------------*/
/**
 * @brief move workflow jobs between qeueus
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  if (Save(to_queue, time_now) == SFS_OK)
  {
    if (Delete(from_queue) == SFS_ERROR)
    {
      eos_static_err("msg=\"failed to remove for move from queue\" queue=\"%s\"",
                     from_queue.c_str());
    }
  }
  else
  {
    eos_static_err("msg=\"failed to save for move to queue\" queue=\"%s\"",
                   to_queue.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
WFE::Job::Delete (std::string queue)
/*----------------------------------------------------------------------------*/
/**
 * @brief delete a workflow job from a queue
 * @return SFS_OK if success
 */
/*----------------------------------------------------------------------------*/
{
  if (mActions.size() != 1)
    return SFS_ERROR;

  std::string workflowdir = gOFS->MgmProcWorkflowPath.c_str();
  workflowdir += "/";
  workflowdir += mActions[0].mWorkflow;
  workflowdir += "/";
  workflowdir += mActions[0].mDay;
  workflowdir += "/";
  workflowdir += queue;
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
                  , false, true))
  {
    return SFS_OK;
  }
  else
  {
    eos_static_err("msg=\"failed to delete job\" job=\"%s\"", mDescription.c_str());
    return SFS_ERROR;
  }
}

/*----------------------------------------------------------------------------*/
void
WFE::Job::DoIt ()
/*----------------------------------------------------------------------------*/
/**
 * @brief execute a workflow
 * @return
 *  */
/*----------------------------------------------------------------------------*/
{
  std::string method;
  std::string args;

  eos_static_info("queue=\"%s\"", mActions[0].mQueue.c_str());
  if (mActions[0].mQueue == "q")
  {
    if (eos::common::StringConversion::SplitKeyValue(mActions[0].mAction, method, args, ":"))
    {
      if (method == "mail")
      {
        std::string recipient;
        std::string freetext;
        if (!eos::common::StringConversion::SplitKeyValue(args, recipient, freetext, ":"))
        {
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
        if (rc.exit_code)
        {
          eos_static_err("msg=\"failed to send workflow notification mail\" job=\"%s\"",
                         mDescription.c_str());
          Move("q", "e");
        }
        else
        {
          eos_static_info("msg=\"done notification\" job=\"%s\"",
                          mDescription.c_str());
          Move("q", "d");
        }
      }
      else if (method == "bash")
      {
        std::string executable;
        std::string executableargs;
        if (!eos::common::StringConversion::SplitKeyValue(args, executable, executableargs, ":"))
        {
          executable = args;
          executableargs = "";
        }

        XrdOucString execargs = executableargs.c_str();

        std::string fullpath;

        if (executable.find("/") == std::string::npos)
        {
          eos::FileMD* fmd = 0;

          // do meta replacement
          gOFS->eosViewRWMutex.LockRead();

          try
          {
            fmd = gOFS->eosFileService->getFileMD(mFid);
            fullpath = gOFS->eosView->getUri(fmd);
          }
          catch (eos::MDException &e)
          {
            eos_static_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
          }
                                                                                                                                                                                                                                                                                                                      \
          if (fmd)
          {
            eos::FileMD fmdCopy(*fmd);
            fmd = &fmdCopy;

            gOFS->eosViewRWMutex.UnLockRead();

            std::string cv;
            eos::FileMD::ctime_t ctime;
            eos::FileMD::ctime_t mtime;
            fmd->getCTime(ctime);
            fmd->getMTime(mtime);

            std::string checksum;

            size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
            for (unsigned int i = 0; i < cxlen; i++)
            {
              char hb[3];
              sprintf(hb, "%02x", (i < cxlen) ? (unsigned char) (fmd->getChecksum().getDataPadded(i)) : 0);
              checksum += hb;
            }

            while (execargs.replace("<path>", fullpath.c_str()))
            {
            }

            while (execargs.replace("<uid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) fmd->getCUid())))
            {
            }

            while (execargs.replace("<gid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) fmd->getCGid())))
            {
            }

            while (execargs.replace("<ctime.s>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_sec)))
            {
            }

            while (execargs.replace("<mtime.s>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_sec)))
            {
            }

            while (execargs.replace("<ctime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_nsec)))
            {
            }

            while (execargs.replace("<mtime.ns>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_nsec)))
            {
            }

            while (execargs.replace("<ctime>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_sec)))
            {
            }

            while (execargs.replace("<mtime>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) ctime.tv_sec)))
            {
            }

            while (execargs.replace("<size>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) fmd->getSize())))
            {
            }

            while (execargs.replace("<cid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) fmd->getContainerId())))
            {
            }

            while (execargs.replace("<fid>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) mFid)))
            {
            }

            XrdOucString hexfid;
            eos::common::FileId::Fid2Hex(mFid, hexfid);

            while (execargs.replace("<fxid>",
                                    hexfid))
            {
            }

            while (execargs.replace("<name>", fmd->getName().c_str()))
            {
            }

            while (execargs.replace("<link>", fmd->getLink().c_str()))
            {
            }

            while (execargs.replace("<checksum>", checksum.c_str()))
            {
            }

            while (execargs.replace("<event>", mActions[0].mEvent.c_str()))
            {
            }

            while (execargs.replace("<queue>", mActions[0].mQueue.c_str()));
            {
            }

            while (execargs.replace("<workflow>", mActions[0].mWorkflow.c_str()));
            {
            }

            time_t now = time(NULL);
            while (execargs.replace("<now>",
                                    eos::common::StringConversion::GetSizeString(cv, (unsigned long long) now)))
            {
            }

            execargs.replace("<action>", mActions[0].mAction.c_str())

            std::string bashcmd = EOS_WFE_BASH_PREFIX + executable + " " + execargs.c_str();

            eos::common::ShellCmd cmd(bashcmd.c_str());
            eos_static_info("shell-cmd=\"%s\"", bashcmd.c_str());
            eos::common::cmd_status rc = cmd.wait(1800);
            if (rc.exit_code)
            {
              eos_static_err("msg=\"failed to run bash workflow\" job=\"%s\"",
                             mDescription.c_str());
              Move("q", "e");
            }
            else
            {
              eos_static_info("msg=\"done bash workflow\" job=\"%s\"",
                              mDescription.c_str());
              Move("q", "d");
            }
          }
          else
          {
            gOFS->eosViewRWMutex.UnLockRead();
            eos_static_err("msg=\"failed to run bash workflow - file gone\" job=\"%s\"",
                           mDescription.c_str());
            Move("q", "g");
          }
        }
        else
        {
          eos_static_err("msg=\"failed to run bash workflow - executable name modifies path\" job=\"%s\"",
                         mDescription.c_str());
          Move("q", "g");
        }
      }
      else
      {
        eos_static_err("msg=\"moving unkown workflow\" job=\"%s\"",
                       mDescription.c_str());
        Move("q", "g");
      }
    }
    else
    {
      eos_static_err("msg=\"moving illegal workflow\" job=\"%s\"",
                     mDescription.c_str());
      Move("q", "g");
    }
  }
  else
  {
    Delete(mActions[0].mQueue);
  }

  gOFS->WFEd.GetSignal()->Signal();
  gOFS->WFEd.DecActiveJobs();
}

/*----------------------------------------------------------------------------*/
void
WFE::PublishActiveJobs ()
/*----------------------------------------------------------------------------*/
/**
 * @brief publish the active job number in the space view
 *
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  char sactive[256];
  snprintf(sactive, sizeof (sactive) - 1, "%lu", GetActiveJobs());
  FsView::gFsView.mSpaceView["default"]->SetConfigMember
          ("stat.workflow.active",
           sactive,
           true,
           "/eos/*/mgm",
           true);
}

EOSMGMNAMESPACE_END
