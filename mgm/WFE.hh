// ----------------------------------------------------------------------
// File: WFE.hh
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

#ifndef __EOSMGM_WFE__HH__
#define __EOSMGM_WFE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Timing.hh"
#include "namespace/interface/IView.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "Xrd/XrdScheduler.hh"
#include "XrdCl/XrdClCopyProcess.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/**
 * @file   WFE.hh
 *
 * @brief  This class implements an WFE engine
 *
 */

class WFE
{
private:
  //............................................................................
  // variables for the WFE thread
  //............................................................................
  pthread_t mThread; //< thread id of the WFE thread
  time_t mMs; //< forced sleep time used for find / scans

  eos::common::Mapping::VirtualIdentity mRootVid; //< we operate with the root vid
  XrdOucErrInfo mError; //< XRootD error object

  mutable XrdSysMutex mActiveJobsMutex;

  /// this are all jobs which are queued and didn't run yet
  size_t mActiveJobs;

  /// condition variabl to get signalled for a done job
  XrdSysCondVar mDoneSignal;

public:

  /* Default Constructor - use it to run the WFE thread by calling Start
   */
  WFE();

  /**
   * @brief get the millisecond sleep time for find
   * @return configured sleep time
   */
  time_t GetMs()
  {
    return mMs;
  }

  /**
   * @brief set the millisecond sleep time for find
   * @param ms sleep time in milliseconds to enforce
   */
  void SetMs(time_t ms)
  {
    mMs = ms;
  }

  /* Start the WFE thread engine
   */
  bool Start();

  /* Stop the WFE thread engine
   */
  void Stop();

  /* Thread start function for WFE thread
   */
  static void* StartWFEThread(void*);

  /* WFE method doing the actual policy scrubbing
   */
  void* WFEr();

  /**
   * @brief Destructor
   *
   */
  ~WFE()
  {
    if (mThread) {
      Stop();
    }
  };

  class Job : XrdJob
  {
  public:

    struct Action {

      Action(std::string a, std::string e, time_t when, std::string workflow,
             std::string queue)
      {
        mAction = a;
        mEvent = e;
        mTime = when;
        mWorkflow = workflow;
        mQueue = queue;
        XrdOucString tst;
        mWhen = eos::common::StringConversion::GetSizeString(tst,
                (unsigned long long) when);
        mDay = eos::common::Timing::UnixTimstamp_to_Day(when);
      }

      std::string mAction;
      std::string mEvent;
      time_t mTime; //! unix timestamp
      std::string mWhen; //! string with unix timestamp
      std::string mDay; //! string with yearmonthday
      std::string mWorkflow;
      std::string mQueue;
    };

    Job()
    {
      mFid = 0;
      mRetry = 0;
    }

    Job(eos::common::FileId::fileid_t fid,
        eos::common::Mapping::VirtualIdentity& vid)
    {
      mFid = fid;
      eos::common::Mapping::Copy(vid, mVid);
    }

    ~Job()
    {
    }

    Job(const Job& other)
    {
      mActions = other.mActions;
      mFid = other.mFid;
      mDescription = other.mDescription;
    }
    // ---------------------------------------------------------------------------
    // Job execution function
    // ---------------------------------------------------------------------------
    void DoIt();

    // -------------------------------------------------------------------------
    // persistency related methods
    // -------------------------------------------------------------------------
    int Save(std::string queue, time_t when = 0, int action = 0, int retry = 0);

    int Load(std::string path2entry);

    int Move(std::string from_queue, std::string to_queue, time_t when = 0,
             int retry = 0);

    int Delete(std::string queue);

    // -------------------------------------------------------------------------

    void AddAction(std::string action,
                   std::string event,
                   time_t when,
                   std::string workflow,
                   std::string queue)
    {
      Action thisaction(action, event, when, workflow, queue);
      mActions.push_back(thisaction);
      mDescription += action;
      mDescription += " ";
      mDescription += "/";
      mDescription += event;
      mDescription += "/";
      std::string tst;
      mDescription += eos::common::StringConversion::GetSizeString(tst,
                      (unsigned long long) when);
      mDescription += "/";
      mDescription += workflow;
      mDescription += "/";
      mDescription += queue;
      mDescription += "/";
      mDescription += eos::common::StringConversion::GetSizeString(tst,
                      (unsigned long long) mFid);
    }

    std::vector<Action> mActions;
    eos::common::FileId::fileid_t mFid;
    std::string mDescription;
    eos::common::Mapping::VirtualIdentity mVid;
    std::string mWorkflowPath;
    int mRetry;///! number of retries
  };

  XrdSysCondVar* GetSignal()
  {
    return &mDoneSignal;
  }

  // ---------------------------------------------------------------------------
  //! Decrement the number of active jobs in the workflow enging
  // ---------------------------------------------------------------------------

  void DecActiveJobs()
  {
    {
      XrdSysMutexHelper lMutex(mActiveJobsMutex);
      mActiveJobs--;
    }
    PublishActiveJobs();
  }

  // ---------------------------------------------------------------------------
  //! Increment the number of active jobs in this converter
  // ---------------------------------------------------------------------------

  void IncActiveJobs()
  {
    {
      XrdSysMutexHelper lMutex(mActiveJobsMutex);
      mActiveJobs++;
    }
    PublishActiveJobs();
  }

  // ---------------------------------------------------------------------------
  //! Publish the number of active jobs in the workflow engine
  // ---------------------------------------------------------------------------
  void PublishActiveJobs();

  // ---------------------------------------------------------------------------
  //! Return active jobs
  // ---------------------------------------------------------------------------

  size_t GetActiveJobs() const
  {
    XrdSysMutexHelper lMutex(mActiveJobsMutex);
    return mActiveJobs;
  }

  /// the scheduler class is providing a destructor-less object,
  /// so we have to create once a singleton of this and keep/share it
  static XrdSysMutex gSchedulerMutex;

  /// singleton object of a scheduler
  static XrdScheduler* gScheduler;
};

EOSMGMNAMESPACE_END

#endif
