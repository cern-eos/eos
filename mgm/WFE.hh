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

#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Timing.hh"
#include "common/FileId.hh"
#include "common/ThreadPool.hh"
#include "common/AssistedThread.hh"
#include "common/xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "Xrd/XrdJob.hh"
#include <sys/types.h>

//! Forward declaration
class XrdScheduler;

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
  AssistedThread mThread; //< thread id of the WFE thread
  time_t mMs; //< forced sleep time used for find / scans

  eos::common::Mapping::VirtualIdentity mRootVid; //< we operate with the root vid
  XrdOucErrInfo mError; //< XRootD error object

  /// number of all jobs which are queued and didn't run yet
  std::atomic_uint_least32_t mActiveJobs;

  /// condition variable to get signalled for a done job
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

  /* WFE method doing the actual policy scrubbing
   */
  void WFEr(ThreadAssistant& assistant) noexcept;

  /**
   * @brief Destructor
   *
   */
  ~WFE()
  {
    Stop();
    std::cerr << __FUNCTION__ << ":: end of destructor" << std::endl;
  }

  class Job : XrdJob
  {
  public:

    struct Action {

      Action(std::string a, std::string e, time_t when, std::string workflow,
             std::string queue)
      {
        mAction = std::move(a);
        mEvent = std::move(e);
        mTime = when;
        mWorkflow = std::move(workflow);
        mQueue = std::move(queue);
        XrdOucString tst;
        mWhen = eos::common::StringConversion::GetSizeString(tst,
                (unsigned long long) when);
        mDay = eos::common::Timing::UnixTimstamp_to_Day(when);
      }

      Action(std::string a, std::string e, time_t when, std::string savedOnDay,
             std::string workflow,
             std::string queue) : Action(std::move(a), std::move(e), when,
                                           std::move(workflow), std::move(queue))
      {
        mSavedOnDay = std::move(savedOnDay);
      }

      std::string mAction;
      std::string mEvent;
      time_t mTime; //! unix timestamp
      std::string mWhen; //! string with unix timestamp
      std::string mDay; //! string with yearmonthday
      std::string mSavedOnDay; //! string with yearmonthday
      std::string mWorkflow;
      std::string mQueue;
    };

    Job()
    {
      mFid = 0;
      mRetry = 0;
    }

    Job(eos::common::FileId::fileid_t fid,
        eos::common::Mapping::VirtualIdentity& vid,
        const std::string& errorMessage = "")
    {
      mFid = fid;
      mRetry = 0;
      eos::common::Mapping::Copy(vid, mVid);
      mErrorMesssage = errorMessage;
    }

    ~Job() override = default;

    Job(const Job& other)
    {
      mActions = other.mActions;
      mFid = other.mFid;
      mDescription = other.mDescription;
      mRetry = other.mRetry;
      mErrorMesssage = other.mErrorMesssage;
    }
    // ---------------------------------------------------------------------------
    // Job execution function
    // ---------------------------------------------------------------------------
    void DoIt() override
    {
      std::string errorMsg;
      DoIt(false, errorMsg);
    }

    //! @ininfo original opaque information of the URL that triggered the event
    int  DoIt(bool issync, std::string& errorMsg, const char * const ininfo = nullptr);

    //! @brief Handles "proto" method events
    //! @param errorMsg out parameter giving the text of any error
    //! @ininfo original opaque information of the URL that triggered the event
    //! @return SFS_OK if success
    int handleProtoMethodEvents(std::string& errorMsg, const char * const ininfo = nullptr);

    //! @brief This method is used for communicating proto event requests
    //! @param jobPtr pointer to the job of the event
    //! @param fullPath path of the file which trigered the event
    //! @param request the protobuf request object
    //! @param retry should retry the request
    //! @param errorMsg out parameter giving the text of any error response
    //! @return whether it was successful or not
    static int SendProtoWFRequest(Job* jobPtr, const std::string& fullPath,
                                  const cta::xrd::Request& request, std::string& errorMsg,
                                  bool retry = false);

    // -------------------------------------------------------------------------
    // persistency related methods
    // -------------------------------------------------------------------------
    int Save(std::string queue, time_t& when, int action = 0, int retry = 0);

    int Load(std::string path2entry);

    int Move(std::string from_queue, std::string to_queue, time_t& when,
             int retry = 0);

    int Results(std::string queue, int retc, XrdOucString log, time_t when);

    int Delete(std::string queue, std::string fromDay);

    // -------------------------------------------------------------------------

    void AddAction(const std::string& action,
                   const std::string& event,
                   time_t when,
                   const std::string& workflow,
                   const std::string& queue)
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

    void AddAction(const std::string& action,
                   const std::string& event,
                   time_t when,
                   const std::string& savedOnDay,
                   const std::string& workflow,
                   const std::string& queue)
    {
      AddAction(action, event, when, workflow, queue);
      mActions[mActions.size() - 1].mSavedOnDay = savedOnDay;
    }

    bool IsSync(const std::string& event = "")
    {
      return (event.length() ? event.substr(0, 6) : mActions[0].mEvent.substr(0,
              6)) == "sync::";
    }

    std::vector<Action> mActions;
    eos::common::FileId::fileid_t mFid;
    std::string mDescription;
    eos::common::Mapping::VirtualIdentity mVid;
    std::string mWorkflowPath;
    std::string mErrorMesssage;
    int mRetry;///! number of retries

  private:
    //! @brief moving proto wf event jobs to retry queue
    //! @param filePath the path of the file concerned
    void MoveToRetry(const std::string& filePath);

    //! @brief move to queues based on the results
    //! @param rcode the return code of the job
    void MoveWithResults(int rcode, std::string fromQueue = "r");
  };

  XrdSysCondVar* GetSignal()
  {
    return &mDoneSignal;
  }

  // ---------------------------------------------------------------------------
  //! Decrement the number of active jobs in the workflow engine
  // ---------------------------------------------------------------------------

  void DecActiveJobs()
  {
    mActiveJobs--;
    PublishActiveJobs();
  }

  // ---------------------------------------------------------------------------
  //! Increment the number of active jobs in this converter
  // ---------------------------------------------------------------------------

  void IncActiveJobs()
  {
    mActiveJobs++;
    PublishActiveJobs();
  }

  // ---------------------------------------------------------------------------
  //! Publish the number of active jobs in the workflow engine
  // ---------------------------------------------------------------------------
  void PublishActiveJobs();

  // ---------------------------------------------------------------------------
  //! Return active jobs
  // ---------------------------------------------------------------------------

  inline auto GetActiveJobs() -> decltype(mActiveJobs.load()) const
  {
    return mActiveJobs.load();
  }

  static std::string GetUserName(uid_t uid);

  static std::string GetGroupName(gid_t gid);

  static IContainerMD::XAttrMap CollectAttributes(const std::string& fullPath);

  static void MoveFromRBackToQ();

  /// the scheduler class is providing a destructor-less object,
  /// so we have to create once a singleton of this and keep/share it
  static XrdSysMutex gSchedulerMutex;

  /// singleton object of a scheduler
  static XrdScheduler* gScheduler;

  /// pool executing asynchronous communications in workflow
  static eos::common::ThreadPool gAsyncCommunicationPool;
};

EOSMGMNAMESPACE_END

#endif
