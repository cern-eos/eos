// ----------------------------------------------------------------------
// File: DrainFS.hh
// Author: Andreas-Joachim Peters - CERN
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSMGM_DRAINFS_HH__
#define __EOSMGM_DRAINFS_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "Xrd/XrdScheduler.hh"
#include "mgm/FsView.hh"
#include <memory>
#include "mgm/drain/DrainTransferJob.hh"
#include <exception>

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/**
 * @file DrainFS.hh
 *
 * @brief Class implementing a thread following a filesystem drain.
 *
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing the draining of a filesystem
 */
/*----------------------------------------------------------------------------*/
class DrainFS: public eos::common::LogId
{

public:

  inline eos::common::FileSystem::eDrainStatus GetDrainStatus()
  {
    return drainStatus;
  };

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param ifsid filesystem id
   */
  // ---------------------------------------------------------------------------

  DrainFS(eos::common::FileSystem::fsid_t ifsid)
  {
    //create the scheduler for Drain Job; the max number of workers should be parametrized
    gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 10, 5);
    gScheduler->Start();
    mThread = 0;
    mFsId = ifsid;

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    } else {
      throw std::exception();
    }

    XrdSysThread::Run(&mThread,
                      DrainFS::StaticThreadProc,
                      static_cast<void*>(this),
                      XRDSYSTHREAD_HOLD,
                      "DrainFS Thread");
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~DrainFS();

  // ---------------------------------------------------------------------------
  // set initial drain counters and status
  // ---------------------------------------------------------------------------
  void SetInitialCounters();

  // ---------------------------------------------------------------------------
  // static thread startup function
  // ---------------------------------------------------------------------------
  static void* StaticThreadProc(void*);

  // ---------------------------------------------------------------------------
  // thread loop implementing the drain job
  // ---------------------------------------------------------------------------
  void* Drain();

  // ---------------------------------------------------------------------------
  // Stop the Drain activities
  // --------------------------------------------------------------------------
  void DrainStop();

  // ---------------------------------------------------------------------------
  // get the space defined drain variables
  // ---------------------------------------------------------------------------
  void GetSpaceConfiguration();

  // ---------------------------------------------------------------------------
  // select a FS as a draining target using GeoTreeEngine
  // ---------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t SelectTargetFS(DrainTransferJob& job);

  // ---------------------------------------------------------------------------
  // get the list of  Jobs filter by status
  // ---------------------------------------------------------------------------
  std::vector<shared_ptr<DrainTransferJob>> GetJobs(DrainTransferJob::Status
                                         status);
  
  // ---------------------------------------------------------------------------
  // get thef jobs count for the given status
  // ---------------------------------------------------------------------------

  int CountJobs(DrainTransferJob::Status status);

  void CompleteDrain() ;

private:
  /// file system id of the draining filesystem
  eos::common::FileSystem::fsid_t mFsId;

  eos::common::FileSystem* fs;

  pthread_t mThread;

  // space where the filesystem resides
  std::string mSpace;

  //group where the filesystem resides
  std::string mGroup;

  XrdScheduler*   gScheduler;

  //list of DrainTransferJob to run
  std::vector<shared_ptr<DrainTransferJob>> drainJobs;

  XrdSysMutex drainJobsMutex;

  eos::common::FileSystem::eDrainStatus drainStatus;

  bool drainStop = false;

  //the max numbers of retries

  int maxretries = 0;

};

EOSMGMNAMESPACE_END

#endif
