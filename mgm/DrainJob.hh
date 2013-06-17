// ----------------------------------------------------------------------
// File: DrainJob.hh
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

#ifndef __EOSMGM_DRAINJOB_HH__
#define __EOSMGM_DRAINJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "semaphore.h"
#include <queue>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file Drainjob.hh
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
class DrainJob {
private:
  /// file system id of the draining filesystem
  eos::common::FileSystem::fsid_t mFsId;
  
  /// space where the filesystem resides
  std::string mSpace;
  
  /// group where the filesystem resides
  std::string mGroup;
  
  /// indicator if draining is initiated by a filesystem OpsError (e.g.IO error)
  bool mOnOpsError;
  
  /// thread id of the draing job
  pthread_t mThread;

public:

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param ifsid filesystem id
   * @param opserror indicator if draining triggered by ops error
   */
  // ---------------------------------------------------------------------------
  DrainJob(eos::common::FileSystem::fsid_t ifsid, bool opserror=false) {
    mThread=0;
    mFsId = ifsid;
    mOnOpsError=opserror;
    XrdSysThread::Run(&mThread, 
                      DrainJob::StaticThreadProc, 
                      static_cast<void *>(this),
                      XRDSYSTHREAD_HOLD, 
                      "DrainJob Thread");
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~DrainJob();
    
  // ---------------------------------------------------------------------------
  // reset all drain counter
  // ---------------------------------------------------------------------------
  void ResetCounter();

  // ---------------------------------------------------------------------------
  // static thread startup function
  // ---------------------------------------------------------------------------
  static void* StaticThreadProc(void*);
  
  // ---------------------------------------------------------------------------
  // thread loop implementing the drain job
  // ---------------------------------------------------------------------------
  void* Drain();

  // ---------------------------------------------------------------------------
  // function notifying all nodes in a drain group to start to pull off files
  // ---------------------------------------------------------------------------
  void SetDrainer();   // enabled the drain pull for all filesytems in the group
  
  // ---------------------------------------------------------------------------
  // set the space defined drain variables on each node participating in the 
  // draining e.g. transfer rate & parallel transfers
  // ---------------------------------------------------------------------------
  void SetSpaceNode(); 
};

EOSMGMNAMESPACE_END

#endif
