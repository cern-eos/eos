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
class DrainFS: public eos::common::LogId  {
private:
  /// file system id of the draining filesystem
  eos::common::FileSystem::fsid_t mFsId;

  /// space where the filesystem resides
  std::string mSpace;

  /// group where the filesystem resides
  std::string mGroup;

  /// thread id of the draing job
  pthread_t mThread;

  XrdScheduler* gScheduler; 

public:

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param ifsid filesystem id
   */
  // ---------------------------------------------------------------------------

  DrainFS (eos::common::FileSystem::fsid_t ifsid)
  {

    //create the scheduler for Drain Job;
    gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
    gScheduler->Start();

    mThread = 0;
    mFsId = ifsid;
    XrdSysThread::Run (&mThread,
                       DrainFS::StaticThreadProc,
                       static_cast<void *> (this),
                       XRDSYSTHREAD_HOLD,
                       "DrainFS Thread");
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~DrainFS ();

  // ---------------------------------------------------------------------------
  // reset all drain counter
  // ---------------------------------------------------------------------------
  void ResetCounter ();

  // ---------------------------------------------------------------------------
  // static thread startup function
  // ---------------------------------------------------------------------------
  static void* StaticThreadProc (void*);

  // ---------------------------------------------------------------------------
  // thread loop implementing the drain job
  // ---------------------------------------------------------------------------
  void* Drain ();

  // ---------------------------------------------------------------------------
  // set the space defined drain variables on each node participating in the 
  // draining e.g. transfer rate & parallel transfers
  // ---------------------------------------------------------------------------
  void SetSpaceNode ();
};

EOSMGMNAMESPACE_END

#endif
