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


EOSMGMNAMESPACE_BEGIN

class DrainJob {
  // ---------------------------------------------------------------------------
  //! This class implements the drain procedure of a filesystem
  // ---------------------------------------------------------------------------
private:
  eos::common::FileSystem::fsid_t fsid;
  std::string space;
  std::string group;
  bool onOpsError;
  pthread_t thread;

public:

  DrainJob(eos::common::FileSystem::fsid_t ifsid, bool opserror=false) {
    thread=0;
    fsid = ifsid;
    onOpsError=opserror;
    XrdSysThread::Run(&thread, DrainJob::StaticThreadProc, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "DrainJob Thread");
  }

  void ResetCounter();

  static void* StaticThreadProc(void*);
  void* Drain();     // the function scheduling from the drain map into shared queues

  void SetDrainer();   // enabled the drain pull for all filesytems in the group
  void SetSpaceNode(); // set's space defined variables on each node involved in draining e.g. transfer rate & nparallel transfers
  virtual ~DrainJob();
};

EOSMGMNAMESPACE_END

#endif
