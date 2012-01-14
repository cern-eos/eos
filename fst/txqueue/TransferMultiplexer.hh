// ----------------------------------------------------------------------
// File: TransferMultiplexer.hh
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

#ifndef __EOSFST_TRANSFERMULTIPLEXER__
#define __EOSFST_TRANSFERMULTIPLEXER__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "fst/txqueue/TransferJob.hh"
/* ------------------------------------------------------------------------- */
#include "Xrd/XrdScheduler.hh"
/* ------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <pthread.h>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

class TransferMultiplexer {

private:
  //  std::deque <std::string> queue;
  std::vector<TransferQueue*> mQueues;
  pthread_t thread;

public: 

  TransferMultiplexer();
  ~TransferMultiplexer();

  void Add(TransferQueue* queue) {
    // add all queues and then call Run()
    mQueues.push_back(queue);
  }

  void Run(); // add all queues beforehand!

  static void* StaticThreadProc(void*);
  void* ThreadProc();
};

EOSFSTNAMESPACE_END
#endif

