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

#include "fst/Namespace.hh"
#include "common/RWMutex.hh"
#include "fst/txqueue/TransferJob.hh"
#include <vector>
#include <pthread.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class TransferMultiplexer
//------------------------------------------------------------------------------
class TransferMultiplexer
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  TransferMultiplexer();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~TransferMultiplexer();

  //----------------------------------------------------------------------------
  //! Add queue to multiplexer
  //!
  //! @param queue new queue to be added
  //----------------------------------------------------------------------------
  void Add(TransferQueue* queue);

  //----------------------------------------------------------------------------
  //! Set number of slots for each of the attached queues
  //!
  //! @param slots number of slots
  //----------------------------------------------------------------------------
  void SetSlots(size_t slots);

  //----------------------------------------------------------------------------
  //! Set bandwidth limitation for each of the attached queues
  //!
  //! @param band bandwidth limit
  //----------------------------------------------------------------------------
  void SetBandwidth(size_t band);

  //----------------------------------------------------------------------------
  //! Start the multiplexer thread. All the queues need to be attached
  //! beforehand.
  //----------------------------------------------------------------------------
  void Run();

  //----------------------------------------------------------------------------
  //! Stop multiplexer thread.
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Static helper function to start the thread.
  //----------------------------------------------------------------------------
  static void* StaticThreadProc(void*);

  //----------------------------------------------------------------------------
  //! Multiplexer thread loop.
  //----------------------------------------------------------------------------
  void* ThreadProc();

private:
  eos::common::RWMutex mMutex;
  std::vector<TransferQueue*> mQueues;
  pthread_t mTid;
};

EOSFSTNAMESPACE_END
#endif
