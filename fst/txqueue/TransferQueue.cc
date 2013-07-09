// ----------------------------------------------------------------------
// File: TransferQueue.cc
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

/* ------------------------------------------------------------------------- */
#include "fst/txqueue/TransferQueue.hh"
#include "fst/txqueue/TransferJob.hh"
#include "common/Logging.hh"
/* ------------------------------------------------------------------------- */
#include <cstdio>

/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
TransferQueue::TransferQueue (eos::common::TransferQueue** queue, const char* name, int slots, int band)
{
  mQueue = queue;
  mName = name;
  mJobsRunning = 0;
  mJobsDone = 0;
  nslots = slots;
  bandwidth = band;
  mJobEndCallback = 0;
}

/* ------------------------------------------------------------------------- */
TransferQueue::~TransferQueue () { }

/* ------------------------------------------------------------------------- */
size_t
TransferQueue::GetBandwidth ()
{
  size_t bw = 0;
  {
    XrdSysMutexHelper(mBandwidthMutex);
    bw = bandwidth;
  }
  return bw;
}

/* ------------------------------------------------------------------------- */
void
TransferQueue::SetBandwidth (size_t band)
{
  XrdSysMutexHelper(mBandwidthMutex);
  bandwidth = band;
}

/* ------------------------------------------------------------------------- */
size_t
TransferQueue::GetSlots ()
{
  size_t n = 0;
  {
    XrdSysMutexHelper(mSlotsMutex);
    n = nslots;
  }
  return n;
}

/* ------------------------------------------------------------------------- */
void
TransferQueue::SetSlots (size_t slots)
{
  XrdSysMutexHelper(mSlotsMutex);
  nslots = slots;
}
EOSFSTNAMESPACE_END
