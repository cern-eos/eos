//------------------------------------------------------------------------------
// File: MutexLatencyWatcher.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#pragma once
#include "common/RWMutex.hh"
#include "common/AssistedThread.hh"
#include <list>
#include <chrono>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Thread to watch how long it takes to acquire a mutex -- used to debug
//! latency spikes.
//------------------------------------------------------------------------------
class MutexLatencyWatcher {
public:
  //----------------------------------------------------------------------------
  //! Datapoint
  //----------------------------------------------------------------------------
  struct Datapoint {
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;

    std::chrono::milliseconds getMilli() const {
      return std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    }
  };

  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  MutexLatencyWatcher();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MutexLatencyWatcher(RWMutex& mutex, const std::string &friendlyName);
  void activate(RWMutex& mutex, const std::string &friendlyName);

  //----------------------------------------------------------------------------
  //! Main thread loop
  //----------------------------------------------------------------------------
  void main(ThreadAssistant &assistant);

private:
  eos::common::RWMutex *mMutex;
  std::string mFriendlyName;
  AssistedThread mThread;

  std::mutex mDataMutex;
  std::list<Datapoint> mData;
};

EOSCOMMONNAMESPACE_END
