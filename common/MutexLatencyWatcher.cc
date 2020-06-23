//------------------------------------------------------------------------------
// File: MutexLatencyWatcher.cc
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

#include "common/MutexLatencyWatcher.hh"
#include "common/Logging.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
MutexLatencyWatcher::MutexLatencyWatcher() {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MutexLatencyWatcher::MutexLatencyWatcher(RWMutex& mutex, const std::string &friendlyName) {
  activate(mutex, friendlyName);
}

//------------------------------------------------------------------------------
// Custom "constructor"
//------------------------------------------------------------------------------
void MutexLatencyWatcher::activate(RWMutex& mutex, const std::string &friendlyName) {
  mMutex = &mutex;
  mFriendlyName = friendlyName;
  mThread.reset(&MutexLatencyWatcher::main, this);
}

//------------------------------------------------------------------------------
// Main thread loop
//------------------------------------------------------------------------------
void MutexLatencyWatcher::main(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    Datapoint point;
    point.start = std::chrono::system_clock::now();
    mMutex->LockWrite();
    mMutex->UnLockWrite();
    point.end = std::chrono::system_clock::now();

    if(point.getMilli() > std::chrono::milliseconds(200)) {
      eos_static_warning("acquisition of mutex %s took %d milliseconds", mFriendlyName.c_str(), point.getMilli().count());
    }

    appendDatapoint(point);
    assistant.wait_for(std::chrono::seconds(2));
  }
}

//------------------------------------------------------------------------------
// Append datapoint
//------------------------------------------------------------------------------
void MutexLatencyWatcher::appendDatapoint(const Datapoint &point) {
  std::lock_guard<std::mutex> lock(mDataMutex);
  mData.push_back(point);

  if(mData.size() > 200) {
    mData.pop_front();
  }
}

//------------------------------------------------------------------------------
// Get latency spikes
//------------------------------------------------------------------------------
MutexLatencyWatcher::LatencySpikes MutexLatencyWatcher::getLatencySpikes() const {
  LatencySpikes spikes;
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();

  std::lock_guard<std::mutex> lock(mDataMutex);

  for(auto it = mData.begin(); it != mData.end(); it++) {
    if(now - it->end <=  std::chrono::minutes(1)) {
      spikes.lastMinute = std::max(spikes.lastMinute, it->getMilli());
    }

    if(now - it->end <= std::chrono::minutes(2)) {
      spikes.last2Minutes = std::max(spikes.last2Minutes, it->getMilli());
    }

    if(now - it->end <= std::chrono::minutes(5)) {
      spikes.last5Minutes = std::max(spikes.last5Minutes, it->getMilli());
    }
  }

  if(!mData.empty()) {
    spikes.last = mData.back().getMilli();
  }

  return spikes;
}

//------------------------------------------------------------------------------
//  Append datapoint
//------------------------------------------------------------------------------

EOSCOMMONNAMESPACE_END
