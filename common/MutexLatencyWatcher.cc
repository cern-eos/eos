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
    point.start = std::chrono::steady_clock::now();
    mMutex->LockWrite();
    mMutex->UnLockWrite();
    point.end = std::chrono::steady_clock::now();

    if(point.getMilli() > std::chrono::milliseconds(200)) {
      eos_static_warning("acquisition of mutex %s took %d milliseconds", mFriendlyName.c_str(), point.getMilli().count());
    }

    assistant.wait_for(std::chrono::seconds(2));
  }
}

EOSCOMMONNAMESPACE_END
