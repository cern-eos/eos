// ----------------------------------------------------------------------
// File: GlobalConfig.cc
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

#include "common/GlobalConfig.hh"
#include "common/Assert.hh"
#include "common/InstanceName.hh"

EOSCOMMONNAMESPACE_BEGIN

GlobalConfig
GlobalConfig::gConfig; //! Singleton for global configuration access

//------------------------------------------------------------------------------
// Add a configuration queue
//------------------------------------------------------------------------------
bool
GlobalConfig::AddConfigQueue(const char* configqueue,
                             const char* broadcastqueue)
{
  eos_static_info("Adding config queue: %s => %s", configqueue, broadcastqueue);

  std::string lConfigQueue = configqueue;
  std::string lBroadCastQueue = broadcastqueue;
  XrdMqSharedHash* lHash = 0;

  if (mSom) {
    mSom->HashMutex.LockRead();

    if (!(lHash = mSom->GetObject(lConfigQueue.c_str(), "hash"))) {
      mSom->HashMutex.UnLockRead();

      // Create the hash object
      if (mSom->CreateSharedHash(lConfigQueue.c_str(), lBroadCastQueue.c_str(),
                                 mSom)) {
        mSom->HashMutex.LockRead();
        lHash = mSom->GetObject(lConfigQueue.c_str(), "hash");
        mBroadCastQueueMap[lConfigQueue] = lBroadCastQueue;
        mSom->HashMutex.UnLockRead();
      } else {
        lHash = 0;
      }
    } else {
      mSom->HashMutex.UnLockRead();
    }
  }

  return (lHash) ? true : false;
}

//------------------------------------------------------------------------------
// Print the broad cast mapping to the given string
//------------------------------------------------------------------------------
void
GlobalConfig::PrintBroadCastMap(std::string& out)
{
  std::map<std::string, std::string>::const_iterator it;

  for (it = mBroadCastQueueMap.begin(); it != mBroadCastQueueMap.end(); it++) {
    char line[1024];
    snprintf(line, sizeof(line) - 1, "# config [%-32s] == broad cast ==> [%s]\n",
             it->first.c_str(), it->second.c_str());
    out += line;
  }
}

//------------------------------------------------------------------------------
// Get a pointer to the hash storing a configuration queue
//------------------------------------------------------------------------------
XrdMqSharedHash*
GlobalConfig::Get(const char* configqueue)
{
  std::string lConfigQueue = configqueue;
  return mSom->GetObject(lConfigQueue.c_str(), "hash");
}

//------------------------------------------------------------------------------
// Get the global MGM configuration queue
//------------------------------------------------------------------------------
std::string
GlobalConfig::GetGlobalMgmConfigQueue() const {
  return SSTR("/config/" << InstanceName::get() << "/mgm");
}

EOSCOMMONNAMESPACE_END
