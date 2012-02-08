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

/*----------------------------------------------------------------------------*/
#include "common/GlobalConfig.hh"
/*----------------------------------------------------------------------------*/



/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/

GlobalConfig GlobalConfig::gConfig; //! Singleton for global configuration access

/*----------------------------------------------------------------------------*/
/** 
 * Constructor
 * 
 */
/*----------------------------------------------------------------------------*/

GlobalConfig::GlobalConfig()
{
  mSom = 0;
}

/*----------------------------------------------------------------------------*/
/** 
 * Store the object manager in the global config
 * 
 * @param som pointer to a shared object manager
 */
/*----------------------------------------------------------------------------*/

void GlobalConfig::SetSOM(XrdMqSharedObjectManager* som) 
{
  mSom = som;
}

/*----------------------------------------------------------------------------*/
/** 
 * Add a configuration queue
 * 
 * @param configqueue name of the configuration queue e.g. /eos/<host:port>/mgm
 * @param broadcastqueue name of the queue where to broadcast  e.g. /eos/'*'/mgm
 * 
 * @return true if success false if failed
 */
/*----------------------------------------------------------------------------*/

bool 
GlobalConfig::AddConfigQueue(const char* configqueue, const char* broadcastqueue)
{
  std::string lConfigQueue    = configqueue;
  std::string lBroadCastQueue = broadcastqueue;
  XrdMqSharedHash* lHash=0;
  
  if (mSom) {
    mSom->HashMutex.LockRead();
    if (! (lHash = mSom->GetObject(lConfigQueue.c_str(),"hash")) ) {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedHash(lConfigQueue.c_str(), lBroadCastQueue.c_str(),mSom)) {
        mSom->HashMutex.LockRead();
        lHash = mSom->GetObject(lConfigQueue.c_str(),"hash");
        mBroadCastQueueMap[lConfigQueue] = lBroadCastQueue;
        mSom->HashMutex.UnLockRead();
      } else {
        lHash = 0;
      }
    } else {
      mSom->HashMutex.UnLockRead();
    }
  } else {
    lHash = 0;
  }

  return (lHash)?true:false;
}

/*----------------------------------------------------------------------------*/
/** 
 * Print the broad cast mapping to the given string
 * 
 * @param out reference to a string where to print 
 */
/*----------------------------------------------------------------------------*/

void
GlobalConfig::PrintBroadCastMap(std::string &out)
{
  std::map<std::string, std::string>::const_iterator it;

  for (it = mBroadCastQueueMap.begin(); it != mBroadCastQueueMap.end(); it++) {
    char line[1024];
    snprintf(line, sizeof(line)-1,"# config [%-32s] == broad cast ==> [%s]\n", it->first.c_str(), it->second.c_str());
    out += line;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Get a pointer to the hash storing a configuration queue
 * 
 * @param configqueue name of the configuration queue
 * 
 * @return pointer to a shared hash representing a configuration queue
 */
/*----------------------------------------------------------------------------*/

XrdMqSharedHash*
GlobalConfig::Get(const char* configqueue) 
{
  std::string lConfigQueue = configqueue;

  return mSom->GetObject(lConfigQueue.c_str(),"hash");
}


/*----------------------------------------------------------------------------*/
/** 
 * Join the prefix with the hostport name extracted from the queue name.
 * E.g. /eos/eostest/space + /eos/host1:port1/fst = /eos/eostest/space/host1:port1
 * 
 * @param prefix 
 * @param queuename 
 * 
 * @return 
 */
/*----------------------------------------------------------------------------*/

std::string 
GlobalConfig::QueuePrefixName(const char* prefix, const char*queuename)
{
  std::string out=prefix;
  out += eos::common::StringConversion::GetHostPortFromQueue(queuename).c_str();
  return out;
}

/*----------------------------------------------------------------------------*/
/** 
 * Reset the configuration object e.g. all attached shared objects
 * 
 */
/*----------------------------------------------------------------------------*/

void
GlobalConfig::Reset()
{
  if (mSom) 
    mSom->Clear();
}
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
/*----------------------------------------------------------------------------*/
