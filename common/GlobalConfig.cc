/*----------------------------------------------------------------------------*/
#include "common/GlobalConfig.hh"
/*----------------------------------------------------------------------------*/



/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/

GlobalConfig GlobalConfig::gConfig; // singleton for global configuration access

/*----------------------------------------------------------------------------*/
GlobalConfig::GlobalConfig()
{
  mSom = 0;
}

/*----------------------------------------------------------------------------*/
void GlobalConfig::SetSOM(XrdMqSharedObjectManager* som) 
{
  mSom = som;
}

/*----------------------------------------------------------------------------*/
bool 
GlobalConfig::AddConfigQueue(const char* configqueue, const char* broadcastqueue)
{
  //----------------------------------------------------------------
  //! adds a global configuration hash and it's broad cast queue ... dont' MUTEX!
  //----------------------------------------------------------------
  
  std::string lConfigQueue    = configqueue;
  std::string lBroadCastQueue = broadcastqueue;
  XrdMqSharedHash* lHash=0;
  
  if (mSom) {
    mSom->HashMutex.LockRead();
    if (! (lHash = mSom->GetObject(lConfigQueue.c_str(),"hash")) ) {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedHash(lConfigQueue.c_str(), lConfigQueue.c_str(),mSom)) {
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
void
GlobalConfig::PrintBroadCastMap(std::string &out)
{
  //----------------------------------------------------------------
  //! prints the added global configuration queues and where they are broadcasted
  //----------------------------------------------------------------

  std::map<std::string, std::string>::const_iterator it;

  for (it = mBroadCastQueueMap.begin(); it != mBroadCastQueueMap.end(); it++) {
    char line[1024];
    snprintf(line, sizeof(line)-1,"# config [%-32s] == broad cast ==> [%s]\n", it->first.c_str(), it->second.c_str());
    out += line;
  }
}

/*----------------------------------------------------------------------------*/
XrdMqSharedHash*
GlobalConfig::Get(const char* configqueue) 
{
  //----------------------------------------------------------------
  //! returns a global hash for the required configqueue ... read and/orMUTEX as long as you use the hash
  //----------------------------------------------------------------

  std::string lConfigQueue = configqueue;

  return mSom->GetObject(lConfigQueue.c_str(),"hash");
}

std::string 
GlobalConfig::QueuePrefixName(const char* prefix, const char*queuename)
{
  //----------------------------------------------------------------
  //! joins the prefix with the hostport name extracted from the queue e.g /eos/eostest/space + /eos/host1:port1/fst = /eos/eostest/space/host1:port1
  //----------------------------------------------------------------
  std::string out=prefix;
  out += eos::common::StringConversion::GetHostPortFromQueue(queuename).c_str();
  return out;
}


/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
/*----------------------------------------------------------------------------*/
