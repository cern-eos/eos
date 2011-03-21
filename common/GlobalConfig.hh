#ifndef __EOSCOMMON_GLOBALCONFIG_HH__
#define __EOSCOMMON_GLOBALCONFIG_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

class GlobalConfig {
private:
  XrdMqSharedObjectManager* mSom;
  std::map<std::string, std::string> mBroadCastQueueMap; // stores which config queue get's broadcasted where ...

public:
  bool AddConfigQueue(const char* configqueue, const char* broadcastqueue);

  XrdMqSharedHash* Get(const char* configqueue); 

  void PrintBroadCastMap(std::string &out);
  
  GlobalConfig();
  ~GlobalConfig(){};

  static std::string QueuePrefixName(const char* prefix, const char* queuename);

  void SetSOM(XrdMqSharedObjectManager* som);
  static GlobalConfig gConfig; // singleton for convenience
};

EOSCOMMONNAMESPACE_END

#endif
