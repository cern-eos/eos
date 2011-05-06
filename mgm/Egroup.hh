#ifndef __EOSMGM_EGROUP__HH__
#define __EOSMGM_EGROUP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSEGROUPCACHETIME 10

class Egroup {
public:
  static XrdSysMutex Mutex;
  static std::map < std::string, std::map <std::string, bool > > Map;

  static std::map < std::string, std::map <std::string, time_t > > LifeTime;

  Egroup(){};
  ~Egroup(){};
  
  static bool Member(std::string &username, std::string &egroupname);
  
};

EOSMGMNAMESPACE_END

#endif
