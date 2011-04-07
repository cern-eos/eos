#ifndef __EOSCOMMON_ACCESS__
#define __EOSCOMMON_ACCESS__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
#include <set>
#include <map>
#include <vector>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Access {
private:
public:

  static std::set<uid_t> gBannedUsers;
  static std::set<gid_t> gBannedGroups;
  static std::set<std::string> gBannedHosts;
  
  static std::map<std::string, std::string> gRedirectionRules;
  static std::map<std::string, std::string> gStallRules;

  static std::map<uid_t, std::string> gUserRedirection;
  static std::map<gid_t, std::string> gGroupRedirection;

  static RWMutex gAccessMutex; // protects all static set's and maps in Access
};

EOSCOMMONNAMESPACE_END

#endif
