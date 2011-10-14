#ifndef __EOSCOMMON_ACCESS__
#define __EOSCOMMON_ACCESS__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
#include <map>
#include <vector>
#include <string>
#include <set>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Access {
private:
public:

  static const char* gUserKey;
  static const char* gGroupKey;
  static const char* gHostKey;
  static const char* gAllowedUserKey;
  static const char* gAllowedGroupKey;
  static const char* gAllowedHostKey;
  static std::set<uid_t> gBannedUsers;
  static std::set<gid_t> gBannedGroups;
  static std::set<uid_t> gAllowedUsers;
  static std::set<gid_t> gAllowedGroups;
  static std::set<std::string> gBannedHosts;
  static std::set<std::string> gAllowedHosts;

  static std::map<std::string, std::string> gRedirectionRules;
  static std::map<std::string, std::string> gStallRules;

  static std::map<uid_t, std::string> gUserRedirection;
  static std::map<gid_t, std::string> gGroupRedirection;

  static eos::common::RWMutex gAccessMutex; // protects all static set's and maps in Access

  static bool SetConfig(std::string key, std::string value);
  static std::string GetConfig(std::string key);


  static void Reset();
  static void ApplyAccessConfig(); // retrieves the config from the global shared hash/config engine and fills the static hashes
  static bool StoreAccessConfig(); // pushed the static hashes into the global shared hash/config engine
};

EOSMGMNAMESPACE_END

#endif
