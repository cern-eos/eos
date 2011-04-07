/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Access.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

std::set<uid_t> Access::gBannedUsers;
std::set<gid_t> Access::gBannedGroups;
std::set<std::string> Access::gBannedHosts;

std::map<std::string, std::string> Access::gRedirectionRules;
std::map<std::string, std::string> Access::gStallRules;

std::map<uid_t, std::string> Access::gUserRedirection;
std::map<gid_t, std::string> Access::gGroupRedirection;

RWMutex Access::gAccessMutex; // protects all static set's and maps in Access

EOSCOMMONNAMESPACE_END








