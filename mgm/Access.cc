/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/Access.hh"
#include "mgm/FsView.hh"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

std::set<uid_t> Access::gBannedUsers;
std::set<gid_t> Access::gBannedGroups;
std::set<std::string> Access::gBannedHosts;

std::map<std::string, std::string> Access::gRedirectionRules;
std::map<std::string, std::string> Access::gStallRules;

std::map<uid_t, std::string> Access::gUserRedirection;
std::map<gid_t, std::string> Access::gGroupRedirection;

eos::common::RWMutex Access::gAccessMutex; // protects all static set's and maps in Access

const char* Access::gUserKey  = "BanUsers";
const char* Access::gGroupKey = "BanGroups";
const char* Access::gHostKey  = "BanHosts";


void
Access::Reset() {
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  Access::gBannedUsers.clear();
  Access::gBannedGroups.clear();
  Access::gBannedHosts.clear();
  Access::gRedirectionRules.clear();
  Access::gStallRules.clear();
  Access::gUserRedirection.clear();
  Access::gGroupRedirection.clear();
}

void
Access::ApplyAccessConfig()
{
  Access::Reset();
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  std::string userval  = FsView::gFsView.GetGlobalConfig(gUserKey);
  std::string groupval = FsView::gFsView.GetGlobalConfig(gGroupKey);
  std::string hostval  = FsView::gFsView.GetGlobalConfig(gHostKey);

  // parse the list's and fill the hash
  std::vector<std::string> tokens;
  std::string delimiter =":";
  
  tokens.clear();
  eos::common::StringConversion::Tokenize(userval,tokens, delimiter);

  for (size_t i=0; i< tokens.size(); i++) {
    if (tokens[i].length()) {
      uid_t uid = atoi(tokens[i].c_str());
      Access::gBannedUsers.insert(uid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(groupval,tokens, delimiter);
  for (size_t i=0; i< tokens.size(); i++) {
    if (tokens[i].length()) {
      gid_t gid = atoi(tokens[i].c_str());
      Access::gBannedGroups.insert(gid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(hostval,tokens, delimiter);
  for (size_t i=0; i< tokens.size(); i++) {
    if (tokens[i].length()) {
      Access::gBannedHosts.insert(tokens[i]);
    }
  }
}


bool
Access::StoreAccessConfig() 
{
  std::set<uid_t>::const_iterator ituid;
  std::set<gid_t>::const_iterator itgid;
  std::set<std::string>::const_iterator ithost;

  std::string userval="";
  std::string groupval="";
  std::string hostval="";
  for (ituid = Access::gBannedUsers.begin(); ituid != Access::gBannedUsers.end(); ituid++) {
    userval  += eos::common::Mapping::UidAsString(*ituid);
    userval  += ":";
  }
  for (itgid = Access::gBannedGroups.begin(); itgid != Access::gBannedGroups.end(); itgid++) {
    groupval += eos::common::Mapping::GidAsString(*itgid);
    groupval += ":";
  }
  for (ithost = Access::gBannedHosts.begin(); ithost != Access::gBannedHosts.end(); ithost++) {
    hostval  += ithost->c_str();
    hostval  += ":";
  }

  std::string ukey = gUserKey;
  std::string gkey = gGroupKey;
  std::string hkey = gHostKey;

  bool ok=1;
  ok &= FsView::gFsView.SetGlobalConfig(ukey,  userval);
  ok &= FsView::gFsView.SetGlobalConfig(gkey, groupval);
  ok &= FsView::gFsView.SetGlobalConfig(hkey,  hostval);
  if (!ok) {
    eos_static_err("unable to store <access> configuration");
    return false;
  }
  return true;
}

EOSMGMNAMESPACE_END








