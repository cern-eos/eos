// ----------------------------------------------------------------------
// File: Access.cc
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

#include "mgm/Namespace.hh"
#include "mgm/Access.hh"
#include "mgm/FsView.hh"
#include "common/StringConversion.hh"

EOSMGMNAMESPACE_BEGIN

//! singleton set for banned user IDs
std::set<uid_t> Access::gBannedUsers;

//! singleton set for banned group IDS
std::set<gid_t> Access::gBannedGroups;

//! singleton set for banned host names
std::set<std::string> Access::gBannedHosts;

//! singleton set for banned host names
std::set<std::string> Access::gBannedDomains;

//! singleton set for allowed user IDs
std::set<uid_t> Access::gAllowedUsers;

//! singleton set for allowed group IDs
std::set<gid_t> Access::gAllowedGroups;

//! singleton set for allowed host names
std::set<std::string> Access::gAllowedHosts;

//! singleton set for allowed host domains
std::set<std::string> Access::gAllowedDomains;

//! singleton map for redirection rules
std::map<std::string, std::string> Access::gRedirectionRules;

//! singleton map for stall rules
std::map<std::string, std::string> Access::gStallRules;

//! singleton map for stall comments
std::map<std::string, std::string> Access::gStallComment;

//! indicates global stall rule
std::atomic<bool> Access::gStallGlobal {false};

//! indicates global read stall
std::atomic<bool> Access::gStallRead {false};

//! indicates global write stall
std::atomic<bool> Access::gStallWrite {false};

//! indicates a user or group rate stall entry
std::atomic<bool> Access::gStallUserGroup {false};

//! singleton map for UID based redirection (not used yet)
std::map<uid_t, std::string> Access::gUserRedirection;

//! singleton map for GID based redirection (not used yet)
std::map<gid_t, std::string> Access::gGroupRedirection;

//! global rw mutex protecting all static singletons
eos::common::RWMutex Access::gAccessMutex;

/*----------------------------------------------------------------------------*/
//! constant used in the configuration store
const char* Access::gUserKey = "BanUsers";

//! constant used in the configuration store
const char* Access::gGroupKey = "BanGroups";

//! constant used in the configuration store
const char* Access::gHostKey = "BanHosts";

//! constant used in the configuration store
const char* Access::gDomainKey = "BanDomains";

//! constant used in the configuration store
const char* Access::gAllowedUserKey = "AllowedUsers";

//! constant used in the configuration store
const char* Access::gAllowedGroupKey = "AllowedGroups";

//! constant used in the configuration store
const char* Access::gAllowedHostKey = "AllowedHosts";

//! constant used in the configuration store
const char* Access::gAllowedDomainKey = "AllowedDomains";

//! constant used in the configuration store
const char* Access::gStallKey = "Stall";

//! constant used in the configuration store
const char* Access::gRedirectionKey = "Redirection";

//------------------------------------------------------------------------------
// Static function to reset all singleton objects defining access rules.
//------------------------------------------------------------------------------
void
Access::Reset(bool skip_stall_redirect)
{
  eos_static_debug("%s", "msg=\"reset all access rules\"");
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  Access::gBannedUsers.clear();
  Access::gBannedGroups.clear();
  Access::gBannedHosts.clear();
  Access::gBannedDomains.clear();
  Access::gAllowedUsers.clear();
  Access::gAllowedGroups.clear();
  Access::gAllowedHosts.clear();
  Access::gAllowedDomains.clear();

  if (skip_stall_redirect == false) {
    Access::gRedirectionRules.clear();
    Access::gStallRules.clear();
    Access::gStallComment.clear();
    Access::gUserRedirection.clear();
    Access::gGroupRedirection.clear();
    Access::gStallGlobal = Access::gStallRead =
                             Access::gStallWrite = Access::gStallUserGroup = false;
  }
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Static function to retrieve the access configuration from the global
 * configuration and apply to the static singleton rules.
 */
/*----------------------------------------------------------------------------*/
void
Access::ApplyAccessConfig(bool applyredirectandstall)
{
  Access::Reset(!applyredirectandstall);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  std::string userval = FsView::gFsView.GetGlobalConfig(gUserKey);
  std::string groupval = FsView::gFsView.GetGlobalConfig(gGroupKey);
  std::string hostval = FsView::gFsView.GetGlobalConfig(gHostKey);
  std::string domainval = FsView::gFsView.GetGlobalConfig(gDomainKey);
  std::string useraval = FsView::gFsView.GetGlobalConfig(gAllowedUserKey);
  std::string groupaval = FsView::gFsView.GetGlobalConfig(gAllowedGroupKey);
  std::string hostaval = FsView::gFsView.GetGlobalConfig(gAllowedHostKey);
  std::string domainaval = FsView::gFsView.GetGlobalConfig(gAllowedDomainKey);
  std::string stall = FsView::gFsView.GetGlobalConfig(gStallKey);
  std::string redirect = FsView::gFsView.GetGlobalConfig(gRedirectionKey);
  // parse the list's and fill the hash
  std::vector<std::string> tokens;
  std::string delimiter = ":";
  std::vector<std::string> subtokens;
  std::string subdelimiter = "~";
  tokens.clear();
  eos::common::StringConversion::Tokenize(userval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      uid_t uid = atoi(tokens[i].c_str());
      Access::gBannedUsers.insert(uid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(groupval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      gid_t gid = atoi(tokens[i].c_str());
      Access::gBannedGroups.insert(gid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(hostval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      Access::gBannedHosts.insert(tokens[i]);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(domainval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      Access::gBannedDomains.insert(tokens[i]);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(useraval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      uid_t uid = atoi(tokens[i].c_str());
      Access::gAllowedUsers.insert(uid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(groupaval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      gid_t gid = atoi(tokens[i].c_str());
      Access::gAllowedGroups.insert(gid);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(hostaval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      Access::gAllowedHosts.insert(tokens[i]);
    }
  }

  tokens.clear();
  eos::common::StringConversion::Tokenize(domainaval, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); i++) {
    if (tokens[i].length()) {
      Access::gAllowedDomains.insert(tokens[i]);
    }
  }

  tokens.clear();
  delimiter = ",";
  eos::common::StringConversion::Tokenize(stall, tokens, delimiter);

  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].length()) {
      if (applyredirectandstall || ( (tokens[i].find("rate:") == 0) || (tokens[i].find("threads:") == 0) )) {
        subtokens.clear();
        eos::common::StringConversion::Tokenize(tokens[i], subtokens,
                                                subdelimiter);
        if (subtokens.size() >= 2) {
          Access::gStallRules[subtokens[0]] = subtokens[1];

          if (subtokens[0] == ("r:*")) {
            gStallRead = true;
          }

          if (subtokens[0] == ("w:*")) {
            gStallWrite = true;
          }

          if (subtokens[0] == ("*")) {
            gStallGlobal = true;
          }

          if ((subtokens[0].find("rate:") == 0)) {
            gStallUserGroup = true;
          }

          if (subtokens.size() == 3) {
            XrdOucString comment = subtokens[2].c_str();

            while (comment.replace("_#KOMMA#_", ",")) {
            }

            while (comment.replace("_#TILDE#_", "~")) {
            }

            Access::gStallComment[subtokens[0]] = comment.c_str();
          }
        }
      }
    }
  }

  if (applyredirectandstall) {
    tokens.clear();
    delimiter = ",";
    eos::common::StringConversion::Tokenize(redirect, tokens, delimiter);

    for (size_t i = 0; i < tokens.size(); i++) {
      if (tokens[i].length()) {
        subtokens.clear();
        eos::common::StringConversion::Tokenize(tokens[i],
                                                subtokens,
                                                subdelimiter);

        if (subtokens.size() == 2) {
          Access::gRedirectionRules[subtokens[0]] = subtokens[1];
        }
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Static function to store all defined rules in the global configuration.
 *
 * @return true if successful, otherwise false
 */
/*----------------------------------------------------------------------------*/
bool
Access::StoreAccessConfig()

{
  std::set<uid_t>::const_iterator ituid;
  std::set<gid_t>::const_iterator itgid;
  std::set<std::string>::const_iterator ithost;
  std::set<std::string>::const_iterator itdomain;
  std::map<std::string, std::string>::const_iterator itstall;
  std::map<std::string, std::string>::const_iterator itredirect;
  std::string userval = "";
  std::string groupval = "";
  std::string hostval = "";
  std::string domainval = "";
  std::string useraval = "";
  std::string groupaval = "";
  std::string hostaval = "";
  std::string domainaval = "";
  std::string stall = "";
  std::string redirect = "";

  for (ituid = Access::gBannedUsers.begin();
       ituid != Access::gBannedUsers.end(); ituid++) {
    userval += eos::common::Mapping::UidAsString(*ituid);
    userval += ":";
  }

  for (itgid = Access::gBannedGroups.begin();
       itgid != Access::gBannedGroups.end(); itgid++) {
    groupval += eos::common::Mapping::GidAsString(*itgid);
    groupval += ":";
  }

  for (ithost = Access::gBannedHosts.begin();
       ithost != Access::gBannedHosts.end(); ithost++) {
    hostval += ithost->c_str();
    hostval += ":";
  }

  for (itdomain = Access::gBannedDomains.begin();
       itdomain != Access::gBannedDomains.end(); itdomain++) {
    hostval += itdomain->c_str();
    hostval += ":";
  }

  for (ituid = Access::gAllowedUsers.begin();
       ituid != Access::gAllowedUsers.end(); ituid++) {
    useraval += eos::common::Mapping::UidAsString(*ituid);
    useraval += ":";
  }

  for (itgid = Access::gAllowedGroups.begin();
       itgid != Access::gAllowedGroups.end(); itgid++) {
    groupaval += eos::common::Mapping::GidAsString(*itgid);
    groupaval += ":";
  }

  for (ithost = Access::gAllowedHosts.begin();
       ithost != Access::gAllowedHosts.end(); ithost++) {
    hostaval += ithost->c_str();
    hostaval += ":";
  }

  for (itdomain = Access::gAllowedDomains.begin();
       itdomain != Access::gAllowedDomains.end(); itdomain++) {
    domainaval += itdomain->c_str();
    domainaval += ":";
  }

  gStallRead = gStallWrite = gStallGlobal = gStallUserGroup = false;

  for (itstall = Access::gStallRules.begin();
       itstall != Access::gStallRules.end(); itstall++) {
    stall += itstall->first.c_str();
    stall += "~";
    stall += itstall->second.c_str();
    stall += "~";
    XrdOucString comment = Access::gStallComment[itstall->first].c_str();

    while (comment.replace(",", "_#KOMMA#_")) {
    }

    while (comment.replace("~", "_#TILDE#_")) {
    }

    stall += comment.c_str();
    stall += ",";

    if (itstall->first == ("r:*")) {
      gStallRead = true;
    }

    if (itstall->first == ("w:*")) {
      gStallWrite = true;
    }

    if (itstall->first == ("*")) {
      gStallGlobal = true;
    }

    if ((itstall->first.find("rate:") == 0)) {
      gStallUserGroup = true;
    }
  }

  for (itredirect = Access::gRedirectionRules.begin();
       itredirect != Access::gRedirectionRules.end(); itredirect++) {
    redirect += itredirect->first.c_str();
    redirect += "~";
    redirect += itredirect->second.c_str();
    redirect += ",";
  }

  std::string ukey = gUserKey;
  std::string gkey = gGroupKey;
  std::string hkey = gHostKey;
  std::string dkey = gDomainKey;
  std::string uakey = gAllowedUserKey;
  std::string gakey = gAllowedGroupKey;
  std::string hakey = gAllowedHostKey;
  std::string dakey = gAllowedDomainKey;
  bool ok = 1;
  ok &= FsView::gFsView.SetGlobalConfig(ukey, userval);
  ok &= FsView::gFsView.SetGlobalConfig(gkey, groupval);
  ok &= FsView::gFsView.SetGlobalConfig(hkey, hostval);
  ok &= FsView::gFsView.SetGlobalConfig(dkey, domainval);
  ok &= FsView::gFsView.SetGlobalConfig(uakey, useraval);
  ok &= FsView::gFsView.SetGlobalConfig(gakey, groupaval);
  ok &= FsView::gFsView.SetGlobalConfig(hakey, hostaval);
  ok &= FsView::gFsView.SetGlobalConfig(dakey, domainaval);
  ok &= FsView::gFsView.SetGlobalConfig(gStallKey, stall);
  ok &= FsView::gFsView.SetGlobalConfig(gRedirectionKey, redirect);

  if (!ok) {
    eos_static_err("unable to store <access> configuration");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get find limits in number of files/dirs returned for a certain user
//------------------------------------------------------------------------------
void
Access::GetFindLimits(const eos::common::VirtualIdentity& vid,
                      uint64_t& dir_limit, uint64_t& file_limit)
{
  eos::common::RWMutexReadLock access_rd_lock(gAccessMutex);

  if (gStallUserGroup) {
    std::string usermatchfiles = "rate:user:";
    usermatchfiles += vid.uid_string;
    usermatchfiles += ":";
    usermatchfiles += "FindFiles";
    std::string groupmatchfiles = "rate:group:";
    groupmatchfiles += vid.gid_string;
    groupmatchfiles += ":";
    groupmatchfiles += "FindFiles";
    std::string userwildcardmatchfiles = "rate:user:*:FindFiles";

    if (gStallRules.count(usermatchfiles)) {
      file_limit = strtoul(gStallRules[usermatchfiles].c_str(), 0, 10);
    } else if (gStallRules.count(groupmatchfiles)) {
      file_limit = strtoul(gStallRules[groupmatchfiles].c_str(), 0, 10);
    } else if (gStallRules.count(userwildcardmatchfiles)) {
      file_limit = strtoul(gStallRules[userwildcardmatchfiles].c_str(), 0, 10);
    }

    std::string usermatchdirs = "rate:user:";
    usermatchdirs += vid.uid_string;
    usermatchdirs += ":";
    usermatchdirs += "FindDirs";
    std::string groupmatchdirs = "rate:group:";
    groupmatchdirs += vid.gid_string;
    groupmatchdirs += ":";
    groupmatchdirs += "FindDirs";
    std::string userwildcardmatchdirs = "rate:user:*:FindDirs";

    if (gStallRules.count(usermatchdirs)) {
      dir_limit = strtoul(gStallRules[usermatchdirs].c_str(), 0, 10);
    } else if (gStallRules.count(groupmatchdirs)) {
      dir_limit = strtoul(gStallRules[groupmatchdirs].c_str(), 0, 10);
    } else if (gStallRules.count(userwildcardmatchdirs)) {
      dir_limit = strtoul(gStallRules[userwildcardmatchdirs].c_str(), 0, 10);
    }
  }
}

//------------------------------------------------------------------------------
// Set global stall rule and save the previous status
//------------------------------------------------------------------------------
void
Access::SetStallRule(const StallInfo& new_stall, StallInfo& old_stall)
{
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  if (new_stall.mType.empty()) {
    return;
  }

  old_stall.mType = new_stall.mType;

  if (Access::gStallRules.count(new_stall.mType)) {
    old_stall.mDelay = Access::gStallRules[old_stall.mType];
    old_stall.mComment = Access::gStallComment[old_stall.mType];
    old_stall.mIsGlobal = Access::gStallGlobal;
  }

  if (new_stall.mDelay.empty()) {
    Access::gStallRules.erase(new_stall.mType);
  } else {
    Access::gStallRules[new_stall.mType] = new_stall.mDelay;
  }

  if (new_stall.mComment.empty()) {
    Access::gStallComment.erase(new_stall.mType);
  } else {
    Access::gStallComment[new_stall.mType] = new_stall.mComment;
  }

  Access::gStallGlobal = new_stall.mIsGlobal;
}

//------------------------------------------------------------------------------
// Set access rules for a slave to master transition. More precisely remove
// any stall and redirection rules
//------------------------------------------------------------------------------
void
Access::SetSlaveToMasterRules()
{
  eos_static_info("%s", "msg=\"remove any stall and redirection rules\"");
  eos::common::RWMutexWriteLock wr_lock(Access::gAccessMutex);
  Access::gRedirectionRules.erase(std::string("w:*"));
  Access::gRedirectionRules.erase(std::string("ENOENT:*"));
  Access::gStallRules.erase(std::string("w:*"));
  Access::gStallWrite = false;
}

//----------------------------------------------------------------------------
// Set access rules for a master to slave transition.
//----------------------------------------------------------------------------
void
Access::SetMasterToSlaveRules(const std::string& other_master_id)
{
  eos::common::RWMutexWriteLock wr_lock(Access::gAccessMutex);

  if (other_master_id.empty()) {
    // No master - remove redirections and put a stall for writes
    eos_static_info("%s", "msg=\"no master, add global stall\"");
    Access::gRedirectionRules.erase(std::string("w:*"));
    Access::gRedirectionRules.erase(std::string("ENOENT:*"));
    Access::gStallRules[std::string("*")] = "60";
    Access::gStallWrite = true;
    Access::gStallGlobal = true;
  } else {
    // We're the slave and there is a master - set redirection to him
    eos_static_info("msg=\"add redirection to master %s\"",
                    other_master_id.c_str());
    std::string host = other_master_id.substr(0, other_master_id.find(':'));
    Access::gRedirectionRules[std::string("w:*")] = host.c_str();
    Access::gRedirectionRules[std::string("ENOENT:*")] = host.c_str();
    // Remove write stall
    Access::gStallRules.erase(std::string("*"));
    Access::gStallRules.erase(std::string("w:*"));
    Access::gStallWrite = false;
    Access::gStallGlobal = false;
  }
}

//------------------------------------------------------------------------------
// Remove stall rule specified by key
//------------------------------------------------------------------------------
void
Access::RemoveStallRule(const std::string& key)
{
  eos::common::RWMutexWriteLock wr_lock(Access::gAccessMutex);
  Access::gStallRules.erase(key);

  if (key.find("w:*") == 0) {
    Access::gStallWrite = false;
  } else if (key.find("r:*") == 0) {
    Access::gStallRead = false;
  } else if (key.find("*") == 0) {
    Access::gStallGlobal = false;
  }
}

//------------------------------------------------------------------------------
// Thread limit by user id
//------------------------------------------------------------------------------
size_t
Access::ThreadLimit(uid_t uid) {
  std::string id = "threads:";
  id += std::to_string(uid);

  eos::common::RWMutexReadLock access_rd_lock(gAccessMutex);
  if (gStallRules.count(id.c_str())) {
    return strtoul(gStallRules[id.c_str()].c_str(),0,10);
  } else {
    if (gStallRules.count("threads:*")) {
      return strtoul(gStallRules["threads:*"].c_str(),0,10);
    }
  }
  return 0;
}

//------------------------------------------------------------------------------
// Global thread limit
//------------------------------------------------------------------------------
size_t
Access::ThreadLimit() {
  eos::common::RWMutexReadLock access_rd_lock(gAccessMutex);
  if (gStallRules.count("threads:max")) {
    return strtoul(gStallRules["threads:max"].c_str(),0,10);
  }
  return 1000000;
}


EOSMGMNAMESPACE_END
