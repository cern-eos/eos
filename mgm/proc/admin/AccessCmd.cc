//------------------------------------------------------------------------------
// @file: AccessCmd.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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


#include "AccessCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Stat.hh"
#include "common/StringUtils.hh"
#include "common/Constants.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <cctype>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process a rule key by converting the given username to a uid if necessary
//------------------------------------------------------------------------------
std::string
ProcessRuleKey(const std::string& key)
{
  std::string new_key = key;

  if (new_key.find("threads:") == 0) {
    size_t pos = new_key.find(":");

    if (pos + 1 == new_key.length()) {
      return std::string();
    }

    std::string target = new_key.substr(pos + 1);
    eos::common::trim(target);

    if (target.empty()) {
      return std::string();
    }

    if ((target != "max") && (target != "*")) {
      // Check if target is a username and then try to convert it
      if (std::find_if(begin(target), end(target), [](unsigned char c) {
      return std::isalpha(c);
      }) != target.end()) {
        int errc = 0;
        uid_t uid_target = eos::common::Mapping::UserNameToUid(target, errc);

        if (errc) {
          return std::string();
        }

        new_key = "threads:" + std::to_string(uid_target);
      }
    }
  }

  return new_key;
}

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
AccessCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::AccessProto access = mReqProto.access();

  if ((mVid.uid != 0) && (!mVid.hasUid(eos::common::ADM_UID)) && (!mVid.hasGid(eos::common::ADM_GID)) &&
      (!mVid.sudoer)) {
    // root and admins only
    reply.set_std_out("");
    reply.set_std_err("error: you are not an access administrator!\"");
    reply.set_retc(EPERM);
    return reply;
  }

  switch (access.subcmd_case()) {
  case eos::console::AccessProto::kLs :
    LsSubcmd(access.ls(), reply);
    break;

  case eos::console::AccessProto::kRm :
    RmSubcmd(access.rm(), reply);
    break;

  case eos::console::AccessProto::kSet :
    SetSubcmd(access.set(), reply);
    break;

  case eos::console::AccessProto::kBan :
    BanSubcmd(access.ban(), reply);
    break;

  case eos::console::AccessProto::kUnban :
    UnbanSubcmd(access.unban(), reply);
    break;

  case eos::console::AccessProto::kAllow :
    AllowSubcmd(access.allow(), reply);
    break;

  case eos::console::AccessProto::kUnallow :
    UnallowSubcmd(access.unallow(), reply);
    break;

  case eos::console::AccessProto::kStallhosts :
    StallhostsSubcmd(access.stallhosts(), reply);
    break;

  default :
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute ls subcommand
//------------------------------------------------------------------------------
void AccessCmd::LsSubcmd(const eos::console::AccessProto_LsProto& ls,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out {""};
  std::ostringstream std_err {""};
  int ret_c {0};
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  int cnt {0};

  if (!Access::gBannedUsers.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Banned Users ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ituid = Access::gBannedUsers.begin();
         ituid != Access::gBannedUsers.end(); ituid++) {
      cnt++;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "user.banned=";
      }

      if (ls.id2name()) {
        std_out << eos::common::Mapping::UidAsString(*ituid).c_str();
      } else {
        int terrc = 0;
        std_out << eos::common::Mapping::UidToUserName(*ituid, terrc).c_str();
      }

      std_out << '\n';
    }
  }

  if (!Access::gBannedGroups.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Banned Groups...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itgid = Access::gBannedGroups.begin();
         itgid != Access::gBannedGroups.end(); itgid++) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "group.banned=";
      }

      if (ls.id2name()) {
        std_out << eos::common::Mapping::GidAsString(*itgid).c_str();
      } else {
        int terrc = 0;
        std_out << eos::common::Mapping::GidToGroupName(*itgid, terrc).c_str();
      }

      std_out << '\n';
    }
  }

  if (!Access::gBannedHosts.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Banned Hosts ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ithost = Access::gBannedHosts.begin();
         ithost != Access::gBannedHosts.end(); ithost++) {
      cnt++;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "host.banned=";
      }

      std_out << ithost->c_str() << '\n';
    }
  }

  if (!Access::gBannedDomains.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Banned Domains ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itdomain = Access::gBannedDomains.begin();
         itdomain != Access::gBannedDomains.end(); ++itdomain) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "domain.banned=";
      }

      std_out << itdomain->c_str() << '\n';
    }
  }

  if (!Access::gBannedTokens.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Banned Tokens ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ittoken = Access::gBannedTokens.begin();
         ittoken != Access::gBannedTokens.end(); ++ittoken) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "tokens.banned=";
      }

      std_out << ittoken->c_str() << '\n';
    }
  }

  if (!Access::gAllowedUsers.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Allowd Users ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ituid = Access::gAllowedUsers.begin();
         ituid != Access::gAllowedUsers.end(); ++ituid) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "user.allowed=";
      }

      if (ls.id2name()) {
        std_out << eos::common::Mapping::UidAsString(*ituid).c_str();
      } else {
        int terrc = 0;
        std_out << eos::common::Mapping::UidToUserName(*ituid, terrc).c_str();
      }

      std_out << '\n';
    }
  }

  if (!Access::gAllowedGroups.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Allowed Groups...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itgid = Access::gAllowedGroups.begin();
         itgid != Access::gAllowedGroups.end(); itgid++) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "group.allowed=";
      }

      if (ls.id2name()) {
        std_out << eos::common::Mapping::GidAsString(*itgid).c_str();
      } else {
        int terrc = 0;
        std_out << eos::common::Mapping::GidToGroupName(*itgid, terrc).c_str();
      }

      std_out << '\n';
    }
  }

  if (!Access::gAllowedHosts.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Allowed Hosts ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ithost = Access::gAllowedHosts.begin();
         ithost != Access::gAllowedHosts.end(); ithost++) {
      cnt++;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "host.allowed=";
      }

      std_out << ithost->c_str() << '\n';
    }
  }

  if (!Access::gAllowedDomains.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Allowed Domains ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itdomain = Access::gAllowedDomains.begin();
         itdomain != Access::gAllowedDomains.end(); ++itdomain) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "domain.allowed=";
      }

      std_out << itdomain->c_str() << '\n';
    }
  }

  if (!Access::gAllowedTokens.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Allowed Tokens ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ittoken = Access::gAllowedTokens.begin();
         ittoken != Access::gAllowedTokens.end(); ++ittoken) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "tokens.allowed=";
      }

      std_out << ittoken->c_str() << '\n';
    }
  }

  if (!Access::gRedirectionRules.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Redirection Rules ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itred = Access::gRedirectionRules.begin();
         itred != Access::gRedirectionRules.end(); ++itred) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[1024];
        snprintf(counter, sizeof(counter) - 1, "[ %02d ] %32s => ", cnt,
                 itred->first.c_str());
        std_out << counter;
      } else {
        std_out << "redirect." << itred->first.c_str() << "=";
      }

      std_out << itred->second.c_str() << '\n';
    }
  }

  if (!Access::gStallRules.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Stall Rules ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto itred = Access::gStallRules.begin();
         itred != Access::gStallRules.end(); ++itred) {
      ++cnt;

      if (!ls.monitoring()) {
        char counter[1024];
        snprintf(counter, sizeof(counter) - 1, "[ %02d ] %32s => ", cnt,
                 itred->first.c_str());
        std_out << counter;
      } else {
        std_out << "stall." << itred->first.c_str() << "=";
      }

      std_out << itred->second.c_str();

      if (!ls.monitoring()) {
        std_out << "\t" << Access::gStallComment[itred->first].c_str();
      } else {
        std_out << " mComment=\"" << Access::gStallComment[itred->first].c_str() <<
                "\"";
      }

      std_out << '\n';
    }
  }

  if (!Access::gStallHosts.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Hosts in the stall white list ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ithost = Access::gStallHosts.begin();
         ithost != Access::gStallHosts.end(); ithost++) {
      cnt++;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "host.stallhosts=";
      }

      std_out << ithost->c_str() << '\n';
    }
  }

  if (!Access::gNoStallHosts.empty()) {
    if (!ls.monitoring()) {
      std_out <<
              "# ....................................................................................\n";
      std_out << "# Hosts in the no-stall black list ...\n";
      std_out <<
              "# ....................................................................................\n";
    }

    cnt = 0;

    for (auto ithost = Access::gNoStallHosts.begin();
         ithost != Access::gNoStallHosts.end(); ithost++) {
      cnt++;

      if (!ls.monitoring()) {
        char counter[16];
        snprintf(counter, sizeof(counter) - 1, "%02d", cnt);
        std_out << "[ " << counter << " ] ";
      } else {
        std_out << "host.nostallhosts=";
      }

      std_out << ithost->c_str() << '\n';
    }
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute rm subcommand
//------------------------------------------------------------------------------
void AccessCmd::RmSubcmd(const eos::console::AccessProto_RmProto& rm,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out {""};
  std::ostringstream std_err {""};
  int ret_c = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (rm.rule()) {
  case eos::console::AccessProto_RmProto::REDIRECT : {
    if (!((Access::gRedirectionRules.count("*") && rm.key().empty()) ||
          (Access::gRedirectionRules.count("r:*") && rm.key() == "r") ||
          (Access::gRedirectionRules.count("w:*") && rm.key() == "w") ||
          (Access::gRedirectionRules.count("ENONET:*") && rm.key() == "ENONET") ||
          (Access::gRedirectionRules.count("ENOENT:*") && rm.key() == "ENOENT") ||
          (Access::gRedirectionRules.count("ENETUNREACH:*") &&
           rm.key() == "ENETUNREACH"))) {
      reply.set_std_err("error: there is no global redirection defined with "
                        "such key: '" + rm.key() + '\'');
      reply.set_retc(EINVAL);
      return;
    } else {
      std_out << "success: removing global redirection";

      if (!rm.key().empty()) {
        std_out << " for <" << rm.key() << ">";
      }

      if (rm.key().empty()) {
        Access::gRedirectionRules.erase("*");
      } else {
        Access::gRedirectionRules.erase(rm.key() + ":*");
      }

      lock.Release();
      eos::common::RWMutexReadLock lock(Access::gAccessMutex);

      if (!Access::StoreAccessConfig()) {
        reply.set_std_err("error: unable to store access configuration");
        reply.set_retc(EIO);
        return;
      }
    }
  }
  break;

  case eos::console::AccessProto_RmProto::STALL :
  case eos::console::AccessProto_RmProto::LIMIT : {
    if (!((Access::gStallRules.count("*") && rm.key().empty()) ||
          (Access::gStallRules.count("r:*") && (rm.key() == "r")) ||
          (Access::gStallRules.count("w:*") && (rm.key() == "w")) ||
          (Access::gStallRules.count("ENONET:*") && (rm.key() == "ENONET")) ||
          (Access::gStallRules.count("ENOENT:*") && (rm.key() == "ENOENT")) ||
          (Access::gStallRules.count("ENETUNREACH:*") && (rm.key() == "ENETUNREACH")) ||
          !rm.key().empty())) {
      reply.set_std_err("error: there is no global redirection defined with "
                        "such key: '" + rm.key() + '\'');
      reply.set_retc(EINVAL);
      return;
    } else {
      std_out << "success: removing global ";

      if (!rm.key().empty()) {
        if ((rm.key().find("rate:user:") == 0) ||
            (rm.key().find("rate:group:") == 0) ||
            (rm.key().find("threads:") == 0)) {
          std_out << "limit";
        } else {
          std_out << "stall";
        }

        std_out << " for <" << rm.key() << ">";
      }

      if ((rm.key().find("rate:user:") == 0) ||
          (rm.key().find("rate:group:") == 0) ||
          (rm.key().find("threads:") == 0)) {
        const std::string rule_key = ProcessRuleKey(rm.key());

        if (rule_key.empty()) {
          reply.set_std_err("error: malformed access rule");
          reply.set_retc(EINVAL);
          return;
        }

        // Remove by uid
        Access::gStallRules.erase(rule_key);
        Access::gStallComment.erase(rule_key);
        // Also remove by username to cover old bug
        Access::gStallRules.erase(rm.key());
        Access::gStallComment.erase(rm.key());
      } else if (rm.key().empty()) {
        Access::gStallRules.erase("*");
        Access::gStallComment.erase("*");
      } else {
        Access::gStallRules.erase(rm.key() + ":*");
        Access::gStallComment.erase(rm.key() + ":*");
      }

      lock.Release();
      eos::common::RWMutexReadLock rlock(Access::gAccessMutex);

      if (!Access::StoreAccessConfig()) {
        reply.set_std_err("error: unable to store access configuration");
        reply.set_retc(EIO);
        return;
      }
    }
  }
  break;

  default : // should never happen
    reply.set_std_err("error: rule not found, it should be one of redirect|stall|limit");
    reply.set_retc(EINVAL);
    return;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void AccessCmd::SetSubcmd(const eos::console::AccessProto_SetProto& set,
                          eos::console::ReplyProto& reply)
{
  std::ostringstream std_out {""};
  std::ostringstream std_err {""};
  int ret_c = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (set.rule()) {
  case eos::console::AccessProto_SetProto::REDIRECT : {
    if (!(set.key().empty() || (set.key() == "r") || (set.key() == "w") ||
          (set.key() == "ENONET") || (set.key() == "ENOENT") ||
          (set.key() == "ENETUNREACH"))) {
      reply.set_std_err("error: there is no redirection to set with such "
                        "key: '" + set.key() + '\'');
      reply.set_retc(EINVAL);
      return;
    } else {
      std_out << "success: setting global redirection to '" << set.target() << '\'';

      if (!set.key().empty()) {
        std_out << " for <" << set.key() << ">";
      }

      if (set.key().empty()) {
        Access::gRedirectionRules["*"] = set.target();
      } else {
        Access::gRedirectionRules[set.key() + ":*"] = set.target();
      }

      lock.Release();
      eos::common::RWMutexReadLock rlock(Access::gAccessMutex);

      if (!Access::StoreAccessConfig()) {
        reply.set_std_err("error: unable to store access configuration");
        reply.set_retc(EIO);
        return;
      }
    }
  }
  break;

  case eos::console::AccessProto_SetProto::STALL :
  case eos::console::AccessProto_SetProto::LIMIT : {
    int target;

    try {
      target = (std::stoi(set.target()));
    } catch (const std::exception& e) {
      reply.set_std_err("error: target must be an integer equal or greater than 0 (value zero allowed just for 'rate:' limit)");
      reply.set_retc(EINVAL);
      return;
    }

    if (!((set.key().find("rate:") == 0 && target >= 0) || (target > 0))) {
      reply.set_std_err("error: target must be an integer equal or greater than 0 (value zero allowed just for 'rate:' limit)");
      reply.set_retc(EINVAL);
      return;
    }

    if (set.key().find("rate:") == 0) {
      std_out << "success: setting rate cutoff at " << set.target()
              << " Hz for rate:<user|group>:<operation>=" << set.key();
    }  else if (set.key().find("threads:") == 0) {
      std_out << "success: setting thread limit at " << set.target()
              << " for " << set.key();
    } else {
      if (!(set.key().empty() || (set.key() == "r") || (set.key() == "w") ||
            (set.key() == "ENONET") || (set.key() == "ENOENT") ||
            (set.key() == "ENETUNREACH"))) {
        reply.set_std_err("error: there is no stall to set with such "
                          "key: '" + set.key() + '\'');
        reply.set_retc(EINVAL);
        return;
      }

      std_out << "success: setting global stall to " << set.target() << " seconds";

      if (!set.key().empty()) {
        std_out << " for <" << set.key() << ">";
      }
    }

    if ((set.key().find("rate:user:") == 0) ||
        (set.key().find("rate:group:") == 0) ||
        (set.key().find("threads:") == 0)) {
      const std::string rule_key = ProcessRuleKey(set.key());

      if (rule_key.empty()) {
        reply.set_std_err("error: malformed access rule");
        reply.set_retc(EINVAL);
        return;
      }

      Access::gStallRules[rule_key] = set.target();
      Access::gStallComment[rule_key] = mReqProto.comment();
    } else if (set.key().empty()) {
      Access::gStallRules["*"] = set.target();
      Access::gStallComment["*"] = mReqProto.comment();
    } else {
      Access::gStallRules[set.key() + ":*"] = set.target();
      Access::gStallComment[set.key() + ":*"] = mReqProto.comment();
    }

    lock.Release();
    eos::common::RWMutexReadLock rlock(Access::gAccessMutex);

    if (!Access::StoreAccessConfig()) {
      reply.set_std_err("error: unable to store access configuration");
      reply.set_retc(EIO);
      return;
    }
  }
  break;

  default : // should never happen
    reply.set_std_err("error: rule not found, it should be one of redirect|stall|limit");
    reply.set_retc(EINVAL);
    return;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}


// @note (faluchet) all this machinery (and more) should be done with templates and funct programming... Later

void AccessCmd::aux(const std::string& sid, std::ostringstream& std_out,
                    std::ostringstream& std_err, int& ret_c)
{
  std::string saction;

  switch (mReqProto.access().subcmd_case()) {
  case eos::console::AccessProto::kBan:
    saction = "ban";
    break;

  case eos::console::AccessProto::kUnban:
    saction = "unban";
    break;

  case eos::console::AccessProto::kAllow:
    saction = "allow";
    break;

  case eos::console::AccessProto::kUnallow:
    saction = "unallow";
    break;

  case eos::console::AccessProto::kStallhosts:
    saction = "(un-)stallhosts";
    break;

  default :
    ;
  }

  eos::common::RWMutexReadLock rlock(Access::gAccessMutex);

  if (Access::StoreAccessConfig()) {
    std_out << "success: " << saction << " '" << sid << '\'';
    ret_c = 0;
  } else {
    std_err << "error: unable to store access configuration";
    ret_c = EIO;
  }
}

//----------------------------------------------------------------------------
// Execute ban subcommand
//----------------------------------------------------------------------------
void AccessCmd::BanSubcmd(const eos::console::AccessProto_BanProto& ban,
                          eos::console::ReplyProto& reply)
{
  std::ostringstream std_out{""};
  std::ostringstream std_err{""};
  int ret_c = 0;
  int errc = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (ban.idtype()) {
  case eos::console::AccessProto_BanProto::USER
      : {
      uid_t uid = eos::common::Mapping::UserNameToUid(ban.id(), errc);

      if (!errc) {
        Access::gBannedUsers.insert(uid);
        lock.Release();
        aux(ban.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: no such user - cannot ban '" << ban.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_BanProto::GROUP
      : {
      gid_t gid = eos::common::Mapping::GroupNameToGid(ban.id(), errc);

      if (!errc) {
        Access::gBannedGroups.insert(gid);
        lock.Release();
        aux(ban.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: no such group - cannot ban '" << ban.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_BanProto::HOST
      : {
      Access::gBannedHosts.insert(ban.id());
      lock.Release();
      aux(ban.id(), std_out, std_err, ret_c);
    }
    break;

  case eos::console::AccessProto_BanProto::DOMAINNAME
      : {
      Access::gBannedDomains.insert(ban.id());
      lock.Release();
      aux(ban.id(), std_out, std_err, ret_c);
    }
    break;

  case eos::console::AccessProto_BanProto::TOKEN
      : {
      Access::gBannedTokens.insert(ban.id());
      lock.Release();
      aux(ban.id(), std_out, std_err, ret_c);
    }
    break;

  default:
    ;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute unban subcommand
//------------------------------------------------------------------------------
void AccessCmd::UnbanSubcmd(const eos::console::AccessProto_UnbanProto& unban,
                            eos::console::ReplyProto& reply)
{
  std::ostringstream std_out{""};
  std::ostringstream std_err{""};
  int ret_c = 0;
  int errc = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (unban.idtype()) {
  case eos::console::AccessProto_UnbanProto::USER
      : {
      uid_t uid = eos::common::Mapping::UserNameToUid(unban.id(), errc);

      if (!errc) {
        if (Access::gBannedUsers.count(uid)) {
          Access::gBannedUsers.erase(uid);
          lock.Release();
          aux(unban.id(), std_out, std_err, ret_c);
        } else {
          std_err << "error: user '" << unban.id() << "' is not banned anyway";
          ret_c = ENOENT;
        }
      } else {
        std_err << "error: no such user - cannot unban '" << unban.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_UnbanProto::GROUP
      : {
      gid_t gid = eos::common::Mapping::GroupNameToGid(unban.id(), errc);

      if (!errc) {
        if (Access::gBannedGroups.count(gid)) {
          Access::gBannedGroups.erase(gid);
          lock.Release();
          aux(unban.id(), std_out, std_err, ret_c);
        } else {
          std_err << "error: group '" << unban.id() << "' is not banned anyway";
          ret_c = ENOENT;
        }
      } else {
        std_err << "error: no such group - cannot unban '" << unban.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_UnbanProto::HOST
      : {
      if (Access::gBannedHosts.count(unban.id())) {
        Access::gBannedHosts.erase(unban.id());
        lock.Release();
        aux(unban.id(), std_out, std_err, ret_c);
        lock.Grab(Access::gAccessMutex);
      } else {
        std_err << "error: host '" << unban.id() << "' is not banned anyway";
        ret_c = ENOENT;
      }
    }
    break;

  case eos::console::AccessProto_UnbanProto::DOMAINNAME
      : {
      if (Access::gBannedDomains.count(unban.id())) {
        Access::gBannedDomains.erase(unban.id());
        lock.Release();
        aux(unban.id(), std_out, std_err, ret_c);
        lock.Grab(Access::gAccessMutex);
      } else {
        std_err << "error: domain '" << unban.id() << "' is not banned anyway";
        ret_c = ENOENT;
      }
    }
    break;

  case eos::console::AccessProto_UnbanProto::TOKEN
      : {
      if (Access::gBannedTokens.count(unban.id())) {
        Access::gBannedTokens.erase(unban.id());
        lock.Release();
        aux(unban.id(), std_out, std_err, ret_c);
        lock.Grab(Access::gAccessMutex);
      } else {
        std_err << "error: token '" << unban.id() << "' is not banned anyway";
        ret_c = ENOENT;
      }
    }
    break;

  default:
    ;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute allow subcommand
//------------------------------------------------------------------------------
void AccessCmd::AllowSubcmd(const eos::console::AccessProto_AllowProto& allow,
                            eos::console::ReplyProto& reply)
{
  std::ostringstream std_out{""};
  std::ostringstream std_err{""};
  int ret_c = 0;
  int errc = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (allow.idtype()) {
  case eos::console::AccessProto_AllowProto::USER
      : {
      uid_t uid = eos::common::Mapping::UserNameToUid(allow.id(), errc);

      if (!errc) {
        Access::gAllowedUsers.insert(uid);
        lock.Release();
        aux(allow.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: no such user - cannot allow '" << allow.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_AllowProto::GROUP
      : {
      gid_t gid = eos::common::Mapping::GroupNameToGid(allow.id(), errc);

      if (!errc) {
        Access::gAllowedGroups.insert(gid);
        lock.Release();
        aux(allow.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: no such group - cannot allow '" << allow.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_AllowProto::HOST
      : {
      Access::gAllowedHosts.insert(allow.id());
      lock.Release();
      aux(allow.id(), std_out, std_err, ret_c);
    }
    break;

  case eos::console::AccessProto_AllowProto::DOMAINNAME
      : {
      Access::gAllowedDomains.insert(allow.id());
      lock.Release();
      aux(allow.id(), std_out, std_err, ret_c);
    }
    break;

  case eos::console::AccessProto_AllowProto::TOKEN
      : {
      Access::gAllowedTokens.insert(allow.id());
      lock.Release();
      aux(allow.id(), std_out, std_err, ret_c);
    }
    break;

  default:
    ;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute unallow subcommand
//------------------------------------------------------------------------------
void
AccessCmd::UnallowSubcmd(const eos::console::AccessProto_UnallowProto& unallow,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out{""};
  std::ostringstream std_err{""};
  int ret_c = 0;
  int errc = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (unallow.idtype()) {
  case eos::console::AccessProto_UnallowProto::USER
      : {
      uid_t uid = eos::common::Mapping::UserNameToUid(unallow.id(), errc);

      if (!errc) {
        if (Access::gAllowedUsers.count(uid)) {
          Access::gAllowedUsers.erase(uid);
          lock.Release();
          aux(unallow.id(), std_out, std_err, ret_c);
        } else {
          std_err << "error: user '" << unallow.id() << "' is not allowed anyway";
          ret_c = ENOENT;
        }
      } else {
        std_err << "error: no such user - cannot unallow '" << unallow.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_UnallowProto::GROUP
      : {
      gid_t gid = eos::common::Mapping::GroupNameToGid(unallow.id(), errc);

      if (!errc) {
        if (Access::gAllowedGroups.count(gid)) {
          Access::gAllowedGroups.erase(gid);
          lock.Release();
          aux(unallow.id(), std_out, std_err, ret_c);
        } else {
          std_err << "error: group '" << unallow.id() << "' is not allowed anyway";
          ret_c = ENOENT;
        }
      } else {
        std_err << "error: no such group - cannot unallow '" << unallow.id() << '\'';
        ret_c = EINVAL;
      }
    }
    break;

  case eos::console::AccessProto_UnallowProto::HOST
      : {
      if (Access::gAllowedHosts.count(unallow.id())) {
        Access::gAllowedHosts.erase(unallow.id());
        lock.Release();
        aux(unallow.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: host '" << unallow.id() << "' is not allowed anyway";
        ret_c = ENOENT;
      }
    }
    break;

  case eos::console::AccessProto_UnallowProto::DOMAINNAME
      : {
      if (Access::gAllowedDomains.count(unallow.id())) {
        Access::gAllowedDomains.erase(unallow.id());
        lock.Release();
        aux(unallow.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: domain '" << unallow.id() << "' is not allowed anyway";
        ret_c = ENOENT;
      }
    }
    break;

  case eos::console::AccessProto_UnallowProto::TOKEN
      : {
      if (Access::gAllowedTokens.count(unallow.id())) {
        Access::gAllowedTokens.erase(unallow.id());
        lock.Release();
        aux(unallow.id(), std_out, std_err, ret_c);
      } else {
        std_err << "error: domain '" << unallow.id() << "' is not allowed anyway";
        ret_c = ENOENT;
      }
    }
    break;

  default:
    ;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//----------------------------------------------------------------------------
// Execute stallhostssubcommand
//----------------------------------------------------------------------------
void AccessCmd::StallhostsSubcmd(const
                                 eos::console::AccessProto_StallHostsProto& stall,
                                 eos::console::ReplyProto& reply)
{
  std::ostringstream std_out{""};
  std::ostringstream std_err{""};
  int ret_c = 0;
  gOFS->MgmStats.Add("AccessControl", mVid.uid, mVid.gid, 1);
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

  switch (stall.type()) {
  case eos::console::AccessProto_StallHostsProto::STALL :
    switch (stall.op()) {
    case eos::console::AccessProto_StallHostsProto::ADD: {
      if (Access::gStallHosts.count(stall.hostpattern())) {
        ret_c = EEXIST;
        std_err << "error: entry exists already in the stall list\n";
      } else {
        if (Access::gNoStallHosts.count(stall.hostpattern())) {
          ret_c = EEXIST;
          std_err << "error: this pattern is in the no-stall list!\n";
        } else {
          Access::gStallHosts.insert(stall.hostpattern());
        }
      }

      lock.Release();

      if (!ret_c) {
        aux(stall.hostpattern(), std_out, std_err, ret_c);
      }

      break;
    }

    case eos::console::AccessProto_StallHostsProto::REMOVE: {
      if (!Access::gStallHosts.count(stall.hostpattern())) {
        ret_c = ENOENT;
        std_err << "error: this pattern is not in the stall list\n";
      } else {
        Access::gStallHosts.erase(stall.hostpattern());
      }

      lock.Release();

      if (!ret_c) {
        aux(stall.hostpattern(), std_out, std_err, ret_c);
      }

      break;
    }

    default:
      lock.Release();
      ret_c = EINVAL;
    }

    break;

  case eos::console::AccessProto_StallHostsProto::NOSTALL :
    switch (stall.op()) {
    case eos::console::AccessProto_StallHostsProto::ADD: {
      if (Access::gNoStallHosts.count(stall.hostpattern())) {
        ret_c = EEXIST;
        std_err << "error: entry exists already in the no-stall list\n";
      } else {
        if (Access::gStallHosts.count(stall.hostpattern())) {
          ret_c = EEXIST;
          std_err << "error: this pattern is in the stall list!\n";
        } else {
          Access::gNoStallHosts.insert(stall.hostpattern());
        }
      }

      lock.Release();
      aux(stall.hostpattern(), std_out, std_err, ret_c);
      break;
    }

    case eos::console::AccessProto_StallHostsProto::REMOVE: {
      if (!Access::gNoStallHosts.count(stall.hostpattern())) {
        ret_c = ENOENT;
        std_err << "error: this pattern is not in the nostall list\n";
      } else {
        Access::gNoStallHosts.erase(stall.hostpattern());
      }

      lock.Release();
      aux(stall.hostpattern(), std_out, std_err, ret_c);
      break;
    }

    default:
      lock.Release();
      ret_c = EINVAL;
      aux(stall.hostpattern(), std_out, std_err, ret_c);
    }

  default:
    break;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}


EOSMGMNAMESPACE_END
