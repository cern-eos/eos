// ----------------------------------------------------------------------
// File: proc/admin/Access.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Access ()
{
  gOFS->MgmStats.Add("AccessControl", pVid->uid, pVid->gid, 1);
  std::string user = "";
  std::string group = "";
  std::string host = "";
  std::string option = "";
  std::string redirect = "";
  std::string stall = "";
  std::string type = "";

  bool monitoring = false;
  bool translate = true;
  user = pOpaque->Get("mgm.access.user") ? pOpaque->Get("mgm.access.user") : "";
  group = pOpaque->Get("mgm.access.group") ? pOpaque->Get("mgm.access.group") : "";
  host = pOpaque->Get("mgm.access.host") ? pOpaque->Get("mgm.access.host") : "";
  option = pOpaque->Get("mgm.access.option") ? pOpaque->Get("mgm.access.option") : "";
  redirect = pOpaque->Get("mgm.access.redirect") ? pOpaque->Get("mgm.access.redirect") : "";
  stall = pOpaque->Get("mgm.access.stall") ? pOpaque->Get("mgm.access.stall") : "";
  type = pOpaque->Get("mgm.access.type") ? pOpaque->Get("mgm.access.type") : "";

  if ((option.find("m")) != std::string::npos)
    monitoring = true;

  if ((option.find("n")) != std::string::npos)
    translate = false;

  if (mSubCmd == "ban")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (user.length())
    {
      int errc = 0;
      uid_t uid = eos::common::Mapping::UserNameToUid(user, errc);

      if (!errc)
      {
        Access::gBannedUsers.insert(uid);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: ban user '", stdOut += user.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: no such user - cannot ban '";
        stdErr += user.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (group.length())
    {
      int errc = 0;
      gid_t gid = eos::common::Mapping::GroupNameToGid(group, errc);
      if (!errc)
      {
        Access::gBannedGroups.insert(gid);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: ban group '", stdOut += group.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: no such group - cannot ban '";
        stdErr += group.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (host.length())
    {
      if (Access::StoreAccessConfig())
      {
        Access::gBannedHosts.insert(host);
        stdOut = "success: ban host '";
        stdOut += host.c_str();
        stdOut += "'";
        retc = 0;
      }
      else
      {
        stdErr = "error: unable to store access configuration";
        retc = EIO;
      }
    }
  }

  if (mSubCmd == "unban")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (user.length())
    {
      int errc = 0;
      uid_t uid = eos::common::Mapping::UserNameToUid(user, errc);
      if (!errc)
      {
        if (Access::gBannedUsers.count(uid))
        {
          if (Access::StoreAccessConfig())
          {
            Access::gBannedUsers.erase(uid);
            if (Access::StoreAccessConfig())
            {
              stdOut = "success: unban user '", stdOut += user.c_str();
              stdOut += "'";
              retc = 0;
            }
            else
            {
              stdErr = "error: unable to store access configuration";
              retc = EIO;
            }
          }
          else
          {
            stdErr = "error: unable to store access configuration";
            retc = EIO;
          }
        }
        else
        {
          stdErr = "error: user '";
          stdErr += user.c_str();
          stdErr += "' is not banned anyway!";
          retc = ENOENT;
        }
      }
      else
      {
        stdErr = "error: no such user - cannot ban '";
        stdErr += user.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (group.length())
    {
      int errc = 0;
      gid_t gid = eos::common::Mapping::GroupNameToGid(group, errc);
      if (!errc)
      {
        if (Access::gBannedGroups.count(gid))
        {
          Access::gBannedGroups.erase(gid);
          if (Access::StoreAccessConfig())
          {
            stdOut = "success: unban group '", stdOut += group.c_str();
            stdOut += "'";
            retc = 0;
          }
          else
          {
            stdErr = "error: unable to store access configuration";
            retc = EIO;
          }
        }
        else
        {
          stdErr = "error: group '";
          stdErr += group.c_str();
          stdErr += "' is not banned anyway!";
          retc = ENOENT;
        }
      }
      else
      {
        stdErr = "error: no such group - cannot unban '";
        stdErr += group.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (host.length())
    {
      if (Access::gBannedHosts.count(host))
      {
        Access::gBannedHosts.erase(host);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: unban host '";
          stdOut += host.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: host '";
        stdErr += host.c_str();
        stdErr += "' is not banned anyway!";
        retc = ENOENT;
      }

    }
  }

  if (mSubCmd == "allow")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (user.length())
    {
      int errc = 0;
      uid_t uid = eos::common::Mapping::UserNameToUid(user, errc);
      if (!errc)
      {
        Access::gAllowedUsers.insert(uid);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: allow user '", stdOut += user.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: no such user - cannot allow '";
        stdErr += user.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (group.length())
    {
      int errc = 0;
      gid_t gid = eos::common::Mapping::GroupNameToGid(group, errc);
      if (!errc)
      {
        Access::gAllowedGroups.insert(gid);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: allow group '", stdOut += group.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: no such group - cannot allow '";
        stdErr += group.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (host.length())
    {
      if (Access::StoreAccessConfig())
      {
        Access::gAllowedHosts.insert(host);
        stdOut = "success: allow host '";
        stdOut += host.c_str();
        stdOut += "'";
        retc = 0;
      }
      else
      {
        stdErr = "error: unable to store access configuration";
        retc = EIO;
      }
    }
  }

  if (mSubCmd == "unallow")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (user.length())
    {
      int errc = 0;
      uid_t uid = eos::common::Mapping::UserNameToUid(user, errc);
      if (!errc)
      {
        if (Access::gAllowedUsers.count(uid))
        {
          if (Access::StoreAccessConfig())
          {
            Access::gAllowedUsers.erase(uid);
            if (Access::StoreAccessConfig())
            {
              stdOut = "success: unallow user '", stdOut += user.c_str();
              stdOut += "'";
              retc = 0;
            }
            else
            {
              stdErr = "error: unable to store access configuration";
              retc = EIO;
            }
          }
          else
          {
            stdErr = "error: unable to store access configuration";
            retc = EIO;
          }
        }
        else
        {
          stdErr = "error: user '";
          stdErr += user.c_str();
          stdErr += "' is not allowed anyway!";
          retc = ENOENT;
        }
      }
      else
      {
        stdErr = "error: no such user - cannot unallow '";
        stdErr += user.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (group.length())
    {
      int errc = 0;
      gid_t gid = eos::common::Mapping::GroupNameToGid(group, errc);
      if (!errc)
      {
        if (Access::gAllowedGroups.count(gid))
        {
          Access::gAllowedGroups.erase(gid);
          if (Access::StoreAccessConfig())
          {
            stdOut = "success: unallow group '", stdOut += group.c_str();
            stdOut += "'";
            retc = 0;
          }
          else
          {
            stdErr = "error: unable to store access configuration";
            retc = EIO;
          }
        }
        else
        {
          stdErr = "error: group '";
          stdErr += group.c_str();
          stdErr += "' is not allowed anyway!";
          retc = ENOENT;
        }
      }
      else
      {
        stdErr = "error: no such group - cannot unallow '";
        stdErr += group.c_str();
        stdErr += "'";
        retc = EINVAL;
      }
    }
    if (host.length())
    {
      if (Access::gAllowedHosts.count(host))
      {
        Access::gAllowedHosts.erase(host);
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: unallow host '";
          stdOut += host.c_str();
          stdOut += "'";
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: host '";
        stdErr += host.c_str();
        stdErr += "' is not banned anyway!";
        retc = ENOENT;
      }
    }
  }

  if (mSubCmd == "set")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (redirect.length() && ((type.length() == 0) || (type == "r") || (type == "w") || (type == "ENONET") || (type == "ENOENT") ) )
    {
      if (type == "r")
      {
        Access::gRedirectionRules[std::string("r:*")] = redirect;
      }
      else
      {
        if (type == "w")
        {
          Access::gRedirectionRules[std::string("w:*")] = redirect;
        }
        else
        {
          if (type == "ENOENT")
          {
            Access::gRedirectionRules[std::string("ENOENT:*")] = redirect;
          }
          else
          {
            if (type == "ENONET")
            {
              Access::gRedirectionRules[std::string("ENONET:*")] = redirect;
            }
            else
            {
              Access::gRedirectionRules[std::string("*")] = redirect;
            }
          }
        }
      }
      if (Access::StoreAccessConfig())
      {
        stdOut = "success: setting global redirection to '";
        stdOut += redirect.c_str();
        stdOut += "'";
        if (type.length())
        {
          stdOut += " for <";
          stdOut += type.c_str();
          stdOut += ">";
        }
        retc = 0;
      }
      else
      {
        stdErr = "error: unable to store access configuration";
        retc = EIO;
      }
    }
    else
    {
      if (stall.length())
      {
        if ((atoi(stall.c_str()) > 0) && ((type.length() == 0) || (type == "r") || (type == "w") || ((type.find("rate:") == 0)) || (type == "ENONET") || (type == "ENOENT")))
        {
          if (type == "r")
          {
            Access::gStallRules[std::string("r:*")] = stall;
            Access::gStallComment[std::string("r:*")] = mComment.c_str();
          }
          else
          {
            if (type == "w")
            {
              Access::gStallRules[std::string("w:*")] = stall;
              Access::gStallComment[std::string("w:*")] = mComment.c_str();
            }
            else
            {
              if ((type.find("rate:user:") == 0) || (type.find("rate:group:") == 0))
              {
                Access::gStallRules[std::string(type.c_str())] = stall;
                Access::gStallComment[std::string(type.c_str())] = mComment.c_str();
              }
              else
              {
                if (type == "ENONET")
                {
                  Access::gStallRules[std::string("ENONET:*")] = stall;
                  Access::gStallComment[std::string("ENONET:*")] = mComment.c_str();
                }
                else
                {
                  if (type == "ENOENT")
                  {
                    Access::gStallRules[std::string("ENOENT:*")] = stall;
                    Access::gStallComment[std::string("ENOENT:*")] = mComment.c_str();
                  }
                  else
                  {
                    Access::gStallRules[std::string("*")] = stall;
                    Access::gStallComment[std::string("*")] = mComment.c_str();
                  }
                }
              }
            }
          }
          if (Access::StoreAccessConfig())
          {
            if (type.find("rate:") == 0)
            {
              stdOut += "success: setting rate cutoff at ";
              stdOut += stall.c_str();
              stdOut += " Hz for rate:<user|group>:<operation>=";
              stdOut += type.c_str();
            }
            else
            {
              stdOut += "success: setting global stall to ";
              stdOut += stall.c_str();
              stdOut += " seconds";
              if (type.length())
              {
                stdOut += " for <";
                stdOut += type.c_str();
                stdOut += ">";
              }
            }
            retc = 0;
          }
          else
          {
            stdErr = "error: unable to store access configuration";
            retc = EIO;
          }
        }
        else
        {
          stdErr = "error: <stalltime> has to be > 0";
          retc = EINVAL;
        }
      }
      else
      {
        stdErr = "error: redirect or stall has to be defined";
        retc = EINVAL;
      }
    }
  }

  if (mSubCmd == "rm")
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (redirect.length())
    {
      if ((Access::gRedirectionRules.count(std::string("*")) && ((type.length() == 0))) ||
          (Access::gRedirectionRules.count(std::string("r:*")) && (type == "r")) ||
          (Access::gRedirectionRules.count(std::string("w:*")) && (type == "w")))
      {
        stdOut = "success: removing global redirection";
        if (type.length())
        {
          stdOut += " for <";
          stdOut += type.c_str();
          stdOut += ">";
        }
        if (type == "r")
        {
          Access::gRedirectionRules.erase(std::string("r:*"));
        }
        else
        {
          if (type == "w")
          {
            Access::gRedirectionRules.erase(std::string("w:*"));
          }
          else
          {
            if (type == "ENONET")
            {
              Access::gRedirectionRules.erase(std::string("ENONET:*"));
            }
            else
            {
              if (type == "ENOENT")
              {
                Access::gRedirectionRules.erase(std::string("ENOENT:*"));
              }
              else
              {
                Access::gRedirectionRules.erase(std::string("*"));
              }
            }
          }
        }
        if (Access::StoreAccessConfig())
        {
          stdOut = "success: removing redirection ";
          if (type.length())
          {
            stdOut += " for <";
            stdOut += type.c_str();
            stdOut += ">";
          }
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: there is no global redirection defined";
        retc = EINVAL;
      }
    }
    else
    {
      if ((Access::gStallRules.count(std::string("*")) && ((type.length() == 0))) ||
          (Access::gStallRules.count(std::string("r:*")) && (type == "r")) ||
          (Access::gStallRules.count(std::string("w:*")) && (type == "w")) ||
          (Access::gStallRules.count(std::string(type.c_str()))))
      {
        stdOut = "success: removing global stall time";
        if (type.length())
        {
          stdOut += " for <";
          stdOut += type.c_str();
          stdOut += ">";
        }
        if (type == "r")
        {
          Access::gStallRules.erase(std::string("r:*"));
          Access::gStallComment.erase(std::string("r:*"));
        }
        else
        {
          if (type == "w")
          {
            Access::gStallRules.erase(std::string("w:*"));
            Access::gStallComment.erase(std::string("w:*"));
          }
          else
          {
            if ((type.find("rate:user:") == 0) || (type.find("rate:group:") == 0))
            {
              Access::gStallRules.erase(std::string(type.c_str()));
              Access::gStallComment.erase(std::string(type.c_str()));
            }
            else
            {
              Access::gStallRules.erase(std::string("*"));
              Access::gStallComment.erase(std::string("*"));
            }
          }
        }
        if (Access::StoreAccessConfig())
        {
          if ((type.find("rate:user:") == 0) || (type.find("rate:group:") == 0))
          {
            stdOut = "success: removing limit ";
            if (type.length())
            {
              stdOut += " for <";
              stdOut += type.c_str();
              stdOut += ">";
            }
          }
          else
          {
            stdOut = "success: removing stall ";
            if (type.length())
            {
              stdOut += " for <";
              stdOut += type.c_str();
              stdOut += ">";
            }
          }
          retc = 0;
        }
        else
        {
          stdErr = "error: unable to store access configuration";
          retc = EIO;
        }
      }
      else
      {
        stdErr = "error: redirect or stall has to be defined";
        retc = EINVAL;
      }
    }
  }

  if (mSubCmd == "ls")
  {
    eos::common::RWMutexReadLock lock(Access::gAccessMutex);
    std::set<uid_t>::const_iterator ituid;
    std::set<gid_t>::const_iterator itgid;
    std::set<std::string>::const_iterator ithost;
    std::map<std::string, std::string>::const_iterator itred;
    int cnt;

    if (Access::gBannedUsers.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Banned Users ...\n";
        stdOut += "# ....................................................................................\n";
      }
      cnt = 0;
      for (ituid = Access::gBannedUsers.begin(); ituid != Access::gBannedUsers.end(); ituid++)
      {
        cnt++;
        if (monitoring)
        {
          stdOut += "user.banned=";
        }
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }

        if (!translate)
        {
          stdOut += eos::common::Mapping::UidAsString(*ituid).c_str();
        }
        else
        {
          int terrc = 0;
          stdOut += eos::common::Mapping::UidToUserName(*ituid, terrc).c_str();
        }

        stdOut += "\n";
      }
    }

    if (Access::gBannedGroups.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Banned Groups...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (itgid = Access::gBannedGroups.begin(); itgid != Access::gBannedGroups.end(); itgid++)
      {
        cnt++;
        if (monitoring)
          stdOut += "group.banned=";
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }

        if (!translate)
        {
          stdOut += eos::common::Mapping::GidAsString(*itgid).c_str();
        }
        else
        {
          int terrc = 0;
          stdOut += eos::common::Mapping::GidToGroupName(*itgid, terrc).c_str();
        }

        stdOut += "\n";
      }
    }

    if (Access::gBannedHosts.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Banned Hosts ...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (ithost = Access::gBannedHosts.begin(); ithost != Access::gBannedHosts.end(); ithost++)
      {
        cnt++;
        if (monitoring)
          stdOut += "host.banned=";
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }
        stdOut += ithost->c_str();
        stdOut += "\n";
      }
    }

    if (Access::gAllowedUsers.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Allowd Users ...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (ituid = Access::gAllowedUsers.begin(); ituid != Access::gAllowedUsers.end(); ituid++)
      {
        cnt++;
        if (monitoring)
          stdOut += "user.allowed=";
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }

        if (!translate)
        {
          stdOut += eos::common::Mapping::UidAsString(*ituid).c_str();
        }
        else
        {
          int terrc = 0;
          stdOut += eos::common::Mapping::UidToUserName(*ituid, terrc).c_str();
        }

        stdOut += "\n";
      }
    }

    if (Access::gAllowedGroups.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Allowed Groups...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (itgid = Access::gAllowedGroups.begin(); itgid != Access::gAllowedGroups.end(); itgid++)
      {
        cnt++;
        if (monitoring)
          stdOut += "group.allowed=";
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }

        if (!translate)
        {
          stdOut += eos::common::Mapping::GidAsString(*itgid).c_str();
        }
        else
        {
          int terrc = 0;
          stdOut += eos::common::Mapping::GidToGroupName(*itgid, terrc).c_str();
        }

        stdOut += "\n";
      }
    }

    if (Access::gAllowedHosts.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Allowed Hosts ...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (ithost = Access::gAllowedHosts.begin(); ithost != Access::gAllowedHosts.end(); ithost++)
      {
        cnt++;
        if (monitoring)
          stdOut += "host.allowed=";
        else
        {
          char counter[16];
          snprintf(counter, sizeof (counter) - 1, "%02d", cnt);
          stdOut += "[ ";
          stdOut += counter;
          stdOut += " ] ";
        }
        stdOut += ithost->c_str();
        stdOut += "\n";
      }
    }

    if (Access::gRedirectionRules.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Redirection Rules ...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (itred = Access::gRedirectionRules.begin(); itred != Access::gRedirectionRules.end(); itred++)
      {
        cnt++;
        if (monitoring)
        {
          stdOut += "redirect.";
          stdOut += itred->first.c_str();
          stdOut += "=";
        }
        else
        {
          char counter[1024];
          snprintf(counter, sizeof (counter) - 1, "[ %02d ] %32s => ", cnt, itred->first.c_str());
          stdOut += counter;
        }

        stdOut += itred->second.c_str();
        stdOut += "\n";
      }
    }

    if (Access::gStallRules.size())
    {
      if (!monitoring)
      {
        stdOut += "# ....................................................................................\n";
        stdOut += "# Stall Rules ...\n";
        stdOut += "# ....................................................................................\n";
      }

      cnt = 0;
      for (itred = Access::gStallRules.begin(); itred != Access::gStallRules.end(); itred++)
      {
        cnt++;
        if (monitoring)
        {
          stdOut += "stall.";
          stdOut += itred->first.c_str();
          stdOut += "=";
        }
        else
        {
          char counter[1024];
          snprintf(counter, sizeof (counter) - 1, "[ %02d ] %32s => ", cnt, itred->first.c_str());
          stdOut += counter;

        }

        stdOut += itred->second.c_str();
        if (monitoring)
        {
          stdOut += " mComment=\"";
          stdOut += Access::gStallComment[itred->first].c_str();
          stdOut += "\"";
        }
        else
        {
          stdOut += "\t";
          stdOut += Access::gStallComment[itred->first].c_str();
        }
        stdOut += "\n";
      }
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
