// ----------------------------------------------------------------------
// File: proc/user/Who.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/******************A******************************************************
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"
#include <json/json.h>
#include "common/Mapping.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Who()
{
  gOFS->MgmStats.Add("Who", pVid->uid, pVid->gid, 1);
  std::map<std::string, int> usernamecount;
  std::map<std::string, int> authcount;
  std::vector<std::string> tokens;
  std::unordered_map<std::string, time_t> active_tidents;
  std::string delimiter = "^";
  std::string option;
  bool json_format = false;
  bool monitoring = false;
  bool showclients = false;
  bool showall = false;
  bool showauth = false;
  bool showsummary = false;
  Json::Value json;

  if (pOpaque->Get("mgm.option")) {
    option = pOpaque->Get("mgm.option");
  }

  if (pOpaque->Get("mgm.format")) {
    std::string format = pOpaque->Get("mgm.format");
    json_format = (format == "json");
  }

  if ((option.find("m")) != std::string::npos) {
    monitoring = true;
  }

  if ((option.find("c")) != std::string::npos) {
    showclients = true;
  }

  if ((option.find("z")) != std::string::npos) {
    showauth = true;
  }

  if ((option.find("a")) != std::string::npos) {
    showall = true;
  }

  if ((option.find("s")) != std::string::npos) {
    showsummary = true;
  }

  for (size_t i = 0; i < eos::common::Mapping::ActiveTidentsSharded.num_shards();
       ++i) {
    for (auto&& it:  eos::common::Mapping::ActiveTidentsSharded.get_shard(i)) {
      std::string username = "";
      tokens.clear();
      eos::common::StringConversion::Tokenize(it.first, tokens, delimiter);
      uid_t uid = atoi(tokens[0].c_str());
      int terrc = 0;
      username = eos::common::Mapping::UidToUserName(uid, terrc);
      usernamecount[username]++;
      authcount[tokens[2]]++;
      active_tidents.emplace(std::move(it.first), std::move(it.second));
    }
  }


  if (showauth || showall) {
    std::map<std::string, int>::const_iterator it;

    for (it = authcount.begin(); it != authcount.end(); it++) {
      char formatline[1024];

      if (monitoring) {
        snprintf(formatline, sizeof(formatline) - 1, "auth=%s nsessions=%d\n",
                 it->first.c_str(), it->second);
        stdOut += formatline;
      } else if (json_format) {
        Json::Value json_auth;
        json_auth["auth"] = it->first;
        json_auth["nsessions"] = it->second;
        json.append(json_auth);
      } else {
        snprintf(formatline, sizeof(formatline) - 1, "auth   : %-24s := %d sessions\n",
                 it->first.c_str(), it->second);
        stdOut += formatline;
      }
    }
  }

  if (!showclients || showall) {
    std::map<std::string, int>::const_iterator ituname;
    std::map<uid_t, int>::const_iterator ituid;

    for (ituname = usernamecount.begin(); ituname != usernamecount.end();
         ituname++) {
      char formatline[1024];

      if (monitoring) {
        snprintf(formatline, sizeof(formatline) - 1, "uid=%s nsessions=%d\n",
                 ituname->first.c_str(), ituname->second);
        stdOut += formatline;
      } else if (json_format) {
        Json::Value json_user;
        json_user["uid"] = ituname->first;
        json_user["nsessions"] = ituname->second;
        json.append(json_user);
      } else {
        snprintf(formatline, sizeof(formatline) - 1, "user   : %-24s := %d sessions\n",
                 ituname->first.c_str(), ituname->second);
        stdOut += formatline;
      }
    }
  }

  unsigned long long cnt = 0;

  if (showclients || showall || showsummary) {
    for (const auto& it : active_tidents) {

      cnt++;
      std::string username = "";
      tokens.clear();
      // std::string intoken = it->first.c_str();
      eos::common::StringConversion::Tokenize(it.first, tokens, delimiter);
      uid_t uid = atoi(tokens[0].c_str());
      int terrc = 0;
      username = eos::common::Mapping::UidToUserName(uid, terrc);
      char formatline[1024];
      time_t now = time(NULL);

      if (monitoring) {
        snprintf(formatline, sizeof(formatline) - 1,
                 "client=%s uid=%s auth=%s idle=%ld gateway=\"%s\" app=%s\n",
                 tokens[1].c_str(), username.c_str(), tokens[2].c_str(),
                 now - it.second, tokens[3].c_str(),
                 (tokens.size() > 4) ? tokens[4].c_str() : "XRoot");
        stdOut += formatline;
      } else if (json_format) {
        if (!showsummary) {
          Json::Value json_client;
          json_client["client"] = tokens[1];
          json_client["uid"] = username;
          json_client["auth"] = tokens[2];
          json_client["idle"] = (Json::UInt64)(now - it.second);
          json_client["gateway"] = tokens[3];
          json_client["app"] =
            (tokens.size() > 4) ? tokens[4].c_str() : "XRoot";
          json.append(json_client);
        }
      } else {
        snprintf(formatline, sizeof(formatline) - 1,
                 "client : %-10s               := %-40s (%5s) [ %-40s ] { %-8s } %lds idle time \n",
                 username.c_str(), tokens[1].c_str(), tokens[2].c_str(),
                 tokens[3].c_str(),
                 ((tokens.size() > 4) && tokens[4].length())
                 ? tokens[4].c_str()
                 : "XRoot",
                 now - it.second);

        if (!showsummary) {
          stdOut += formatline;
        }
      }
    }
  }

  if (showsummary) {
    char formatline[1024];

    if (monitoring) {
      snprintf(formatline, sizeof(formatline) - 1, "nclients=%llu\n", cnt);
      stdOut += formatline;
    } else if (json_format) {
      Json::Value json_count;
      json_count["nclients"] = Json::UInt64(cnt);
      json.append(json_count);
    } else {
      snprintf(formatline, sizeof(formatline) - 1, "sum(clients) : %llu\n", cnt);
      stdOut += formatline;
    }
  }

  if (json_format) {
    stdOut = "";
    stdJson += Json::StyledWriter().write(json).c_str();
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
