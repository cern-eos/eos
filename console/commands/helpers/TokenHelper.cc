//------------------------------------------------------------------------------
//! @file TokenHelper.cc
//! @author Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "console/commands/helpers/TokenHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/Path.hh"

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
TokenHelper::ParseCommand(const char* arg)
{
  const char* option;
  std::string soption;
  eos::console::TokenProto* token = mReq.mutable_token();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::map<std::string, std::set<std::string>> args;

  do {
    std::string element;
    option = tokenizer.GetToken();

    if (option) {
      element = option;
    } else {
      break;
    }

    if (element.empty()) {
      break;
    }

    if (element.substr(0, 2) == "--") {
      element.erase(0, 2);

      if (element != "tree") {
        option = tokenizer.GetToken();

        if (option) {
          std::string value = option;
          args[element].insert(value);
        }
      } else {
        args[element].insert("dummy");
      }
    }
  } while (1);

  if (args.count("token")) {
    // this is a show token request
    token->set_vtoken(*(args["token"].begin()));
  } else {
    bool isdir = false;

    if (
      !args.count("path")
    ) {
      return false;
    } else {
      if (args["path"].begin()->back() == '/') {
        isdir = true;
      }
    }

    eos::common::Path cPath(*args["path"].begin());

    if (!args.count("permission")) {
      args["permission"].insert("rx");
    }

    token->set_path(std::string(cPath.GetPath()) + (isdir ? "/" : ""));
    token->set_permission(*args["permission"].begin());

    if (args.count("expires")) {
      token->set_expires(strtoull((args["expires"].begin())->c_str(), 0, 10));
    } else {
      // ask by default for 5 min token
      token->set_expires(time(NULL) + 300);
    }

    if (args.count("owner")) {
      token->set_owner(*args["owner"].begin());
    }

    if (args.count("group")) {
      token->set_group(*args["group"].begin());
    }

    if (args.count("tree")) {
      token->set_allowtree(true);
    }

    if (args.count("origin")) {
      for (auto it = args["origin"].begin(); it != args["origin"].end(); ++it) {
        std::vector<std::string> info;
        eos::common::StringConversion::Tokenize(*it, info, ":");
        eos::console::TokenAuth* auth = token->add_origins();

        if (info.size() > 0) {
          auth->set_host(info[0]);

          if (info.size() > 1) {
            auth->set_name(info[1]);

            if (info.size() > 2) {
              auth->set_prot(info[2]);
            } else {
              auth->set_prot("(.*)");
            }
          } else {
            auth->set_name("(.*)");
            auth->set_prot("(.*)");
          }
        }
      }
    }
  }

  return true;
}
