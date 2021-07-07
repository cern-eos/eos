//------------------------------------------------------------------------------
//! @file AclHelper.cc
//! @author Elvin Sindrilaru - CERN
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

#include "console/commands/helpers/AclHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "console/ConsoleMain.hh"
#include "proto/Acl.pb.h"
#include <algorithm>

//------------------------------------------------------------------------------
// Set the path doing any necessary modifications to the the absolute path
//------------------------------------------------------------------------------
bool
AclHelper::SetPath(const std::string& in_path)
{
  eos::console::AclProto* acl = mReq.mutable_acl();

  if (in_path.empty()) {
    return false;
  }

  if (in_path.at(0) == '/') {
    acl->set_path(in_path);
  } else {
    acl->set_path(abspath(in_path.c_str()));
  }

  return true;
}

//------------------------------------------------------------------------------
// Check that the id respects the expected format
//------------------------------------------------------------------------------
bool
AclHelper::CheckId(const std::string& id)
{
  static const std::string allowed_chars =
    "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-";

  if ((id.length() > 2) &&
      ((id.at(0) == 'u' && id.at(1) == ':') ||
       (id.at(0) == 'k' && id.at(1) == ':') ||
       (id.at(0) == 'g' && id.at(1) == ':'))) {
    return (id.find_first_not_of(allowed_chars, 2) == std::string::npos);
  }

  if ((id.find("egroup") == 0) && (id.length() > 7) && (id.at(6) == ':')) {
    return (id.find_first_not_of(allowed_chars, 7) == std::string::npos);
  }

  return false;
}

//------------------------------------------------------------------------------
// Check that the flags respect the expected format
//------------------------------------------------------------------------------
bool
AclHelper::CheckFlags(const std::string& flags)
{
  static const std::string allowed_chars = "!+-rwoxmduqc";
  return flags.find_first_not_of(allowed_chars) == std::string::npos;
}

//------------------------------------------------------------------------------
// Check that the rule respects the expected format
//------------------------------------------------------------------------------
bool
AclHelper::CheckRule(const std::string& rule)
{
  size_t pos_del_first, pos_del_last, pos_equal;
  pos_del_first = rule.find(":");
  pos_del_last  = rule.rfind(":");
  pos_equal     = rule.find("=");
  std::string id, flags;

  if ((pos_del_first == pos_del_last) && (pos_equal != std::string::npos)) {
    // u:id=rw+x
    id = std::string(rule.begin(), rule.begin() + pos_equal);

    if (!CheckId(id)) {
      return false;
    }

    flags = std::string(rule.begin() + pos_equal + 1, rule.end());

    if (!CheckFlags(flags)) {
      return false;
    }

    return true;
  } else {
    if ((pos_del_first != pos_del_last) &&
        (pos_del_first != std::string::npos) &&
        (pos_del_last  != std::string::npos)) {
      // u:id:+rwx
      id = std::string(rule.begin(), rule.begin() + pos_del_last);

      if (!CheckId(id)) {
        return false;
      }

      flags = std::string(rule.begin() + pos_del_last + 1, rule.end());

      if (!CheckFlags(flags)) {
        return false;
      }

      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
AclHelper::ParseCommand(const char* arg)
{
  using eos::console::AclProto;
  std::string token;
  const char* temp;
  AclProto* acl = mReq.mutable_acl();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  bool type_set = false;
  // Get opts
  while ((temp = tokenizer.GetToken(false)) != 0) {
    // Trimming
    token = std::string(temp);
    eos::common::trim(token);

    if (token == "") {
      continue;
    }

    if (token == "-lR" || token == "-Rl") {
      acl->set_recursive(true);
      acl->set_op(AclProto::LIST);
      continue;
    }

    if ((token == "-R") || (token == "--recursive")) {
      acl->set_recursive(true);
      continue;
    }
    if ((token == "-f") || (token == "--front")) {
      if (acl->position()) {
        std::cerr << "error: set only one of position or front argument" << std::endl;
        return false;
      }
      acl->set_position(1);
      continue;
    }

    if ((token == "-p") || (token == "--position")) {
      if (acl->position()) {
        std::cerr << "error: set only one of position or front argument" << std::endl;
        return false;
      }

      std::string spos;
      if (!tokenizer.NextToken(spos)) {
        std::cerr << "error: position needs an argument!" << std::endl;
        return false;
      }
      try {
        int pos = std::stoi(spos);
        if (pos > 0)
          acl->set_position(pos);
      } catch (const std::exception& e) {
        std::cerr << "error: position needs to be integer" << std::endl;
        return false;
      }
      continue;
    }

    if ((token == "-l") || (token == "--list")) {
      acl->set_op(AclProto::LIST);
      continue;
    }

    if (token == "--sys") {
      acl->set_sys_acl(true);
      type_set = true;
      continue;
    }

    if (token == "--user") {
      acl->set_sys_acl(false);
      type_set = true;
      continue;
    }

    // If there is unsupported flag
    if (token.at(0) == '-') {
      std::cerr << "error: unrecognized flag " << token << std::endl;
      return false;
    } else {
      if (acl->op() == AclProto::LIST) {
        // Set the absolute path if necessary
        if (!SetPath(token)) {
          std::cerr << "error: failed to the the absolute path" << std::endl;
          return false;
        }
      } else {
        acl->set_op(AclProto::MODIFY);

        if (!CheckRule(token)) {
          std::cerr << "error: unrecognized rule format" << std::endl;
          return false;
        }

        acl->set_rule(token);

        if ((temp = tokenizer.GetToken(false)) != 0) {
          token = std::string(temp);
          eos::common::trim(token);

          if (!SetPath(token)) {
            std::cerr << "error: failed to the the absolute path" << std::endl;
            return false;
          }
        } else {
          return false;
        }
      }

      break;
    }
  }

  if ((acl->op() == AclProto::NONE) ||
      acl->path().empty()) {
    return false;
  }

  // If proc type not enforced try to deduce it
  if (!type_set) {
    return SetDefaultRole();
  }

  return true;
}

//------------------------------------------------------------------------------
// Set the default role - sys or user
//------------------------------------------------------------------------------
bool
AclHelper::SetDefaultRole()
{
  eos::console::AclProto* acl = mReq.mutable_acl();
  XrdOucString cmd("mgm.cmd=whoami");
  std::unique_ptr<XrdOucEnv> env(client_command(cmd, false, nullptr));
  std::string result = (env->Get("mgm.proc.stdout") ? env->Get("mgm.proc.stdout")
                        : "");

  if (!result.empty()) {
    size_t pos = 0;

    if ((pos = result.find("uid=")) != std::string::npos) {
      if ((result.at(pos + 4) >= '0') && (result.at(pos + 4) <= '4') &&
          (result.at(pos + 5) == ' ')) {
        acl->set_sys_acl(true);
      } else {
        acl->set_sys_acl(false);
      }

      return true;
    }

    std::cerr << "error: failed to get uid from whoami command" << std::endl;
    return false;
  }

  std::cerr << "error: failed to execute whoami command" << std::endl;
  return false;
}
