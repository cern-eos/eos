//------------------------------------------------------------------------------
//! @file com_acl.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//!         Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "console/ConsoleMain.hh"
#include "console/MgmExecute.hh"
#include "common/Acl.pb.h"
#include "common/ConsoleRequest.pb.h"
#include <iostream>

using eos::console::AclProto_OpType;

//------------------------------------------------------------------------------
//! Class AclHelper
//------------------------------------------------------------------------------
class AclHelper
{
public:
  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);

  //----------------------------------------------------------------------------
  //! Set default role - sys or user using the identity of the client
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDefaultRole();

  //----------------------------------------------------------------------------
  //! Execute command and display any output information
  //!
  //! @return command return code
  //----------------------------------------------------------------------------
  int Execute();

private:
  //----------------------------------------------------------------------------
  //! Check that the rule respects the expected format
  //!
  //! @param rule client supplied rule
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckRule(const std::string& rule);

  //----------------------------------------------------------------------------
  //! Check that the id respects the expected format
  //!
  //! @param id client supplied id
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckId(const std::string& id);

  //----------------------------------------------------------------------------
  //! Check that the flags respect the expected format
  //!
  //! @param flags client supplied flags
  //!
  //! @return true if correct, otherwise false
  //----------------------------------------------------------------------------
  static bool CheckFlags(const std::string& flags);

  //----------------------------------------------------------------------------
  //! Set the path doing any neccessary modfications to the the absolute path
  //!
  //! @param in_path input path
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetPath(const std::string& in_path);


  eos::console::AclProto mAclProto; ///< Protobuf representation of the command
  MgmExecute mMgmExec; ///< Wrapper for executing commands at the MGM
};

//------------------------------------------------------------------------------
// Set the path doing any neccessary modfications to the the absolute path
//------------------------------------------------------------------------------
bool
AclHelper::SetPath(const std::string& in_path)
{
  if (in_path.empty()) {
    return false;
  }

  if (in_path.at(0) == '/') {
    mAclProto.set_path(in_path);
  } else {
    mAclProto.set_path(abspath(in_path.c_str()));
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

  if ((id.at(0) == 'u' && id.at(1) == ':') ||
      (id.at(0) == 'g' && id.at(1) == ':')) {
    return id.find_first_not_of(allowed_chars, 2) == std::string::npos;
  }

  if (id.find("egroup") == 0 && id.at(6) == ':') {
    return id.find_first_not_of(allowed_chars, 7) == std::string::npos;
  }

  return false;
}

//------------------------------------------------------------------------------
// Check that the flags respect the expected format
//------------------------------------------------------------------------------
bool
AclHelper::CheckFlags(const std::string& flags)
{
  static const std::string allowed_chars = "!+-rwxmduqc";
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
  std::string token;
  const char* temp;
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();

  // Get opts
  while ((temp = tokenizer.GetToken()) != 0) {
    // Trimming
    token = std::string(temp);
    token.erase(std::find_if(token.rbegin(), token.rend(),
                             std::not1(std::ptr_fun<int, int> (std::isspace))).base(),
                token.end());

    if (token == "") {
      continue;
    }

    if (token == "-lR" || token == "-Rl") {
      mAclProto.set_recursive(true);
      mAclProto.set_op(AclProto_OpType::AclProto_OpType_LIST);
      continue;
    }

    if ((token == "-R") || (token == "--recursive")) {
      mAclProto.set_recursive(true);
      continue;
    }

    if ((token == "-l") || (token == "--lists")) {
      mAclProto.set_op(AclProto_OpType::AclProto_OpType_LIST);
      continue;
    }

    if (token == "--sys") {
      mAclProto.set_sys_acl(true);
      continue;
    }

    if (token == "--user") {
      mAclProto.set_sys_acl(false);
      continue;
    }

    // If there is unsupported flag
    if (token.at(0) == '-') {
      std::cerr << "error: unercognized flag " << token << std:: endl;
      return false;
    } else {
      if (mAclProto.op() == AclProto_OpType::AclProto_OpType_LIST) {
        // Set the absolute path if neccessary
        if (!SetPath(token)) {
          std::cerr << "error: failed to the the absolute path" << std::endl;
          return false;
        }
      } else {
        mAclProto.set_op(AclProto_OpType::AclProto_OpType_MODIFY);

        if (!CheckRule(token)) {
          std::cerr << "error: unrecognized rule format" << std::endl;
          return false;
        }

        mAclProto.set_rule(token);

        if ((temp = tokenizer.GetToken()) != 0) {
          token = std::string(temp);
          token.erase(std::find_if(token.rbegin(), token.rend(),
                                   std::not1(std::ptr_fun<int, int> (std::isspace))).base(),
                      token.end());

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

  if ((mAclProto.op() == AclProto_OpType::AclProto_OpType_NONE) ||
      mAclProto.path().empty()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Set the default role - sys or user
//------------------------------------------------------------------------------
bool
AclHelper::SetDefaultRole()
{
  if (!mAclProto.sys_acl()) {
    if (!mMgmExec.ExecuteCommand("mgm.cmd=whoami")) {
      std::string result = mMgmExec.GetResult();
      size_t pos = 0;

      if ((pos = result.find("uid=")) != std::string::npos) {
        if ((result.at(pos + 4) >= '0') && (result.at(pos + 4) <= '4') &&
            (result.at(pos + 5) == ' ')) {
          mAclProto.set_sys_acl(true);
        } else {
          mAclProto.set_sys_acl(false);
        }

        return true;
      }

      std::cerr << "error: failed to get uid form whoami command" << std::endl;
      return false;
    }

    std::cerr << "error: failed to execute whoami command" << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Execute command and display any output information
// @todo (esindril): This should be moved to console main and made generic
//       using templates.
//------------------------------------------------------------------------------
int
AclHelper::Execute()
{
  eos::console::RequestProto req;
  req.set_type(eos::console::RequestProto_OpType::RequestProto_OpType_ACL);
  *req.mutable_acl() = mAclProto;
  size_t sz = req.ByteSize();
  std::string buffer(sz , '\0');
  google::protobuf::io::ArrayOutputStream aos((void*)buffer.data(), sz);

  if (!req.SerializeToZeroCopyStream(&aos)) {
    std::cerr << "error: failed to serialize ProtobolBuffer request"
              << std::endl;
    return EINVAL;
  }

  std::string b64buff;

  if (!eos::common::SymKey::Base64Encode(buffer.data(), buffer.size(), b64buff)) {
    std::cerr << "error: failed to base64 encode the request" << std::endl;
    return EINVAL;
  }

  std::string cmd = "mgm.cmd.proto=";
  cmd += b64buff;
  int retc = mMgmExec.ExecuteCommand(cmd.c_str());

  if (retc) {
    std::cerr << mMgmExec.GetError() << std::endl;
  } else {
    if (mMgmExec.GetResult().size()) {
      std::cout << mMgmExec.GetResult() << std::endl;
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_acl_help()
{
  std::cerr << "Usage: eos acl [-l|--list] [-R|--recursive]";
  std::cerr << "[--sys|--user] <rule> <path>" << std::endl;
  std::cerr << std::endl;
  std::cerr << "    --help           Print help" << std::endl;
  std::cerr << "-R, --recursive      Apply on directories recursively" <<
            std::endl;
  std::cerr << "-l, --lists          List ACL rules" << std::endl;
  std::cerr << "    --user           Set user.acl rules on directory" <<
            std::endl;
  std::cerr << "    --sys            Set sys.acl rules on directory" << std::endl;
  std::cerr << "<rule> is created based on chmod rules. " << std::endl;
  std::cerr <<
            "Every rule begins with [u|g|egroup] followed with : and identifier." <<
            std::endl;
  std::cerr <<  std::endl;
  std::cerr << "Afterwards can be:" << std::endl;
  std::cerr << "= for setting new permission ." << std::endl;
  std::cerr << ": for modification of existing permission." << std::endl;
  std::cerr << std::endl;
  std::cerr << "This is followed by the rule definition." << std::endl;
  std::cerr << "Every ACL flag can be added with + or removed with -, or in case"
            << std::endl;
  std::cerr << "of setting new ACL permission just enter the ACL flag." <<
            std::endl;
}

//------------------------------------------------------------------------------
// Acl command entrypoint
//------------------------------------------------------------------------------
int com_acl(char* arg)
{
  if (wants_help(arg)) {
    com_acl_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  AclHelper acl;

  if (!acl.ParseCommand(arg) || !acl.SetDefaultRole()) {
    com_acl_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = acl.Execute();
  return global_retc;
}
