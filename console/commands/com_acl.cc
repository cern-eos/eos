// ----------------------------------------------------------------------
// File com_acl.cc
// Author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
// ----------------------------------------------------------------------

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
  eos::console::AclProto mAclProto; ///< Protobuf representation of the command
  MgmExecute mMgmExec; ///< Wrapper for executing commands at the MGM
};


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
                             std::not1(std::ptr_fun<int, int> (std::isspace))).base(), token.end());

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
      mAclProto.set_usr_acl(true);
      continue;
    }

    // If there is unsupported flag
    if (token.at(0) == '-') {
      std::cerr << "Error: unercognized flag " << token << std:: endl;
      return false;
    } else {
      if (mAclProto.op() == AclProto_OpType::AclProto_OpType_LIST) {
        mAclProto.set_path(token);
      } else {
        mAclProto.set_op(AclProto_OpType::AclProto_OpType_MODIFY);
        mAclProto.set_rule(token);

        if ((temp = tokenizer.GetToken()) != 0) {
          token = std::string(temp);
          token.erase(std::find_if(token.rbegin(), token.rend(),
                                   std::not1(std::ptr_fun<int, int> (std::isspace))).base(), token.end());
          mAclProto.set_path(token);
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
  if (mAclProto.sys_acl() && mAclProto.usr_acl()) {
    std::cerr << "Error: both user and sys flags set" << std::endl;
    return false;
  }

  // Making sure if list flag exists and no other acl type flag exists
  // to usr acl flag be default.
  if ((mAclProto.op() == AclProto_OpType::AclProto_OpType_LIST) &&
      !mAclProto.sys_acl() && !mAclProto.usr_acl()) {
    mAclProto.set_usr_acl(true);
    return true;
  }

  if (!mAclProto.sys_acl() && !mAclProto.usr_acl()) {
    if (mMgmExec.ExecuteCommand("mgm.cmd=whoami")) {
      std::string result = mMgmExec.GetResult();
      size_t pos = 0;

      if ((pos = result.find("uid=")) != std::string::npos) {
        if ((result.at(pos + 4) >= '0') && (result.at(pos + 4) <= '4') &&
            (result.at(pos + 5) == ' ')) {
          mAclProto.set_sys_acl(true);
        } else {
          mAclProto.set_usr_acl(true);
        }

        return true;
      }

      std::cerr << "Error: failed to get uid form whoami command" << std::endl;
      return false;
    }

    std::cerr << "Error: failed to execute whoami command" << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Execute command and display any output information
//------------------------------------------------------------------------------
int
AclHelper::Execute()
{
  size_t sz = mAclProto.ByteSizeLong();
  std::string buffer(sz , '\0');
  google::protobuf::io::ArrayOutputStream aos((void*)buffer.data(), sz);

  if (!mAclProto.SerializeToZeroCopyStream(&aos)) {
    std::cerr << "Error: failed to serialize ProtobolBuffer request"
              << std::endl;
    return EINVAL;
  }

  std::string b64buff;

  if (!eos::common::SymKey::Base64Encode(buffer.data(), buffer.size(), b64buff)) {
    std::cerr << "Error: failed to base64 encode the request" << std::endl;
    return EINVAL;
  }

  std::string cmd = "mgm.cmd.proto=";
  cmd += b64buff;
  int retc = mMgmExec.ExecuteCommand(cmd.c_str());

  if (retc) {
    std::cerr << mMgmExec.GetError() << std::endl;
  } else {
    std::cout << mMgmExec.GetResult() << std::endl;
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
