//------------------------------------------------------------------------------
// File: com_proto_register.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "common/StringTokenizer.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

int com_protoregister(char*);
void com_register_help();

//------------------------------------------------------------------------------
//! Class RegisterHelper
//------------------------------------------------------------------------------
class RegisterHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  RegisterHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RegisterHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

bool
RegisterHelper::ParseCommand(const char* arg)
{
  XrdOucString option;
  eos::console::FileRegisterProto* reg = mReq.mutable_record();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();

  do {
    XrdOucString s = tokenizer.GetToken();
    if (s == "-u") {
      reg->set_update(true);
      s = tokenizer.GetToken();
    }
    if (s.length()) {
      std::string param = s.c_str();
      if (param.substr(0,4) == "uid=") {
	if (eos::common::StringConversion::IsDecimalNumber(param.substr(4))) {
	  reg->mutable_owner()->set_uid(std::stoi(param.substr(4).c_str(),0,10));
	} else {
	  reg->mutable_owner()->set_username(param.substr(4));
	}
      } else if (param.substr(0,4) == "gid=") {
	if (eos::common::StringConversion::IsDecimalNumber(param.substr(4))) {
	  reg->mutable_owner()->set_gid(std::stoi(param.substr(4).c_str(),0,10));
	} else {
	  reg->mutable_owner()->set_groupname(param.substr(4));
	}
      } else if (param.substr(0,5) == "size=") {
	if (eos::common::StringConversion::IsDecimalNumber(param.substr(5))) {
	  reg->set_size(std::stoull(param.substr(5).c_str()));
	} else {
	  return false;
	}
      } else if (param.substr(0,5) == "path=") {
	std::string path = param.substr(5);
	path = eos::common::StringConversion::UnQuote(path);
	if (path.empty()) {
	  return false;
	}
	if (path.front() != '/') {
	  return false;
	}
	reg->set_path(path);
      } else if (param.substr(0,6) == "xattr=") {
	std::string key,value;
	std::string kv = eos::common::StringConversion::UnQuote(param.substr(6));
	eos::common::StringConversion::SplitKeyValue(kv, key, value, "=");
	value = eos::common::StringConversion::UnQuote(value);
	if (key.length()) {
	  (*reg->mutable_attr())[key] = value;
	}
      } else if (param.substr(0,6) == "ctime=") {
	std::string t = param.substr(6);
	struct timespec ts;
	if (eos::common::Timing::Timespec_from_TimespecStr(t,ts)) {
	  return false;
	}
	reg->mutable_ctime()->set_sec(ts.tv_sec);
	reg->mutable_ctime()->set_nsec(ts.tv_nsec);
      } else if (param.substr(0,6) == "atime=") {	
	std::string t = param.substr(6);
	struct timespec ts;
	if (eos::common::Timing::Timespec_from_TimespecStr(t,ts)) {
	  return false;
	}
	reg->mutable_atime()->set_sec(ts.tv_sec);
	reg->mutable_atime()->set_nsec(ts.tv_nsec);
      } else if (param.substr(0,13) == "atimeifnewer=") {	
	std::string t = param.substr(13);
	struct timespec ts;
	if (eos::common::Timing::Timespec_from_TimespecStr(t,ts)) {
	  return false;
	}
	reg->mutable_atime()->set_sec(ts.tv_sec);
	reg->mutable_atime()->set_nsec(ts.tv_nsec);
	reg->set_atimeifnewer(true);
      } else if (param.substr(0,6) == "btime=") {
	std::string t = param.substr(6);
	struct timespec ts;
	if (eos::common::Timing::Timespec_from_TimespecStr(t,ts)) {
	  return false;
	}
	reg->mutable_btime()->set_sec(ts.tv_sec);
	reg->mutable_btime()->set_nsec(ts.tv_nsec);
      } else if (param.substr(0,6) == "mtime=") {
	std::string t = param.substr(6);
	struct timespec ts;
	if (eos::common::Timing::Timespec_from_TimespecStr(t,ts)) {
	  return false;
	}
	reg->mutable_mtime()->set_sec(ts.tv_sec);
	reg->mutable_mtime()->set_nsec(ts.tv_nsec);
      } else if (param.substr(0,5) == "mode=") {
	if (eos::common::StringConversion::IsDecimalNumber(param.substr(5))) {
	  reg->set_mode(std::stoi(param.substr(5).c_str(),0, 8));
	} else {
	  return false;
	}
      } else if (param.substr(0,9) == "location=") {
	reg->mutable_locations()->Add(std::stoi(param.substr(9).c_str(),0,10));
      } else if (param.substr(0,9) == "layoutid=") {
	reg->set_layoutid(std::stoi(param.substr(9).c_str(),0,10));
	fprintf(stderr,"layoutid:%d %s\n", std::stoi(param.substr(9).c_str(),0,10), param.substr(9).c_str());
      } else if (param.substr(0,9) == "checksum=") {
	reg->set_checksum(param.substr(9));
      } else {
	std::string path = param;
	path = eos::common::StringConversion::UnQuote(path);
	if (path.empty()) {
	  return false;
	}
	if (path.front() != '/') {
	  return false;
	}
	reg->set_path(path);
      } 
    } else {
      break;
    }
  } while (true);

  if (reg->path().empty()) {
    return false;
  }
  return true;
}

//------------------------------------------------------------------------------
// Register command entry point
//------------------------------------------------------------------------------
int com_protoregister(char* arg)
{
  if (wants_help(arg)) {
    com_register_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  RegisterHelper reg(gGlobalOpts);

  if (!reg.ParseCommand(arg)) {
    com_register_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = reg.Execute(true, true);

  return global_retc;
}

void com_register_help()
{
  std::ostringstream oss;
  oss << "Usage: register [-u] <path> {tag1,tag2,tag3...}"
      << std::endl;
  oss << "          :  when called without the -u flag the parent has to exist while the basename should not exist"  << std::endl;
  oss << "       -u :  if the file exists this will update all the provided meta-data of a file" << std::endl;
  oss << std::endl;
  oss << "       tagN is optional, but can be one or many of: "
      << std::endl;
  oss << "             size=100" << std::endl;
  oss << "             uid=101 | username=foo" << std::endl;
  oss << "             gid=102 | username=bar" << std::endl;
  oss << "             checksum=abcdabcd" << std::endl;
  oss << "             layoutid=00100112" << std::endl;
  oss << "             location=1 location=2 ..." << std::endl;
  oss << "             mode=777" << std::endl;
  oss << "             btime=1670334863.101232" << std::endl;
  oss << "             atime=1670334863.101232" << std::endl;
  oss << "             ctime=1670334863.110123" << std::endl;
  oss << "             mtime=1670334863.11234d" << std::endl;
  oss << "             attr=\"sys.acl=u:100:rwx\"" << std::endl;
  oss << "             attr=\"user.md=private\"" << std::endl;
  oss << "             path=\"/eos/newfile\"   # can be used instead of the regular path argument of the path" << std::endl;
  oss << "             atimeifnewer=1670334863.101233  # only update if this atime is newer than the existing one!" << std::endl;
  std::cerr << oss.str() << std::endl;
}
