//------------------------------------------------------------------------------
//! @file ShareHelper.cc
//! @author Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "console/commands/helpers/ShareHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/Path.hh"

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
ShareHelper::ParseCommand(const char* arg)
{
  const char* option {nullptr};
  std::string soption;
  eos::console::ShareProto* share = mReq.mutable_share();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if ((cmd == "ls") || cmd.empty() || (cmd == "-m")) {
    eos::console::ShareProto_LsShare* ls = share->mutable_ls();

    while ((option = tokenizer.GetToken())) {
      soption = option;

      if (soption == "-m") {
	ls->set_outformat(eos::console::ShareProto::LsShare::MONITORING);
      } else if (soption == "-l") {
	ls->set_outformat(eos::console::ShareProto::LsShare::LISTING);
      } else {
	if (soption.at(0) == '-') {
	  return false;
	} else {
	  ls->set_selection(soption);
	}
      }
    }
    return true;
  } else {
    eos::console::ShareProto_OperateShare* op = share->mutable_op();
    // share name
    option = tokenizer.GetToken();
    if (!option) return false;
    op->set_share(option);

    if ((cmd == "create")) {
      op->set_op(eos::console::ShareProto::OperateShare::CREATE);
      // acl
      option = tokenizer.GetToken();
      if (!option) return false;
      op->set_acl(option);
      // optional path
      option = tokenizer.GetToken();
      if (option) {
	op->set_path(option);
      }
      return true;
    }

    if ((cmd == "remove")) {
      op->set_op(eos::console::ShareProto::OperateShare::REMOVE);
      return true;
    }

    if ((cmd == "share")) {
      op->set_op(eos::console::ShareProto::OperateShare::SHARE);
      // path
      option = tokenizer.GetToken();
      if (!option) return false;
      op->set_acl(option);
      option = tokenizer.GetToken();
      if (!option) return false;
      op->set_path(option);
      return true;
    }

    if ((cmd == "modify")) {
      op->set_op(eos::console::ShareProto::OperateShare::MODIFY);
      // acl
      option = tokenizer.GetToken();
      if (!option) return false;
      op->set_acl(option);
      return true;
    }

    if ((cmd == "unshare")) {
      op->set_op(eos::console::ShareProto::OperateShare::UNSHARE);
      // path
      option = tokenizer.GetToken();
      if (option) {
	op->set_path(option);
      }
      return true;
    }

    if ((cmd == "access")) {
      op->set_op(eos::console::ShareProto::OperateShare::ACCESS);
      // path
      option = tokenizer.GetToken();
      if (!option) return false;
      op->set_path(option);
      return true;
    }
  }
  return false;
}
