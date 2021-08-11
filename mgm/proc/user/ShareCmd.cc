//------------------------------------------------------------------------------
//! @file ShareCmd.cc
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

#include "ShareCmd.hh"
#include "mgm/Share.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
eos::console::ReplyProto
ShareCmd::ProcessRequest() noexcept
{
  using eos::mgm::Share;
  using eos::console::ShareProto;
  std::string err_msg;
  eos::console::ReplyProto reply;
  ShareProto shareproto = mReqProto.share();
  ShareProto::SubcmdCase subcmd = shareproto.subcmd_case();
  std::string std_out, std_err;

  if (subcmd == ShareProto::kLs) {
    const eos::console::ShareProto_LsShare& ls = shareproto.ls();
    bool monitoring = (ls.outformat() == ls.MONITORING)? true:false;
    gOFS->mShare->getProc().List(mVid, "").Dump(std_out, monitoring);
    if (std_out.length()) {
      reply.set_std_out(std_out.c_str());
    }

    if (std_err.length()) {
      reply.set_std_err(std_err.c_str());
    }

    reply.set_retc(0);
  } else if (subcmd == ShareProto::kOp) {
    const eos::console::ShareProto_OperateShare& share = shareproto.op();

    std::string std_out, std_err;

    if (share.op() == eos::console::ShareProto::OperateShare::CREATE) {
      // create
      std::string share_name = share.share();
      std::string share_root = share.path();
      std::string share_acl = share.acl();
      if (gOFS->mShare->getProc().Create(mVid, share_name, share_root, share_acl)) {
	if (errno == EEXIST) {
	  std_err = std::string("error: share '") + share_name + std::string("' already exists for ") + mVid.uid_string + std::string("\n");
	  reply.set_retc(EEXIST);
	} else {
	  std_err = std::string("error: share '") + share_name + std::string("' could not be created - errno:") + std::to_string(errno) + std::string("\n");
	  reply.set_retc(errno?errno:EFAULT);
	}
      } else {
	std_out = std::string("success: share '") + share_name + std::string("' has been created\n");
      }
    } else if (share.op() == eos::console::ShareProto::OperateShare::REMOVE) {
      // remove
      std::string share_name = share.share();
      std_out = "remove";
      if (gOFS->mShare->getProc().Delete(mVid, share_name)) {
	if (errno == ENOENT) {
	  std_err = std::string("error: share '") + share_name + std::string("' does not exists for ") + mVid.uid_string + std::string("\n");
	  reply.set_retc(ENOENT);
	} else {
	  std_err = std::string("error: share '") + share_name + std::string("' could not be removed - errno:") + std::to_string(errno) + std::string("\n");
	  reply.set_retc(errno?errno:EFAULT);
	}
      } else {
	std_out = std::string("success: share '") + share_name + std::string("' has been removed\n");
      }
    } else if (share.op() == eos::console::ShareProto::OperateShare::SHARE) {
      // share
      // create
      std::string share_name = share.share();
      std::string share_root = share.path();
      std::string share_acl = share.acl();
      if (gOFS->mShare->getProc().Share(mVid, share_name, share_root, share_acl)) {
	if (errno == EEXIST) {
	  std_err = std::string("error: share '") + share_name + std::string("' already shared for ") + mVid.uid_string + std::string("\n");
	  reply.set_retc(EEXIST);
	} else {
	  std_err = std::string("error: share '") + share_name + std::string("' could not been shared - errno:") + std::to_string(errno) + std::string("\n");
	  reply.set_retc(errno?errno:EFAULT);
	}
      } else {
	std_out = std::string("success: share '") + share_name + std::string("' has been shared\n");
	reply.set_retc(0);
      }
    } else if (share.op() == eos::console::ShareProto::OperateShare::UNSHARE) {
      // unshare
      std::string share_name = share.share();
      std::string share_root = share.path();
      if (gOFS->mShare->getProc().UnShare(mVid, share_name, share_root)) {
	std_err = std::string("error: share '") + share_name + std::string("' could not been unshared - errno:") + std::to_string(errno) + std::string("\n");
	reply.set_retc(errno?errno:EFAULT);
      } else {
	std_out = std::string("success: share '") + share_name + std::string("' has been unshared\n");
      }
    } else if (share.op() == eos::console::ShareProto::OperateShare::ACCESS) {
      // access
      std::string share_name = share.share();
      std::string out;
      std::string user = share.user();
      std::string group = share.group();

      if (gOFS->mShare->getProc().Access(mVid, share_name,out,user,group)) {
	if (errno == ENOENT) {
	  std_err = std::string("error: share '") + share_name + std::string("' does not exist ") + mVid.uid_string + std::string("\n");
	  reply.set_retc(EEXIST);
	} else {
	  std_err = std::string("error: share '") + share_name + std::string("' could not be accessed - errno:") + std::to_string(errno) + std::string("\n");
	  reply.set_retc(errno?errno:EFAULT);
	}
      } else {
	std_out = out;
      }
    } else if (share.op() == eos::console::ShareProto::OperateShare::MODIFY) {
      // modify acl
      std::string share_name = share.share();
      std::string share_acl = share.acl();
      if (gOFS->mShare->getProc().Modify(mVid, share_name, share_acl)) {
	if (errno == ENOENT) {
	  std_err = std::string("error: share '") + share_name + std::string("' does not exist ") + mVid.uid_string + std::string("\n");
	  reply.set_retc(EEXIST);
	} else {
	  std_err = std::string("error: share '") + share_name + std::string("' could not be modified - errno:") + std::to_string(errno) + std::string("\n");
	  reply.set_retc(errno?errno:EFAULT);
	}
      } else {
	std_out = std::string("success: share '") + share_name + std::string("' has been modified\n");
      }
    } else {
      reply.set_retc(EINVAL);
      std_err = "error: operation not defined";
    }
    if (reply.retc()) {
      reply.set_std_err(std_err.c_str());
    } else {
      reply.set_std_out(std_out.c_str());
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

EOSMGMNAMESPACE_END
