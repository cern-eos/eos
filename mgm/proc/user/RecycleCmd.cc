//------------------------------------------------------------------------------
//! @file RecycleCmd.cc
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

#include "RecycleCmd.hh"
#include "mgm/Recycle.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
eos::console::ReplyProto
RecycleCmd::ProcessRequest() noexcept
{
  using eos::mgm::Recycle;
  using eos::console::RecycleProto;
  std::string err_msg;
  eos::console::ReplyProto reply;
  RecycleProto recycle = mReqProto.recycle();
  RecycleProto::SubcmdCase subcmd = recycle.subcmd_case();
  std::string std_out, std_err;

  if (subcmd == RecycleProto::kLs) {
    const eos::console::RecycleProto_LsProto& ls = recycle.ls();

    if (ls.date().empty()) {
      Recycle::PrintOld(std_out, std_err, mVid, ls.monitorfmt(),
                        !ls.numericids(), ls.fulldetails());
    }

    Recycle::Print(std_out, std_err, mVid, ls.monitorfmt(),
                   !ls.numericids(), ls.fulldetails(),
                   ls.date(), ls.all());

    if (std_out.length()) {
      reply.set_std_out(std_out.c_str());
    }

    if (std_err.length()) {
      reply.set_std_err(std_err.c_str());
    }

    reply.set_retc(0);
  } else if (subcmd == RecycleProto::kPurge) {
    const eos::console::RecycleProto_PurgeProto& purge = recycle.purge();
    reply.set_retc(Recycle::Purge(std_out, std_err, mVid, purge.date(),
				  purge.all(),
				  purge.key()));

    if (reply.retc()) {
      reply.set_std_err(std_err.c_str());
    } else {
      reply.set_std_out(std_out.c_str());
    }
  } else if (subcmd == RecycleProto::kRestore) {
    const eos::console::RecycleProto_RestoreProto& restore = recycle.restore();
    reply.set_retc(Recycle::Restore(std_out, std_err, mVid, restore.key().c_str(),
                                    restore.forceorigname(),
                                    restore.restoreversions(),
				    restore.makepath()));

    if (reply.retc()) {
      reply.set_std_err(std_err.c_str());
    } else {
      reply.set_std_out(std_out.c_str());
    }
  } else if (subcmd == RecycleProto::kConfig) {
    using eos::mgm::Quota;
    int retc = 0;
    const eos::console::RecycleProto_ConfigProto& config = recycle.config();

    if (mVid.uid != 0) {
      reply.set_std_err("error: you need to be root to configure the recycle bin"
                        " and/or recycle policies");
      reply.set_retc(EPERM);
      return reply;
    }

    if (config.op() == eos::console::RecycleProto_ConfigProto::ADD_BIN) {
      retc = Recycle::Config(std_out, std_err, mVid, "--add-bin",
                             config.subtree());
    } else if (config.op() == eos::console::RecycleProto_ConfigProto::RM_BIN) {
      retc = Recycle::Config(std_out, std_err, mVid, "--remove-bin",
                             config.subtree());
    } else if (config.op() == eos::console::RecycleProto_ConfigProto::LIFETIME) {
      retc = Recycle::Config(std_out, std_err, mVid, "--lifetime",
                             std::to_string(config.lifetimesec()));
    } else if (config.op() == eos::console::RecycleProto_ConfigProto::RATIO) {
      retc = Recycle::Config(std_out, std_err, mVid, "--ratio",
                             std::to_string(config.ratio()));
    } else if (config.op() == eos::console::RecycleProto_ConfigProto::SIZE) {
      std::string msg;

      if (!Quota::SetQuotaTypeForId(Recycle::gRecyclingPrefix, Quota::gProjectId,
                                    Quota::IdT::kGid, Quota::Type::kVolume, config.size(),
                                    msg, retc)) {
        reply.set_std_err(msg);
        reply.set_retc(retc);
        return reply;
      }
    } else if (config.op() == eos::console::RecycleProto_ConfigProto::INODES) {
      std::string msg;

      if (!Quota::SetQuotaTypeForId(Recycle::gRecyclingPrefix, Quota::gProjectId,
                                    Quota::IdT::kGid, Quota::Type::kInode, config.size(),
                                    msg, retc)) {
        reply.set_std_err(msg);
        reply.set_retc(retc);
        return reply;
      }
    }

    reply.set_retc(retc);

    if (retc == 0) {
      reply.set_std_out(std_out.c_str());
    } else {
      reply.set_std_err(std_err.c_str());
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

EOSMGMNAMESPACE_END
