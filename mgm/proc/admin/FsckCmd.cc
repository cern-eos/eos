//------------------------------------------------------------------------------
//! @file FsckCmd.cc
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

#include "FsckCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/fsck/Fsck.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
FsckCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::FsckProto fsck = mReqProto.fsck();
  eos::console::FsckProto::SubcmdCase subcmd = fsck.subcmd_case();

  if (mVid.uid != 0) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: only admin can execute this command");
    return reply;
  }

  if (subcmd == eos::console::FsckProto::kStat) {
    std::string output;
    gOFS->mFsckEngine->PrintOut(output);
    reply.set_std_out(std::move(output));
  } else if (subcmd == eos::console::FsckProto::kConfig) {
    const eos::console::FsckProto::ConfigProto& config = fsck.config();
    std::string msg;

    if (!gOFS->mFsckEngine->Config(config.key(), config.value(), msg)) {
      reply.set_retc(EINVAL);

      if (msg.empty()) {
        reply.set_std_err(SSTR("error: failed to set " << config.key()
                               << "=" << config.value()).c_str());
      } else {
        reply.set_std_err(msg);
      }
    }
  } else if (subcmd == eos::console::FsckProto::kReport) {
    const eos::console::FsckProto::ReportProto& report = fsck.report();
    std::set<std::string> tags;

    // Collect all the tags
    for (const auto& elem : report.tags()) {
      tags.insert(elem);
    }

    std::string out;

    if (gOFS->mFsckEngine->Report(out, tags, report.display_per_fs(),
                                  report.display_fxid(), report.display_lfn(),
                                  report.display_json())) {
      reply.set_std_out(out);
    } else {
      reply.set_retc(EINVAL);
      reply.set_std_err(out);
    }
  } else if (subcmd == eos::console::FsckProto::kRepair) {
    std::string out;
    const eos::console::FsckProto::RepairProto& repair = fsck.repair();

    if (gOFS->mFsckEngine->RepairEntry(repair.fid(), repair.fsid_err(),
                                       repair.error(), repair.async(),
                                       out)) {
      reply.set_std_out(out);
    } else {
      reply.set_std_err(out);
      reply.set_retc(EINVAL);
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

EOSMGMNAMESPACE_END
