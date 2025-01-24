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
#include "mgm/FsView.hh"
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

  if ((subcmd != eos::console::FsckProto::kReport) && (mVid.uid != 0)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: only admin can execute this command");
    return reply;
  }

  if (subcmd == eos::console::FsckProto::kStat) {
    const bool monitor_fmt = (mReqProto.format() ==
                              eos::console::RequestProto_FormatType_FUSE);
    std::string output;
    gOFS->mFsckEngine->PrintOut(output, monitor_fmt);
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
    const eos::common::FileSystem::fsid_t fsid_err = repair.fsid_err();

    if (gOFS->mFsckEngine->RepairEntry(repair.fid(), {fsid_err},
                                       repair.error(), repair.async(),
                                       out)) {
      reply.set_std_out(out);
    } else {
      reply.set_std_err(out);
      reply.set_retc(EINVAL);
    }
  } else if (subcmd == eos::console::FsckProto::kCleanOrphans) {
    const eos::console::FsckProto::CleanOrphansProto& clean = fsck.clean_orphans();
    eos::common::FileSystem::fsid_t fsid = clean.fsid();
    std::string query = "/?fst.pcmd=clean_orphans&fst.fsid=" + std::to_string(fsid);
    std::set<std::string> endpoints;

    if (fsid == 0ul) {
      // Send command to all FSTs (nodes)
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

      for (const auto& elem : FsView::gFsView.mNodeView) {
        if (elem.second->GetActiveStatus() == eos::common::ActiveStatus::kOnline) {
          eos_static_debug("msg=\"fsck clean_orphans\" hostport=\"%s\"",
                           elem.second->GetMember("hostport").c_str());
          endpoints.insert(elem.second->GetMember("hostport"));
        }
      }

      // Force clean QDB orphans irrespective of the actual cleanup on disk
      if (clean.force_qdb_cleanup()) {
        gOFS->mFsckEngine->ForceCleanQdbOrphans();
      }
    } else {
      // Send command only to the corresponding FST (node)
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

      if (!fs) {
        reply.set_retc(EINVAL);
        reply.set_std_err("error: given file system does not exist");
        return reply;
      }

      std::ostringstream endpoint;
      endpoint << fs->GetHost() << ":" << fs->getCoreParams().getLocator().getPort();
      endpoints.insert(endpoint.str());
    }

    // Map of responses from each individual endpoint
    std::map<std::string, std::pair<int, std::string>> responses;

    if (gOFS->BroadcastQuery(query, endpoints, responses)) {
      std::ostringstream err_msg;
      err_msg << "error: failed orphans clean for the following endpoints\n";

      for (const auto& elem : responses) {
        if (elem.second.first) {
          err_msg << "node: " << elem.first
                  << " errc: " << elem.second.first
                  << " msg: " << elem.second.second;
        }
      }

      reply.set_std_err(err_msg.str());
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out("info: orphans successfully cleaned");
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

#ifdef EOS_GRPC_GATEWAY
//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed and
// streaming the response via the grpc::ServerWriter
//------------------------------------------------------------------------------
void
FsckCmd::ProcessRequest(grpc::ServerWriter<eos::console::ReplyProto>* writer)
{
  eos::console::ReplyProto StreamReply;
  eos::console::FsckProto fsck = mReqProto.fsck();
  eos::console::FsckProto::SubcmdCase subcmd = fsck.subcmd_case();

  // Check for admin privileges
  if ((subcmd != eos::console::FsckProto::kReport) && (mVid.uid != 0)) {
    StreamReply.set_retc(EPERM);
    StreamReply.set_std_err("error: only admin can execute this command");
    writer->Write(StreamReply);
    return;
  }

  if (subcmd == eos::console::FsckProto::kStat) {
    const bool monitor_fmt = (mReqProto.format() ==
                              eos::console::RequestProto_FormatType_FUSE);
    std::string output;
    gOFS->mFsckEngine->PrintOut(output, monitor_fmt);
    StreamReply.set_std_out(std::move(output));
    writer->Write(StreamReply);
  } else if (subcmd == eos::console::FsckProto::kConfig) {
    const eos::console::FsckProto::ConfigProto& config = fsck.config();
    std::string msg;

    if (!gOFS->mFsckEngine->Config(config.key(), config.value(), msg)) {
      StreamReply.set_retc(EINVAL);

      if (msg.empty()) {
        StreamReply.set_std_err(SSTR("error: failed to set " << config.key()
                                     << "=" << config.value()).c_str());
      } else {
        StreamReply.set_std_err(msg);
      }
    } else {
      StreamReply.set_std_out("info: configuration applied successfully");
    }

    writer->Write(StreamReply);
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
      StreamReply.set_std_out(out);
      writer->Write(StreamReply);
    } else {
      StreamReply.set_retc(EINVAL);
      StreamReply.set_std_err(out);
      writer->Write(StreamReply);
    }
  } else if (subcmd == eos::console::FsckProto::kRepair) {
    std::string out;
    const eos::console::FsckProto::RepairProto& repair = fsck.repair();
    const eos::common::FileSystem::fsid_t fsid_err = repair.fsid_err();

    if (gOFS->mFsckEngine->RepairEntry(repair.fid(), {fsid_err},
                                       repair.error(), repair.async(),
                                       out)) {
      StreamReply.set_std_out(out);
    } else {
      StreamReply.set_std_err(out);
      StreamReply.set_retc(EINVAL);
    }

    writer->Write(StreamReply);
  } else if (subcmd == eos::console::FsckProto::kCleanOrphans) {
    const eos::console::FsckProto::CleanOrphansProto& clean = fsck.clean_orphans();
    eos::common::FileSystem::fsid_t fsid = clean.fsid();
    std::string query = "/?fst.pcmd=clean_orphans&fst.fsid=" + std::to_string(fsid);
    std::set<std::string> endpoints;

    if (fsid == 0ul) {
      // Send command to all FSTs (nodes)
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

      for (const auto& elem : FsView::gFsView.mNodeView) {
        if (elem.second->GetActiveStatus() == eos::common::ActiveStatus::kOnline) {
          eos_static_debug("msg=\"fsck clean_orphans\" hostport=\"%s\"",
                           elem.second->GetMember("hostport").c_str());
          endpoints.insert(elem.second->GetMember("hostport"));
        }
      }

      // Force clean QDB orphans irrespective of the actual cleanup on disk
      if (clean.force_qdb_cleanup()) {
        gOFS->mFsckEngine->ForceCleanQdbOrphans();
      }
    } else {
      // Send command only to the corresponding FST (node)
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

      if (!fs) {
        StreamReply.set_retc(EINVAL);
        StreamReply.set_std_err("error: given file system does not exist");
        writer->Write(StreamReply);
        return;
      }

      std::ostringstream endpoint;
      endpoint << fs->GetHost() << ":" << fs->getCoreParams().getLocator().getPort();
      endpoints.insert(endpoint.str());
    }

    // Map of responses from each individual endpoint
    std::map<std::string, std::pair<int, std::string>> responses;

    if (gOFS->BroadcastQuery(query, endpoints, responses)) {
      std::ostringstream err_msg;
      err_msg << "error: failed orphans clean for the following endpoints\n";

      for (const auto& elem : responses) {
        if (elem.second.first) {
          err_msg << "node: " << elem.first
                  << " errc: " << elem.second.first
                  << " msg: " << elem.second.second;
        }
      }

      StreamReply.set_std_err(err_msg.str());
      StreamReply.set_retc(EINVAL);
    } else {
      StreamReply.set_std_out("info: orphans successfully cleaned");
    }

    writer->Write(StreamReply);
  } else {
    StreamReply.set_retc(EINVAL);
    StreamReply.set_std_err("error: not supported");
    writer->Write(StreamReply);
  }
}

#endif

EOSMGMNAMESPACE_END
