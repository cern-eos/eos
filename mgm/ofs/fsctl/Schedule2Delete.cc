// ----------------------------------------------------------------------
// File: Schedule2Delete.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "common/Logging.hh"
#include "common/BufferManager.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFsView.hh"
#include "mgm/stat/Stat.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/messaging/Messaging.hh"
#include "proto/Delete.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/json_util.h>
#include <XrdOuc/XrdOucEnv.hh>

//------------------------------------------------------------------------------
// Schedule deletion for FSTs
//------------------------------------------------------------------------------
int
XrdMgmOfs::Schedule2Delete(const char* path,
                           const char* ininfo,
                           XrdOucEnv& env,
                           XrdOucErrInfo& error,
                           eos::common::VirtualIdentity& vid,
                           const XrdSecEntity* client)
{
  static const char* epname = "Schedule2Delete";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Scheduled2Delete");
  gOFS->MgmStats.Add("Schedule2Delete", 0, 0, 1);
  const std::string nodename = (env.Get("mgm.target.nodename") ?
                                env.Get("mgm.target.nodename") : "-none-");
  eos_static_debug("msg=\"handle query\" nodename=%s", nodename.c_str());
  // Retrieve filesystems from the given node and save the following info
  // <fsid, fs_path, fs_queue> in the tuple below
  std::list<std::tuple<unsigned long, std::string, std::string>> fs_info;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if (!FsView::gFsView.mNodeView.count(nodename)) {
      eos_static_warning("msg=\"node is not configured\" name=%s",
                         nodename.c_str());
      return Emsg(epname, error, EINVAL, "schedule delete - unknown node "
                  "[EINVAL]", nodename.c_str());
    }

    for (auto set_it = FsView::gFsView.mNodeView[nodename]->begin();
         set_it != FsView::gFsView.mNodeView[nodename]->end(); ++set_it) {
      auto* fs = FsView::gFsView.mIdView.lookupByID(*set_it);

      // Don't send messages if filesystem is down, booting or offline
      if ((fs == nullptr) ||
          (fs->GetActiveStatus() == eos::common::ActiveStatus::kOffline) ||
          (fs->GetConfigStatus() <= eos::common::ConfigStatus::kOff) ||
          (fs->GetStatus() != eos::common::BootStatus::kBooted)) {
        continue;
      }

      fs_info.emplace_back(fs->GetId(), fs->GetPath(), fs->GetQueue());
    }
  }
  size_t total_del = 0;
  eos::fst::DeletionsProto del_fst;

  // Go through each filesystem and collect unlinked files
  for (const auto& info : fs_info) {
    eos::fst::DeletionsFsProto* del = del_fst.mutable_fs()->Add();
    unsigned long fsid = std::get<0>(info);
    std::string fs_path = std::get<1>(info);
    std::string fs_queue = std::get<2>(info);
    std::unordered_set<eos::IFileMD::id_t> set_fids;

    // Collect all the file ids to be deleted from the current filesystem
    for (auto it_fid = gOFS->eosFsView->getUnlinkedFileList(fsid);
         (it_fid && it_fid->valid()); it_fid->next()) {
      set_fids.insert(it_fid->getElement());
    }

    del->set_fsid(fsid);
    del->set_path(fs_path);

    // Add file ids to the deletion message
    for (auto fid : set_fids) {
      del->mutable_fids()->Add(fid);
      ++total_del;

      if (total_del > 1024) {
        break;
      }
    }

    if (total_del > 1024) {
      break;
    }
  }

  if (total_del) {
    if (EOS_LOGS_DEBUG) {
      std::string json;
      (void) google::protobuf::util::MessageToJsonString(del_fst, &json);
      eos_static_debug("msg=\"query2delete reponse\" data=\"%s\"", json.c_str());
    }

#if GOOGLE_PROTOBUF_VERSION < 3004000
    const auto sz = del_fst.ByteSize();
#else
    const auto sz = del_fst.ByteSizeLong();
#endif
    const uint32_t aligned_sz = eos::common::GetPowerCeil(sz, 2 * eos::common::KB);
    XrdOucBuffer* buff = mXrdBuffPool.Alloc(aligned_sz);

    if (buff == nullptr) {
      eos_static_err("msg=\"requested buffer allocation size too big\" "
                     "req_sz=%llu max_sz=%i", sz, mXrdBuffPool.MaxSize());
      error.setErrInfo(ENOMEM, "requested buffer too big");
      EXEC_TIMING_END("Scheduled2Delete");
      return SFS_ERROR;
    }

    google::protobuf::io::ArrayOutputStream aos((void*)buff->Buffer(), sz);
    buff->SetLen(sz);

    if (!del_fst.SerializeToZeroCopyStream(&aos)) {
      eos_static_err("%s", "msg=\"failed protobuf serialization\"");
      error.setErrInfo(EINVAL, "failed protobuf serialization\"");
      EXEC_TIMING_END("Scheduled2Delete");
      return SFS_ERROR;
    }

    error.setErrInfo(buff->DataLen(), buff);
    gOFS->MgmStats.Add("Scheduled2Delete", 0, 0, total_del);
  } else {
    error.setErrInfo(0, "");
  }

  EXEC_TIMING_END("Scheduled2Delete");
  return SFS_DATA;
}
