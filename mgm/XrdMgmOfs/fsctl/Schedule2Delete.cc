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
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFsView.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/MessagingRealm.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/FsView.hh"
#include "mgm/Messaging.hh"
#include "proto/Delete.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/json_util.h>
#include "XrdOuc/XrdOucEnv.hh"

//----------------------------------------------------------------------------
// Utility function of sending message declared in anonymous namespace
//----------------------------------------------------------------------------
namespace
{
bool SendDeleteMsg(int fsid, const std::string& local_prefix,
                   const char* idlist,
                   const char* receiver,
                   std::chrono::seconds capValidity)
{
  using namespace eos::common;
  XrdOucString capability;
  capability = "&mgm.access=delete";
  capability += "&mgm.manager=";
  capability += gOFS->ManagerId.c_str();
  capability += "&mgm.fsid=";
  capability += fsid;
  capability += "&mgm.localprefix=";
  capability += local_prefix.c_str();
  capability += "&mgm.fids=";
  capability += idlist;
  XrdOucEnv incapenv(capability.c_str());
  XrdOucEnv* outcapenv = 0;
  SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  int rc = SymKey::CreateCapability(&incapenv, outcapenv, symkey, capValidity);

  if (rc) {
    eos_static_err("unable to create capability - incap=%s errno=%u",
                   capability.c_str(), rc);
  } else {
    int caplen = 0;
    XrdOucString msgbody = "mgm.cmd=drop";
    msgbody += outcapenv->Env(caplen);
    eos::mq::MessagingRealm::Response response =
      gOFS->mMessagingRealm->sendMessage("deletion", msgbody.c_str(), receiver);

    if (!response.ok()) {
      eos_static_err("msg=\"unable to send deletion message to %s\"",
                     receiver);
      rc = -1;
    }
  }

  if (outcapenv) {
    delete outcapenv;
  }

  return (rc == 0);
}
}

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
  eos_static_debug("nodename=%s", nodename.c_str());
  bool reply_with_data = (strcmp(env.Get("mgm.pcmd"), "query2delete") == 0);
  // Retrieve filesystems from the given node and save the following info
  // <fsid, fs_path, fs_queue> in the tuple below
  std::list<std::tuple<unsigned long, std::string, std::string>> fs_info;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex, __FUNCTION__,
                                      __LINE__, __FILE__);

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
    {
      eos::common::RWMutexReadLock ns_rd_lock;

      // This look is only needed for the in-memory namespace implementation
      if (gOFS->eosView->inMemory()) {
        ns_rd_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
      }

      // Collect all the file ids to be deleted from the current filesystem
      for (auto it_fid = gOFS->eosFsView->getUnlinkedFileList(fsid);
           (it_fid && it_fid->valid()); it_fid->next()) {
        set_fids.insert(it_fid->getElement());
      }
    }

    // Reply for query2delete
    if (reply_with_data) {
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
    } else { // Reply for schedule2delere request
      XrdOucString receiver = fs_queue.c_str();
      XrdOucString idlist = "";
      int ndeleted = 0;

      // Loop over all files and emit deletion message
      for (auto fid : set_fids) {
        eos_static_info("msg=\"add to deletion message\" fxid=%08llx fsid=%lu",
                        fid, fsid);
        idlist += eos::common::FileId::Fid2Hex(fid).c_str();
        idlist += ",";
        ++ndeleted;
        ++total_del;

        if (ndeleted > 1024) {
          // Send deletions in bunches of max 1024 for efficiency
          SendDeleteMsg(fsid, fs_path, idlist.c_str(),
                        receiver.c_str(), mCapabilityValidity);
          ndeleted = 0;
          idlist = "";
        }
      }

      // Send the remaining ids
      if (idlist.length()) {
        SendDeleteMsg(fsid, fs_path, idlist.c_str(),
                      receiver.c_str(), mCapabilityValidity);
      }
    }
  }

  if (total_del) {
    if (reply_with_data) {
      if (EOS_LOGS_DEBUG) {
        std::string json;
        (void) google::protobuf::util::MessageToJsonString(del_fst, &json);
        eos_static_debug("msg=\"query2delete reponse\" data=\"%s\"", json.c_str());
      }

      const auto sz = del_fst.ByteSizeLong();
      XrdOucBuffer* buff = mXrdBuffPool.Alloc(sz);

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
    } else {
      error.setErrInfo(0, "submitted");
    }

    gOFS->MgmStats.Add("Scheduled2Delete", 0, 0, total_del);
  } else {
    error.setErrInfo(0, "");
  }

  EXEC_TIMING_END("Scheduled2Delete");
  return SFS_DATA;
}
