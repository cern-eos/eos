//------------------------------------------------------------------------------
// File: EvictCmd.cc
// Author: Jozsef Makai - CERN
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

#include "common/Path.hh"
#include "common/Timing.hh"
#include "EvictCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/EosCtaReporter.hh"
#include "mgm/Acl.hh"
#include "common/Constants.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::RequestProto eos::mgm::EvictCmd::convertStagerRmToEvict(const eos::console::RequestProto & req, std::ostringstream & errStream, int & ret_c) {

  struct timespec ts_now;
  eos::console::RequestProto new_req;
  auto req_evict = new_req.mutable_evict();
  auto req_stagerrm = req.stagerrm();

  for (int i = 0; i < req_stagerrm.file_size(); i++) {
    const auto& file_stagerrm = req_stagerrm.file(i);
    switch (file_stagerrm.File_case()) {
    case eos::console::StagerRmProto::FileProto::kPath:
      req_evict->add_file()->set_path(file_stagerrm.path());
      break;
    case eos::console::StagerRmProto::FileProto::kFid:
      req_evict->add_file()->set_fid(file_stagerrm.fid());
      break;
    default:
      errStream << "error: Received a file with neither a path nor an fid, unable to convert stagerrm request to evict request" <<
          std::endl;
      ret_c = EINVAL;
      EosCtaReporterEvict eosLog;
      eosLog
          .addParam(EosCtaReportParam::SEC_APP, "tape_evict")
          .addParam(EosCtaReportParam::LOG, std::string(gOFS->logId))
          .addParam(EosCtaReportParam::RUID, mVid.uid)
          .addParam(EosCtaReportParam::RGID, mVid.gid)
          .addParam(EosCtaReportParam::TD, mVid.tident.c_str())
          .addParam(EosCtaReportParam::TS, ts_now.tv_sec)
          .addParam(EosCtaReportParam::TNS, ts_now.tv_nsec)
          .addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      break;
    }
  }
  return new_req;
}

eos::console::ReplyProto
eos::mgm::EvictCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream errStream;
  std::ostringstream outStream;
  bool allReplicasRemoved = false;
  int ret_c = 0;

  // TODO: Remove this segment of code when the StagerRm command is deprecated, and replace by the line bellow
  eos::console::RequestProto req;
  if (mReqProto.command_case() == eos::console::RequestProto::kStagerRm) {
    req = convertStagerRmToEvict(mReqProto, errStream, ret_c);
  } else {
    req = mReqProto;
  }
  const auto& evict = req.evict();

  // TODO: Replace the code removed above by this line
  // const auto& evict = mReqProto.evict();

  XrdOucErrInfo errInfo;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  struct timespec ts_now;
  eos::common::Timing::GetTimeSpec(ts_now);
  std::optional<uint64_t> fsid =
      evict.has_evictsinglereplica() ? std::optional(evict.evictsinglereplica().fsid()) : std::nullopt;
  bool force = evict.force();

  if (fsid.has_value() && !force) {
    reply.set_retc(EINVAL);
    errStream << "error: Parameter 'fsid' can only be used with 'force'" << std::endl;
    reply.set_std_err(errStream.str());
    reply.set_std_out(outStream.str());
    return reply;
  }

  int count_some_disk_replicas_removed = 0;
  int count_all_disk_replicas_removed = 0;
  int count_evict_counter_not_zero = 0;
  for (int i = 0; i < evict.file_size(); i++) {
    EosCtaReporterEvict eosLog;
    eosLog
    .addParam(EosCtaReportParam::SEC_APP, "tape_evict")
    .addParam(EosCtaReportParam::LOG, std::string(gOFS->logId))
    .addParam(EosCtaReportParam::RUID, mVid.uid)
    .addParam(EosCtaReportParam::RGID, mVid.gid)
    .addParam(EosCtaReportParam::TD, mVid.tident.c_str())
    .addParam(EosCtaReportParam::TS, ts_now.tv_sec)
    .addParam(EosCtaReportParam::TNS, ts_now.tv_nsec);
    const auto& file = evict.file(i);
    std::string path;
    std::string err;

    switch (file.File_case()) {
    case eos::console::EvictProto::FileProto::kPath:
      path = file.path();

      if (0 == path.length()) {
        errStream << "error: Received an empty string path" << std::endl;
        ret_c = EINVAL;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(EosCtaReportParam::PATH, path);
      break;

    case eos::console::EvictProto::FileProto::kFid:
      GetPathFromFid(path, file.fid(), err);

      if (0 == path.length()) {
        errStream << "error: Received an unknown fid: value=" << file.fid() <<
                  std::endl;
        ret_c = EINVAL;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(EosCtaReportParam::PATH, path);
      break;

    default:
      errStream << "error: Received a file with neither a path nor an fid" <<
                std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // check that we have the correct permission
    eos::common::Path cPath(path.c_str());
    errInfo.clear();

    if (gOFS->_access(cPath.GetParentPath(), P_OK, errInfo, mVid, "") != 0) {
      errStream << "error: you don't have 'p' acl flag permission on path '"
                << cPath.GetParentPath() << "'" << std::endl;
      ret_c = EPERM;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;
    errInfo.clear();

    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'" <<
                std::endl;
      ret_c = errno;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file with path '" << path << "'" << std::endl;
      ret_c = ENODATA;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      errStream << "error: given path is a directory '" << path << "'" << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    struct stat buf;

    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                    false) != 0) {
      errStream << "error: unable to run stat for replicas on path '" << path << "'"
                << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // we don't remove anything if it's not on tape
    if ((buf.st_mode & EOS_TAPE_MODE_T) == 0) {
      errStream << "error: no tape replicas for file '" << path << "'" << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    int diskReplicaCount = 0;
    XrdOucString options;

    if (fsid.has_value()) {
      auto fmd = gOFS->eosView->getFile(path.c_str());
      bool diskReplicaFound = false;
      for (auto location : fmd->getLocations()) {
        // Ignore tape replica
        if (location == eos::common::TAPE_FS_ID) continue;
        if (location == fsid.value()) diskReplicaFound = true;
        ++diskReplicaCount;
      }
      if (!diskReplicaFound) {
        eos_static_err("msg=\"unable to find disk replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: unable to find disk replica of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_FSID,  fsid.value());
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
        continue;
      }
    } else {
      auto fmd = gOFS->eosView->getFile(path.c_str());
      for (auto location : fmd->getLocations()) {
        // Ignore tape replica
        if (location == eos::common::TAPE_FS_ID) continue;
        ++diskReplicaCount;
      }
      if (diskReplicaCount == 0) {
        eos_static_err("msg=\"unable to find any disk replica of %s\" reason=\"%s\"",
                       path.c_str(), errInfo.getErrText());
        errStream << "error: unable to find any disk replica of '" << path << "'" <<
            std::endl;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
        continue;
      }
    }

    errInfo.clear();

    if (fsid.has_value() && force) {
      // Drop single stripe
      if (gOFS->_dropstripe(path.c_str(), 0, errInfo, root_vid, fsid.value(), true) != 0) {
        eos_static_err("msg=\"could not delete replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: could not delete replica of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_FSID, fsid.value());
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
      } else {
        if (diskReplicaCount <= 1) {
          allReplicasRemoved = true;
          count_all_disk_replicas_removed++;
        } else {
          count_some_disk_replicas_removed++;
        }
      }
    } else {
      // May drop all stripes
      if (!force) {
        // Check the eviction counter first, if not force
        int evictionCounter = 0;
        try {
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosView->getFile(path.c_str());

          if (fmd->hasAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME)) {
            evictionCounter = std::stoi(fmd->getAttribute(
                eos::common::RETRIEVE_EVICT_COUNTER_NAME));
          }

          eosLog.addParam(EosCtaReportParam::EVICTCMD_EVICTCOUNTER, evictionCounter);
          evictionCounter = std::max(0, evictionCounter - 1);
          fmd->setAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME,
                            std::to_string(evictionCounter));
          gOFS->eosView->updateFileStore(fmd.get());
        } catch (eos::MDException& ex) {
          eos_static_err("msg=\"could not update eviction counter for file %s\"",
                         path.c_str());
        }

        if (evictionCounter > 0) {
          // Do not remove if eviction counter not zero
          eosLog.addParam(EosCtaReportParam::EVICTCMD_FILEREMOVED, false);
          count_evict_counter_not_zero++;
          continue;
        }
      }

      // Drop all stripes
      if (gOFS->_dropallstripes(path.c_str(), errInfo, root_vid, true) != 0) {
        eos_static_err("msg=\"could not delete all disk replicas of %s\" reason=\"%s\"",
                       path.c_str(), errInfo.getErrText());
        errStream << "error: could not delete all disk replicas of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
      } else {
        count_all_disk_replicas_removed++;
        allReplicasRemoved = true;
      }
    }

    if (allReplicasRemoved) {
      // reset the retrieves counter in case of success
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      try {
        auto fmd = gOFS->eosView->getFile(path.c_str());
        fmd->setAttribute(eos::common::RETRIEVE_REQID_ATTR_NAME, "");
        fmd->setAttribute(eos::common::RETRIEVE_REQTIME_ATTR_NAME, "");
        fmd->removeAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME);
        gOFS->eosView->updateFileStore(fmd.get());
      } catch (eos::MDException& ex) {
        eos_static_err("msg=\"could not reset Prepare request ID list or eviction counter for "
                       "file %s. Try removing the %s, %s or %s attributes\"",
                       path.c_str(), eos::common::RETRIEVE_REQID_ATTR_NAME,
                       eos::common::RETRIEVE_REQTIME_ATTR_NAME,
                       eos::common::RETRIEVE_EVICT_COUNTER_NAME);
      }
      if (fsid.has_value()) {
        eosLog.addParam(EosCtaReportParam::EVICTCMD_FSID, fsid.value());
      }
      eosLog.addParam(EosCtaReportParam::EVICTCMD_FILEREMOVED, true);
    }
  }

  reply.set_retc(ret_c);
  reply.set_std_err(errStream.str());
  std::string stdout_reply_s;
  if ((count_all_disk_replicas_removed + count_some_disk_replicas_removed + count_evict_counter_not_zero) > 0) {
    if (fsid.has_value()) {
      outStream << "found and removed the fsid="<< fsid.value() << " disk replica for "
                << (count_all_disk_replicas_removed + count_some_disk_replicas_removed)
                << "/" << evict.file_size() << " files";
    } else  {
      outStream << "removed all disk replicas for "
                << count_all_disk_replicas_removed << "/" << evict.file_size() << " files";
      if (!force) {
        outStream << "; reduced evict counter for " << count_evict_counter_not_zero << "/"
                  << evict.file_size() << " files";
      }
    }
  }
  reply.set_std_out(outStream.str());
  return reply;
}

EOSMGMNAMESPACE_END
