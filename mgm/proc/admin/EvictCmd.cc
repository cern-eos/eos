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
#include "mgm/cta/EosCtaReporter.hh"
#include "common/Definitions.hh"
#include "common/Constants.hh"
#include "namespace/interface/IView.hh"
#include <optional>

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::EvictCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream errStream;
  std::ostringstream outStream;
  bool allReplicasRemoved = false;
  int ret_c = 0;
  eos::console::RequestProto req = mReqProto;

  const auto& evict = req.evict();
  // TODO: Replace the code removed above by this line
  // const auto& evict = mReqProto.evict();
  XrdOucErrInfo errInfo;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  struct timespec ts_now;
  eos::common::Timing::GetTimeSpec(ts_now);
  std::optional<uint64_t> fsid =
    evict.has_evictsinglereplica() ? std::optional(
      evict.evictsinglereplica().fsid()) : std::nullopt;
  bool ignoreEvictCounter = evict.ignoreevictcounter();
  bool ignoreRemovalOnFst = evict.ignoreremovalonfst();

  if (fsid.has_value() && !ignoreEvictCounter) {
    reply.set_retc(EINVAL);
    errStream << "error: Parameter 'fsid' can only be used with 'ignore-evict-counter'" <<
              std::endl;
    reply.set_std_err(errStream.str());
    reply.set_std_out(outStream.str());
    return reply;
  }

  if(ignoreRemovalOnFst && !fsid.has_value()) {
    reply.set_retc(EINVAL);
    errStream << "error: Parameter 'ignore-removal-on-fst' can only be used with 'fsid'" <<
        std::endl;
    reply.set_std_err(errStream.str());
    reply.set_std_out(outStream.str());
    return reply;
  }

  int count_some_disk_replicas_removed = 0;
  int count_all_disk_replicas_removed = 0;
  int count_evict_counter_not_zero = 0;

  for (int i = 0; i < evict.file_size(); i++) {
    cta::ReporterEvict eosLog;
    eosLog
    .addParam(cta::ReportParam::SEC_APP, "tape_evict")
    .addParam(cta::ReportParam::LOG, std::string(gOFS->logId))
    .addParam(cta::ReportParam::RUID, mVid.uid)
    .addParam(cta::ReportParam::RGID, mVid.gid)
    .addParam(cta::ReportParam::TD, mVid.tident.c_str())
    .addParam(cta::ReportParam::TS, ts_now.tv_sec)
    .addParam(cta::ReportParam::TNS, ts_now.tv_nsec);
    const auto& file = evict.file(i);
    std::string path;
    std::string err;

    switch (file.File_case()) {
    case eos::console::EvictProto::FileProto::kPath:
      path = file.path();

      if (0 == path.length()) {
        errStream << "error: Received an empty string path";
        ret_c = EINVAL;
        eosLog.addParam(cta::ReportParam::EVICTCMD_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(cta::ReportParam::PATH, path);
      break;

    case eos::console::EvictProto::FileProto::kFid:
      GetPathFromFid(path, file.fid(), err);

      if (0 == path.length()) {
        errStream << "error: Received an unknown fid: value=" << file.fid();
        ret_c = EINVAL;
        eosLog.addParam(cta::ReportParam::EVICTCMD_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(cta::ReportParam::PATH, path);
      break;

    default:
      errStream << "error: Received a file with neither a path nor an fid";
      ret_c = EINVAL;
      eosLog.addParam(cta::ReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // check that we have the correct permission
    eos::common::Path cPath(path.c_str());
    errInfo.clear();

    if (gOFS->_access(cPath.GetParentPath(), P_OK, errInfo, mVid, "") != 0) {
      errStream << "error: you don't have 'p' acl flag permission on path '"
                << cPath.GetParentPath() << "'";
      ret_c = EPERM;
      eosLog.addParam(cta::EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;
    errInfo.clear();

    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'";
      ret_c = errno;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file with path '" << path << "'";
      ret_c = ENODATA;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      errStream << "error: given path is a directory '" << path << "'";
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    struct stat buf;

    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                    false) != 0) {
      errStream << "error: unable to run stat for replicas on path '" << path << "'";
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
      continue;
    }

    // we don't remove anything if it's not on tape
    if ((buf.st_mode & EOS_TAPE_MODE_T) == 0) {
      errStream << "error: no tape replicas for file '" << path << "'";
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
        if (location == eos::common::TAPE_FS_ID) {
          continue;
        }

        if (location == fsid.value()) {
          diskReplicaFound = true;
        }

        ++diskReplicaCount;
      }

      if (!diskReplicaFound) {
        eos_static_err("msg=\"unable to find disk replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: unable to find disk replica of '" << path << "'";
        eosLog.addParam(EosCtaReportParam::EVICTCMD_FSID,  fsid.value());
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
        continue;
      }
    } else {
      auto fmd = gOFS->eosView->getFile(path.c_str());

      for (auto location : fmd->getLocations()) {
        // Ignore tape replica
        if (location == eos::common::TAPE_FS_ID) {
          continue;
        }

        ++diskReplicaCount;
      }

      if (diskReplicaCount == 0) {
        eos_static_err("msg=\"unable to find any disk replica of %s\" reason=\"%s\"",
                       path.c_str(), errInfo.getErrText());
        errStream << "error: unable to find any disk replica of '" << path << "'";
        eosLog.addParam(EosCtaReportParam::EVICTCMD_ERROR, errStream.str());
        ret_c = SFS_ERROR;
        continue;
      }
    }

    errInfo.clear();

    if (fsid.has_value() && ignoreEvictCounter) {
      // Drop single stripe
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
      if (gOFS->_dropstripe(path.c_str(), 0, errInfo, root_vid, fsid.value(),
                            ignoreRemovalOnFst) != 0) {
        eos_static_err("msg=\"could not delete replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: could not delete replica of '" << path << "'";
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
#pragma GCC diagnostic pop
    } else {
      // May drop all stripes
      if (!ignoreEvictCounter) {
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
      if (gOFS->_dropallstripes(path.c_str(), errInfo, root_vid) != 0) {
        eos_static_err("msg=\"could not delete all disk replicas of %s\" reason=\"%s\"",
                       path.c_str(), errInfo.getErrText());
        errStream << "error: could not delete all disk replicas of '" << path << "'";
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

  if ((count_all_disk_replicas_removed + count_some_disk_replicas_removed +
       count_evict_counter_not_zero) > 0) {
    if (fsid.has_value()) {
      outStream << "found and removed the fsid=" << fsid.value() <<
                " disk replica for "
                << (count_all_disk_replicas_removed + count_some_disk_replicas_removed)
                << "/" << evict.file_size() << " files";
    } else  {
      outStream << "removed all disk replicas for "
                << count_all_disk_replicas_removed << "/" << evict.file_size() << " files";

      if (!ignoreEvictCounter) {
        outStream << "; reduced evict counter for " << count_evict_counter_not_zero <<
                  "/"
                  << evict.file_size() << " files";
      }
    }
  }

  reply.set_std_out(outStream.str());
  return reply;
}

EOSMGMNAMESPACE_END
