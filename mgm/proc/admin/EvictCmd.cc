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

#include "EvictCmd.hh"
#include "common/Constants.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "mgm/Acl.hh"
#include "mgm/EosCtaReporter.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::EvictCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream errStream;
  std::ostringstream outStream;
  bool allReplicasRemoved = false;
  int ret_c = 0;
  const auto& stagerRm = mReqProto.stagerrm();
  XrdOucErrInfo errInfo;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  struct timespec ts_now;
  eos::common::Timing::GetTimeSpec(ts_now);
  std::optional<uint64_t> fsid = 0;
  fsid = stagerRm.has_stagerrmsinglereplica() ? std::optional(stagerRm.stagerrmsinglereplica().fsid()) : std::nullopt;

  for (auto i = 0; i < stagerRm.file_size(); i++) {
    EosCtaReporterStagerRm eosLog;
    eosLog
    .addParam(EosCtaReportParam::SEC_APP, "tape_evict")
    .addParam(EosCtaReportParam::LOG, std::string(gOFS->logId))
    .addParam(EosCtaReportParam::RUID, mVid.uid)
    .addParam(EosCtaReportParam::RGID, mVid.gid)
    .addParam(EosCtaReportParam::TD, mVid.tident.c_str())
    .addParam(EosCtaReportParam::TS, ts_now.tv_sec)
    .addParam(EosCtaReportParam::TNS, ts_now.tv_nsec);
    const auto& file = stagerRm.file(i);
    std::string path;
    std::string err;

    switch (file.File_case()) {
    case eos::console::StagerRmProto::FileProto::kPath:
      path = file.path();

      if (0 == path.length()) {
        errStream << "error: Received an empty string path" << std::endl;
        ret_c = EINVAL;
        eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(EosCtaReportParam::PATH, path);
      break;

    case eos::console::StagerRmProto::FileProto::kFid:
      GetPathFromFid(path, file.fid(), err);

      if (0 == path.length()) {
        errStream << "error: Received an unknown fid: value=" << file.fid() <<
                  std::endl;
        ret_c = EINVAL;
        eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
        continue;
      }

      eosLog.addParam(EosCtaReportParam::PATH, path);
      break;

    default:
      errStream << "error: Received a file with neither a path nor an fid" <<
                std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    }

    // check that we have the correct permission
    eos::common::Path cPath(path.c_str());
    errInfo.clear();

    if (gOFS->_access(cPath.GetParentPath(), P_OK, errInfo, mVid, "") != 0) {
      errStream << "error: you don't have 'p' acl flag permission on path '"
                << cPath.GetParentPath() << "'" << std::endl;
      ret_c = EPERM;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;
    errInfo.clear();

    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'" <<
                std::endl;
      ret_c = errno;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file with path '" << path << "'" << std::endl;
      ret_c = ENODATA;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      errStream << "error: given path is a directory '" << path << "'" << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    }

    struct stat buf;

    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                    false) != 0) {
      errStream << "error: unable to run stat for replicas on path '" << path << "'"
                << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
      continue;
    }

    // we don't remove anything if it's not on tape
    if ((buf.st_mode & EOS_TAPE_MODE_T) == 0) {
      errStream << "error: no tape replicas for file '" << path << "'" << std::endl;
      ret_c = EINVAL;
      eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
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
        eos_static_err("msg=\"unable to find replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: unable to find replica of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::STAGERRM_FSID,  fsid.value());
        eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
        ret_c = SFS_ERROR;
        continue;
      }
    }

    errInfo.clear();

    if (fsid.has_value()) {
      // Drop single stripe
      if (gOFS->_dropstripe(path.c_str(), 0, errInfo, root_vid, fsid.value(), true) != 0) {
        eos_static_err("msg=\"could not delete replica of %s\" fsid=\"%u\" reason=\"%s\"",
                       path.c_str(), fsid.value(), errInfo.getErrText());
        errStream << "error: could not delete replica of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::STAGERRM_FSID, fsid.value());
        eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
        ret_c = SFS_ERROR;
      } else {
        if (diskReplicaCount <= 1) {
          allReplicasRemoved = true;
        }
      }
    } else {
      // Drop all stripes
      if (gOFS->_dropallstripes(path.c_str(), errInfo, root_vid, true) != 0) {
        eos_static_err("msg=\"could not delete all replicas of %s\" reason=\"%s\"",
                       path.c_str(), errInfo.getErrText());
        errStream << "error: could not delete all replicas of '" << path << "'" <<
                  std::endl;
        eosLog.addParam(EosCtaReportParam::STAGERRM_ERROR, errStream.str());
        ret_c = SFS_ERROR;
      } else {
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
        eosLog.addParam(EosCtaReportParam::STAGERRM_FSID, fsid.value());
      }
      eosLog.addParam(EosCtaReportParam::STAGERRM_FILEREMOVED, true);
    }
  }

  reply.set_retc(ret_c);
  reply.set_std_err(errStream.str());
  std::string stdout_reply_s;
  if (ret_c == 0) {
    if (fsid.has_value()) {
      outStream << "success: removed fsid "<< fsid.value() << " replica for all given files";
    } else {
      outStream << "success: removed all replicas for all given files";
    }
  }
  reply.set_std_out(outStream.str());
  return reply;
}

EOSMGMNAMESPACE_END
