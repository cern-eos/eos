//------------------------------------------------------------------------------
// File: StagerRmCmd.cc
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
#include "StagerRmCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "common/Constants.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::StagerRmCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream errStream;
  int ret_c = 0;
  const auto& stagerRm = mReqProto.stagerrm();
  XrdOucErrInfo errInfo;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

  for (auto i = 0; i < stagerRm.file_size(); i++) {
    const auto& file = stagerRm.file(i);
    std::string path;
    std::string err;

    switch (file.File_case()) {
    case eos::console::StagerRmProto::FileProto::kPath:
      path = file.path();

      if (0 == path.length()) {
        errStream << "error: Received an empty string path" << std::endl;
        ret_c = EINVAL;
        continue;
      }

      break;

    case eos::console::StagerRmProto::FileProto::kFid:
      GetPathFromFid(path, file.fid(), err);

      if (0 == path.length()) {
        errStream << "error: Received an unknown fid: value=" << file.fid() <<
                  std::endl;
        ret_c = EINVAL;
        continue;
      }

      break;

    default:
      errStream << "error: Received a file with neither a path nor an fid" <<
                std::endl;
      ret_c = EINVAL;
      continue;
    }

    // check that we have the correct permission
    eos::common::Path cPath(path.c_str());
    errInfo.clear();

    if (gOFS->_access(cPath.GetParentPath(), P_OK, errInfo, mVid, "") != 0) {
      errStream << "error: you don't have 'p' acl flag permission on path '"
                << cPath.GetParentPath() << "'" << std::endl;
      ret_c = EPERM;
      continue;
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;
    errInfo.clear();

    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'" <<
                std::endl;
      ret_c = errno;
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file with path '" << path << "'" << std::endl;
      ret_c = ENODATA;
      continue;
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      errStream << "error: given path is a directory '" << path << "'" << std::endl;
      ret_c = EINVAL;
      continue;
    }

    struct stat buf;

    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, nullptr, nullptr,
                    false) != 0) {
      errStream << "error: unable to run stat for replicas on path '" << path << "'"
                << std::endl;
      ret_c = EINVAL;
      continue;
    }

    // we don't remove anything if it's not on tape
    if ((buf.st_mode & EOS_TAPE_MODE_T) == 0) {
      errStream << "error: no tape replicas for file '" << path << "'" << std::endl;
      ret_c = EINVAL;
      continue;
    }

    // check eviction counter must reach zero for file to be deleted
    int evictionCounter = 0;

    try {
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                         __FILE__);
      auto fmd = gOFS->eosView->getFile(path.c_str());
      if (fmd->hasAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME)) {
        evictionCounter = std::stoi(fmd->getAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME));
      }
      evictionCounter = std::max(0, evictionCounter-1);
      fmd->setAttribute(eos::common::RETRIEVE_EVICT_COUNTER_NAME,
                        std::to_string(evictionCounter));
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& ex) {
      eos_static_err("msg=\"could not update eviction counter for file %s\"",
                     path.c_str());
    }

    if (evictionCounter > 0) {
      continue;
    }

    errInfo.clear();

    if (gOFS->_dropallstripes(path.c_str(), errInfo, root_vid, false) != 0) {
      eos_static_err("msg=\"could not delete all replicas of %s\" reason=\"%s\"",
                     path.c_str(), errInfo.getErrText());
      errStream << "error: could not delete all replicas of '" << path << "'" <<
                std::endl;
      ret_c = SFS_ERROR;
    } else {
      // reset the retrieves counter in case of success
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                         __FILE__);

      try {
        auto fmd = gOFS->eosView->getFile(path.c_str());
        fmd->setAttribute(eos::common::RETRIEVE_REQID_ATTR_NAME, "");
        fmd->setAttribute(eos::common::RETRIEVE_REQTIME_ATTR_NAME, "");
        gOFS->eosView->updateFileStore(fmd.get());
      } catch (eos::MDException& ex) {
        eos_static_err("msg=\"could not reset Prepare request ID list for "
                       "file %s. Try removing the %s and %s attributes\"",
                       path.c_str(), eos::common::RETRIEVE_REQID_ATTR_NAME,
                       eos::common::RETRIEVE_REQTIME_ATTR_NAME);
      }
    }
  }

  reply.set_retc(ret_c);
  reply.set_std_err(errStream.str());
  reply.set_std_out(ret_c == 0 ?
                    "success: removed all replicas for all given files" : "");
  return reply;
}

EOSMGMNAMESPACE_END
