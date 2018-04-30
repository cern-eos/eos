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
eos::mgm::StagerRmCmd::ProcessRequest() {
  eos::console::ReplyProto reply;
  std::ostringstream errStream;
  retc = 0;

  const auto& stagerRm = mReqProto.stagerrm();

  XrdOucErrInfo errInfo;
  eos::common::Mapping::VirtualIdentity root_vid;
  eos::common::Mapping::Root(root_vid);
  for (auto i = 0; i < stagerRm.path_size(); i++) {
    const auto& path = stagerRm.path(i);

    // check that we have the correct permission
    eos::common::Path cPath(path.c_str());
    errInfo.clear();
    if (gOFS->_access(cPath.GetParentPath(), P_OK, errInfo, mVid, "") != 0) {
      errStream << "error: you don't have 'p' acl flag permission on path '"
                << cPath.GetParentPath() << "'" << std::endl;
      retc = EPERM;
      continue;
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;
    errInfo.clear();
    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'" << std::endl;
      retc = errno;
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file with path '" << path << "'" << std::endl;
      retc = ENODATA;
      continue;
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      errStream << "error: given path is a directory '" << path << "'" << std::endl;
      retc = EINVAL;
      continue;
    }

    struct stat buf;
    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, nullptr, nullptr, false) != 0) {
      errStream << "error: unable to run stat for replicas on path '" << path << "'" << std::endl;
      retc = EINVAL;
      continue;
    }

    // we don't remove anything if it's not on tape
    if ((buf.st_mode & EOS_TAPE_MODE_T) == 0) {
      errStream << "error: no tape replicas for file '" << path << "'" << std::endl;
      retc = EINVAL;
      continue;
    }

    errInfo.clear();
    if (gOFS->_dropallstripes(path.c_str(), errInfo, root_vid, true) != 0) {
      eos_static_err("Could not delete all replicas of %s. Reason: %s",
                     path.c_str(), errInfo.getErrText());
      errStream << "error: could not delete all replicas of '" << path << "'" << std::endl;
      retc = SFS_ERROR;
    } else {
      // reset the retrieves counter in case of success
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      try {
        auto fmd = gOFS->eosView->getFile(path);
        fmd->setAttribute(RETRIEVES_ATTR_NAME, "0");
        gOFS->eosView->updateFileStore(fmd.get());
      } catch (eos::MDException& ex) {
        eos_static_err("Could not reset retrieves counter for file %s. Try setting the %s attribute to 0.",
                       path.c_str(), RETRIEVES_ATTR_NAME);
      }
    }
  }

  reply.set_retc(retc);
  reply.set_std_err(errStream.str());
  reply.set_std_out(retc == 0 ? "success: removed all replicas for all given files" : "");
  return reply;
}

EOSMGMNAMESPACE_END