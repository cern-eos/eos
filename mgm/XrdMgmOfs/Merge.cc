// ----------------------------------------------------------------------
// File: Merge.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

//------------------------------------------------------------------------------
// Merge one file into another one
//------------------------------------------------------------------------------
int
XrdMgmOfs::merge(const char* src, const char* dst, XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid)
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  std::shared_ptr<eos::IFileMD> src_fmd;
  std::shared_ptr<eos::IFileMD> dst_fmd;

  if (!src || !dst) {
    return Emsg("merge", error, EINVAL,
                "merge source into destination path - source or target missing");
  }

  uid_t save_uid = 99;
  gid_t save_gid = 99;
  std::string src_path = src;
  std::string dst_path = dst;
  {
    eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex);

    try {
      src_fmd = gOFS->eosView->getFile(src_path);
      dst_fmd = gOFS->eosView->getFile(dst_path);
      // Save the uid and gid and apply it after removing the src_fmd from a
      // possibly exising quota node. Otherwise the quota accounting will be
      // wrong.
      save_uid = dst_fmd->getCUid();
      save_gid = dst_fmd->getCGid();
      // Inherit some core meta data, the checksum must be right by construction,
      // so we don't copy it.
      eos::IFileMD::ctime_t mtime, ctime;
      dst_fmd->getCTime(ctime);
      src_fmd->setCTime(ctime);
      dst_fmd->getMTime(mtime);
      src_fmd->setMTime(mtime);
      src_fmd->setFlags(dst_fmd->getFlags());
      // Copy also the sys.tmp.etag if present
      auto xattr_map = dst_fmd->getAttributes();

      for (const auto& elem : xattr_map) {
        src_fmd->setAttribute(elem.first, elem.second);
      }

      if (dst_fmd->hasLocation(eos::common::TAPE_FS_ID)) {
	// pre-serve TAPE locations
	src_fmd->addLocation(eos::common::TAPE_FS_ID);
      }

      const std::string etag = "sys.tmp.etag";

      if (!src_fmd->hasAttribute(etag)) {
        std::string etag_value;
        eos::calculateEtag(dst_fmd.get(), etag_value);
        src_fmd->setAttribute(etag, etag_value);
      }

      eosView->updateFileStore(src_fmd.get());
      eos::FileIdentifier f_id = src_fmd->getIdentifier();
      viewLock.Release();
      gOFS->FuseXCastFile(f_id);
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("caught exception %d %s\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  {
    // Parameters to be passed to Workflow::Init()
    eos::IContainerMD::XAttrMap originalContainerAttributes;
    const auto &original_path = dst_path;
    eos::common::FileId::fileid_t new_f_id = 0;

    // Take a lock and get the values of the Workflow::Init() parameters
    {
      eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex);
      new_f_id = src_fmd->getId();
      const auto originalContainerId = dst_fmd->getContainerId();
      const auto originalContainer = gOFS->eosDirectoryService->getContainerMD(originalContainerId);
      if (SFS_OK != _attr_ls(gOFS->eosView->getUri(originalContainer.get()).c_str(), error, rootvid, 0,
        originalContainerAttributes, false)) {
        std::ostringstream msg;
        msg << "merge source into destination path - failed to list extended attributes of enclosing directory";
        return Emsg("merge", error, EINVAL, msg.str().c_str(), src_path.c_str());
      }
    }

    // Trigger file ID update workflow using the new file ID because the proto
    // workflow end point already knows the old ID.
    //
    // As far as the proto workflow end point is concerned only the original
    // container and file path is of interest.
    Workflow workflow;
    workflow.Init(&originalContainerAttributes, original_path, new_f_id);
    const char *const empty_ininfo = "";
    std::string wfError;
    const int wfRc = workflow.Trigger("sync::update_fid", "default", vid, empty_ininfo, wfError);
    if (wfRc && ENOKEY == errno && mTapeEnabled) {
      std::ostringstream msg;
      msg << "merge source into destination path - mgmofs.tapeenabled is set to true but the sync::update_fid workflow is not defined";
      return Emsg("merge", error, EINVAL, msg.str().c_str(), src_path.c_str());
    }
    if (wfRc && ENOKEY != errno) {
      std::ostringstream msg;
      msg << "merge source into destination path - the sync::update_fid workflow failed - " << wfError;
      return Emsg("merge", error, EINVAL, msg.str().c_str(), src_path.c_str());
    }
  }

  int rc = SFS_OK;

  if (src_fmd && dst_fmd) {
    // Remove the destination file
    rc |= gOFS->_rem(dst_path.c_str(), error, rootvid, "", false, false, true);

    if (rc == 0) {
      // Rename the source to destination
      rc |= gOFS->_rename(src_path.c_str(), dst_path.c_str(), error, rootvid, "",
                          "", false, false);

      if (rc == 0) {
        // Finally update the uid/gid
        rc |= gOFS->_chown(dst_path.c_str(), save_uid, save_gid, error,
                           rootvid, "");
      }
    }
  } else {
    return Emsg("merge", error, EINVAL, "merge source into destination path - "
                "cannot get file meta data ", src_path.c_str());
  }

  return rc;
}
