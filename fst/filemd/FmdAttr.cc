//------------------------------------------------------------------------------
//! @file FmdAttr.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "FmdAttr.hh"
#include "FmdHandler.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/TransformAttr.hh"
#include <functional>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FmdAttrHandler::FmdAttrHandler(std::unique_ptr<FSPathHandler>&& _FSPathHandler)
  : mFSPathHandler(std::move(_FSPathHandler))
{}

//------------------------------------------------------------------------------
// Low level Fmd retrieve method
//------------------------------------------------------------------------------
std::pair<bool, eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                                 eos::common::FileSystem::fsid_t fsid,
                                 const std::string& path)
{
  if (path.empty()) {
    return LocalRetrieveFmd(mFSPathHandler->GetPath(fid, fsid));
  } else {
    return LocalRetrieveFmd(path);
  }
}

//------------------------------------------------------------------------------
// Low level Fmd retrieve method by path
//------------------------------------------------------------------------------
std::pair<bool, eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(const std::string& path)
{
  std::string attrval;
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(path));
  int result = io->attrGet(gFmdAttrName, attrval);

  if (result != 0) {
    eos_debug("msg=\"failed to retrieve fmd attribute\" path=\"%s\" errno=%d",
              path.c_str(), errno);
    return {false, eos::common::FmdHelper{}};
  }

  eos::common::FmdHelper fmd;
  bool status = fmd.mProtoFmd.ParsePartialFromString(attrval);

  if (!status) {
    eos_err("msg=\"failed parsing fmd attribute\" attr_sz=%lu", attrval.size());
  }

  return {status, std::move(fmd)};
}

//------------------------------------------------------------------------------
// Set Fmd xattr for the given file identifier
//------------------------------------------------------------------------------
bool
FmdAttrHandler::LocalPutFmd(const eos::common::FmdHelper& fmd,
                            eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            const std::string& path)
{
  if (path.empty()) {
    return LocalPutFmd(fmd, mFSPathHandler->GetPath(fid, fsid));
  } else {
    return LocalPutFmd(fmd, path);
  }
}

//------------------------------------------------------------------------------
// Set Fmd xattr for the corresponding file path
//------------------------------------------------------------------------------
bool
FmdAttrHandler::LocalPutFmd(const eos::common::FmdHelper& fmd,
                            const std::string& path)
{
  struct stat info;
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(path));

  if (io->fileStat(&info)) {
    eos_err("msg=\"file not existing\" path=\"%s\"", path.c_str());
    return false;
  }

  std::string attrval;
  fmd.mProtoFmd.SerializePartialToString(&attrval);
  int rc = io->attrSet(gFmdAttrName, attrval.c_str(), attrval.length());

  if (rc != 0) {
    eos_err("msg=\"failed to set xattr\" path=\"%s\" errno=%d",
            path.c_str(), errno);
  }

  return rc == 0;
}

//------------------------------------------------------------------------------
// Delete Fmd xattr for the corresponding file identifier
//------------------------------------------------------------------------------
void
FmdAttrHandler::LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                               eos::common::FileSystem::fsid_t fsid,
                               bool drop_file)
{
  return LocalDeleteFmd(mFSPathHandler->GetPath(fid, fsid), drop_file);
}

//------------------------------------------------------------------------------
// Delete Fmd xattr for the corresponding file path
//------------------------------------------------------------------------------
void
FmdAttrHandler::LocalDeleteFmd(const std::string& path, bool drop_file)
{
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(path));

  if (drop_file) {
    int rc = io->fileRemove();

    if (rc && errno != ENOENT) {
      eos_err("Failed to drop file at path=%s, errno=%d", path.c_str(), errno)
    }

    return;
  }

  if (int rc = io->attrDelete(gFmdAttrName);
      rc != 0) {
    if (errno == ENOATTR || errno == ENOENT) {
      return;
    }

    eos_err("Failed to Delete Fmd Attribute at path:%s, rc=%d", path.c_str(),
            errno);
  }
}


bool
FmdAttrHandler::Commit(eos::common::FmdHelper* fmd, bool lockit,
                       std::string* path)
{
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  fmd->mProtoFmd.set_mtime(tv.tv_sec);
  fmd->mProtoFmd.set_atime(tv.tv_sec);
  fmd->mProtoFmd.set_mtime_ns(tv.tv_usec * 1000);
  fmd->mProtoFmd.set_atime_ns(tv.tv_usec * 1000);

  if (path != nullptr) {
    return LocalPutFmd(*fmd, *path);
  }

  return LocalPutFmd(*fmd, fmd->mProtoFmd.fid(), fmd->mProtoFmd.fsid());
}

std::unique_ptr<eos::common::FmdHelper>
FmdAttrHandler::LocalGetFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            bool force_retrieve, bool do_create,
                            uid_t uid, gid_t gid,
                            eos::common::LayoutId::layoutid_t layoutid)
{
  auto [status, _fmd] = LocalRetrieveFmd(fid, fsid);

  if (!status && !do_create) {
    eos_warning("msg=\"no fmd record found\" fid=%08llx fsid=%lu", fid, fsid);
    return nullptr;
  }

  // Check the various conditions if we have a fmd attr already
  if (status) {
    auto fmd = std::make_unique<eos::common::FmdHelper>(std::move(_fmd.mProtoFmd));

    if ((fmd->mProtoFmd.fid() != fid) || (fmd->mProtoFmd.fsid() != fsid)) {
      eos_crit("msg=\"mismatch between requested fid/fsid and retrieved ones\" "
               "fxid=%08llx retrieved_fxid=%08llx fsid=%lu retrieved_fsid=%lu",
               fid, fmd->mProtoFmd.fid(), fsid, fmd->mProtoFmd.fsid());

      if (!force_retrieve) {
        return nullptr;
      }
    }

    if (force_retrieve) {
      return fmd;
    }

    if (!eos::common::LayoutId::IsRain(fmd->mProtoFmd.lid())) {
      if (!do_create &&
          ((fmd->mProtoFmd.disksize() &&
            (fmd->mProtoFmd.disksize() != eos::common::FmdHelper::UNDEF) &&
            (fmd->mProtoFmd.disksize() != fmd->mProtoFmd.size())) ||
           (fmd->mProtoFmd.mgmsize() &&
            (fmd->mProtoFmd.mgmsize() != eos::common::FmdHelper::UNDEF) &&
            (fmd->mProtoFmd.mgmsize() != fmd->mProtoFmd.size())))) {
        eos_crit("msg=\"size mismatch disk/mgm vs memory\" fxid=%08llx "
                 "fsid=%lu size=%llu disksize=%llu mgmsize=%llu",
                 fid, (unsigned long) fsid, fmd->mProtoFmd.size(),
                 fmd->mProtoFmd.disksize(), fmd->mProtoFmd.mgmsize());
        return nullptr;
      }

      if (!do_create &&
          ((fmd->mProtoFmd.filecxerror() == 1) ||
           (fmd->mProtoFmd.mgmchecksum().length() &&
            (fmd->mProtoFmd.mgmchecksum() != fmd->mProtoFmd.checksum())))) {
        eos_crit("msg=\"checksum error flagged/detected\" fxid=%08llx "
                 "fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s "
                 "filecxerror=%d blockcxerror=%d", fid,
                 (unsigned long) fsid, fmd->mProtoFmd.checksum().c_str(),
                 fmd->mProtoFmd.diskchecksum().c_str(),
                 fmd->mProtoFmd.mgmchecksum().c_str(),
                 fmd->mProtoFmd.filecxerror(),
                 fmd->mProtoFmd.blockcxerror());
      }
    } else {
      if (fmd->mProtoFmd.blockcxerror() == 1) {
        eos_crit("msg=\"blockxs error detected\" fxid=%08llx fsid=%lu",
                 fid, fsid);
        return nullptr;
      }
    }

    return fmd;
  } // status || force_retrieve

  // Creating an fmd
  auto fmd = std::make_unique<common::FmdHelper>();
  fmd->mProtoFmd.set_uid(uid);
  fmd->mProtoFmd.set_gid(gid);
  fmd->mProtoFmd.set_lid(layoutid);
  fmd->mProtoFmd.set_fsid(fsid);
  fmd->mProtoFmd.set_fid(fid);
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  fmd->mProtoFmd.set_ctime(tv.tv_sec);
  fmd->mProtoFmd.set_ctime_ns(tv.tv_usec * 1000);

  if (Commit(fmd.get(), false)) {
    eos_debug("msg=\"return fmd object\" fid=%08llx fsid=%lu", fid, fsid);
    return fmd;
  }

  eos_crit("msg=\"failed to commit fmd to storage\" fid=%08llx fsid=%lu",
           fid, fsid);
  return nullptr;
}

bool
FmdAttrHandler::ResetDiskInformation(eos::common::FileSystem::fsid_t fsid)
{
  std::error_code ec;
  WalkFSTree(mFSPathHandler->GetFSPath(fsid),
  [](std::string path) {
    TransformAttr(path, gFmdAttrName,
                  &FmdHandler::ResetFmdDiskInfo);
  }, ec);

  if (ec) {
    eos_err("msg=\"Failed to walk FST Tree\" error=%s", ec.message().c_str());
  }

  return !ec;
}

bool
FmdAttrHandler::ResetMgmInformation(eos::common::FileSystem::fsid_t fsid)
{
  std::error_code ec;
  WalkFSTree(mFSPathHandler->GetFSPath(fsid),
  [](std::string path) {
    TransformAttr(path, gFmdAttrName,
                  &FmdHandler::ResetFmdMgmInfo);
  }, ec);

  if (ec) {
    eos_err("msg=\"Failed to walk FST Tree\" error=%s", ec.message().c_str());
  }

  return !ec;
}

EOSFSTNAMESPACE_END
