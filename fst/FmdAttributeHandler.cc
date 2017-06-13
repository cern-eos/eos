//------------------------------------------------------------------------------
//! \file FmdAttributeHandler.cc
//! \author Jozsef Makai<jmakai@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "FmdAttributeHandler.hh"
#include "fst/io/local/FsIo.hh"
#include "common/Path.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/XrdFstOfs.hh"

#include <fts.h>

EOSFSTNAMESPACE_BEGIN

FmdAttributeHandler gFmdAttributeHandler{&gFmdClient};

Fmd
FmdAttributeHandler::FmdAttrGet(FileIo* fileIo) const {
  std::string fmdAttrValue;
  int result = fileIo->attrGet(FmdAttributeHandler::fmdAttrName, fmdAttrValue);

  if (result != 0) {
    throw fmd_attribute_error {std::string("Meta data attribute is not present for file: ") + fileIo->GetPath()};
  }

  Fmd fmd;
  fmd.ParsePartialFromString(fmdAttrValue);

  return fmd;
}

Fmd
FmdAttributeHandler::FmdAttrGet(const std::string& filePath) const {
  FsIo fsIo{filePath};
  return FmdAttrGet(&fsIo);
}

Fmd
FmdAttributeHandler::FmdAttrGet(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid,
                                XrdOucEnv* env) const {
  FsIo fsIo{FullPathOfFile(fid, fsid, env).c_str()};
  return FmdAttrGet(&fsIo);
}

void
FmdAttributeHandler::FmdAttrSet(FileIo* fileIo, const Fmd& fmd) const {
  eos_info("fmd=%s", fmd.DebugString().c_str());
  int result = fileIo->attrSet(FmdAttributeHandler::fmdAttrName, fmd.SerializePartialAsString());

  if (result != 0) {
    throw fmd_attribute_error {"Could not set meta data attribute for the file."};
  }
}

void
FmdAttributeHandler::FmdAttrSet(const Fmd& fmd, eos::common::FileId::fileid_t fid,
                                eos::common::FileSystem::fsid_t fsid, XrdOucEnv* env) const {
  FsIo fsIo{FullPathOfFile(fid, fsid, env).c_str()};
  FmdAttrSet(&fsIo, fmd);
}

void
FmdAttributeHandler::FmdAttrDelete(FileIo* fileIo) const {
  if (fileIo->attrDelete(FmdAttributeHandler::fmdAttrName) != 0) {
    throw fmd_attribute_error {"Could not delete meta data attribute for the file."};
  }
}

void
FmdAttributeHandler::CreateFileAndSetFmd(FileIo* fileIo, Fmd& fmd, eos::common::FileSystem::fsid_t fsid) const {
  // check if it exists on disk
  if (fileIo->fileExists() != 0) {
    FsIo fsIo(fileIo->GetPath());
    fsIo.fileOpen(SFS_O_CREAT | SFS_O_RDWR);
    fsIo.fileClose();

    fmd.set_layouterror(fmd.layouterror() | eos::common::LayoutId::kMissing);
    eos_warning("found missing replica for fid=%llu on fsid=%lu", fmd.fid(),
                (unsigned long) fsid);
  }

  FmdAttrSet(fileIo, fmd);
}

bool
FmdAttributeHandler::ResyncMgm(FileIo* fileIo, eos::common::FileSystem::fsid_t fsid,
                               eos::common::FileId::fileid_t fid, const char* manager) const {
  struct Fmd fmd;
  bool fmdUpdated = false;

  try {
    // get local fmd object if possible and update that to not to lose information
    fmd = FmdAttrGet(fileIo);
  } catch (fmd_attribute_error& error) {
    FmdHelper::Reset(fmd);
    // we have to set the fsid locally
    fmd.set_fsid(fsid);
    fmdUpdated = true;
  }

  Fmd oldFmd = fmd;

  int rc = fmdClient->GetMgmFmd(manager, fid, fmd);

  if (rc == 0) {
    if (!fmdUpdated) {
      fmdUpdated = IsFmdUpdated(oldFmd, fmd);

      if (!fmdUpdated) {
        eos_info("meta data is up to date for fid=%llu", fid);
        return true;
      }
    }

    // define layout errors
    fmd.set_layouterror(FmdHelper::LayoutError(fsid, fmd.lid(), fmd.locations()));

    try {
      CreateFileAndSetFmd(fileIo, fmd, fsid);
    } catch (...) {
      eos_err("failed to get/create fmd for fid=%08llx", fmd.fid());
      return false;
    }

  } else {
    if (rc == ENODATA) {
      eos_warning("no such file on MGM for fid=%llu", fid);
    } else {
      eos_err("failed to retrieve MGM fmd for fid=%08llx", fid);
    }

    return false;
  }

  return true;
}

bool
FmdAttributeHandler::ResyncMgm(const std::string& filePath, eos::common::FileSystem::fsid_t fsid,
                               eos::common::FileId::fileid_t fid, const char* manager) const {
  FsIo fsIo{filePath};
  return ResyncMgm(&fsIo, fsid, fid, manager);
}

bool
FmdAttributeHandler::ResyncMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid,
                               const char* manager) const {
  return ResyncMgm(FullPathOfFile(fid, fsid).c_str(), fsid, fid, manager);
}

bool
FmdAttributeHandler::ResyncAllMgm(eos::common::FileSystem::fsid_t fsid, const char* manager) {

  XrdOucString consolestring = "/proc/admin/?&mgm.format=fuse&mgm.cmd=fs&"
    "mgm.subcmd=dumpmd&mgm.dumpmd.storetime=1&mgm.dumpmd.option=m&mgm.fsid=";
  consolestring += (int) fsid;
  XrdOucString url = "root://";
  url += manager;
  url += "//";
  url += consolestring;
  // We run an external command and parse the output
  char tmpfile[] = "/tmp/efstd.XXXXXX";
  int tmp_fd = mkstemp(tmpfile);

  if (tmp_fd == -1) {
    eos_err("failed to create a temporary file");
    return false;
  }

  (void) close(tmp_fd);
  XrdOucString cmd = "env XrdSecPROTOCOL=sss xrdcp -f -s \"";
  cmd += url;
  cmd += "\" ";
  cmd += tmpfile;
  int rc = system(cmd.c_str());

  if (WEXITSTATUS(rc)) {
    eos_err("%s returned %d", cmd.c_str(), WEXITSTATUS(rc));
    return false;
  } else {
    eos_debug("%s executed successfully", cmd.c_str());
  }

  // Parse the result and unlink temporary file
  std::ifstream inFile(tmpfile);
  std::string dumpentry;
  unlink(tmpfile);
  unsigned long long cnt = 0;

  while (std::getline(inFile, dumpentry)) {
    cnt++;
    eos_debug("line=%s", dumpentry.c_str());
    std::unique_ptr<XrdOucEnv> env{new XrdOucEnv(dumpentry.c_str())};

    if (env != nullptr) {
      struct Fmd fMd;
      FmdHelper::Reset(fMd);

      if (fmdClient->EnvMgmToFmdSqlite(*env, fMd)) {
        fMd.set_layouterror(FmdHelper::LayoutError(fsid, fMd.lid(), fMd.locations()));

        XrdOucString filePath = FullPathOfFile(fMd.fid(), fsid, env.get());

        try {
          FsIo fsIo{filePath.c_str()};
          CreateFileAndSetFmd(&fsIo, fMd, fsid);
        } catch (...) {
          eos_err("failed to get/create fmd for fid=%08llx", fMd.fid());
          return false;
        }

      } else {
        eos_err("failed to convert %s", dumpentry.c_str());
      }
    }

    if (!(cnt % 10000)) {
      eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt,
               (unsigned long) fsid);
    }
  }

  isSyncing[fsid] = false;
  return true;
}

bool
FmdAttributeHandler::ResyncDisk(const char* path, eos::common::FileSystem::fsid_t fsid,
                                bool flaglayouterror) const {
  bool retc = true;
  eos::common::Path cPath(path);
  eos::common::FileId::fileid_t fid = eos::common::FileId::Hex2Fid(cPath.GetName());
  off_t disksize = 0;

  if (fid) {
    std::unique_ptr<eos::fst::FileIo> io(eos::fst::FileIoPluginHelper::GetIoObject(path));

    if (io.get()) {
      struct stat buf;

      if ((!io->fileStat(&buf)) && S_ISREG(buf.st_mode)) {
        std::string checksumType, checksumStamp, filecxError, blockcxError;
        std::string diskchecksum = "";
        char checksumVal[SHA_DIGEST_LENGTH];
        size_t checksumLen = 0;
        unsigned long checktime = 0;
        // got the file size
        disksize = buf.st_size;
        memset(checksumVal, 0, sizeof(checksumVal));
        checksumLen = SHA_DIGEST_LENGTH;

        if (io->attrGet("user.eos.checksum", checksumVal, checksumLen)) {
          checksumLen = 0;
        }

        io->attrGet("user.eos.checksumtype", checksumType);
        io->attrGet("user.eos.filecxerror", filecxError);
        io->attrGet("user.eos.blockcxerror", blockcxError);
        checktime = (strtoull(checksumStamp.c_str(), 0, 10) / 1000000);

        if (checksumLen) {
          // retrieve a checksum object to get the hex representation
          XrdOucString envstring = "eos.layout.checksum=";
          envstring += checksumType.c_str();
          XrdOucEnv env(envstring.c_str());
          int checksumtype = eos::common::LayoutId::GetChecksumFromEnv(env);
          eos::common::LayoutId::layoutid_t layoutid = eos::common::LayoutId::GetId(
            eos::common::LayoutId::kPlain, checksumtype);
          eos::fst::CheckSum* checksum = eos::fst::ChecksumPlugins::GetChecksumObject(
            layoutid, false);

          if (checksum) {
            if (checksum->SetBinChecksum(checksumVal, checksumLen)) {
              diskchecksum = checksum->GetHexChecksum();
            }

            delete checksum;
          }
        }

        Fmd fmd;
        try {
          fmd = FmdAttrGet(io.get());
        } catch (fmd_attribute_error& error) {}

        fmd.set_disksize(disksize);
        // fix the reference value from disk
        fmd.set_size(disksize);
        fmd.set_checksum(diskchecksum);
        fmd.set_fid(fid);
        fmd.set_fsid(fsid);
        fmd.set_diskchecksum(diskchecksum);
        fmd.set_checktime(checktime);
        fmd.set_filecxerror((filecxError == "1") ? 1 : 0);
        fmd.set_blockcxerror((blockcxError == "1") ? 1 : 0);

        if (flaglayouterror) {
          /* if the mgm sync is run afterwards, every disk file is by construction an
             orphan, until it is synced from the mgm */
          fmd.set_layouterror(eos::common::LayoutId::kOrphan);
        }

        try {
          FmdAttrSet(io.get(), fmd);
        } catch (fmd_attribute_error& error) {
          eos_err("failed to update file meta data for fsid=%lu fid=%08llx",
                  (unsigned long) fsid, fid);
          retc = false;
        }
      }
    }
  } else {
    eos_debug("would convert %s (%s) to fid 0", cPath.GetName(), path);
    retc = false;
  }

  return retc;
}

bool
FmdAttributeHandler::ResyncAllDisk(const char* path, eos::common::FileSystem::fsid_t fsid,
                                   bool flaglayouterror) {
  char** paths = (char**) calloc(2, sizeof(char*));

  if (!paths) {
    eos_err("error: failed to allocate memory");
    return false;
  }

  paths[0] = (char*) path;
  paths[1] = 0;

  if (flaglayouterror) {
    isSyncing[fsid] = true;
  }

  // scan all the files
  FTS* tree = fts_open(paths, FTS_NOCHDIR, 0);

  if (!tree) {
    eos_err("fts_open failed");
    free(paths);
    return false;
  }

  FTSENT* node;
  unsigned long long cnt = 0;

  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        XrdOucString filePath = node->fts_accpath;

        if (!filePath.matches("*.xsmap")) {
          cnt++;
          eos_debug("file=%s", filePath.c_str());
          ResyncDisk(filePath.c_str(), fsid, flaglayouterror);

          if (!(cnt % 10000)) {
            eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt,
                     (unsigned long) fsid);
          }
        }
      }
    }
  }

  if (fts_close(tree)) {
    eos_err("fts_close failed");
    free(paths);
    return false;
  }

  free(paths);
  return true;
}

XrdOucString
FmdAttributeHandler::FullPathOfFile(eos::common::FileId::fileid_t fid,
                                    eos::common::FileSystem::fsid_t fsid,
                                    XrdOucEnv* env) const {
  XrdOucString hexId;
  eos::common::FileId::Fid2Hex(fid, hexId);
  XrdOucString filePath;
  eos::common::FileId::FidPrefix2FullPath(hexId.c_str(), gOFS.getLocalPrefix(env, fsid).c_str(), filePath);
  return filePath;
}

inline bool
FmdAttributeHandler::IsFmdUpdated(const Fmd& oldFmd, const Fmd& newFmd) const {
  return (oldFmd.fid() != newFmd.fid() ||
          oldFmd.cid() != newFmd.cid() ||
          oldFmd.ctime() != newFmd.ctime() ||
          oldFmd.ctime_ns() != newFmd.ctime_ns() ||
          oldFmd.mtime() != newFmd.mtime() ||
          oldFmd.mtime_ns() != newFmd.mtime_ns() ||
          oldFmd.size() != newFmd.size() ||
          oldFmd.checksum() != newFmd.checksum() ||
          oldFmd.lid() != newFmd.lid() ||
          oldFmd.uid() != newFmd.uid() ||
          oldFmd.gid() != newFmd.gid());
}

void
FmdAttributeHandler::ReportFmdInconsistency(const std::string& filePath, eos::common::FileId::fileid_t fid,
                                            eos::common::FileSystem::fsid_t fsid) const {
  Fmd fmd;
  try {
    fmd = FmdAttrGet(filePath);
  } catch (fmd_attribute_error& error) {
    eos_err("Could not get meta data for fid=%llu, fsid=%llu", fid, fsid);
    return;
  }

  ReportFmdInconsistency(fmd);
}

void
FmdAttributeHandler::ReportFmdInconsistency(const Fmd& fmd) const {
  auto fid = fmd.fid();
  auto fsid = fmd.fsid();

  XrdOucString fsckOpaque = "/?mgm.pcmd=fsck&mgm.file.id=";
  fsckOpaque += std::to_string(fid).c_str();
  fsckOpaque += "&mgm.file.fsid=";
  fsckOpaque += std::to_string(fsid).c_str();
  XrdOucErrInfo error;

  if (fmd.layouterror() & eos::common::LayoutId::kOrphan) {
    XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=orphans_n";
    error.Reset();
    int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
    if(rc != 0) {
      eos_err("Could not report orphans_n inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
              fid, fsid, error.getErrText());
    }
  }

  if (fmd.layouterror() & eos::common::LayoutId::kUnregistered) {
    XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=unreg_n";
    error.Reset();
    int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
    if(rc != 0) {
      eos_err("Could not report unreg_n inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
              fid, fsid, error.getErrText());
    }
  }

  if (fmd.layouterror() & eos::common::LayoutId::kReplicaWrong) {
    XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=rep_diff_n";
    error.Reset();
    int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
    if(rc != 0) {
      eos_err("Could not report rep_diff_n inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
              fid, fsid, error.getErrText());
    }
  }

  if (fmd.layouterror() & eos::common::LayoutId::kMissing) {
    XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=rep_missing_n";
    error.Reset();
    int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
    if(rc != 0) {
      eos_err("Could not report rep_missing_n inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
              fid, fsid, error.getErrText());
    }
  }

  if (!fmd.layouterror()) {
    if (fmd.size() && fmd.mgmchecksum().length() && (fmd.mgmchecksum() != fmd.checksum())) {
      XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=m_cx_diff";
      error.Reset();
      int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
      if(rc != 0) {
        eos_err("Could not report m_cx_diff inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
                fid, fsid, error.getErrText());
      }
    }

    if (fmd.size() && fmd.diskchecksum().length() && (fmd.diskchecksum() != fmd.checksum())) {
      XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=d_cx_diff";
      error.Reset();
      int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
      if(rc != 0) {
        eos_err("Could not report d_cx_diff inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
                fid, fsid, error.getErrText());
      }
    }
  }

  if (fmd.disksize() != 0xfffffffffff1ULL && fmd.size() != 0xfffffffffff1ULL) {
    if (fmd.size() != fmd.disksize()) {
      XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=d_mem_sz_diff";
      error.Reset();
      int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
      if(rc != 0) {
        eos_err("Could not report d_mem_sz_diff inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
                fid, fsid, error.getErrText());
      }
    }
  }

  if (fmd.disksize() != 0xfffffffffff1ULL && fmd.size() != 0xfffffffffff1ULL) {
    if (fmd.size() != fmd.mgmsize()) {
      XrdOucString opaqueWithInconsistency = fsckOpaque + "&mgm.file.inconsistency=m_mem_sz_diff";
      error.Reset();
      int rc = gOFS.CallManager(&error, "/", nullptr, opaqueWithInconsistency);
      if(rc != 0) {
        eos_err("Could not report m_mem_sz_diff inconsistency to mgm for fid: %llu, fsid: %llu. Error: %s",
                fid, fsid, error.getErrText());
      }
    }
  }
}

EOSFSTNAMESPACE_END
