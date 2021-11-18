#include "FmdHandler.hh"
#include "common/Path.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/filemd/FmdMgm.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "qclient/structures/QSet.hh"
#include <fts.h>


EOSFSTNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Check if entry has a file checksum error
//------------------------------------------------------------------------------
bool
FmdHandler::FileHasXsError(const std::string& lpath,
                           eos::common::FileSystem::fsid_t fsid)
{
  bool has_xs_err = false;
  // First check the local db for any filecxerror flags
  auto fid = eos::common::FileId::PathToFid(lpath.c_str());
  auto fmd = LocalGetFmd(fid, fsid, true);

  if (fmd && fmd->mProtoFmd.filecxerror()) {
    has_xs_err = true;
  }

  // If no error found then also check the xattr on the physical file
  if (!has_xs_err) {
    std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(lpath.c_str()));
    std::string xattr_xs_err = "0";

    if (io->attrGet("user.eos.filecxerror", xattr_xs_err) == 0) {
      has_xs_err = (xattr_xs_err == "1");
    }
  }

  return has_xs_err;
}

//------------------------------------------------------------------------------
// Update fmd with disk info i.e. physical file extended attributes
//------------------------------------------------------------------------------
bool
FmdHandler::UpdateWithDiskInfo(eos::common::FileSystem::fsid_t fsid,
                               eos::common::FileId::fileid_t fid,
                               unsigned long long disk_size,
                               const std::string& disk_xs,
                               unsigned long check_ts_sec,
                               bool filexs_err,
                               bool blockxs_err,
                               bool layout_err)
{
  if (!fid) {
    eos_err("%s", "msg=\"skipping insert of file with fid=0\"");
    return false;
  }

  eos_debug("fsid=%lu fxid=%08llx disksize=%llu diskchecksum=%s checktime=%llu "
            "fcxerror=%d bcxerror=%d flaglayouterror=%d",
            fsid, fid, disk_size, disk_xs.c_str(), check_ts_sec,
            filexs_err, blockxs_err, layout_err);
  auto [status, valfmd] = LocalRetrieveFmd(fid, fsid);
  valfmd.mProtoFmd.set_fid(fid);
  valfmd.mProtoFmd.set_fsid(fsid);
  valfmd.mProtoFmd.set_disksize(disk_size);
  valfmd.mProtoFmd.set_diskchecksum(disk_xs);
  valfmd.mProtoFmd.set_checktime(check_ts_sec);
  valfmd.mProtoFmd.set_filecxerror(filexs_err ? 1 : 0);
  valfmd.mProtoFmd.set_blockcxerror(blockxs_err ? 1 : 0);

  // Update reference size only if undefined
  if (valfmd.mProtoFmd.size() == eos::common::FmdHelper::UNDEF) {
    valfmd.mProtoFmd.set_size(disk_size);
      // This is done only for non-rain layouts
      if (!eos::common::LayoutId::IsRain(valfmd.mProtoFmd.lid())) {
        valfmd.mProtoFmd.set_size(disk_size);
      }
  }

  // Update the reference checksum only if empty
  if (valfmd.mProtoFmd.checksum().empty()) {
    valfmd.mProtoFmd.set_checksum(disk_xs);
  }

  if (layout_err) {
    // If the mgm sync is run afterwards, every disk file is by construction an
    // orphan, until it is synced from the mgm
    valfmd.mProtoFmd.set_layouterror(LayoutId::kOrphan);
  }

  return LocalPutFmd(fid, fsid, valfmd);
}


//------------------------------------------------------------------------------
// Update fmd from MGM metadata
//------------------------------------------------------------------------------
bool
FmdHandler::UpdateWithMgmInfo(eos::common::FileSystem::fsid_t fsid,
                              eos::common::FileId::fileid_t fid,
                              eos::common::FileId::fileid_t cid,
                              eos::common::LayoutId::layoutid_t lid,
                              unsigned long long mgmsize,
                              std::string mgmchecksum,
                              uid_t uid, gid_t gid,
                              unsigned long long ctime,
                              unsigned long long ctime_ns,
                              unsigned long long mtime,
                              unsigned long long mtime_ns,
                              int layouterror, std::string locations)
{
  if (!fid) {
    eos_err("msg=\"skip inserting file with fid=0\"");
    return false;
  }

  eos_debug("fxid=%08llx fsid=%lu cid=%llu lid=%lx mgmsize=%llu mgmchecksum=%s",
            fid, fsid, cid, lid, mgmsize, mgmchecksum.c_str());
  auto [status, valfmd] = LocalRetrieveFmd(fid, fsid);
  valfmd.mProtoFmd.set_mgmsize(mgmsize);
  valfmd.mProtoFmd.set_mgmchecksum(mgmchecksum);
  valfmd.mProtoFmd.set_cid(cid);
  valfmd.mProtoFmd.set_lid(lid);
  valfmd.mProtoFmd.set_uid(uid);
  valfmd.mProtoFmd.set_gid(gid);
  valfmd.mProtoFmd.set_ctime(ctime);
  valfmd.mProtoFmd.set_ctime_ns(ctime_ns);
  valfmd.mProtoFmd.set_mtime(mtime);
  valfmd.mProtoFmd.set_mtime_ns(mtime_ns);
  valfmd.mProtoFmd.set_layouterror(layouterror);
  valfmd.mProtoFmd.set_locations(locations);
  // Truncate the checksum to the right length
  size_t cslen = LayoutId::GetChecksumLen(lid) * 2;
  valfmd.mProtoFmd.set_mgmchecksum(std::string(
                                     valfmd.mProtoFmd.mgmchecksum()).erase
                                   (std::min(valfmd.mProtoFmd.mgmchecksum().length(), cslen)));

  // Update reference size only if undefined
  if (valfmd.mProtoFmd.size() == eos::common::FmdHelper::UNDEF) {
    valfmd.mProtoFmd.set_size(mgmsize);
  } else {
    // For RAIN layouts the logical size (should) matche the MGM size
    // even if it is already set
    if (eos::common::LayoutId::IsRain(lid)) {
      valfmd.mProtoFmd.set_size(mgmsize);
    }
  }

  // Update the reference checksum only if empty
  if (valfmd.mProtoFmd.checksum().empty()) {
    valfmd.mProtoFmd.set_checksum(valfmd.mProtoFmd.mgmchecksum());
  }

  return LocalPutFmd(fid, fsid, valfmd);
}

//------------------------------------------------------------------------------
// Update local fmd with info from the scanner
//------------------------------------------------------------------------------
void
FmdHandler::UpdateWithScanInfo(eos::common::FileId::fileid_t fid,
                                    eos::common::FileSystem::fsid_t fsid,
                                    const std::string& fpath,
                                    uint64_t scan_sz,
                                    const std::string& scan_xs_hex,
                                    std::shared_ptr<qclient::QClient> qcl)
{
  eos_debug("msg=\"resyncing qdb and disk info\" fxid=%08llx fsid=%lu",
            fid, fsid);

  if (ResyncFileFromQdb(fid, fsid, fpath, qcl)) {
    return;
  }

  int rd_rc = ResyncDisk(fpath.c_str(), fsid, false, scan_sz, scan_xs_hex);

  if (rd_rc) {
    if (rd_rc == ENOENT) {
      // File no longer on disk - mark it as missing unless it's a 0-size file
      auto fmd = LocalGetFmd(fid, fsid, true);

      if (fmd && fmd->mProtoFmd.mgmsize()) {
        fmd->mProtoFmd.set_layouterror(fmd->mProtoFmd.layouterror() |
                                       LayoutId::kMissing);
        Commit(fmd.get());
      }
    }
  }
}

//------------------------------------------------------------------------------
// Resync a single entry from disk
//------------------------------------------------------------------------------
int
FmdHandler::ResyncDisk(const char* path,
                       eos::common::FileSystem::fsid_t fsid,
                       bool flaglayouterror,
                       uint64_t scan_sz, const std::string& scan_xs_hex)
{
  eos::common::Path cPath(path);
  eos::common::FileId::fileid_t fid =
    eos::common::FileId::Hex2Fid(cPath.GetName());

  if (fid == 0) {
    eos_err("%s", "msg=\"unable to sync fid=0\"");
    return EINVAL;
  }

  std::unique_ptr<eos::fst::FileIo>
  io(eos::fst::FileIoPluginHelper::GetIoObject(path));

  if (io == nullptr) {
    eos_crit("msg=\"failed to get IO object\" path=%s", path);
    return ENOMEM;
  }

  struct stat buf;

  if ((!io->fileStat(&buf)) && S_ISREG(buf.st_mode)) {
    std::string sxs_type, scheck_stamp, filexs_err, blockxs_err;
    char xs_val[SHA256_DIGEST_LENGTH];
    size_t xs_len = SHA256_DIGEST_LENGTH;
    memset(xs_val, 0, sizeof(xs_val));
    io->attrGet("user.eos.checksumtype", sxs_type);
    io->attrGet("user.eos.filecxerror", filexs_err);
    io->attrGet("user.eos.blockcxerror", blockxs_err);
    io->attrGet("user.eos.timestamp", scheck_stamp);

    // Handle the old format in microseconds, truncate to seconds
    if (scheck_stamp.length() > 10) {
      scheck_stamp.erase(10);
    }

    unsigned long check_ts_sec {0ul};

    try {
      check_ts_sec = std::stoul(scheck_stamp);
    } catch (...) {
      // ignore
    }

    std::string disk_xs_hex;
    off_t disk_size {0ull};

    if (scan_sz && !scan_xs_hex.empty()) {
      disk_size = scan_sz;
      disk_xs_hex = scan_xs_hex;
    } else {
      disk_size = buf.st_size;

      if (io->attrGet("user.eos.checksum", xs_val, xs_len) == 0) {
        std::unique_ptr<CheckSum> xs_obj {ChecksumPlugins::GetXsObj(sxs_type)};

        if (xs_obj) {
          if (xs_obj->SetBinChecksum(xs_val, xs_len)) {
            disk_xs_hex = xs_obj->GetHexChecksum();
          }
        }
      }
    }

    // Update the DB
    if (!UpdateWithDiskInfo(fsid, fid, disk_size, disk_xs_hex, check_ts_sec,
                            (filexs_err == "1"), (blockxs_err == "1"),
                            flaglayouterror)) {
      // eos_err("msg=\"failed to update DB\" dbpath=%s fxid=%08llx fsid=%lu",
      //         eos::common::DbMap::getDbType().c_str(), fid, fsid);
      return false;
    }
  } else {
    eos_err("msg=\"failed stat or entry is not a file\" path=%s", path);
    return ENOENT;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Resync files under path into DB
//------------------------------------------------------------------------------
bool
FmdHandler::ResyncAllDisk(const char* path,
                          eos::common::FileSystem::fsid_t fsid,
                          bool flaglayouterror)
{
  char** paths = (char**) calloc(2, sizeof(char*));

  if (!paths) {
    eos_err("error: failed to allocate memory");
    return false;
  }

  paths[0] = (char*) path;
  paths[1] = 0;

  if (flaglayouterror) {
    SetSyncStatus(fsid, true);
  }

  if (!ResetDiskInformation(fsid)) {
    eos_err("failed to reset the disk information before resyncing fsid=%lu",
            fsid);
    free(paths);
    return false;
  }

  // Scan all the files
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
            eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%u", cnt,
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

//------------------------------------------------------------------------------
// Resync file meta data from MGM into local database
//------------------------------------------------------------------------------
bool
FmdHandler::ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                           eos::common::FileId::fileid_t fid,
                           const char* manager)
{
  eos::common::FmdHelper fMd;
  int rc = FmdMgmHandler::GetMgmFmd((manager ? manager : ""), fid, fMd);

  if ((rc == 0) || (rc == ENODATA)) {
    if (rc == ENODATA) {
      eos_warning("msg=\"file not found on MGM\" fxid=%08llx", fid);
      fMd.mProtoFmd.set_fid(fid);

      if (fid == 0) {
        eos_warning("msg=\"removing fxid=0 entry\"");
        LocalDeleteFmd(fMd.mProtoFmd.fid(), fsid);
        return true;
      }
    }

    // Define layouterrors
    fMd.mProtoFmd.set_layouterror(fMd.LayoutError(fsid));
    // Get an existing record without creating the record !!!
    std::unique_ptr<eos::common::FmdHelper> fmd {
      LocalGetFmd(fMd.mProtoFmd.fid(), fsid, true, false, fMd.mProtoFmd.uid(),
      fMd.mProtoFmd.gid(), fMd.mProtoFmd.lid())};

    if (fmd) {
      // Check if exists on disk
      if (fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) {
        if (fMd.mProtoFmd.layouterror() & LayoutId::kUnregistered) {
          // There is no replica supposed to be here and there is nothing on
          // disk, so remove it from the database
          eos_warning("msg=\"removing ghost fmd from db\" fsid=%u fxid=%08llx",
                      fsid, fid);
          LocalDeleteFmd(fMd.mProtoFmd.fid(), fsid);
          return true;
        }
      }
    } else {
      // No file locally and also not registered with the MGM
      if ((fMd.mProtoFmd.layouterror() & LayoutId::kUnregistered) ||
          (fMd.mProtoFmd.layouterror() & LayoutId::kOrphan)) {
        return true;
      }
    }

    // Get/create a record
    fmd = LocalGetFmd(fMd.mProtoFmd.fid(), fsid, true, true,
                      fMd.mProtoFmd.uid(), fMd.mProtoFmd.gid(),
                      fMd.mProtoFmd.lid());

    if (fmd) {
      // Check if it exists on disk
      if ((fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) &&
          (fMd.mProtoFmd.mgmsize())) {
        fMd.mProtoFmd.set_layouterror(fMd.mProtoFmd.layouterror() | LayoutId::kMissing);
        eos_warning("msg=\"mark missing replica\" fxid=%08llx on fsid=%u",
                    fid, fsid);
      }

      if (!UpdateWithMgmInfo(fsid, fMd.mProtoFmd.fid(), fMd.mProtoFmd.cid(),
                             fMd.mProtoFmd.lid(), fMd.mProtoFmd.mgmsize(),
                             fMd.mProtoFmd.mgmchecksum(), fMd.mProtoFmd.uid(),
                             fMd.mProtoFmd.gid(), fMd.mProtoFmd.ctime(),
                             fMd.mProtoFmd.ctime_ns(), fMd.mProtoFmd.mtime(),
                             fMd.mProtoFmd.mtime_ns(), fMd.mProtoFmd.layouterror(),
                             fMd.mProtoFmd.locations())) {
        eos_err("msg=\"failed to update fmd with mgm info\" fxid=%08llx", fid);
        return false;
      }

      // Check if it exists on disk and at the mgm
      if ((fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) &&
          (fMd.mProtoFmd.mgmsize() == eos::common::FmdHelper::UNDEF)) {
        // There is no replica supposed to be here and there is nothing on
        // disk, so remove it from the database
        eos_warning("removing <ghost> entry for fxid=%08llx on fsid=%u", fid,
                    (unsigned long) fsid);
        LocalDeleteFmd(fMd.mProtoFmd.fid(), fsid);
        return true;
      }
    } else {
      eos_err("failed to create fmd for fxid=%08llx", fid);
      return false;
    }
  } else {
    eos_err("failed to retrieve MGM fmd for fxid=%08llx", fid);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Resync all meta data from MGM into local database
//------------------------------------------------------------------------------
bool
FmdHandler::ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                         const char* manager)
{
  if (!ResetMgmInformation(fsid)) {
    eos_err("%s", "msg=\"failed to reset the mgm information before resyncing\"");
    SetSyncStatus(fsid, false);
    return false;
  }

  std::string tmpfile;

  if (!FmdMgmHandler::ExecuteDumpmd(manager, fsid, tmpfile)) {
    SetSyncStatus(fsid, false);
    return false;
  }

  // Parse the result and unlink temporary file
  std::ifstream inFile(tmpfile);
  std::string dumpentry;
  unlink(tmpfile.c_str());
  unsigned long long cnt = 0;

  while (std::getline(inFile, dumpentry)) {
    cnt++;
    eos_debug("line=%s", dumpentry.c_str());
    std::unique_ptr<XrdOucEnv> env(new XrdOucEnv(dumpentry.c_str()));

    if (env) {
      eos::common::FmdHelper fMd;

      if (FmdMgmHandler::EnvMgmToFmd(*env, fMd)) {
        // get/create one
        auto fmd = LocalGetFmd(fMd.mProtoFmd.fid(), fsid, true, true,
                               fMd.mProtoFmd.uid(), fMd.mProtoFmd.gid(),
                               fMd.mProtoFmd.lid());
        fMd.mProtoFmd.set_layouterror(fMd.LayoutError(fsid));

        if (fmd) {
          // Check if it exists on disk
          if ((fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) &&
              (fmd->mProtoFmd.mgmsize())) {
            fMd.mProtoFmd.set_layouterror(fMd.mProtoFmd.layouterror() | LayoutId::kMissing);
            eos_warning("found missing replica for fxid=%08llx on fsid=%u",
                        fMd.mProtoFmd.fid(), (unsigned long) fsid);
          }

          if (!UpdateWithMgmInfo(fsid, fMd.mProtoFmd.fid(), fMd.mProtoFmd.cid(),
                                 fMd.mProtoFmd.lid(), fMd.mProtoFmd.mgmsize(),
                                 fMd.mProtoFmd.mgmchecksum(), fMd.mProtoFmd.uid(),
                                 fMd.mProtoFmd.gid(), fMd.mProtoFmd.ctime(),
                                 fMd.mProtoFmd.ctime_ns(), fMd.mProtoFmd.mtime(),
                                 fMd.mProtoFmd.mtime_ns(), fMd.mProtoFmd.layouterror(),
                                 fMd.mProtoFmd.locations())) {
            eos_err("msg=\"failed to update fmd\" entry=\"%s\"",
                    dumpentry.c_str());
          }
        } else {
          eos_err("msg=\"failed to get/create fmd\" enrty=\"%s\"",
                  dumpentry.c_str());
        }
      } else {
        eos_err("msg=\"failed to convert\" entry=\"%s\"", dumpentry.c_str());
      }
    }

    if (!(cnt % 10000)) {
      eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%u", cnt,
               (unsigned long) fsid);
    }
  }

  SetSyncStatus(fsid, false);
  return true;
}

//------------------------------------------------------------------------------
// Move given file to orphans directory and also set its extended attribute
// to reflect the original path to the file.
//------------------------------------------------------------------------------
void
FmdHandler::MoveToOrphans(const std::string& fpath)
{
  eos::common::Path cpath(fpath.c_str());
  size_t cpath_sz = cpath.GetSubPathSize();

  if (cpath_sz <= 2) {
    eos_static_err("msg=\"failed to extract FST mount/fid hex\" path=%s",
                   fpath.c_str());
    return;
  }

  std::string fid_hex = cpath.GetName();
  std::ostringstream oss;
  oss << cpath.GetSubPath(cpath_sz - 2) << ".eosorphans/" << fid_hex;
  std::string forphan = oss.str();
  // Store the original path name as an extended attribute in case ...
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath));
  io->attrSet("user.eos.orphaned", fpath.c_str());

  // If orphan move it into the orphaned directory
  if (!rename(fpath.c_str(), forphan.c_str())) {
    eos_static_warning("msg=\"orphaned/unregistered quarantined\" "
                "fst-path=%s orphan-path=%s", fpath.c_str(), forphan.c_str());
  } else {
    eos_static_err("msg=\"failed to quarantine orphaned/unregistered\" "
                   "fst-path=%s orphan-path=%s", fpath.c_str(), forphan.c_str());
  }
}

//------------------------------------------------------------------------------
// Resync file meta data from QuarkDB into local database
//------------------------------------------------------------------------------
int
FmdHandler::ResyncFileFromQdb(eos::common::FileId::fileid_t fid,
                                   eos::common::FileSystem::fsid_t fsid,
                                   const std::string& fpath,
                                   std::shared_ptr<qclient::QClient> qcl)
{
  using eos::common::FileId;

  if (qcl == nullptr) {
    eos_notice("msg=\"no qclient present, skipping file resync\" fxid=%08llx"
               " fid=%lu", fid, fsid);
    return EINVAL;
  }

  eos::common::FmdHelper ns_fmd;
  auto file_fut = eos::MetadataFetcher::getFileFromId(*qcl.get(),
                  eos::FileIdentifier(fid));

  try {
    FmdMgmHandler::NsFileProtoToFmd(std::move(file_fut).get(), ns_fmd);
  } catch (const eos::MDException& e) {
    eos_err("msg=\"failed to get metadata from QDB: %s\" fxid=%08llx",
            e.what(), fid);

    // If there is any transient error with QDB then we skip this file,
    // otherwise it might be wronly marked as orphan below.
    if (e.getErrno() != ENOENT) {
      eos_err("msg=\"skip file update due to QDB error\" msg_err=\"%s\" "
              "fxid=%08llx", e.what(), fid);
      return e.getErrno();
    }
  }

  // Mark any possible layout error, if fid not found in QDB then this is
  // marked as orphan
  ns_fmd.mProtoFmd.set_layouterror(ns_fmd.LayoutError(fsid));
  // Get an existing local record without creating the record!!!
  std::unique_ptr<eos::common::FmdHelper> local_fmd {
    LocalGetFmd(fid, fsid, true, false)};

  if (!local_fmd) {
    // Create the local record
    if (!(local_fmd = LocalGetFmd(fid, fsid, true, true))) {
      eos_err("msg=\"failed to create local fmd entry\" fxid=%08llx fsid=%u",
              fid, fsid);
      return EINVAL;
    }
  }

  // Orphan files get moved to a special directory .eosorphans
  if (ns_fmd.mProtoFmd.layouterror() & eos::common::LayoutId::kOrphan) {
    FmdHandler::MoveToOrphans(fpath);
    // Also mark it as orphan in leveldb
    local_fmd->mProtoFmd.set_layouterror(LayoutId::kOrphan);

    if (!Commit(local_fmd.get())) {
      eos_static_err("msg=\"failed to mark orphan entry\" fxid=%08llx fsid=%u",
                     fid, fsid);
    }

    return ENOENT;
  }

  // Never mark an ns 0-size file without replicas on disk as missing
  if (ns_fmd.mProtoFmd.mgmsize() == 0) {
    ns_fmd.mProtoFmd.set_layouterror(ns_fmd.mProtoFmd.layouterror() &
                                     ~LayoutId::kMissing);
  } else {
    // If file is not on disk or already marked as missing then keep the
    // missing flag
    if ((local_fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) ||
        (local_fmd->mProtoFmd.layouterror() & LayoutId::kMissing)) {
      eos_warning("msg=\"mark missing replica\" fxid=%08llx fsid=%u", fid, fsid);
      ns_fmd.mProtoFmd.set_layouterror(ns_fmd.mProtoFmd.layouterror() |
                                       LayoutId::kMissing);
    }
  }

  if (!UpdateWithMgmInfo(fsid, fid, ns_fmd.mProtoFmd.cid(),
                         ns_fmd.mProtoFmd.lid(), ns_fmd.mProtoFmd.mgmsize(),
                         ns_fmd.mProtoFmd.mgmchecksum(), ns_fmd.mProtoFmd.uid(),
                         ns_fmd.mProtoFmd.gid(), ns_fmd.mProtoFmd.ctime(),
                         ns_fmd.mProtoFmd.ctime_ns(), ns_fmd.mProtoFmd.mtime(),
                         ns_fmd.mProtoFmd.mtime_ns(), ns_fmd.mProtoFmd.layouterror(),
                         ns_fmd.mProtoFmd.locations())) {
    eos_err("msg=\"failed to update fmd with qdb info\" fxid=%08llx", fid);
    return EINVAL;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Resync all meta data from QuarkdDB
//------------------------------------------------------------------------------
bool
FmdHandler::ResyncAllFromQdb(const QdbContactDetails& contact_details,
                                  eos::common::FileSystem::fsid_t fsid)
{
  using namespace std::chrono;

  if (!ResetMgmInformation(fsid)) {
    eos_err("%s", "msg=\"failed to reset the mgm info before resyncing\"");
    SetSyncStatus(fsid, false);
    return false;
  }

  // Collect all file ids on the desired file system
  auto start = steady_clock::now();
  qclient::QClient qcl(contact_details.members,
                       contact_details.constructOptions());
  std::unordered_set<eos::IFileMD::id_t> file_ids;
  qclient::QSet qset(qcl, eos::RequestBuilder::keyFilesystemFiles(fsid));

  try {
    for (qclient::QSet::Iterator its = qset.getIterator(); its.valid();
         its.next()) {
      try {
        file_ids.insert(std::stoull(its.getElement()));
      } catch (...) {
        eos_err("msg=\"failed to convert fid entry\" data=\"%s\"",
                its.getElement().c_str());
      }
    }
  } catch (const std::runtime_error& e) {
    // There are no files on current filesystem
  }

  uint64_t total = file_ids.size();
  eos_info("msg=\"resyncing %llu files for file_system %u\"", total, fsid);
  uint64_t num_files = 0;
  auto it = file_ids.begin();
  std::list<std::pair<eos::common::FileId::fileid_t,
      folly::Future<eos::ns::FileMdProto>>> files;

  // Pre-fetch the first 1000 files
  while ((it != file_ids.end()) && (num_files < 1000)) {
    ++num_files;
    files.emplace_back(*it, MetadataFetcher::getFileFromId(qcl,
                       FileIdentifier(*it)));
    ++it;
  }

  while (!files.empty()) {
    eos::common::FmdHelper ns_fmd;
    eos::common::FileId::fileid_t fid = files.front().first;

    try {
      FmdMgmHandler::NsFileProtoToFmd(std::move(files.front().second).get(), ns_fmd);
    } catch (const eos::MDException& e) {
      eos_err("msg=\"failed to get metadata from QDB: %s\"", e.what());
    }

    files.pop_front();
    // Mark any possible layout error, if fid not found in QDB then this is
    // marked as orphan
    ns_fmd.mProtoFmd.set_layouterror(ns_fmd.LayoutError(fsid));
    // Get an existing local record without creating the record!!!
    std::unique_ptr<eos::common::FmdHelper> local_fmd {
      LocalGetFmd(fid, fsid, true, false)};

    if (!local_fmd) {
      // Create the local record
      if (!(local_fmd = LocalGetFmd(fid, fsid, true, true))) {
        eos_err("msg=\"failed to create local fmd entry\" fxid=%08llx", fid);
        continue;
      }
    }

    // If file does not exist on disk and is not 0-size then mark as missing
    if ((local_fmd->mProtoFmd.disksize() == eos::common::FmdHelper::UNDEF) &&
        (ns_fmd.mProtoFmd.mgmsize())) {
      ns_fmd.mProtoFmd.set_layouterror(ns_fmd.mProtoFmd.layouterror() |
                                       LayoutId::kMissing);
      eos_warning("msg=\"mark missing replica\" fxid=%08llx fsid=%u", fid, fsid);
    }

    if (!UpdateWithMgmInfo(fsid, fid, ns_fmd.mProtoFmd.cid(),
                           ns_fmd.mProtoFmd.lid(), ns_fmd.mProtoFmd.mgmsize(),
                           ns_fmd.mProtoFmd.mgmchecksum(), ns_fmd.mProtoFmd.uid(),
                           ns_fmd.mProtoFmd.gid(), ns_fmd.mProtoFmd.ctime(),
                           ns_fmd.mProtoFmd.ctime_ns(), ns_fmd.mProtoFmd.mtime(),
                           ns_fmd.mProtoFmd.mtime_ns(), ns_fmd.mProtoFmd.layouterror(),
                           ns_fmd.mProtoFmd.locations())) {
      eos_err("msg=\"failed to update fmd with qdb info\" fxid=%08llx", fid);
      continue;
    }

    if (it != file_ids.end()) {
      files.emplace_back(*it, MetadataFetcher::getFileFromId(qcl,
                         FileIdentifier(*it)));
      ++num_files;
      ++it;
    }

    if (num_files % 10000 == 0) {
      double rate = 0;
      auto duration = steady_clock::now() - start;
      auto ms = duration_cast<milliseconds>(duration);

      if (ms.count()) {
        rate = (num_files * 1000.0) / (double)ms.count();
      }

      eos_info("fsid=%u resynced %llu/%llu files at a rate of %.2f Hz",
               fsid, num_files, total, rate);
    }
  }

  double rate = 0;
  auto duration = steady_clock::now() - start;
  auto ms = duration_cast<milliseconds>(duration);

  if (ms.count()) {
    rate = (num_files * 1000.0) / (double)ms.count();
  }

  SetSyncStatus(fsid, false);
  eos_info("msg=\"fsid=%u resynced %llu/%llu files at a rate of %.2f Hz\"",
           fsid, num_files, total, rate);
  return true;
}


EOSFSTNAMESPACE_END
