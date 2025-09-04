//------------------------------------------------------------------------------
// File: ScanDir.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "fst/ScanDir.hh"
#include "common/Path.hh"
#include "common/Constants.hh"
#include "common/StringSplit.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "console/commands/helpers/FsHelper.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Deletion.hh"
#include "fst/utils/IoPriority.hh"
#include "fst/filemd/FmdMgm.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/layout/HeaderCRC.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/LayoutPlugin.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "qclient/structures/QSet.hh"
#include "mgm/Constants.hh"
#include <sys/stat.h>
#include <sys/types.h>
#include <fts.h>
#include <sys/time.h>
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
#ifndef __APPLE__
#include <sys/syscall.h>
#include <asm/unistd.h>
#include <algorithm>
#endif


namespace
{
std::chrono::seconds getScanTimestamp(eos::fst::FileIo* io,
                                      const std::string& key)
{
  std::string scan_ts_sec = "0";
  io->attrGet(key, scan_ts_sec);

  // Handle the old format in microseconds, truncate to seconds
  if (scan_ts_sec.length() > 10) {
    scan_ts_sec.erase(10);
  }

  try {
    std::int64_t seconds = std::stoll(scan_ts_sec);
    return std::chrono::seconds(seconds);
  } catch (...) {
    return std::chrono::seconds(-1);
  }
}
}

EOSFSTNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ScanDir::ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
                 eos::fst::Load* fstload, bool bgthread,
                 long int file_rescan_interval, int ratebandwidth,
                 bool fake_clock) :
  mFstLoad(fstload), mFsId(fsid), mDirPath(dirpath),
  mRateBandwidth(ratebandwidth),
  mNsInterval(DEFAULT_NS_INTERVAL),
  mDiskInterval(DEFAULT_DISK_INTERVAL),
  mEntryInterval(file_rescan_interval),
  mRainEntryInterval(DEFAULT_RAIN_RESCAN_INTERVAL),
  mAltXsDoSync(false), // by default sync is disabled
  mAltXsSyncInterval(0), // by default the sync is done only once
  mAltXsInterval(0), // by default it's disabled
  mNumScannedFiles(0), mNumCorruptedFiles(0),
  mNumHWCorruptedFiles(0),  mTotalScanSize(0), mNumTotalFiles(0),
  mNumSkippedFiles(0), mBuffer(nullptr),
  mBufferSize(0), mBgThread(bgthread), mClock(fake_clock), mRateLimit(nullptr)
{
  long alignment = pathconf((mDirPath[0] != '/') ? "/" : mDirPath.c_str(),
                            _PC_REC_XFER_ALIGN);

  if (alignment > 0) {
    mBufferSize = 256 * alignment;

    if (posix_memalign((void**) &mBuffer, alignment, mBufferSize)) {
      fprintf(stderr, "error: error calling posix_memaling on dirpath=%s. \n",
              mDirPath.c_str());
      std::abort();
    }
  } else {
    mBufferSize = 256 * 1024;
    mBuffer = (char*) malloc(mBufferSize);
    fprintf(stderr,
            "error: OS does not provide alignment or path does not exist\n");
  }

  if (mBgThread) {
    openlog("scandir", LOG_PID | LOG_NDELAY, LOG_USER);
    mDiskThread.reset(&ScanDir::RunDiskScan, this);
#ifndef _NOOFS
    mRateLimit.reset(new eos::common::RequestRateLimit());
    mRateLimit->SetRatePerSecond(sDefaultNsScanRate);
    mNsThread.reset(&ScanDir::RunNsScan, this);
    mAltXsThread.reset(&ScanDir::RunAltXsScan, this);
#endif
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ScanDir::~ScanDir()
{
  if (mBgThread) {
    mDiskThread.join();
    mNsThread.join();
    mAltXsThread.join();
    closelog();
  }

  if (mBuffer) {
    free(mBuffer);
  }
}

//------------------------------------------------------------------------------
// Update scanner configuration
//------------------------------------------------------------------------------
void
ScanDir::SetConfig(const std::string& key, long long value)
{
  eos_info("msg=\"update scanner configuration\" key=\"%s\" value=\"%s\" fsid=%lu",
           key.c_str(), std::to_string(value).c_str(), mFsId);

  if (key == eos::common::SCAN_IO_RATE_NAME) {
    mRateBandwidth.store(static_cast<int>(value), std::memory_order_relaxed);
  } else if (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) {
    mEntryInterval.store(value, std::memory_order_relaxed);
  } else if (key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) {
    mRainEntryInterval.store(value, std::memory_order_relaxed);
  } else if (key == eos::common::SCAN_DISK_INTERVAL_NAME) {
    mDiskInterval.set(value);
  } else if (key == eos::common::SCAN_NS_INTERVAL_NAME) {
#ifndef _NOOFS
    mNsInterval.set(value);
#endif
  } else if (key == eos::common::SCAN_NS_RATE_NAME) {
    mRateLimit->SetRatePerSecond(value);
  } else if (key == eos::common::SCAN_ALTXS_INTERVAL_NAME) {
    mAltXsInterval.set(value);
  } else if (key == eos::common::ALTXS_SYNC) {
    mAltXsDoSync.store(value, std::memory_order_relaxed);
  } else if (key == eos::common::ALTXS_SYNC_INTERVAL) {
    mAltXsSyncInterval.store(value, std::memory_order_relaxed);
  }
}

#ifndef _NOOFS
//------------------------------------------------------------------------------
// Infinite loop doing the scanning of namespace entries
//------------------------------------------------------------------------------
void
ScanDir::RunNsScan(ThreadAssistant& assistant) noexcept
{
  using namespace std::chrono;
  using eos::common::FileId;
  eos_info("msg=\"started the ns scan thread\" fsid=%lu dirpath=\"%s\" "
           "ns_scan_interval_sec=%llu", mFsId, mDirPath.c_str(), mNsInterval.get());

  if (gOFS.mQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, skipping ns scan\"");
    return;
  }

  // Wait for the corresponding file system to boot before starting
  while ((gOFS.Storage->ExistsFs(mFsId) == false) ||
         gOFS.Storage->IsFsBooting(mFsId)) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      eos_info("%s", "msg=\"stopping ns scan thread\"");
      return;
    }
  }

  mNsInterval.random_wait(assistant, true, [this](uint64_t sleep_time) {
    eos_info("msg=\"delay ns scan thread by %llu seconds\" fsid=%lu dirpath=\"%s\"",
             sleep_time, mFsId, mDirPath.c_str());
  });

  while (!assistant.terminationRequested()) {
    AccountMissing();
    CleanupUnlinked();
    mNsInterval.wait(assistant);
  }
}

namespace
{
// Get the set of alternative checksums configured at the container
// level in the namespace.
std::string AltXsConfigFromNS(
  const eos::ns::ContainerMdProto& container)
{
  if (!container.xattrs().contains(eos::mgm::SYS_ALTCHECKSUMS)) {
    return {};
  }

  return container.xattrs().at(eos::mgm::SYS_ALTCHECKSUMS);
}

// Get the set of alternative checksums already computed on the file
// that are stored in the namespace.
std::vector<std::string> ExtractAltXsOnFile(
  eos::ns::FileMdProto& file)
{
  std::vector<std::string> altxs;

  for (const auto & [xs, _] : file.altchecksums()) {
    altxs.emplace_back(eos::common::LayoutId::GetChecksumString(xs));
  }

  return altxs;
}

// Create the map of alternative checksums that will be commited
// to the MGM.
std::map<eos::common::LayoutId::eChecksum, std::string>
PrepareAltXsResponse(eos::fst::ChecksumGroup* xs)
{
  std::map<eos::common::LayoutId::eChecksum, std::string> altxs;
  auto computed = xs->GetAlternatives();

  for (const auto& [type, xs] : computed) {
    altxs.insert({type, xs->GetHexChecksum()});
  }

  return altxs;
}

// Parse a string of the type "md5,sha1" in a set of checksum types
std::set<eos::common::LayoutId::eChecksum> ParseAltXsConfigString(
  std::string xs_str)
{
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(xs_str, tokens, ",");
  std::set<eos::common::LayoutId::eChecksum> altxs;

  for (const auto& tkn : tokens) {
    auto xs = eos::common::LayoutId::GetChecksumFromString(tkn);
    altxs.emplace(static_cast<eos::common::LayoutId::eChecksum>(xs));
  }

  return altxs;
}

// Get the local view of the alternative checksums configured in the MGM
// at the directory level.
std::set<eos::common::LayoutId::eChecksum> LocalConfiguredAltXs(
  eos::fst::FileIo* io)
{
  std::string attr;
  io->attrGet("user.eos.altxs", attr);
  return ParseAltXsConfigString(attr);
}

// Get the local view of the alternative checksums computed and stored
// in the namespace.
std::set<eos::common::LayoutId::eChecksum> LocalAltXsComputed(
  eos::fst::FileIo* io)
{
  std::string attr;
  io->attrGet("user.eos.altxs_mgm", attr);
  return ParseAltXsConfigString(attr);
}

// Store the list of configured alternative checksums locally in the
// xattrs of the file. The list is passed as a commad separated string
// of the names of the alternative checksums, for example "md5,sha1"
void StoreAltXsOnMGM(eos::fst::FileIo* io, const std::string& xs)
{
  io->attrSet("user.eos.altxs_mgm", xs);
}

// Store the list of configured alternative checksums locally in the
// xattrs of the file.
void StoreAltXsOnMGM(eos::fst::FileIo* io, const std::vector<std::string>& xs)
{
  StoreAltXsOnMGM(io, eos::common::StringConversion::Join(xs, ","));
}

// Store the list of configured alternative checksums locally in the
// xattrs of the file.
void StoreAltXsOnMGM(eos::fst::FileIo* io,
                     const std::set<eos::common::LayoutId::eChecksum>& xs)
{
  std::vector<std::string> lst;

  for (const auto& x : xs) {
    lst.emplace_back(eos::common::LayoutId::GetChecksumString(x));
  }

  StoreAltXsOnMGM(io, lst);
}

// Commit the alternative checksums for the file identified by the file id <fid>.
// If the list to_delete is not empty, it will instruct the MGM to delete
// those checksums from the namespace.
bool CommitAlternativeChecksums(uint64_t fid,
                                const std::map<eos::common::LayoutId::eChecksum, std::string>& alt_xs,
                                const std::set<eos::common::LayoutId::eChecksum>* to_delete = nullptr)
{
#ifndef _NOOFS

  if (alt_xs.empty() && (to_delete == nullptr || to_delete->empty())) {
    return true;
  }

  XrdOucString capOpaqueFile = "";
  XrdOucString mTimeString = "";
  capOpaqueFile += "/?";
  capOpaqueFile += "&mgm.pcmd=commit";
  capOpaqueFile += "&mgm.fid=";
  capOpaqueFile += eos::common::FileId::Fid2Hex(fid).c_str();
  capOpaqueFile += "&mgm.commit.altxs=1";

  if (!alt_xs.empty()) {
    std::vector<std::string> alt;

    for (auto const& [type, xs] : alt_xs) {
      alt.emplace_back(std::string(eos::common::LayoutId::GetChecksumString(
                                     type)) + ":" + xs);
    }

    capOpaqueFile += "&mgm.altxs=";
    capOpaqueFile += eos::common::StringConversion::Join(alt, ",").c_str();
  }

  if (to_delete != nullptr && !to_delete->empty()) {
    capOpaqueFile += "&mgm.altxs.delete=";
    std::vector<std::string> del;

    for (auto const& xs : *to_delete) {
      del.emplace_back(std::string(eos::common::LayoutId::GetChecksumString(xs)));
    }

    capOpaqueFile += eos::common::StringConversion::Join(del, ",").c_str();
  }

  XrdOucErrInfo error;
  int rc = gOFS.CallManager(&error, nullptr, nullptr, capOpaqueFile);

  if (rc) {
    eos_static_err("unable to commit alternative checksums fxid=%08llx", fid);
    return false;
  }

#endif
  return true;
}
};

//------------------------------------------------------------------------------
// Infinite loop doing the computation of alternative checksums
//------------------------------------------------------------------------------
void ScanDir::RunAltXsScan(ThreadAssistant& assistant) noexcept
{
  auto compute_alt_xs = [this](const std::string & fpath) {
    eos_debug("msg=\"running alt xs scan for file\" path=\"%s\" fsid=%d",
              fpath.c_str(),
              mFsId);
    auto fid = eos::common::FileId::PathToFid(fpath.c_str());

    if (!fid) {
      eos_static_info("msg=\"skip file which is not a eos data file\", "
                      "path=\"%s\"", fpath.c_str());
      return;
    }

    auto fmd = gOFS.mFmdHandler->LocalGetFmd(fid, mFsId, true);

    if (!fmd) {
      eos_warning("msg=\"cannot find fmd object on file\" fxid=%08llx fsid=%d", fid,
                  mFsId);
      return;
    }

    std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath));
    auto cfg = LocalConfiguredAltXs(io.get());
    auto on_file = LocalAltXsComputed(io.get());
    std::set<eos::common::LayoutId::eChecksum> missing;
    std::set<eos::common::LayoutId::eChecksum> common;
    std::set_intersection(cfg.begin(), cfg.end(), on_file.begin(), on_file.end(),
                          std::inserter(common, common.begin()));
    std::set_difference(cfg.begin(), cfg.end(), common.begin(), common.end(),
                        std::inserter(missing, missing.begin()));
    eos_debug("msg=\"%d alt xs to compute for file\" fxid=%08llx fsid=%d",
              missing.size(),
              fid, mFsId);

    if (missing.size() == 0) {
      // nothing to compute
      if (on_file.size() != cfg.size()) {
        // we have to delete the ones that are on the files and not in the config anymore
        std::set<eos::common::LayoutId::eChecksum> to_delete;
        std::set_difference(on_file.begin(), on_file.end(), cfg.begin(), cfg.end(),
                            std::inserter(to_delete, to_delete.begin()));

        if (CommitAlternativeChecksums(fid, {}, &to_delete)) {
          StoreAltXsOnMGM(io.get(), cfg);
        }
      }

      return;
    }

    std::unique_ptr<eos::fst::ChecksumGroup> xs{new eos::fst::ChecksumGroup};

    for (auto m : cfg) {
      xs->AddAlternative(m);
    }

    std::string fullpath = "root://";
    {
      XrdSysMutexHelper lock(gConfig.Mutex);
      fullpath += gConfig.Manager.c_str();
    }
    fullpath += SSTR("//?eos.lfn=fxid:" << eos::common::FileId::Fid2Hex(
                       fid) << "&xrd.wantprot=sss");
    auto f = std::make_unique<XrdCl::File>();
    auto status = f->Open(fullpath, XrdCl::OpenFlags::Read);

    if (!status.IsOK()) {
      eos_err("msg=\"error opening file\" fullpath='%s' fsid=%d error='%s'",
              fullpath.c_str(),
              mFsId, status.GetErrorMessage().c_str());
      return;
    }

    uint64_t offset = 0;
    constexpr size_t buff_size = 256 * 1024;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(buff_size);
    uint32_t nread = 0;

    while (true) {
      status = f->Read(offset, buff_size, buffer.get(), nread);

      if (!status.IsOK()) {
        eos_err("msg=\"error reading file\" fullpath='%s'\" fsid=%d error='%s'",
                fullpath.c_str(),
                mFsId, status.GetErrorMessage().c_str());
        return;
      }

      if (nread == 0) {
        // end of file
        break;
      }

      xs->Add(buffer.get(), nread, offset);
      offset += nread;
    }

    xs->Finalize();
    auto alt_xs = PrepareAltXsResponse(xs.get());

    if (CommitAlternativeChecksums(fid, alt_xs)) {
      StoreAltXsOnMGM(io.get(), cfg);
    }
  };
  eos_info("msg=\"started the alt xs scan thread\" fsid=%lu dirpath=\"%s\" "
           "altxs_scan_interval_sec=%llu", mFsId, mDirPath.c_str(),
           mAltXsInterval.get());

  if (gOFS.mQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, skipping ns scan\"");
    return;
  }

  // Wait for the corresponding file system to boot before starting
  while ((gOFS.Storage->ExistsFs(mFsId) == false) ||
         gOFS.Storage->IsFsBooting(mFsId)) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      eos_info("%s", "msg=\"stopping alt xs scan thread\"");
      return;
    }
  }

  mAltXsInterval.random_wait(assistant, true, [this](uint64_t sleep_time) {
    eos_info("msg=\"pausing alt xs scan thread\" time=%d fsid=%d", sleep_time,
             mFsId);
  });

  while (!assistant.terminationRequested()) {
    eos_info("msg=\"starting alt xs scan thread loop\" fsid=%d", mFsId);
    ScanFsTree(assistant, compute_alt_xs, true, &mAltXsInterval);
    eos_info("msg=\"finished alt xs scan thread loop\" fsid=%d", mFsId);
    mAltXsInterval.wait(assistant, true);
  }
}

//----------------------------------------------------------------------------
// Account for missing replicas
//----------------------------------------------------------------------------
void
ScanDir::AccountMissing()
{
  using eos::common::FileId;
  struct stat info;
  eos::common::FsckErrsPerFsMap errs_map;
  auto fids = CollectNsFids(eos::fsview::sFilesSuffix);
  eos_info("msg=\"scanning %llu attached namespace entries\"", fids.size());

  while (!fids.empty()) {
    // Tag any missing replicas
    eos::IFileMD::id_t fid = fids.front();
    fids.pop_front();
    std::string fpath =
      FileId::FidPrefix2FullPath(FileId::Fid2Hex(fid).c_str(), mDirPath.c_str());

    if (stat(fpath.c_str(), &info)) {
      // Double check that this not a file which was deleted in the meantime
      try {
        if (IsBeingDeleted(fid)) {
          // Give it one more kick by dropping the file from disk and QDB
          XrdOucErrInfo tmp_err;

          if (gOFS._rem("/DELETION_FSCK", tmp_err, nullptr, nullptr, fpath.c_str(),
                        fid, mFsId, true)) {
            eos_err("msg=\"failed to remove local file\" path=%s fxid=%08llx "
                    "fsid=%lu", fpath.c_str(), fid, mFsId);
          }
        } else {
          // File missing on disk, mark it but check the MGM info since the
          // file might be 0-size so we need to remove the kMissing flag
          eos::common::FmdHelper ns_fmd;
          auto file = eos::MetadataFetcher::getFileFromId(*gOFS.mQcl.get(),
                      eos::FileIdentifier(fid));
          FmdMgmHandler::NsFileProtoToFmd(std::move(file).get(), ns_fmd);

          // Mark as missing only if this is not a zero size file and if the
          // file metadata entry at the MGM contains the current fsid as one
          // of the locations.
          if ((ns_fmd.mProtoFmd.mgmsize() != 0) && ns_fmd.HasLocation(mFsId)) {
            // Mark as missing and also mark the current fsid
            ns_fmd.mProtoFmd.set_fsid(mFsId);
            ns_fmd.mProtoFmd.set_layouterror(ns_fmd.mProtoFmd.layouterror() |
                                             LayoutId::kMissing);
          }

          CollectInconsistencies(ns_fmd, mFsId, errs_map);
        }
      } catch (eos::MDException& e) {
        // No file on disk, no ns file metadata object but we have a ghost entry
        // in the file system view - delete it
        if (!DropGhostFid(mFsId, fid)) {
          eos_err("msg=\"failed to drop ghost entry\" fxid=%08llx fsid=%lu",
                  fid, mFsId);
        }
      }
    }

    // Rate limit enforced for the current disk
    mRateLimit->Allow();
  }

  // Push collected errors to QDB
  if (!gOFS.Storage->PushToQdb(mFsId, errs_map)) {
    eos_err("msg=\"failed to push fsck errors to QDB\" fsid=%lu", mFsId);
  }
}

//----------------------------------------------------------------------------
// Cleanup unlinked replicas older than 10 min still laying around
//----------------------------------------------------------------------------
void
ScanDir::CleanupUnlinked()
{
  using eos::common::FileId;
  // Loop over the unlinked files and force unlink them if too old
  auto unlinked_fids = CollectNsFids(eos::fsview::sUnlinkedSuffix);
  eos_info("msg=\"scanning %llu unlinked namespace entries\"",
           unlinked_fids.size());

  while (!unlinked_fids.empty()) {
    eos::IFileMD::id_t fid = unlinked_fids.front();
    unlinked_fids.pop_front();

    try {
      if (IsBeingDeleted(fid) == false) {
        // Put the fid in the queue of files to be deleted and this should
        // clean both the disk file and update the namespace entry
        eos_info("msg=\"resubmit for deletion\" fxid=%08llx fsid=%lu",
                 fid, mFsId);
        std::vector<unsigned long long> id_vect {fid};
        auto deletion = std::make_unique<Deletion>(id_vect, mFsId);
        gOFS.Storage->AddDeletion(std::move(deletion));
      }
    } catch (eos::MDException& e) {
      // There is no file metadata object so we delete any potential file from
      // the local disk and also drop the ghost entry from the file system view
      eos_info("msg=\"cleanup ghost unlinked file\" fxid=%08llx fsid=%lu",
               fid, mFsId);
      std::string fpath =
        FileId::FidPrefix2FullPath(FileId::Fid2Hex(fid).c_str(), mDirPath.c_str());
      // Drop the file from disk and local DB
      XrdOucErrInfo tmp_err;

      if (gOFS._rem("/DELETION_FSCK", tmp_err, nullptr, nullptr, fpath.c_str(),
                    fid, mFsId, true)) {
        eos_err("msg=\"failed remove local file\" path=%s fxid=%08llx fsid=%lu",
                fpath.c_str(), fid, mFsId);
      }

      if (!DropGhostFid(mFsId, fid)) {
        eos_err("msg=\"failed to drop ghost entry\" fxid=%08llx fsid=%lu",
                fid, mFsId);
      }
    }

    mRateLimit->Allow();
  }
}

//------------------------------------------------------------------------------
// Drop ghost fid from the given file system id
//------------------------------------------------------------------------------
bool
ScanDir::DropGhostFid(const eos::common::FileSystem::fsid_t fsid,
                      const eos::IFileMD::id_t fid) const
{
  GlobalOptions opts;
  opts.mForceSss = true;
  FsHelper fs_cmd(opts);

  if (fs_cmd.ParseCommand(SSTR("fs dropghosts " << fsid
                               << " --fid " << fid).c_str())) {
    eos_err("%s", "msg=\"failed to parse fs dropghosts command\"");
    return false;
  }

  if (fs_cmd.ExecuteWithoutPrint()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if file is unlinked from the namespace and in the process of being
// deleted from the disk. Files that are unlinked for more than 10 min
// definetely have a problem and we don't account them as in the process of
// being deleted.
//------------------------------------------------------------------------------
bool
ScanDir::IsBeingDeleted(const eos::IFileMD::id_t fid) const
{
  using namespace std::chrono;
  auto file_fut = eos::MetadataFetcher::getFileFromId(*gOFS.mQcl.get(),
                  eos::FileIdentifier(fid));
  // Throws an exception if file metadata object doesn't exist
  eos::ns::FileMdProto fmd = std::move(file_fut).get();
  return (fmd.cont_id() == 0ull);
}

//------------------------------------------------------------------------------
// Collect all file ids present on the current file system from the NS view
//------------------------------------------------------------------------------
std::deque<eos::IFileMD::id_t>
ScanDir::CollectNsFids(const std::string& type) const
{
  std::deque<eos::IFileMD::id_t> queue;

  if ((type != eos::fsview::sFilesSuffix) &&
      (type != eos::fsview::sUnlinkedSuffix)) {
    eos_err("msg=\"unsupported type %s\"", type.c_str());
    return queue;
  }

  std::ostringstream oss;
  oss << eos::fsview::sPrefix << mFsId << ":" << type;
  const std::string key = oss.str();
  qclient::QSet qset(*gOFS.mQcl.get(), key);

  try {
    for (qclient::QSet::Iterator it = qset.getIterator(); it.valid(); it.next()) {
      try {
        queue.push_back(std::stoull(it.getElement()));
      } catch (...) {
        eos_err("msg=\"failed to convert fid entry\" data=\"%s\"",
                it.getElement().c_str());
      }
    }
  } catch (const std::runtime_error& e) {
    // There is no such set in QDB
  }

  return queue;
}

#endif

//------------------------------------------------------------------------------
// Infinite loop doing the scanning
//------------------------------------------------------------------------------
void
ScanDir::RunDiskScan(ThreadAssistant& assistant) noexcept
{
  eos_info("msg=\"started the disk scan thread\" fsid=%lu dirpath=\"%s\" "
           "disk_scan_interval_sec=%llu", mFsId, mDirPath.c_str(), mDiskInterval.get());
  using namespace std::chrono;
  pid_t tid = 0;

  if (mBgThread) {
    tid = (pid_t) syscall(SYS_gettid);
    int retc = 0;

    if ((retc = ioprio_set(IOPRIO_WHO_PROCESS,
                           IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7)))) {
      eos_err("msg=\"cannot set io priority to lowest best effort\" "
              "retc=%d errno=%d\n", retc, errno);
    } else {
      eos_notice("msg=\"set io priority to 7(lowest best-effort)\" pid=%u "
                 "fsid=%lu", tid, mFsId);
    }
  }

#ifndef _NOOFS

  // Wait for the corresponding file system to boot before starting
  while (gOFS.Storage->IsFsBooting(mFsId)) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      eos_info("%s", "msg=\"stopping disk scan thread\"");
      return;
    }
  }

  if (gOFS.mQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, skipping disk scan\"");
    return;
  }

#endif

  if (mBgThread) {
    mDiskInterval.random_wait(assistant, true, [this](uint64_t sleep_time) {
      eos_info("msg=\"pausing disk scanning\" fsid=%lu init_delay_sec=%llu", mFsId,
               sleep_time);
    });
  }

  while (!assistant.terminationRequested()) {
    mNumScannedFiles =  mTotalScanSize =  mNumCorruptedFiles = 0;
    mNumHWCorruptedFiles =  mNumTotalFiles = mNumSkippedFiles = 0;
    auto start_ts = std::chrono::system_clock::now();
    // Do the heavy work
    CheckTree(assistant);
    auto finish_ts = std::chrono::system_clock::now();
    seconds duration = duration_cast<seconds>(finish_ts - start_ts);
    // Check if there was a config update before we sleep
    std::string log_msg =
      SSTR("[ScanDir] Directory: " << mDirPath << " files=" << mNumTotalFiles
           << " scanduration=" << duration.count() << " [s] scansize="
           << mTotalScanSize << " [Bytes] [ " << (mTotalScanSize / 1e6)
           << " MB ] scannedfiles=" << mNumScannedFiles << " corruptedfiles="
           << mNumCorruptedFiles << " hwcorrupted=" << mNumHWCorruptedFiles
           << " skippedfiles=" << mNumSkippedFiles
           << " disk_scan_interval_sec=" << mDiskInterval.get());

    if (mBgThread) {
      syslog(LOG_ERR, "%s\n", log_msg.c_str());
      eos_notice("%s", log_msg.c_str());
    } else {
      fprintf(stderr, "%s\n", log_msg.c_str());
    }

    if (mBgThread) {
      mDiskInterval.wait(assistant);
    } else {
      break;
    }
  }

  eos_notice("msg=\"done disk scan\" pid=%u fsid=%lu", tid, mFsId);
}

void ScanDir::ScanFsTree(ThreadAssistant& assistant, ScanFunc f,
                         bool skip_internal, const WaitInterval* interval) noexcept
{
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(
                               mDirPath.c_str()));

  if (!io) {
    LogMsg(LOG_ERR, "msg=\"no IO plug-in available\" url=\"%s\"",
           mDirPath.c_str());
    return;
  }

  std::unique_ptr<FileIo::FtsHandle> handle {io->ftsOpen(FTS_NOSTAT)};

  if (!handle) {
    LogMsg(LOG_ERR, "msg=\"fts_open failed\" dir=%s", mDirPath.c_str());
    return;
  }

  std::string fpath;

  while ((fpath = io->ftsRead(handle.get())) != "") {
    if (!mBgThread) {
      fprintf(stderr, "[ScanDir] processing file %s\n", fpath.c_str());
    }

    if (interval) {
      interval->wait_if_zero(assistant);
    }

    if (assistant.terminationRequested()) {
      if (io->ftsClose(handle.get())) {
        LogMsg(LOG_ERR, "msg=\"fts_close failed\" dir=%s", mDirPath.c_str());
      }

      return;
    }

    if (skip_internal) {
      // Skip scanning orphan files
      if (fpath.find("/.eosorphans") != std::string::npos) {
        eos_debug("msg=\"skip orphan file\" path=\"%s\"", fpath.c_str());
        continue;
      }

      // Skip scanning our scrub files (/scrub.write-once.X, /scrub.re-write.X)
      if ((fpath.find("/scrub.") != std::string::npos) ||
          (fpath.find(".xsmap") != std::string::npos)) {
        eos_debug("msg=\"skip scrub/xs file\" path=\"%s\"", fpath.c_str());
        continue;
      }
    }

    f(fpath);
  }

  if (io->ftsClose(handle.get())) {
    LogMsg(LOG_ERR, "msg=\"fts_close failed\" dir=%s", mDirPath.c_str());
  }
}

void ScanDir::CheckTree(ThreadAssistant& assistant) noexcept
{
  eos::common::FsckErrsPerFsMap errs_map;
  auto scan_func = [this, &errs_map](const std::string & fpath) {
    std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath));

    if (CheckFile(io.get(), fpath)) {
#ifndef _NOOFS
      // Collect fsck errors and save them to be sent later on to QDB
      auto fid = eos::common::FileId::PathToFid(fpath.c_str());

      if (!fid) {
        eos_static_info("msg=\"skip file which is not a eos data file\", "
                        "path=\"%s\"", fpath.c_str());
        return;
      }

      auto fmd = gOFS.mFmdHandler->LocalGetFmd(fid, mFsId, true, false);

      if (fmd) {
        CollectInconsistencies(*fmd.get(), mFsId, errs_map);
      }

      UpdateLocalAltXsMetadata(io.get(), *fmd.get());
#endif
    }

    if (assistant.terminationRequested()) {
      break; // make sure to close the FTS handle!
    }
  };
  ScanFsTree(assistant, scan_func);
#ifndef _NOOFS

  // Push collected errors to QDB
  if (!gOFS.Storage->PushToQdb(mFsId, errs_map)) {
    eos_err("msg=\"failed to push fsck errors to QDB\" fsid=%lu", mFsId);
  }

#endif
}

void ScanDir::UpdateLocalAltXsMetadata(eos::fst::FileIo* io,
                                       const eos::common::FmdHelper& fmd)
{
#ifndef _NOOFS

  if (!DoAltXsSync(io)) {
    return;
  }

  // Get fmd from namespace
  eos::ns::ContainerMdProto container;
  eos::ns::FileMdProto file;

  try {
    auto container_fut = eos::MetadataFetcher::getContainerFromId(
                           *gOFS.mQcl.get(),
                           eos::ContainerIdentifier(fmd.mProtoFmd.cid()));
    auto file_fut = eos::MetadataFetcher::getFileFromId(*gOFS.mQcl.get(),
                    eos::FileIdentifier(fmd.mProtoFmd.fid()));
    container = std::move(container_fut).get();
    file = std::move(file_fut).get();
  } catch (const eos::MDException& e) {
    return;
  }

  auto cfg = AltXsConfigFromNS(container);
  auto on_file = ExtractAltXsOnFile(file);
  io->attrSet("user.eos.altxs", cfg);
  StoreAltXsOnMGM(io, on_file);
  SetAltXsSynced(io);
#endif
  return;
}

void ScanDir::SetAltXsSynced(eos::fst::FileIo* io)
{
  auto now_ts = std::chrono::duration_cast<std::chrono::seconds>
                (std::chrono::system_clock::now().time_since_epoch()).count();
  io->attrSet("user.eos.altxs_sync", std::to_string(now_ts));
}

bool ScanDir::DoAltXsSync(eos::fst::FileIo* io)
{
  if (!mAltXsDoSync) {
    return false;
  }

  auto last_sync = getScanTimestamp(io, "user.eos.altxs_sync");

  if (last_sync.count() <= 0) {
    // The sync has never been done
    return true;
  }

  auto sync_interval = mAltXsSyncInterval.load();

  if (sync_interval == 0) {
    // The sync has been done once
    return false;
  }

  std::chrono::system_clock::time_point now_ts(std::chrono::system_clock::now());
  std::chrono::system_clock::time_point sync_ts(last_sync);
  uint64_t elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>
                         (now_ts - sync_ts).count();
  return elapsed_sec >= sync_interval;
}

//------------------------------------------------------------------------------
// Check the given RAIN file
//------------------------------------------------------------------------------
bool ScanDir::CheckReplicaFile(eos::fst::FileIo* io,
                               eos::common::FileId::fileid_t fid,
                               time_t mtime)
{
  auto last_scan = getScanTimestamp(io, "user.eos.timestamp");

  if (!DoRescan(last_scan)) {
    ++mNumSkippedFiles;
    return false;
  }

#ifndef _NOOFS
  gOFS.mFmdHandler->ClearErrors(fid, mFsId, false);
#endif
  return ScanFile(io, io->GetPath(), fid, last_scan, mtime);
}

#ifndef _NOOFS
//------------------------------------------------------------------------------
// Check the given replica file
//------------------------------------------------------------------------------
bool ScanDir::CheckRainFile(eos::fst::FileIo* io,
                            eos::common::FmdHelper* fmd)
{
  auto last_scan = getScanTimestamp(io, "user.eos.rain_timestamp");

  if (!DoRescan(last_scan, true)) {
    ++mNumSkippedFiles;
    return false;
  }

  gOFS.mFmdHandler->ClearErrors(fmd->mProtoFmd.fid(), mFsId, true);
  return ScanRainFile(io, fmd, last_scan);
}
#endif

//------------------------------------------------------------------------------
// Check the given file for errors and properly account them both at the
// scanner level and also by setting the proper xattrs on the file.
//------------------------------------------------------------------------------
bool
ScanDir::CheckFile(eos::fst::FileIo* io, const std::string& fpath)
{
  using eos::common::LayoutId;
  eos_debug("msg=\"start file check\" path=\"%s\"", fpath.c_str());
  auto fid = eos::common::FileId::PathToFid(fpath.c_str());

  if (!fid) {
    eos_static_info("msg=\"skip file which is not an eos data file\", "
                    "path=\"%s\"", fpath.c_str());
    return false;
  }

  // Get last modification time
  struct stat info;

  if ((io->fileOpen(0, 0)) || io->fileStat(&info)) {
    LogMsg(LOG_ERR, "msg=\"open/stat failed\" path=\"%s\"", fpath.c_str());
    return false;
  }

  ++mNumTotalFiles;
#ifndef _NOOFS

  if (mBgThread) {
    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      syslog(LOG_ERR, "skipping scan w-open file: localpath=%s fsid=%d fxid=%08llx\n",
             fpath.c_str(), mFsId, fid);
      eos_warning("msg=\"skipping scan of w-open file\" localpath=%s fsid=%d "
                  "fxid=%08llx", fpath.c_str(), mFsId, fid);
      return false;
    }
  }

  auto fmd = gOFS.mFmdHandler->LocalGetFmd(fid, mFsId, true, false);

  if (!fmd) {
    if (info.st_size == 0) {
      // The file doesn't have an attached fmd object and the size is 0
      // It might be a leftover from an Open with create flag: EOS-6423
      if (io->fileRemove()) {
        LogMsg(LOG_ERR, "msg=\"failed to remove file\" path=\"%s\"", fpath.c_str());
      }
    }

    return false;
  }

  if (LayoutId::IsRain(fmd->mProtoFmd.lid())) {
    return CheckRainFile(io, fmd.get());
  }

#endif
  return CheckReplicaFile(io, fid, info.st_mtime);
}

//------------------------------------------------------------------------------
// Get block checksum object for the given file. First we need to check if
// there is a block checksum file (.xsmap) corresponding to the given raw file.
//------------------------------------------------------------------------------
std::unique_ptr<eos::fst::CheckSum>
ScanDir::GetBlockXS(const std::string& file_path)
{
  using eos::common::LayoutId;
  std::string str_bxs_type, str_bxs_size;
  std::string filexs_path = file_path + ".xsmap";
  std::unique_ptr<eos::fst::FileIo> io(FileIoPluginHelper::GetIoObject(
                                         filexs_path));
  struct stat info;

  if (!io->fileStat(&info, 0)) {
    io->attrGet("user.eos.blockchecksum", str_bxs_type);
    io->attrGet("user.eos.blocksize", str_bxs_size);

    if (str_bxs_type.compare("")) {
      unsigned long bxs_type = LayoutId::GetBlockChecksumFromString(str_bxs_type);
      int bxs_size = atoi(str_bxs_size.c_str());
      int bxs_size_type = LayoutId::BlockSizeEnum(bxs_size);
      auto layoutid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kNone, 0,
                                      bxs_size_type, bxs_type);
      std::unique_ptr<eos::fst::CheckSum> checksum =
        eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, true);

      if (checksum) {
        if (checksum->OpenMap(filexs_path.c_str(), info.st_size, bxs_size, false)) {
          return checksum;
        } else {
          return nullptr;
        }
      } else {
        LogMsg(LOG_ERR, "%s", SSTR("msg=\"failed to get checksum object\" "
                                   << "layoutid=" << std::hex << layoutid
                                   << std::dec << "path=" << filexs_path).c_str());
      }
    } else {
      LogMsg(LOG_ERR, "%s", SSTR("msg=\"file has no blockchecksum xattr\""
                                 << " path=" << filexs_path).c_str());
    }
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Decide if a rescan is needed based on the timestamp provided and the
// configured rescan interval
//------------------------------------------------------------------------------
bool
ScanDir::DoRescan(std::chrono::seconds last_scan, bool rain_ts) const
{
  using namespace std::chrono;
  uint64_t rescan_interval = rain_ts ? mRainEntryInterval.load() :
                             mEntryInterval.load();

  if (rescan_interval == 0) {
    // scan is disabled when this setting is 0
    return false;
  }

  if (last_scan.count() <= 0) {
    return true;
  }

  uint64_t elapsed_sec {0ull};

  // Used only during testing
  if (mClock.IsFake()) {
    steady_clock::time_point old_ts(last_scan);
    steady_clock::time_point now_ts(mClock.getTime());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  } else {
    system_clock::time_point old_ts(last_scan);
    system_clock::time_point now_ts(system_clock::now());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  }

  return elapsed_sec >= rescan_interval;
}

//------------------------------------------------------------------------------
// Check the given file for errors and properly account them both at the
// scanner level and also by setting the proper xattrs on the file.
//------------------------------------------------------------------------------
bool
ScanDir::ScanFile(eos::fst::FileIo* io,
                  const std::string& fpath,
                  eos::common::FileId::fileid_t fid,
                  std::chrono::seconds last_scan,
                  time_t mtime)
{
  std::string lfn, previous_xs_err;
  io->attrGet("user.eos.lfn", lfn);
  io->attrGet("user.eos.filecxerror", previous_xs_err);
  bool was_healthy = (previous_xs_err == "0");
  // Flag if file has been modified since the last time we scanned it
  bool didnt_change = (mtime < last_scan.count());
  bool blockxs_err = false;
  bool filexs_err = false;
  unsigned long long scan_size{0ull};
  std::string scan_xs_hex;

  if (!ScanFileLoadAware(io, scan_size, scan_xs_hex, filexs_err, blockxs_err)) {
    return false;
  }

  bool reopened = false;
#ifndef _NOOFS

  if (mBgThread) {
    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      eos_err("msg=\"file reopened during the scan, ignore checksum error\" "
              "path=%s", fpath.c_str());
      reopened = true;
    }
  }

#endif
  // If file changed or opened for update in the meantime then skip the scan
  struct stat info;

  if (reopened || io->fileStat(&info) || (mtime != info.st_mtime)) {
    LogMsg(LOG_ERR, "msg=\"[ScanDir] skip file modified during scan path=%s",
           fpath.c_str());
    return false;
  }

  if (filexs_err) {
    if (mBgThread) {
      syslog(LOG_ERR, "corrupted file checksum path=%s lfn=%s\n",
             fpath.c_str(), lfn.c_str());
      eos_err("msg=\"corrupted file checksum\" path=\"%s\" lfn=\"%s\"",
              fpath.c_str(), lfn.c_str());
    } else {
      fprintf(stderr, "[ScanDir] corrupted file checksum path=%s lfn=%s\n",
              fpath.c_str(), lfn.c_str());
    }

    if (was_healthy && didnt_change) {
      ++mNumHWCorruptedFiles;

      if (mBgThread) {
        syslog(LOG_ERR, "HW corrupted file found path=%s lfn=%s\n",
               fpath.c_str(), lfn.c_str());
      } else {
        fprintf(stderr, "HW corrupted file found path=%s lfn=%s\n",
                fpath.c_str(), lfn.c_str());
      }
    }
  }

  // Collect statistics
  mTotalScanSize += scan_size;

  if ((io->attrSet("user.eos.timestamp", GetTimestampSmearedSec())) ||
      (io->attrSet("user.eos.filecxerror", filexs_err ? "1" : "0")) ||
      (io->attrSet("user.eos.blockcxerror", blockxs_err ? "1" : "0"))) {
    LogMsg(LOG_ERR, "msg=\"failed to set xattrs\" path=%s", fpath.c_str());
  }

#ifndef _NOOFS

  if (mBgThread) {
    gOFS.mFmdHandler->UpdateWithScanInfo(fid, mFsId, fpath, scan_size,
                                         scan_xs_hex, gOFS.mQcl);
  }

#endif
  return true;
}
//------------------------------------------------------------------------------
// Scan file taking the load into consideration
//------------------------------------------------------------------------------
bool
ScanDir::ScanFileLoadAware(eos::fst::FileIo* io,
                           unsigned long long& scan_size,
                           std::string& scan_xs_hex,
                           bool& filexs_err, bool& blockxs_err)
{
  scan_size = 0ull;
  filexs_err = blockxs_err = false;
  int scan_rate = mRateBandwidth.load(std::memory_order_relaxed);
  std::string file_path = io->GetPath();
  struct stat info;

  if (io->fileStat(&info)) {
    eos_err("msg=\"failed stat\" path=%s\"", file_path.c_str());
    return false;
  }

  // Get checksum type and value
  std::string xs_type;
  char xs_val[SHA256_DIGEST_LENGTH];
  memset(xs_val, 0, sizeof(xs_val));
  size_t xs_len = SHA256_DIGEST_LENGTH;
  io->attrGet("user.eos.checksumtype", xs_type);
  io->attrGet("user.eos.checksum", xs_val, xs_len);
  auto comp_file_xs = eos::fst::ChecksumPlugins::GetXsObj(xs_type);
  std::unique_ptr<eos::fst::CheckSum> blockXS {GetBlockXS(file_path)};

  if (comp_file_xs) {
    comp_file_xs->Reset();
  } else {
    eos_static_warning("msg=\"file has no checksum type xattr\", path=\"%s\"",
                       file_path.c_str());
    //@todo(esindril) if this happens we should get the checksum type
    // from the Fmd information attached also as an xattr.
  }

  int64_t nread = 0;
  off_t offset = 0;
  const auto open_ts = std::chrono::system_clock::now();

  do {
    nread = io->fileRead(offset, mBuffer, mBufferSize);

    if (nread < 0) {
      if (blockXS) {
        blockXS->CloseMap();
      }

      eos_err("msg=\"failed read\" offset=%llu path=%s", offset,
              file_path.c_str());
      return false;
    }

    if (nread) {
      if (nread > mBufferSize) {
        eos_err("msg=\"read returned more than the buffer size\" buff_sz=%llu "
                "nread=%lli\"", mBufferSize, nread);

        if (blockXS) {
          blockXS->CloseMap();
        }

        return false;
      }

      if (blockXS && (blockxs_err == false)) {
        if (!blockXS->CheckBlockSum(offset, mBuffer, nread)) {
          blockxs_err = true;
        }
      }

      if (comp_file_xs) {
        comp_file_xs->Add(mBuffer, nread, offset);
      }

      offset += nread;
      EnforceAndAdjustScanRate(offset, open_ts, scan_rate);
    }
  } while (nread == mBufferSize);

  scan_size = (unsigned long long) offset;
  const auto close_ts = std::chrono::system_clock().now();
  auto tx_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>
                        (close_ts - open_ts).count();
  eos_static_debug("path=%s size(bytes)=%llu scan_duration_ms=%llu rate(MB/s)=%.02f",
                   file_path.c_str(), scan_size, tx_duration_ms,
                   (((1.0 * offset) / (1024 * 1024)) * 1000) / tx_duration_ms)

  // Check file checksum only for replica layouts
  if (comp_file_xs) {
    comp_file_xs->Finalize();
    scan_xs_hex = comp_file_xs->GetHexChecksum();

    if (!comp_file_xs->Compare(xs_val)) {
      auto exp_file_xs = eos::fst::ChecksumPlugins::GetXsObj(xs_type);
      exp_file_xs->SetBinChecksum(xs_val, xs_len);
      LogMsg(LOG_ERR, "msg=\"file checksum error\" expected_file_xs=%s "
             "computed_file_xs=%s scan_size=%llu path=%s",
             exp_file_xs->GetHexChecksum(), comp_file_xs->GetHexChecksum(),
             scan_size, file_path.c_str());
      ++mNumCorruptedFiles;
      filexs_err = true;
    }
  }

  // Check block checksum
  if (blockxs_err) {
    LogMsg(LOG_ERR, "msg=\"corrupted block checksum\" path=%s "
           "blockxs_path=%s.xsmap", file_path.c_str(), file_path.c_str());

    if (mBgThread) {
      syslog(LOG_ERR, "corrupted block checksum path=%s blockxs_path=%s.xsmap\n",
             file_path.c_str(), file_path.c_str());
    }
  }

  if (blockXS) {
    blockXS->CloseMap();
  }

  ++mNumScannedFiles;
  return true;
}

#ifndef _NOOFS
//------------------------------------------------------------------------------
// Check the given file for rain stripes errors
//------------------------------------------------------------------------------
bool ScanDir::ScanRainFile(eos::fst::FileIo* io, eos::common::FmdHelper* fmd,
                           std::chrono::seconds last_scan)
{
  auto fid = fmd->mProtoFmd.fid();
  auto path = io->GetPath();
  std::set<eos::common::FileSystem::fsid_t> invalid_fsid;
  eos_debug("msg=\"starting scanning rain file\" path=%s fsid=%d fxid=%08llx",
            path.c_str(), mFsId, fid);

  if (mBgThread) {
    //  Skip check if file is open for reading, as this can mean we are in the
    //  middle of a recovery operation, and another stripe is open for write
    if (gOFS.openedForReading.isOpen(mFsId, fid) ||
        gOFS.openedForWriting.isOpen(mFsId, fid)) {
      syslog(LOG_ERR, "skipping rain scan rd/wr-open file: localpath=%s fsid=%d "
             "fxid=%08lx\n", path.c_str(), mFsId, fid);
      eos_warning("msg=\"skipping rain scan of rd/wr-open file\" localpath=%s "
                  "fsid=%d fxid=%08llx", path.c_str(), mFsId, fid);
      return false;
    }
  }

  struct stat info_before;

  if (io->fileStat(&info_before)) {
    LogMsg(LOG_ERR, "msg=\"stat failed\" path=%s\"", path.c_str());
    return false;
  }

  if (info_before.st_ctime < last_scan.count()) {
    eos_static_debug("msg=\"skip rain check for unmodified file\" path=\"%s\"",
                     path.c_str());
    return false;
  }

  std::unique_ptr<HeaderCRC> hd(new HeaderCRC(0, 0));

  if (!hd) {
    eos_static_err("msg=\"failed to allocate header\" path=\"%s\"",
                   path.c_str());
    return false;
  }

  hd->ReadFromFile(io, 0);

  if (!hd->IsValid()) {
    // the stripe is corrupted
    eos_debug("msg=\"header file is not valid\" path=%s fsid=%d fxid=%08llx",
              path.c_str(), mFsId, fid);
    ReportInvalidFsid(io, fid, {mFsId});
    return true;
  }

  if (fmd->GetLocations().size() > LayoutId::GetStripeNumber(
        fmd->mProtoFmd.lid()) + 1) {
    if (!ScanRainFileLoadAware(fid, invalid_fsid)) {
      return false;
    }

    if (ShouldSkipAfterCheck(io, fid, info_before)) {
      return false;
    }

    ReportInvalidFsid(io, fid, invalid_fsid);
    return true;
  }

  // We use the new fast method only if the checksum has been computed
  // for the stripe, otherwise we use the old method, since we don't have
  // enough information to get the invalid fsid.
  // The fast method is run by each FSTs, where each of them is checking
  // the stripe that is storing.
  // The old method will only be run by the replica 0 file, and the unitchecksum
  // computation will be triggered.
  // Compute the checksum of the stripe
  std::unique_ptr<eos::fst::CheckSum>
  xs {ChecksumPlugins::GetXsObj(eos::common::LayoutId::eChecksum::kAdler)};
  unsigned long long scansize = 0;
  float scantime = 0; // is ms

  if (!xs->ScanFile(path.c_str(), scansize, scantime, mRateBandwidth.load(),
                    hd->GetSize())) {
    eos_err("msg=\"checksum scanning failed\" path=%s", path.c_str());
    return false;
  }

  XrdOucString sizestring;
  eos_debug("info=\"scanned stripe checksum\" path=%s size=%s time=%.02fms "
            "rate=%.02fMB/s comp_xs=%s", path.c_str(),
            eos::common::StringConversion::GetReadableSizeString(sizestring,
                scansize, "B"),
            scantime,
            1.0 * scansize / 1000 / (scantime ? scantime : 99999999999999LL),
            xs->GetHexChecksum());

  if (fmd->mProtoFmd.has_stripechecksum()) {
    eos_debug("msg=\"stripe checksum available\" xs=%s path=%s fsid=%d fxid=%08llx",
              xs->GetHexChecksum(), path.c_str(), mFsId, fid);

    if (xs->GetHexChecksum() != fmd->mProtoFmd.stripechecksum()) {
      eos_debug("msg=\"checksums do not match\" expected_xs=%s computed_xs=%s "
                "path=%s fsid=%d fxid=%08llx",
                fmd->mProtoFmd.stripechecksum().c_str(), xs->GetHexChecksum(),
                path.c_str(), mFsId, fid);
      invalid_fsid.insert(mFsId);
    }
  } else {
#ifndef _NOOFS
    // The stripe checkum is not stored in the file header
    // So we fallback to the old procedure, storing the checksum
    // for the future checks.
    fmd->mProtoFmd.set_stripechecksum(xs->GetHexChecksum());
    gOFS.mFmdHandler->Commit(fmd);

    // Run the full RAIN check only on the FST storing the first stripe
    if (hd->GetIdStripe() != 0) {
      return false;
    }

    if (!ScanRainFileLoadAware(fid, invalid_fsid)) {
      return false;
    }

#endif
  }

  if (invalid_fsid.empty()) {
    return true;
  }

  if (ShouldSkipAfterCheck(io, fid, info_before)) {
    return false;
  }

  ReportInvalidFsid(io, fid, invalid_fsid);
  return true;
}

//------------------------------------------------------------------------------
// Check if the file was open or update during the scan
//------------------------------------------------------------------------------
bool
ScanDir::ShouldSkipAfterCheck(eos::fst::FileIo* io,
                              eos::common::FileId::fileid_t fid,
                              const struct stat& stat_before)
{
  if (mBgThread) {
    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      eos_static_err("msg=\"file reopened during the scan, ignore stripe "
                     "error\" path=\"%s\"", io->GetPath().c_str());
      return true;
    }
  }

  struct stat info_after;

  if (io->fileStat(&info_after) ||
      (stat_before.st_ctime != info_after.st_ctime)) {
    eos_static_err("msg=\"skip file modified during scan\" path=\"%s\"",
                   io->GetPath().c_str());
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Update local fmd with info from the stripe check
//------------------------------------------------------------------------------
void
ScanDir::ReportInvalidFsid(eos::fst::FileIo* io,
                           eos::common::FileId::fileid_t fid,
                           const std::set<eos::common::FileSystem::fsid_t>& invalid_fsid)
{
  if (io->attrSet("user.eos.rain_timestamp", GetTimestampSmearedSec(true))) {
    eos_static_err("msg=\"failed to set xattr rain_timestamp\" path=\"%s\"",
                   io->GetPath().c_str());
  }

  if (mBgThread) {
    gOFS.mFmdHandler->UpdateWithStripeCheckInfo(fid, mFsId, invalid_fsid);
  }
}

//------------------------------------------------------------------------------
// Check if a given stripe combination can recreate the original file
//------------------------------------------------------------------------------
bool
ScanDir::IsValidStripeCombination(
  const std::vector<std::pair<int, std::string>>& stripes,
  const std::string& xs_val, std::unique_ptr<CheckSum>& xs_obj,
  LayoutId::layoutid_t layout, const std::string& opaqueInfo)
{
  std::unique_ptr<RainMetaLayout> redundancyObj;

  if (LayoutId::GetLayoutType(layout) == LayoutId::kRaidDP) {
    redundancyObj = std::make_unique<RaidDpLayout>
                    (nullptr, layout, nullptr, nullptr,
                     stripes.front().second.c_str(), nullptr, 0, false);
  } else {
    redundancyObj = std::make_unique<ReedSLayout>
                    (nullptr, layout, nullptr, nullptr,
                     stripes.front().second.c_str(), nullptr, 0, false);
  }

  if (redundancyObj->OpenPio(stripes, 0, 0, opaqueInfo.c_str())) {
    eos_static_err("msg=\"unable to pio open\" opaque=\"%s\"", opaqueInfo.c_str());
    redundancyObj->Close();
    return false;
  }

  off_t offsetXrd = 0;
  const auto open_ts = std::chrono::system_clock::now();
  int scan_rate = mRateBandwidth.load(std::memory_order_relaxed);
  xs_obj->Reset();

  while (true) {
    int64_t nread = redundancyObj->Read(offsetXrd, mBuffer, mBufferSize);

    if (nread == 0) {
      break;
    }

    if (nread == -1) {
      redundancyObj->Close();
      return false;
    }

    xs_obj->Add(mBuffer, nread, offsetXrd);
    offsetXrd += nread;
    EnforceAndAdjustScanRate(offsetXrd, open_ts, scan_rate);
  }

  redundancyObj->Close();
  xs_obj->Finalize();
  return !strcmp(xs_obj->GetHexChecksum(), xs_val.c_str());
}

//------------------------------------------------------------------------------
// Return the list of stripes for the file
//------------------------------------------------------------------------------
bool ScanDir::GetPioOpenInfo(eos::common::FileId::fileid_t fid,
                             std::vector<stripe_s>& stripes,
                             std::string& opaqueInfo,
                             uint32_t num_locations)
{
  const std::string mgr = gConfig.GetManager();

  if (mgr.empty()) {
    eos_static_err("%s", "msg=\"no manager info available\"");
    return false;
  }

  const std::string address = SSTR("root://" << mgr << "/");
  const XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("msg=\"invalid url\" url=\"%s\"", address.c_str());
    return false;
  }

  // Query MGM for list of stripes to open
  XrdCl::Buffer arg;
  XrdCl::Buffer* resp_raw = nullptr;
  const std::string opaque = SSTR("/.fxid:" << std::hex << fid << std::dec
                                  << "?mgm.pcmd=open"
                                  << "&eos.ruid=" << DAEMONUID
                                  << "&eos.rgid=" << DAEMONGID
                                  << "&xrd.wantprot=sss"
                                  << "&eos.app=eos/fsck_scan");
  arg.FromString(opaque);
  XrdCl::FileSystem fs(url);
  const XrdCl::XRootDStatus status =
    fs.Query(XrdCl::QueryCode::OpaqueFile, arg, resp_raw);

  if (!status.IsOK()) {
    eos_static_err("msg=\"MGM query failed: '%s'\" opaque=\"%s\"",
                   status.ToString().c_str(), opaque.c_str());
    delete resp_raw;
    return false;
  }

  const std::string response(resp_raw->GetBuffer(), resp_raw->GetSize());
  delete resp_raw;
  // @note: fragile design as it depends on the location of mgm.logid!
  const char* ptr = strstr(response.c_str(), "&mgm.logid");

  if (ptr == nullptr) {
    eos_static_err("msg=\"MGM open reply not in the expected format, maybe "
                   "a redirect\" reply_data=\"%s\"", response.c_str());
    return false;
  }

  opaqueInfo = ptr;
  std::unique_ptr<XrdOucEnv> openOpaque(new XrdOucEnv(response.c_str()));
  XrdOucEnv* raw_cap_opaque = nullptr;
  eos::common::SymKey::ExtractCapability(openOpaque.get(), raw_cap_opaque);
  std::unique_ptr<XrdOucEnv> capOpaque(raw_cap_opaque);

  if (!capOpaque->Get("mgm.path")) {
    eos_static_err("msg=\"no path in mgm cap response\" response=\"%s\"",
                   response.c_str());
    return false;
  }

  const std::string ns_path = capOpaque->Get("mgm.path");
  FileSystem::fsid_t stripeFsId = 0;
  std::string stripeUrl;
  std::string pio;
  std::string tag;

  for (unsigned long i = 0; i < num_locations; ++i) {
    tag = SSTR("pio." << i);

    // Skip files with missing replicas, they will be detected elsewhere.
    if (!openOpaque->Get(tag.c_str())) {
      return false;
    }

    pio = openOpaque->Get(tag.c_str());
    stripeUrl = SSTR("root://" << pio << "/" << ns_path);
    stripeFsId = capOpaque->GetInt(SSTR("mgm.fsid" << i).c_str());
    // Start by marking all stripes invalid. Mark them unknown once we
    // have successfully read their headers.
    stripes.push_back({stripeFsId, stripeUrl, stripe_s::Invalid, 0});
  }

  return true;
}

//------------------------------------------------------------------------------
// Check for stripes that are unable to reconstruct the original file
//------------------------------------------------------------------------------
bool
ScanDir::ScanRainFileLoadAware(eos::common::FileId::fileid_t fid,
                               std::set<eos::common::FileSystem::fsid_t>&
                               invalid_fsid)
{
  eos_static_debug("msg=\"scan rain file load aware\" fxid=%08llx", fid);
  std::string xs_mgm;
  uint32_t num_locations;
  LayoutId::layoutid_t layout;
  {
    // Reduce scope of the FmdHelper object
    auto fmd = gOFS.mFmdHandler->LocalGetFmd(fid, mFsId, true, false);

    if (!fmd) {
      eos_static_err("msg=\"could not get fmd from manager\" fxid=%08llx", fid);
      return false;
    }

    layout = fmd->mProtoFmd.lid();

    if (!LayoutId::IsRain(layout)) {
      eos_static_err("msg=\"layout is not rain\" fixd=%08llx", fid);
      return false;
    }

    num_locations = fmd->GetLocations().size();
    xs_mgm = fmd->mProtoFmd.mgmchecksum();
  }

  if (xs_mgm.empty() || (num_locations == 0)) {
    eos_static_err("msg=\"mgm checksum empty or no locations\" fxid=%08llx",
                   fid);
    return false;
  }

  const auto nStripes = LayoutId::GetStripeNumber(layout) + 1;
  const auto nParityStripes = LayoutId::GetRedundancyStripeNumber(layout);
  const auto nDataStripes = nStripes - nParityStripes;
  std::vector<stripe_s> stripes;
  stripes.reserve(num_locations);
  std::string opaqueInfo;

  if (!GetPioOpenInfo(fid, stripes, opaqueInfo, num_locations)) {
    eos_static_err("msg=\"skip rain file scan due to missing open info\" "
                   "fxid=%08llx", fid);
    return false;
  }

  std::unique_ptr<HeaderCRC> hd {new HeaderCRC(0, 0)};

  if (!hd) {
    eos_static_err("%s", "msg=\"failed to instantiate header object\"");
    return false;
  }

  // Read each header to check if it is valid
  std::map<unsigned int, std::set<unsigned long>> mapPL;

  for (unsigned long i = 0; i < stripes.size(); ++i) {
    std::unique_ptr<FileIo> file{FileIoPlugin::GetIoObject(stripes[i].url)};

    if (file) {
      const std::string new_opaque =
        SSTR(opaqueInfo << "&mgm.replicaindex=" << i);
      file->fileOpen(SFS_O_RDONLY, 0, new_opaque);
      hd->ReadFromFile(file.get(), 0);
      file->fileClose();

      // If stripe id is greater than nStripe, it's invalid
      if (hd->IsValid() && (hd->GetIdStripe() < nStripes)) {
        stripes[i].id = hd->GetIdStripe();
        stripes[i].state = stripe_s::Unknown;
        mapPL[hd->GetIdStripe()].insert(i);
      }
    }
  }

  if (mapPL.size() < nDataStripes) {
    eos_static_err("msg=\"not enough valid stripes to reconstruct\" "
                   "fxid=%08llx", fid);
    invalid_fsid.insert(0);
    return true;
  }

  std::unique_ptr<CheckSum> xs_obj(ChecksumPlugins::GetChecksumObject(layout));

  if (!xs_obj) {
    eos_static_err("msg=\"invalid xs_type\" fxid=%08llx", fid);
    return false;
  }

  std::vector<bool> combinations(num_locations, false);
  std::fill(combinations.begin(), combinations.begin() + nDataStripes, true);
  std::vector<std::pair<int, std::string>>
                                        stripeCombination(nParityStripes, std::make_pair(0, "root://__offline_"));
  stripeCombination.reserve(nStripes);

  // Try to find a valid stripe combination
  do {
    stripeCombination.erase(stripeCombination.begin() + nParityStripes,
                            stripeCombination.end());

    for (unsigned long i = 0; i < combinations.size(); ++i) {
      if (combinations[i]) {
        // Skip combinations with invalid stripes
        if (stripes[i].state == stripe_s::Invalid) {
          break;
        }

        // Skip if multiple duplicated stripes are in the same combination
        auto HasDuplicate = [i, &combinations](unsigned long j) {
          return i != j && combinations[j];
        };
        auto& stripe_loc = mapPL[stripes[i].id];

        if (std::find_if(stripe_loc.begin(), stripe_loc.end(), HasDuplicate) !=
            stripe_loc.end()) {
          break;
        }

        stripeCombination.emplace_back(i, stripes[i].url);
      }
    }

    // Skip combination if we exited early from previous loop
    if (stripeCombination.size() != nStripes) {
      continue;
    }

    if (IsValidStripeCombination(stripeCombination, xs_mgm, xs_obj,
                                 layout, opaqueInfo)) {
      for (unsigned long i = 0; i < combinations.size(); ++i) {
        if (combinations[i]) {
          stripes[i].state = stripe_s::Valid;
        }
      }

      break;
    }
  } while (std::prev_permutation(combinations.begin(), combinations.end()));

  auto isValid = [](const stripe_s & s) {
    return s.state == stripe_s::Valid;
  };

  if (std::none_of(stripes.begin(), stripes.end(), isValid)) {
    eos_static_err("msg=\"not enough valid stripes for reconstruction\" "
                   "fxid=%08llx", fid);
    invalid_fsid.insert(0);
    return true;
  }

  // Found a valid combination, check the rest of the stripes
  for (unsigned long i = 0; i < stripes.size(); ++i) {
    if (stripes[i].state == stripe_s::Unknown) {
      stripeCombination.erase(stripeCombination.begin() + nParityStripes,
                              stripeCombination.end());
      // Try combinations with 1 unknown stripe and nDataStripes - 1 valid stripes.
      // Exclude duplicates from the combination.
      stripeCombination.emplace_back(i, stripes[i].url);
      auto& skipStripes = mapPL[stripes[i].id];

      for (unsigned long j = 0; j < stripes.size(); ++j) {
        if (stripes[j].state == stripe_s::Valid &&
            skipStripes.find(j) == skipStripes.end()) {
          stripeCombination.emplace_back(j, stripes[j].url);

          if (stripeCombination.size() == nStripes) {
            break;
          }
        }
      }

      if (IsValidStripeCombination(stripeCombination, xs_mgm, xs_obj,
                                   layout, opaqueInfo)) {
        stripes[i].state = stripe_s::Valid;
      } else {
        stripes[i].state = stripe_s::Invalid;
      }
    }
  }

  // Collect the fsids of all the invalid stripes
  for (unsigned long i = 0; i < stripes.size(); i++) {
    if (stripes[i].state == stripe_s::Invalid) {
      eos_static_err("msg=\"stripe %d on fst %d is invalid\" fxid=%08llx", i,
                     stripes[i].fsid, fid);
      invalid_fsid.insert(stripes[i].fsid);
    }
  }

  // Collect the fsids of all the duplicated stripes, keeping the replica
  // with the lowest fsid
  for (auto [_, replicas] : mapPL) {
    if (replicas.size() > 1) {
      // Used to get the index of the replica which is both valid
      // and has the lowest fsid.
      auto fsidCmp = [&stripes](const auto & a, const auto & b) {
        return stripes[b].state != stripe_s::Valid ||
               (stripes[a].state == stripe_s::Valid &&
                stripes[a].fsid < stripes[b].fsid);
      };
      auto min = *std::min_element(replicas.begin(), replicas.end(), fsidCmp);

      // Skip if all replicas are invalid
      if (stripes[min].state == stripe_s::Valid) {
        for (auto i : replicas) {
          if ((i != min) && (stripes[i].state == stripe_s::Valid)) {
            eos_static_info("msg=\"marking excess stripe %d on fst %d as "
                            "invalid\" fxid=%08llx", i, stripes[i].fsid, fid);
            invalid_fsid.insert(stripes[i].fsid);
          }
        }
      }
    }
  }

  return true;
}
#endif
//------------------------------------------------------------------------------
// Enforce the scan rate by throttling the current thread and also adjust it
// depending on the IO load on the mountpoint
//------------------------------------------------------------------------------
void
ScanDir::EnforceAndAdjustScanRate(const off_t offset,
                                  const std::chrono::time_point
                                  <std::chrono::system_clock> open_ts,
                                  int& scan_rate)
{
  using namespace std::chrono;

  if (scan_rate && mFstLoad) {
    const auto now_ts = std::chrono::system_clock::now();
    uint64_t scan_duration_msec =
      duration_cast<milliseconds>(now_ts - open_ts).count();
    uint64_t expect_duration_msec =
      (uint64_t)((1.0 * offset) / (scan_rate * 1024 * 1024)) * 1000;

    if (expect_duration_msec > scan_duration_msec) {
      std::this_thread::sleep_for(milliseconds(expect_duration_msec -
                                  scan_duration_msec));
    }

    // Adjust the rate according to the load information
    double load = mFstLoad->GetDiskRate(mDirPath.c_str(), "millisIO") / 1000.0;

    if (load > 0.7) {
      // Adjust the scan_rate which is in MB/s but no lower then 5 MB/s
      if (scan_rate > 5) {
        scan_rate = 0.9 * scan_rate;
      }
    } else {
      scan_rate = mRateBandwidth.load(std::memory_order_relaxed);
    }
  }
}
//------------------------------------------------------------------------------
// Get timestamp smeared +/-20% of mEntryIntervalSec around the current
// timestamp value
//------------------------------------------------------------------------------
std::string
ScanDir::GetTimestampSmearedSec(bool rain_ts) const
{
  using namespace std::chrono;
  uint64_t entry_interval_sec;

  if (rain_ts) {
    entry_interval_sec = mRainEntryInterval.load();
  } else {
    entry_interval_sec = mEntryInterval.load();
  }

  int64_t smearing =
    (int64_t)(0.2 * 2 * entry_interval_sec * random() / RAND_MAX) -
    (int64_t)(0.2 * entry_interval_sec);
  uint64_t ts_sec;

  if (mClock.IsFake()) {
    ts_sec = duration_cast<seconds>(mClock.getTime().time_since_epoch()).count();
  } else {
    ts_sec = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  }

  // Avoid underflow when using the steady_clock for testing
  if ((uint64_t)std::abs(smearing) < ts_sec) {
    ts_sec += smearing;
  }

  return std::to_string(ts_sec);
}
EOSFSTNAMESPACE_END
