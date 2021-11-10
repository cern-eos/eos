// ----------------------------------------------------------------------
// File: Fsck.cc
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

#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "fst/Fsck.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/filemd/FmdMgm.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "qclient/structures/QSet.hh"
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#ifndef __APPLE__
#include <sys/syscall.h>
#endif
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
#include <thread>
#include <chrono>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Fsck::Fsck(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
           eos::fst::Load* fstload, long int testinterval, long int filerate,
           const std::string managerhostport , bool issilent) :
  fstLoad(fstload), fsId(fsid), dirPath(dirpath), mTestInterval(testinterval),
  mScanRate(filerate), managerHostPort(managerhostport), silent(issilent)
{
  thread = 0;
  noCorruptFiles = noTotalFiles = 0;
  durationScan = 0;
  totalScanSize = bufferSize = 0;
  buffer = 0;
  useQuarkDB = false;

  if (mScanRate < 1) {
    mScanRate = 1;
  }

  alignment = pathconf((dirpath[0] != '/') ? "/" : dirPath.c_str(),
                       _PC_REC_XFER_ALIGN);
  size_t palignment = alignment;

  if (alignment > 0) {
    bufferSize = 256 * alignment;

    if (posix_memalign((void**) &buffer, palignment, bufferSize)) {
      buffer = 0;
      fprintf(stderr, "error: error calling posix_memaling on dirpath=%s. \n",
              dirPath.c_str());
      return;
    }

#ifdef __APPLE__
    palignment = 0;
#endif
  } else {
    fprintf(stderr, "error: OS does not provide alignment\n");
    exit(-1);
  }

  errors["missing"] = 0;
  errors["zeromis"] = 0;
  errors["size"] = 0;
  errors["checksum"] = 0;
  errors["replica"] = 0;
  errors["detached"] = 0;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

Fsck::~Fsck()
{
  if (buffer) {
    free(buffer);
  }
}

//------------------------------------------------------------------------------
// Update scanner configuration
//------------------------------------------------------------------------------

void
Fsck::SetConfig(const std::string& key, long long value)
{
  eos_info("msg=\"update scanner configuration\" key=\"%s\" value=\"%s\"",
           key.c_str(), std::to_string(value).c_str());

  if (key == "scaninterval") {
    mTestInterval = value;
  }
}

/*----------------------------------------------------------------------------*/
void
scandir_cleanup_handle(void* arg)
{
  FileIo::FtsHandle* handle = static_cast<FileIo::FtsHandle*>(arg);

  if (handle) {
    delete handle;
  }
}

/*----------------------------------------------------------------------------*/
void
Fsck::ScanFiles()
{
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(dirPath.c_str()));

  if (!io) {
    fprintf(stderr, "error: no IO plug-in available for url=%s\n", dirPath.c_str());
    return;
  }

  FileIo::FtsHandle* handle = io->ftsOpen();

  if (!handle) {
    fprintf(stderr, "error: fts_open failed! \n");
    return;
  }

  pthread_cleanup_push(scandir_cleanup_handle, handle);
  std::string filePath;
  size_t nfiles = 0;
  long long start = eos::common::Timing::GetNowInNs();

  while ((filePath = io->ftsRead(handle)) != "") {
    nfiles++;

    if (!silent) {
      fprintf(stdout, "[Fsck] [ DSK ] [ %07ld ] processing file %s\n", nfiles,
              filePath.c_str());
    }

    CheckFile(filePath.c_str());
    unsigned long long age = eos::common::Timing::GetAgeInNs(start);
    unsigned long long expected_age = 1000000000.0 / mScanRate * nfiles;

    if (age < expected_age) {
      // throttle the rate
      std::this_thread::sleep_for(std::chrono::nanoseconds(expected_age - age));
    }
  }

  if (io->ftsClose(handle)) {
    fprintf(stderr, "error: fts_close failed \n");
  }

  delete handle;
  pthread_cleanup_pop(0);
}

/*----------------------------------------------------------------------------*/
void
Fsck::CheckFile(const char* filepath)
{
  std::string filePath, checksumType, checksumStamp, logicalFileName,
      previousFileCxError, previousBlockCxError;
  char checksumVal[SHA256_DIGEST_LENGTH];
  size_t checksumLen;
  bool has_filecxerror = false;
  bool has_blockcxerror = false;
  filePath = filepath;
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(filepath));
  noTotalFiles++;
  // get last modification time
  struct stat buf1;

  if ((io->fileOpen(0, 0)) || io->fileStat(&buf1)) {
    fprintf(stderr, "error: cannot open/stat %s\n", filePath.c_str());
    return;
  }

  io->attrGet("user.eos.checksumtype", checksumType);
  memset(checksumVal, 0, sizeof(checksumVal));
  checksumLen = SHA256_DIGEST_LENGTH;

  if (io->attrGet("user.eos.checksum", checksumVal, checksumLen)) {
    checksumLen = 0;
  }

  io->attrGet("user.eos.timestamp", checksumStamp);
  io->attrGet("user.eos.lfn", logicalFileName);

  if (!io->attrGet("user.eos.filecxerror", previousFileCxError)) {
    has_filecxerror = true;
  }

  if (!io->attrGet("user.eos.blockcxerror", previousBlockCxError)) {
    has_blockcxerror = true;
  }

  io->fileClose();
  // do checks
  uint64_t fid = eos::common::FileId::PathToFid(filepath);

  if (fid) {
    if (!mMd.count(fid)) {
      fprintf(stderr,
              "[Fsck] [ERROR] [ DETACHE ] fsid:%d cxid:???????? fxid:%08lx "
              "path:%s is detached on disk\n",
              fsId, fid, filepath);
      errors["detached"]++;
    } else {
      auto& proto_fmd = mMd[fid].mProtoFmd;

      if (checksumLen) {
        std::unique_ptr<CheckSum> checksum =
          ChecksumPlugins::GetChecksumObject(proto_fmd.lid(), false);
        checksum->SetBinChecksum(checksumVal, checksumLen);
        std::string hex_checksum = checksum->GetHexChecksum();
        proto_fmd.set_diskchecksum(hex_checksum);
      }

      if ((previousFileCxError == "0") || (!has_filecxerror)) {
        proto_fmd.set_filecxerror(0);
      } else {
        proto_fmd.set_filecxerror(1);
      }

      if ((previousBlockCxError == "0") || (!has_blockcxerror)) {
        proto_fmd.set_blockcxerror(0);
      } else {
        proto_fmd.set_blockcxerror(1);
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
std::string
Fsck::GetTimestamp()
{
  char buffer[65536];
  size_t size = sizeof(buffer) - 1;
  long long timestamp;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  timestamp = tv.tv_sec * 1000000 + tv.tv_usec;
  snprintf(buffer, size, "%lli", timestamp);
  return std::string(buffer);
}

/*----------------------------------------------------------------------------*/
std::string
Fsck::GetTimestampSmeared()
{
  char buffer[65536];
  size_t size = sizeof(buffer) - 1;
  long long timestamp;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  timestamp = tv.tv_sec * 1000000 + tv.tv_usec;
  // smear +- 20% of mTestInterval around the value
  long int smearing = (long int)((0.2 * 2 * mTestInterval * random() / RAND_MAX))
                      - ((long int)(0.2 * mTestInterval));
  snprintf(buffer, size, "%lli", timestamp + smearing);
  return std::string(buffer);
}

/*----------------------------------------------------------------------------*/
bool
Fsck::RescanFile(std::string fileTimestamp)
{
  if (!fileTimestamp.compare("")) {
    return true; //first time we check
  }

  long long oldTime = atoll(fileTimestamp.c_str());
  long long newTime = atoll(GetTimestamp().c_str());

  if (((newTime - oldTime) / 1000000) < mTestInterval) {
    return false;
  } else {
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void*
Fsck::StaticThreadProc(void* arg)
{
  return reinterpret_cast<Fsck*>(arg)->ThreadProc();
}

/*----------------------------------------------------------------------------*/
void*
Fsck::ThreadProc(void)
{
  struct timezone tz;
  struct timeval tv_start, tv_end;
  totalScanSize = 0;
  noCorruptFiles = 0;
  noTotalFiles = 0;
  gettimeofday(&tv_start, &tz);

  if (useQuarkDB) {
    ScanMdQdb();
  } else {
    if (managerHostPort.length()) {
      ScanMd();
    }
  }

  ScanFiles();
  ReportFiles();
  gettimeofday(&tv_end, &tz);
  durationScan = ((tv_end.tv_sec - tv_start.tv_sec) * 1000.0) + ((
                   tv_end.tv_usec - tv_start.tv_usec) / 1000.0);

  for (auto it = errors.begin(); it != errors.end(); ++it) {
    fprintf(stdout, "[Fsck] [ESTAT] error:%-16s cnt:%lu\n", it->first.c_str(),
            it->second);
  }

  fprintf(stdout,
          "[Fsck] [ESUMM]: %s, fsid=%d files=%li fsckduration=%.02f [s] corruptedfiles=%ld\n",
          dirPath.c_str(), fsId, noTotalFiles, (durationScan / 1000.0), noCorruptFiles);
  return NULL;
}

/*----------------------------------------------------------------------------*/
void
Fsck::ScanMd()
{
  std::string tmpfile;

  // dump all metadata from mgm
  if (!eos::fst::FmdMgmHandler::ExecuteDumpmd(managerHostPort, fsId, tmpfile)) {
    return;
  }

  // Parse the result and unlink temporary file
  std::ifstream inFile(tmpfile);
  std::string dumpentry;
  unlink(tmpfile.c_str());
  size_t nfiles = 0;
  long long start = eos::common::Timing::GetNowInNs();

  while (std::getline(inFile, dumpentry)) {
    nfiles++;
    std::unique_ptr<XrdOucEnv> env(new XrdOucEnv(dumpentry.c_str()));

    if (env) {
      eos::common::FmdHelper fMd;

      if (eos::fst::FmdMgmHandler::EnvMgmToFmd(*env, fMd)) {
        // now the MD object is filled
        CheckFile(fMd, nfiles);
        mMd[fMd.mProtoFmd.fid()] = fMd;
      } else {
        fprintf(stderr, "failed to convert %s\n", dumpentry.c_str());
      }
    }

    unsigned long long age = eos::common::Timing::GetAgeInNs(start);
    unsigned long long expected_age = 1000000000.0 / mScanRate * nfiles;

    if (age < expected_age) {
      // throttle the rate
      std::this_thread::sleep_for(std::chrono::nanoseconds(expected_age - age));
    }

    if (!(nfiles % 10000)) {
      if (!silent) fprintf(stdout,
                             "msg=\"synced files so far\" nfiles=%lu fsid=%lu\n", nfiles,
                             (unsigned long) fsId);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
Fsck::ScanMdQdb()
{
  using namespace std::chrono;
  // Collect all file ids on the desired file system
  std::string cursor = "0";
  long long count = 250000;
  std::pair<std::string, std::vector < std::string>> reply;
  std::unique_ptr<qclient::QClient>
  qcl(new qclient::QClient(contactDetails.members,
                           contactDetails.constructOptions()));
  qclient::QSet qset(*qcl.get(), eos::RequestBuilder::keyFilesystemFiles(fsId));
  std::unordered_set<eos::IFileMD::id_t> file_ids;

  try {
    do {
      reply = qset.sscan(cursor, count);
      cursor = reply.first;

      for (const auto& elem : reply.second) {
        file_ids.insert(std::stoull(elem));
      }
    } while (cursor != "0");
  } catch (const std::runtime_error& e) {
    // It means there are no records for the current file system
  }

  uint64_t total = file_ids.size();

  if (silent) {
    fprintf(stdout, "resyncing %lu files for file_system %u", total, fsId);
  }

  uint64_t num_files = 0;
  auto it = file_ids.begin();
  std::list<folly::Future < eos::ns::FileMdProto>> files;

  // Pre-fetch the first 1000 files
  while ((it != file_ids.end()) && (num_files < 1000)) {
    ++num_files;
    files.emplace_back(MetadataFetcher::getFileFromId(*qcl.get(),
                       FileIdentifier(*it)));
    ++it;
  }

  size_t nfiles = 0;

  while (!files.empty()) {
    nfiles++;
    eos::common::FmdHelper fMd;

    try {
      FmdMgmHandler::NsFileProtoToFmd(std::move(files.front()).get(), fMd);
      CheckFile(fMd, nfiles);
      mMd[fMd.mProtoFmd.fid()] = fMd;
      files.pop_front();
    } catch (const eos::MDException& e) {
      fprintf(stderr, "msg=\"failed to get metadata from QuarkDB: %s\"\n", e.what());
      files.pop_front();
    }

    if (it != file_ids.end()) {
      ++num_files;
      files.emplace_back(MetadataFetcher::getFileFromId(*qcl.get(),
                         FileIdentifier(*it)));
      ++it;
    }
  }
}

/*----------------------------------------------------------------------------*/
void
Fsck::CheckFile(eos::common::FmdHelper& fMd, size_t nfiles)
{
  char fxid[1024];
  sprintf(fxid, "%08lx", fMd.mProtoFmd.fid());
  std::string fullpath = eos::common::FileId::FidPrefix2FullPath(fxid,
                         dirPath.c_str());
  struct stat buf;

  if (!silent) {
    fprintf(stdout,
            "[Fsck] [ MGM ] [ %07lu ] processing file cxid:%08lx fxid:%08lx path:%s\n",
            nfiles, fMd.mProtoFmd.cid(), fMd.mProtoFmd.fid(), fullpath.c_str());
  }

  if (stat(fullpath.c_str(), &buf)) {
    if (!fMd.mProtoFmd.size()) {
      fprintf(stderr,
              "[Fsck] [ERROR] [ ZEROMIS ] fsid:%d cxid:%08lx fxid:%08lx path:%s "
              "is missing  on disk\n",
              fsId, fMd.mProtoFmd.cid(), fMd.mProtoFmd.fid(), fullpath.c_str());
      errors["zeromis"]++;
    } else {
      fprintf(stderr,
              "[Fsck] [ERROR] [ MISSING ] fsid:%d cxid:%08lx fxid:%08lx "
              "path:%s is missing  on disk\n",
              fsId, fMd.mProtoFmd.cid(), fMd.mProtoFmd.fid(), fullpath.c_str());
      errors["missing"]++;
      fMd.mProtoFmd.set_disksize(-1);
    }

    fMd.mProtoFmd.set_disksize(-1);
  } else {
    fMd.mProtoFmd.set_disksize(buf.st_size);
  }

  // for the moment we don't have an extra field to store this, but we don't checksum here
  fMd.mProtoFmd.set_checksum(fullpath.c_str());
}

/*----------------------------------------------------------------------------*/
void
Fsck::ReportFiles()
{
  // don't get confused here, it->second.checksum() contains the local fst path
  for (auto it = mMd.begin();  it != mMd.end(); ++it) {
    bool corrupted = false;
    auto& proto_fmd = it->second.mProtoFmd;

    if (proto_fmd.disksize() != (unsigned long) - 1) {
      if (eos::common::LayoutId::GetLayoutType(proto_fmd.lid()) <=
          eos::common::LayoutId::kReplica) {
        if (proto_fmd.disksize() != proto_fmd.mgmsize()) {
          fprintf(stderr,
                  "[Fsck] [ERROR] [ SIZE    ] fsid:%d cxid:%08lx fxid:%08lx "
                  "path:%s size mismatch disksize=%lu mgmsize=%lu\n",
                  fsId, proto_fmd.cid(), proto_fmd.fid(), proto_fmd.checksum().c_str(),
                  proto_fmd.disksize(), proto_fmd.mgmsize());
          errors["size"]++;
          corrupted = true;
        }
      }
    }

    if (eos::common::LayoutId::GetChecksum(proto_fmd.lid()) !=
        eos::common::LayoutId::kNone) {
      if (proto_fmd.diskchecksum().length()) {
        if (proto_fmd.diskchecksum() != proto_fmd.mgmchecksum()) {
          fprintf(stderr,
                  "[Fsck] [ERROR] [ CKS     ] fsid:%d cxid:%08lx fxid:%08lx "
                  "path:%s checksum mismatch diskxs=%s mgmxs=%s\n",
                  fsId, proto_fmd.cid(), proto_fmd.fid(), proto_fmd.checksum().c_str(),
                  proto_fmd.diskchecksum().c_str(), proto_fmd.mgmchecksum().c_str());
          errors["checksum"]++;
          corrupted = true;
        }
      }

      if (proto_fmd.filecxerror()) {
        fprintf(stderr,
                "[Fsck] [ERROR] [ CKSFLAG ] fsid:%d cxid:%08lx fxid:%08lx "
                "path:%s checksum error flagged diskxs=%s mgmxs=%s\n",
                fsId, proto_fmd.cid(), proto_fmd.fid(), proto_fmd.checksum().c_str(),
                proto_fmd.diskchecksum().c_str(), proto_fmd.mgmchecksum().c_str());
        errors["checksumflag"]++;
        corrupted = true;
      }

      if (proto_fmd.blockcxerror()) {
        fprintf(stderr,
                "[Fsck] [ERROR] [ BXSFLAG ] fsid:%d cxid:%08lx fxid:%08lx "
                "path:%s block checksum error flagged diskxs=%s mgmxs=%s\n",
                fsId, proto_fmd.cid(), proto_fmd.fid(), proto_fmd.checksum().c_str(),
                proto_fmd.diskchecksum().c_str(), proto_fmd.mgmchecksum().c_str());
        errors["blockcksflag"]++;
        corrupted = true;
      }

      if (corrupted) {
        noCorruptFiles++;
      }
    }

    auto location_set = it->second.GetLocations();
    size_t nstripes = eos::common::LayoutId::GetStripeNumber(proto_fmd.lid()) + 1;

    if (nstripes != location_set.size()) {
      if (proto_fmd.mgmsize() != 0) {
        // suppress 0 size files in the mgm
        fprintf(stderr,
                "[Fsck] [ERROR] [ REPLICA ] fsid:%d cxid:%08lx fxid:%08lx "
                "path:%s replica count wrong is=%lu expected=%lu\n",
                fsId, proto_fmd.cid(), proto_fmd.fid(),
                proto_fmd.checksum().c_str(), location_set.size(), nstripes);
        errors["replica"]++;
      }
    }
  }
}
EOSFSTNAMESPACE_END
