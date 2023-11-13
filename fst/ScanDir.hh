//------------------------------------------------------------------------------
// File: ScanDir.hh
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

#pragma once
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/FileId.hh"
#include "common/AssistedThread.hh"
#include "common/SteadyClock.hh"
#include "common/RateLimit.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include <deque>

EOSFSTNAMESPACE_BEGIN

class Load;
class FileIo;
class CheckSum;

constexpr uint64_t DEFAULT_DISK_INTERVAL = 4 * 3600;
constexpr uint64_t DEFAULT_NS_INTERVAL = 3 * 24 * 3600;
//------------------------------------------------------------------------------
//! Class ScanDir
//! @brief Scan a directory tree and checks checksums (and blockchecksums if
//! present) on a regular interval with limited bandwidth
//------------------------------------------------------------------------------
class ScanDir: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
          eos::fst::Load* fstload, bool bgthread = true,
          long int file_rescan_interval = 60, int ratebandwidth = 50,
          bool fake_clock = false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ScanDir();

  //----------------------------------------------------------------------------
  //! Update scanner configuration
  //!
  //! @param key configuration type
  //! @param value configuration value
  //----------------------------------------------------------------------------
  void SetConfig(const std::string&, long long value);

  //------------------------------------------------------------------------------
  //! Infinite loop doing the scanning and verification of disk entries
  //!
  //! @param assistant thread running the job
  //------------------------------------------------------------------------------
  void RunDiskScan(ThreadAssistant& assistant) noexcept;

#ifndef _NOOFS
  //------------------------------------------------------------------------------
  //! Infinite loop doing the scanning of namespace entries
  //!
  //! @param assistant thread running the job
  //------------------------------------------------------------------------------
  void RunNsScan(ThreadAssistant& assistant) noexcept;
#endif

  //----------------------------------------------------------------------------
  //! Method traversing all the files in the subtree and potentially rescanning
  //! some of them
  //!
  //! @param assistant thread running the job
  //----------------------------------------------------------------------------
  void ScanSubtree(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Decide if a rescan is needed based on the timestamp provided and the
  //! configured rescan interval
  //!
  //! @param timestamp_us timestamp in seconds
  //----------------------------------------------------------------------------
  bool DoRescan(const std::string& timestamp_sec) const;

  //----------------------------------------------------------------------------
  //! Check the given file for errors and properly account them both at the
  //! scanner level and also by setting the proper xattrs on the file.
  //!
  //! @param fpath file path
  //!
  //! @return true if file check, otherwise false
  //----------------------------------------------------------------------------
  bool CheckFile(const std::string& fpath);

  //----------------------------------------------------------------------------
  //! Get block checksum object for the given file. First we need to check if
  //! there is a block checksum file (.xsmap) correspnding to the given raw
  //! file.
  //!
  //! @param file_path full path to raw file
  //!
  //! @return block checksum object
  //----------------------------------------------------------------------------
  std::unique_ptr<eos::fst::CheckSum>
  GetBlockXS(const std::string& file_path);

  //----------------------------------------------------------------------------
  //! Scan the given file for checksum errors taking the load into consideration
  //!
  //! @param io io object attached to the file
  //! @param scan_size final scan size
  //! @param scan_xs_hex scanned file checksum in hex
  //! @param filexs_err set to true if file has a checksum error
  //! @param blockxs_err set to true if file has a block checksum errror
  //!
  //! @return true if file is correct, otherwise false if file does not exist,
  //!        or there is any type of checksum error
  //----------------------------------------------------------------------------
  bool ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>& io,
                         unsigned long long& scan_size,
                         std::string& scan_xs_hex,
                         bool& filexs_err, bool& blockxs_err);

  //----------------------------------------------------------------------------
  //! Check if a given stripe combination can reconstruct the original file
  //!
  //! @param fid file id
  //!
  //! @return set of fsid with invalid stripes
  //----------------------------------------------------------------------------
  std::set<eos::common::FileSystem::fsid_t>
  CheckRainStripes(eos::common::FileId::fileid_t fid);

  //----------------------------------------------------------------------------
  //! Check for stripes that are unable to reconstruct the original file
  //!
  //! @param stripes list of stripe urls
  //! @param XS expected checksum
  //! @param xsObj checksum object used to calculate the checksum
  //! @param layout layout id
  //! @param opaqueInfo opaque information
  //!
  //! @return true if file has expected checksum, false otherwise
  //----------------------------------------------------------------------------
  bool isValidStripeCombination(const std::vector<std::string>& stripes,
                                const std::string& XS, CheckSum* xsObj,
                                eos::common::LayoutId::layoutid_t layout,
                                const std::string& opaqueInfo);

  //----------------------------------------------------------------------------
  //! Get clock reference for testing purposes
  //----------------------------------------------------------------------------
  inline eos::common::SteadyClock& GetClock()
  {
    return mClock;
  }

  //----------------------------------------------------------------------------
  //! Get timestamp in seconds smeared +/-20% of mRescanIntervalSec around the
  //!  current timestamp value
  //!
  //! @return string representing timestamp in seconds since epoch
  //----------------------------------------------------------------------------
  std::string GetTimestampSmearedSec() const;

private:
#ifdef IN_TEST_HARNESS
public:
#endif

  //----------------------------------------------------------------------------
  //! Enforce the scan rate by throttling the current thread and also adjust it
  //! depending on the IO load on the mountpoint
  //!
  //! @param offset current offset in file
  //! @param open_ts time point when file was opened
  //! @param scan_rate current scan rate, if 0 then then rate limiting is
  //!        disabled
  //----------------------------------------------------------------------------
  void EnforceAndAdjustScanRate(const off_t offset,
                                std::chrono::time_point
                                <std::chrono::system_clock> open_ts,
                                int& scan_rate);

#ifndef _NOOFS
  //----------------------------------------------------------------------------
  //! Collect all file ids present on the current file system from the NS view
  //!
  //! @param type can be either eos::fsview::sFilesSuffix or
  //!        eos::fsview::sUnlinkedSuffix
  //!
  //! @return queue holding the file ids
  //----------------------------------------------------------------------------
  std::deque<eos::IFileMD::id_t> CollectNsFids(const std::string& type) const;

  //----------------------------------------------------------------------------
  //! Account for missing replicas
  //----------------------------------------------------------------------------
  void AccountMissing();

  //----------------------------------------------------------------------------
  //! Cleanup unlinked replicas which are older than 1 hour
  //----------------------------------------------------------------------------
  void CleanupUnlinked();

#endif

  //! Default ns scan rate is bound by the number of IO ops a disk can handle
  //! and we set it to half the average max IOOPS for HDD which is 100.
  static constexpr unsigned long long sDefaultNsScanRate {50};

  //----------------------------------------------------------------------------
  //! Check if file is unlinked from the namespace and in the process of being
  //! deleted from the disk. Files that are unlinked for more than 30 min
  //! definetely have a problem and we don't account them as in the process of
  //! being deleted.
  //!
  //! @param fid file identifier
  //!
  //! @return true if file is being deleted, otherwise false
  //----------------------------------------------------------------------------
  bool IsBeingDeleted(const eos::IFileMD::id_t fid) const;

  //----------------------------------------------------------------------------
  //! Drop ghost fid from the given file system id
  //!
  //! @param fsid file system id
  //! @param fid file identifier
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DropGhostFid(const eos::common::FileSystem::fsid_t fsid,
                    const eos::IFileMD::id_t fid) const;

  //----------------------------------------------------------------------------
  //! Print log message - depending on whether or not we run in standalone mode
  //! or inside the FST daemon
  //!
  //! @param log_level log level used for the printout
  //----------------------------------------------------------------------------
  template <typename ... Args>
  void LogMsg(int log_level, Args&& ... args)
  {
    if (mBgThread) {
      eos_static_log(log_level, std::forward<Args>(args) ...);
    } else {
      if ((log_level == LOG_INFO) || (log_level == LOG_DEBUG)) {
        fprintf(stdout, std::forward<Args>(args) ...);
      } else {
        fprintf(stderr, std::forward<Args>(args) ...);
        fprintf(stderr, "%s", "\n");
      }
    }
  }

  eos::fst::Load* mFstLoad; ///< Object for providing load information
  eos::common::FileSystem::fsid_t mFsId; ///< Corresponding file system id
  std::string mDirPath; ///< Root directory used by the scanner
  std::atomic<int> mRateBandwidth; ///< Max scan IO rate in MB/s
  //! Time interval after which a file is rescanned in seconds, if 0 then
  //! rescanning is completely disabled
  std::atomic<uint64_t> mEntryIntervalSec;
  //! Time interval after which the disk scanner will run again, default 4h
  std::atomic<uint64_t> mDiskIntervalSec;
  //! Time interval after which the scanner will run again, default 3 days
  std::atomic<uint64_t> mNsIntervalSec;

  // Configuration variable to track changes in disk scan intervals
  uint64_t mConfDiskIntervalSec;

  // Statistics
  long int mNumScannedFiles;
  long int mNumCorruptedFiles;
  long int mNumHWCorruptedFiles;
  long long int mTotalScanSize;
  long int mNumTotalFiles;
  long int mNumSkippedFiles;
  char* mBuffer; ///< Buffer used for reading
  uint32_t mBufferSize; ///< Size of the reading buffer
  bool mBgThread; ///< If true running as background thread inside the FST
  AssistedThread mDiskThread; ///< Thread doing the scanning of the disk
  AssistedThread mNsThread; ///< Thread doing the scanning of NS entries
  eos::common::SteadyClock mClock; ///< Clock wrapper used for testing
  //! Rate limiter for ns scanning which actually limits the number of stat
  //! requests send across the disks in one FSTs.
  std::unique_ptr<eos::common::IRateLimit> mRateLimit;
};

EOSFSTNAMESPACE_END
