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

EOSFSTNAMESPACE_BEGIN

class Load;
class FileIo;
class CheckSum;

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
          bool setchecksum = false, bool fake_clock = false);

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
  //! Infinite loop doing the scanning and verification
  //!
  //! @param assistant thread running the job
  //------------------------------------------------------------------------------
  void Run(ThreadAssistant& assistant) noexcept;

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
  //----------------------------------------------------------------------------
  void CheckFile(const std::string& fpath);

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
  //! @param xs_type string representing the checksum type
  //! @param xs_val reference file checksum value to compare against
  //! @param lfn logical file name (NS path)
  //! @param filexs_err set to true if file has a checksum error
  //! @param blockxs_err set to true if file has a block checksum errror
  //!
  //! @return true if file is correct, otherwise false if file does not exist,
  //!        or there is any type of checksum error
  //----------------------------------------------------------------------------
  bool ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>& io,
                         unsigned long long& scan_size,
                         const std::string& xs_type, const char* xs_val,
                         const std::string& lfn, bool& filexs_err,
                         bool& blockxs_err);

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
  //! @param open_ts_sec open timestamp in seconds from epoch
  //! @param scan_rate current scan rate, if 0 then then rate limiting is
  //!        disabled
  //----------------------------------------------------------------------------
  void EnforceAndAdjustScanRate(const off_t offset, const uint64_t open_ts_sec,
                                int& scan_rate);

  //----------------------------------------------------------------------------
  //! Update the local database based on the checksum information
  //!
  //! @param file_path
  //! @param fid file identifier extracted from the path
  //! @param filexs_error true if file has a checksum error
  //! @param blocxs_error true if file has block checksum error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool UpdateLocalDB(const std::string& file_path,
                     eos::common::FileId::fileid_t fid,
                     bool filexs_error, bool blockxs_error);

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
      eos_log(log_level, std::forward<Args>(args) ...);
    } else {
      if ((log_level == LOG_INFO) || (log_level == LOG_DEBUG)) {
        fprintf(stdout, std::forward<Args>(args) ...);
      } else {
        fprintf(stderr, std::forward<Args>(args) ...);
        fprintf(stderr, "%s", "\n");
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Update the forced scan flag based on the existence of the .eosscan file
  //! on the FST mountpoint
  //----------------------------------------------------------------------------
  void UpdateForcedScan();

  eos::fst::Load* mFstLoad; ///< Object for providing load information
  eos::common::FileSystem::fsid_t mFsId; ///< Corresponding file system id
  std::string mDirPath; ///< Root directory used by the scanner
  //! Time interval after which a file is rescanned in seconds, if 0 then
  //! rescanning is completely disabled
  std::atomic<uint64_t> mRescanIntervalSec;
  //! Time interval after which the scanner will run again, default 4h
  std::atomic<uint64_t> mRerunIntervalSec;
  std::atomic<int> mRateBandwidth; ///< Max scan rate in MB/s

  // Statistics
  long int mNumScannedFiles;
  long int mNumCorruptedFiles;
  long int mNumHWCorruptedFiles;
  long long int mTotalScanSize;
  long int mNumTotalFiles;
  long int mNumSkippedFiles;
  bool mSetChecksum; ///< If true update the xattr checksum value
  char* mBuffer; ///< Buffer used for reading
  uint32_t mBufferSize; ///< Size of the reading buffer
  bool mBgThread; ///< If true running as background thread inside the FST
  bool mForcedScan; ///< Mark if scanner is in force mode
  AssistedThread mThread; ///< Thread doing the scanning
  bool mFakeClock; ///< Mark if we're using a fake clock (testing)
  eos::common::SteadyClock mClock; ///< Clock wrapper also used for testing
};

EOSFSTNAMESPACE_END
