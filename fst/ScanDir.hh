//------------------------------------------------------------------------------
// File: ScanDir.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include <pthread.h>
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include <sys/syscall.h>
#ifndef __APPLE__
#include <asm/unistd.h>
#endif

EOSFSTNAMESPACE_BEGIN

class Load;
class FileIo;
class CheckSum;

//------------------------------------------------------------------------------
//! Class ScanDir
//! @brief Scan a directory tree and checks checksums (and blockchecksums if
//! present) on a regular interval with limited bandwidth
//------------------------------------------------------------------------------
class ScanDir : eos::common::LogId
{
public:
  static void* StaticThreadProc(void*);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
          eos::fst::Load* fstload, bool bgthread = true, long int testinterval = 10,
          int ratebandwidth = 50, bool setchecksum = false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ScanDir();

  //----------------------------------------------------------------------------
  //! Decide if a rescan is needed based on the timestamp provided and the
  //! configured rescan interval
  //!
  //! @param timestamp_us timestamp in microseconds
  //----------------------------------------------------------------------------
  bool DoRescan(const std::string& timestamp_us) const;

  //----------------------------------------------------------------------------
  //! Update scanner configuration
  //!
  //! @param key configuration type
  //! @param value configuration value
  //----------------------------------------------------------------------------
  void SetConfig(const std::string&, long long value);

  //----------------------------------------------------------------------------
  //! Method traversing all the files in the subtree and potentially rescanning
  //! some of them.
  //----------------------------------------------------------------------------
  void ScanFiles();

  //----------------------------------------------------------------------------
  //! Check the given file for errors and properly account them both at the
  //! scanner level and also by setting the proper xattrs on the file.
  //!
  //! @param fpath file path
  //----------------------------------------------------------------------------
  void CheckFile(const char* fpath);

  //------------------------------------------------------------------------------
  //!
  //------------------------------------------------------------------------------
  void* ThreadProc();

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

  bool ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>&,
                         unsigned long long&, float&, const char*,
                         unsigned long, const char* lfn,
                         bool& filecxerror, bool& blockxserror);

  //----------------------------------------------------------------------------
  //! Timestamp in microseconds
  //----------------------------------------------------------------------------
  std::string GetTimestamp() const;

  std::string GetTimestampSmeared();

private:
  eos::fst::Load* mFstLoad; ///< Object for providing load information
  eos::common::FileSystem::fsid_t mFsId; ///< Corresponding file system id
  std::string mDirPath; ///< Root directory used by the scanner
  ///< Time interval after which a file is rescanned in seconds
  std::atomic<uint64_t> mRescanIntervalSec;
  std::atomic<int> mRateBandwidth; ///< Max scan rate in MB/s

  // Statistics
  float mScanDuration;
  long int mNumScannedFiles;
  long int mNumCorruptedFiles;
  long int mNumHWCorruptedFiles;
  long long int mTotalScanSize;
  long int mNumTotalFiles;
  long int mNumSkippedFiles;
  bool mSetChecksum; ///< If true update the xattr checksum value
  char* mBuffer; ///< Buffer used for reading
  uint32_t mBufferSize; ///< Size of the reading buffer
  pthread_t thread;
  bool bgThread;
  bool mForcedScan;
};

EOSFSTNAMESPACE_END
