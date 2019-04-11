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

#ifndef __EOSFST_FSCK_HH__
#define __EOSFST_FSCK_HH__

#include <pthread.h>
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "XrdOuc/XrdOucString.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

#include <sys/syscall.h>
#ifndef __APPLE__
#include <asm/unistd.h>
#endif

EOSFSTNAMESPACE_BEGIN

class Load;
class FileIo;
class CheckSum;

//------------------------------------------------------------------------------
//! Class Fsck
//! @brief Fsck a directory tree and checks checksums (and blockchecksums if
//! present)
//------------------------------------------------------------------------------

class Fsck : eos::common::LogId
{
public:
  static void* StaticThreadProc(void*);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Fsck(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
       eos::fst::Load* fstload, long int testinterval = 10,
       long int rate = 100,
       const std::string mamager = "", 
       bool silent=false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Fsck();

  //----------------------------------------------------------------------------
  //! Update scanner configuration
  //!
  //! @param key configuration type
  //! @param value configuration value
  //----------------------------------------------------------------------------
  void SetConfig(const std::string&, long long value);

  void* ThreadProc();

  void ScanFiles();
  void ReportFiles();

  void CheckFile(const char*);
  void CheckFile(struct Fmd& fmd, size_t nfiles);

  std::string GetTimestamp();
  std::string GetTimestampSmeared();

  bool RescanFile(std::string);

  void ScanMd();
  
  void ScanMdQdb();

  void SetQdbContactDetails(const QdbContactDetails& _contactDetails) {
    contactDetails = _contactDetails;
    useQuarkDB = true;
  }
  
private:
  eos::fst::Load* fstLoad;
  eos::common::FileSystem::fsid_t fsId;
  XrdOucString dirPath;
  std::atomic<long long> mTestInterval; ///< Test interval in seconds
  std::atomic<long long> mScanRate; ///< meta-data files/s rate limiting
  
  std::map<uint64_t, struct Fmd> mMd;
  // Statistics
  long int noCorruptFiles;
  float durationScan;
  long long int totalScanSize;
  long long int bufferSize;
  long int noTotalFiles;
  std::string managerHostPort;
  bool useQuarkDB;
  QdbContactDetails contactDetails;
  
  bool setChecksum;
  bool silent;

  long alignment;
  char* buffer;
  pthread_t thread;

  std::map<std::string, uint64_t> errors;
};

EOSFSTNAMESPACE_END

#endif
